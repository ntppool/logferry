package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/tracing"
	"go.ntppool.org/common/ulid"
	"go.ntppool.org/common/version"
)

const workerCount = 2

const maxUploadAttempts = 3

var retryDelays = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	5 * time.Second,
}

type retryableError struct {
	err error
}

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

type uploadSet struct {
	client              *httpClientManager
	fl                  *fileLock
	uploadTimeHistogram prometheus.Histogram
	retryCounter        prometheus.Counter
}

func uploader(ctx context.Context, clientManager *httpClientManager, logpath string, metrics prometheus.Registerer) error {
	log := logger.Setup()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	g, ctx := errgroup.WithContext(ctx)

	// todo: detect if the list is full and rescan directory
	processCh := make(chan string, 10000)

	metrics.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "queue_internal_count",
		Help: "Files in the internal queue to be processed",
	}, func() float64 {
		size := len(processCh)
		return float64(size)
	}))

	metrics.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "queue_disk_count",
		Help: "Files in the disk queue to be processed",
	}, func() float64 {
		dir, err := os.ReadDir(logpath)
		if err != nil {
			return -1
		}
		count := 0

		for _, fi := range dir {
			if isAvroFileName(fi.Name()) {
				count++
			}
		}
		return float64(count)
	}))

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				log.Info("fswatcher shutting down")
				return nil
			case event, ok := <-watcher.Events:
				if !ok {
					return fmt.Errorf("got not ok event")
				}
				// log.Info("event info", "event", event)
				// if event.Has(fsnotify.Write) {
				// 	log.Info("modified file", "name", event.Name)
				// }

				if event.Has(fsnotify.Rename) || event.Has(fsnotify.Create) {
					if isAvroFileName(event.Name) {
						processCh <- event.Name
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return nil
				}
				log.Error("fswatcher error", "err", err)
				return err
			}
		}
	})

	err = watcher.Add(logpath)
	if err != nil {
		return err
	}

	uploadOkCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "upload_success_total",
		Help: "Files uploaded ok",
	})
	metrics.MustRegister(uploadOkCounter)
	uploadErrorCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "upload_failure_total",
		Help: "File upload failures",
	})
	metrics.MustRegister(uploadErrorCounter)

	uploadTimeHistogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "upload_duration_seconds",
		Help:    "Upload time for successful requests",
		Buckets: []float64{.5, 1, 1.5, 2, 2.5, 3, 3.5, 5, 10},
	})
	metrics.MustRegister(uploadTimeHistogram)

	uploadRetryCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "upload_retry_total",
		Help: "Upload retry attempts",
	})
	metrics.MustRegister(uploadRetryCounter)

	metrics.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "client_cert_expiry_timestamp_seconds",
		Help: "Unix timestamp of client certificate NotAfter",
	}, func() float64 {
		expiry, err := clientManager.CertExpiry()
		if err != nil {
			return 0
		}
		return float64(expiry.Unix())
	}))

	metrics.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "client_cert_expired",
		Help: "1 if client certificate is expired, 0 if valid",
	}, func() float64 {
		if clientManager.CertValid() {
			return 0
		}
		return 1
	}))

	up := uploadSet{
		client:              clientManager,
		fl:                  NewFileLock(),
		uploadTimeHistogram: uploadTimeHistogram,
		retryCounter:        uploadRetryCounter,
	}

	g.Go(func() error {
		first := true

		// start with the new files for a few moments
		time.Sleep(15 * time.Second)

		for {
			if !(first || len(processCh) < 2) {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(20 * time.Second):
					continue // check queue length again
				}
			}

			dir, err := os.ReadDir(logpath)
			if err != nil {
				return err
			}

			for _, fi := range dir {
				files := []string{}
				if isAvroFileName(fi.Name()) {
					files = append(files, fi.Name())
				}
				slices.Sort(files)
				for _, n := range files {
					fullName := path.Join(logpath, n)
					if !up.fl.IsLocked(fullName) {
						processCh <- fullName
					}
				}
			}

			first = false

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Minute):
			}
		}
	})

	for workerN := 1; workerN <= workerCount; workerN++ {
		workerN := workerN
		log.Info("starting upload worker", "n", workerN)
		g.Go(func() error {
			log := log.With("n", workerN)

			for {
				select {
				case <-ctx.Done():
					return nil
				case p := <-processCh:
					// Pause uploads while certificate is expired
					certWarned := false
					for !up.client.CertValid() {
						if !certWarned {
							log.Warn("client certificate expired, pausing uploads")
							certWarned = true
						}
						select {
						case <-ctx.Done():
							return nil
						case <-time.After(30 * time.Second):
						}
					}
					if certWarned {
						log.Info("client certificate renewed, resuming uploads")
					}

					log.Debug("change to file", "path", p)

					id, err := ulid.MakeULID(time.Now())
					if err != nil {
						log.Error("could not generate ulid", "err", err)
					}

					log := log.With("id", id.String())

					err = up.upload(ctx, log, id.String(), p)
					if err != nil {
						log.Error("error uploading", "path", p, "err", err)
						uploadErrorCounter.Inc()
						continue
					}
					uploadOkCounter.Inc()
				}
			}
		})
	}

	return g.Wait()
}

func isAvroFileName(name string) bool {
	return strings.HasSuffix(name, ".avro") && len(name) > 5
}

func (up *uploadSet) upload(ctx context.Context, log *slog.Logger, id string, filePath string) error {
	ctx, span := tracing.Tracer().Start(ctx, "upload", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	log = log.With("traceid", span.SpanContext().TraceID())

	_, baseName := path.Split(filePath)

	span.SetAttributes(attribute.String("file", baseName))
	log = log.With("file", baseName)

	if !up.fl.Lock(filePath) {
		err := fmt.Errorf("%s is already being uploaded", baseName)
		span.RecordError(err)
		return err
	}
	defer up.fl.Unlock(filePath)

	timer := prometheus.NewTimer(up.uploadTimeHistogram)

	fh, err := os.Open(filePath)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer fh.Close()

	stat, err := fh.Stat()
	if err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.Int64("size", stat.Size()))

	reqCtx, cancel := context.WithTimeout(ctx, time.Second*120) // todo: get from config
	defer cancel()

	sha := sha256.New()
	_, err = io.Copy(sha, fh)
	if err != nil {
		return err
	}
	shaSum := sha.Sum(nil)

	var lastErr error
	for attempt := 0; attempt < maxUploadAttempts; attempt++ {
		if attempt > 0 {
			up.retryCounter.Inc()

			delay := retryDelays[attempt-1]
			log.Warn("retrying upload", "attempt", attempt+1, "delay", delay, "err", lastErr)

			select {
			case <-reqCtx.Done():
				return reqCtx.Err()
			case <-time.After(delay):
			}
		}

		// Seek file back to start for each attempt
		if _, err := fh.Seek(0, io.SeekStart); err != nil {
			return err
		}

		lastErr = up.uploadAttempt(reqCtx, log, id, fh, stat, shaSum)
		if lastErr == nil {
			duration := timer.ObserveDuration()
			log.Info("uploaded", "duration", duration)

			if err := os.Remove(filePath); err != nil {
				err = fmt.Errorf("error removing log: %w", err)
				span.RecordError(err)
				return err
			}
			return nil
		}

		span.RecordError(lastErr)

		var re *retryableError
		if !errors.As(lastErr, &re) {
			return lastErr
		}

		up.client.Client().CloseIdleConnections()
	}

	return lastErr
}

func (up *uploadSet) uploadAttempt(ctx context.Context, log *slog.Logger, id string, fh *os.File, stat os.FileInfo, shaSum []byte) error {
	ctx, span := tracing.Tracer().Start(ctx, "upload_attempt")
	defer span.End()

	// Check if all targets are in backoff and wait if needed
	if _, wait := up.client.GetTargetURL(); wait > 0 {
		log.Info("all targets in backoff, waiting", "wait", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	// Get target URL from SRV pool (handles priority, weight, and health)
	targetURL, _ := up.client.GetTargetURL()

	targetAddr := GetTargetAddress(targetURL)
	uploadURL, err := url.JoinPath(targetURL, "api/v1/geodns/avro")
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.SetAttributes(attribute.String("target", targetAddr))
	log = log.With("server", targetAddr)

	// Record that we're making a request to this target
	up.client.TargetPool().RecordRequest(targetAddr)

	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, fh)
	if err != nil {
		span.RecordError(err)
		return err
	}

	req.Header.Add("Request-ID", id)
	req.Header.Add("User-Agent", "logferry/"+version.Version())
	req.Header.Add("file-size", strconv.FormatInt(stat.Size(), 10))
	req.Header.Add("content-sha256", base64.StdEncoding.EncodeToString(shaSum))

	cl := up.client.Client()

	resp, err := cl.Do(req)
	if err != nil {
		up.client.TargetPool().MarkUnhealthy(targetAddr, err)
		cl.CloseIdleConnections()
		span.RecordError(err)
		return &retryableError{err}
	}
	if resp.Body != nil {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			span.RecordError(fmt.Errorf("error reading response body: %w", err))
			log.Warn("error reading response body", "err", err)
		}
		if len(b) > 0 {
			log.Warn("unexpected bytes in response body (expected empty)", "count", len(b), "data", string(b))
		}
		resp.Body.Close()
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		up.client.TargetPool().MarkHealthy(targetAddr)
		return nil
	} else if resp.StatusCode == http.StatusTooManyRequests {
		err := fmt.Errorf("upload rate limited %d (%s)", resp.StatusCode, resp.Status)
		up.client.TargetPool().MarkUnhealthy(targetAddr, err)
		log.Warn("upload rate limited", "code", resp.StatusCode, "status", resp.Status)
		span.RecordError(err)
		return &retryableError{err}
	} else if resp.StatusCode >= 500 {
		err := fmt.Errorf("upload error %d (%s)", resp.StatusCode, resp.Status)
		up.client.TargetPool().MarkUnhealthy(targetAddr, err)
		log.Warn("upload error", "code", resp.StatusCode, "status", resp.Status)
		span.RecordError(err)
		return &retryableError{err}
	} else {
		// Other 4xx errors are client errors, don't retry
		log.Warn("upload error", "code", resp.StatusCode, "status", resp.Status)
		err := fmt.Errorf("upload error %d (%s)", resp.StatusCode, resp.Status)
		span.RecordError(err)
		return err
	}
}
