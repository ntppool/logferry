package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/ulid"
)

func uploader(ctx context.Context, client *http.Client, logpath, apiURL string, metrics prometheus.Registerer) error {

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

	dir, err := os.ReadDir(logpath)
	if err != nil {
		return err
	}

	g.Go(func() error {
		for _, fi := range dir {
			files := []string{}
			if isAvroFileName(fi.Name()) {
				files = append(files, fi.Name())
			}
			slices.Sort(files)
			for _, n := range files {
				processCh <- path.Join(logpath, n)
			}
		}
		return nil
	})

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
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10},
	})
	metrics.MustRegister(uploadTimeHistogram)

	for workerN := 1; workerN <= 2; workerN++ {
		log.Info("Starting upload worker", "n", workerN)

		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case p := <-processCh:
					log.Debug("change to file", "path", p)

					id, err := ulid.MakeULID(time.Now())
					if err != nil {
						log.Error("could not generate ulid", "err", err)
					}

					err = upload(ctx, id.String(), client, p, apiURL, uploadTimeHistogram)
					if err != nil {
						log.Error("error uploading", "id", id, "path", p, "err", err)
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
	return strings.HasSuffix(name, ".avro")
}

func upload(ctx context.Context, id string, client *http.Client, filePath, apiURL string, histogram prometheus.Histogram) error {

	timer := prometheus.NewTimer(histogram)

	log := logger.Setup().With("id", id)

	_, baseName := path.Split(filePath)
	log = log.With("file", baseName)

	fh, err := os.Open(filePath)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, fh)
	if err != nil {
		return err
	}
	req.Header.Add("Request-ID", id)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		duration := timer.ObserveDuration()
		log.Info("uploaded", "code", resp.StatusCode, "duration", duration)
	} else {
		log.Warn("upload error", "code", resp.StatusCode, "status", resp.Status)
		return fmt.Errorf("upload error %d (%s)", resp.StatusCode, resp.Status)
	}

	err = os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("error removing log: %w", err)
	}

	return nil
}
