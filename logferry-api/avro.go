package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/tracing"

	"github.com/abh/geodns/v3/querylog"
)

type AvroHandler struct {
	clientTimeout  time.Duration // for the config API
	schema         avro.Schema
	writer         *kafka.Writer
	requestLimiter chan bool
}

func NewAvroHandler(writer *kafka.Writer, timeoutStick time.Duration) (*AvroHandler, error) {
	schema, err := querylog.AvroSchema()
	if err != nil {
		return nil, fmt.Errorf("avro schema setup: %w", err)
	}

	return &AvroHandler{
		clientTimeout:  timeoutStick + 5*time.Second,
		schema:         schema,
		writer:         writer,
		requestLimiter: make(chan bool, 20),
	}, nil
}

func (ah *AvroHandler) Config(c echo.Context) error {
	log := logger.Setup()
	ctx := c.Request().Context()
	hostname := ctx.Value(CertificateKey).(string)
	ctx, span := tracing.Tracer().Start(
		ctx, "config",
		trace.WithAttributes(
			attribute.String("hostname", hostname),
		),
	)
	defer span.End()
	log = log.With("host", hostname, "traceid", span.SpanContext().TraceID())

	return c.JSON(http.StatusOK, struct {
		Timeout time.Duration
	}{
		Timeout: ah.clientTimeout,
	})

}

func (ah *AvroHandler) Post(c echo.Context) error {
	log := logger.Setup()
	ctx := c.Request().Context()
	hostname := ctx.Value(CertificateKey).(string)
	ctx, span := tracing.Tracer().Start(
		ctx, "post_avro",
		trace.WithAttributes(
			attribute.String("hostname", hostname),
		),
	)
	defer span.End()
	defer c.Request().Body.Close()

	if len(hostname) == 0 {
		return c.NoContent(http.StatusUnauthorized)
	}

	id := c.Request().Header.Get("Request-ID")
	span.SetAttributes(attribute.String("Request-ID", id))
	log = log.With("host", hostname, "id", id, "traceid", span.SpanContext().TraceID())

	if len(ah.requestLimiter) > 10 {
		log.Debug("rate limiting queue",
			"length", len(ah.requestLimiter),
			"capacity", cap(ah.requestLimiter),
		)
	}

	select {
	case ah.requestLimiter <- true:
		defer func() {
			<-ah.requestLimiter
		}()
	default:
		span.SetAttributes(attribute.Bool("Rate-Limit", true))
		c.Request().Close = true
		c.Response().Header().Add("Retry-After", "5")
		return c.String(http.StatusTooManyRequests, "try later")
	}

	fh, sha, size, err := ah.readPostedFile(ctx, c.Request())
	if fh != nil {
		defer func() {
			fileName := fh.Name()
			fh.Close()
			os.Remove(fileName)
		}()
	}

	expectedDataSize := c.Request().Header.Get("file-size")
	expectedSha := c.Request().Header.Get("content-sha256")
	log.Debug("expected data", "size", expectedDataSize, "sha256", expectedSha)

	if len(expectedDataSize) > 0 {
		span.SetAttributes(attribute.Int64("Size", size))
		if expectedSize, err := strconv.Atoi(expectedDataSize); err == nil {
			span.SetAttributes(attribute.Int64("Expected-Size", int64(expectedSize)))
			if size != int64(expectedSize) {
				log.Warn("unexpected content size", "expected", expectedSize, "received", size)
				span.SetStatus(codes.Error, "unexpected content size")
				return c.NoContent(http.StatusBadRequest)
			}
		}
	}

	if len(expectedSha) > 0 {
		span.SetAttributes(attribute.String("Expected-Sha256", expectedSha))
		span.SetAttributes(attribute.String("Sha256", sha))
		if sha != expectedSha {
			log.Warn("unexpected sha256", "expected", expectedSha, "received", sha)
			span.SetStatus(codes.Error, "unexpected sha256")
			return c.NoContent(http.StatusBadRequest)
		}
	}

	buffer, err := ah.parseAvro(ctx, fh, hostname)
	if err != nil {
		c.NoContent(http.StatusInternalServerError)
	}

	_, kafkaSpan := tracing.Tracer().Start(ctx,
		"post_avro.kafka_write",
		trace.WithAttributes(attribute.Int("count", len(buffer))),
	)
	defer kafkaSpan.End()
	// log.Info("writing to kafka", "count", len(buffer))

	err = ah.writer.WriteMessages(ctx, buffer...)
	if err != nil {
		kafkaSpan.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", err.Error())))
		log.Error("kafka write", "err", err)
		return c.NoContent(http.StatusBadGateway)
	}

	kafkaSpan.AddEvent("kafak accepted messages", trace.WithAttributes(attribute.Int("count", len(buffer))))
	log.Info("accepted messages", "count", len(buffer))

	return c.NoContent(http.StatusAccepted)

}

func (ah *AvroHandler) parseAvro(ctx context.Context, fh io.Reader, hostname string) ([]kafka.Message, error) {
	log := logger.Setup()
	ctx, span := tracing.Tracer().Start(ctx, "parse_avro")
	defer span.End()

	dec, err := ocf.NewDecoder(fh)
	if err != nil {
		return nil, fmt.Errorf("could not get decoder")
	}

	count := 0
	buffer := make([]kafka.Message, 0, 0)

	for dec.HasNext() {
		e := querylog.Entry{}

		err := dec.Decode(&e)
		if err != nil {
			span.AddEvent(
				fmt.Sprintf("avro decode error: %s", err),
				trace.WithAttributes(attribute.String("err", err.Error())),
				trace.WithAttributes(attribute.Int("messages_read", count)),
			)

			log.Info("decode error", "err", err, "messages_read", count)
			return nil, fmt.Errorf("decode error")
		}

		e.Hostname = hostname

		// spew.Dump(e)

		d, err := avro.Marshal(ah.schema, e)
		if err != nil {
			span.AddEvent(
				"exception",
				trace.WithAttributes(
					attribute.String("msg", "avro marshal error"),
					attribute.String("err", err.Error())),
			)
			log.Warn("avro marshal error", "err", err)
			return nil, fmt.Errorf("avro marshal error: %w", err)
		}

		count++
		buffer = append(buffer, kafka.Message{
			Value: d,
		})

		// if len(buffer) > 100 {
		// 	writer.WriteMessages(ctx, buffer...)
		// 	buffer = []kafka.Message{}
		// }

		// log.Info("query", "name", e.Name)
	}
	if dec.Error() != nil {
		// exception or status? Huh?
		span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", dec.Error().Error())))
		span.SetStatus(codes.Error, "avro error: "+dec.Error().Error())
		log.Error("avro decoder error", "err", dec.Error())
	}

	return buffer, nil
}

func (ah *AvroHandler) readPostedFile(ctx context.Context, req *http.Request) (*os.File, string, int64, error) {
	defer req.Body.Close()
	ctx, span := tracing.Tracer().Start(ctx, "read_data")
	defer span.End()

	fh, err := os.CreateTemp("", "")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, "", 0, err
	}

	// geodns is configred to 5MB files, so 20 should be plenty
	body := io.LimitReader(req.Body, 20*1024*1024)

	sha := sha256.New()
	rd := io.TeeReader(body, sha)
	buf := make([]byte, 1024*256)
	cnt, err := io.CopyBuffer(fh, rd, buf)
	if err != nil {
		return nil, "", cnt, err
	}

	shabase64 := base64.StdEncoding.EncodeToString(sha.Sum(nil))
	span.SetAttributes(attribute.Int64("bytes_read", cnt))
	span.SetAttributes(attribute.String("sha256", shabase64))

	offset, err := fh.Seek(0, io.SeekStart)
	if offset != 0 {
		err = fmt.Errorf("could not seek to the beginning of the buffer file: %w", err)
		span.SetStatus(codes.Error, err.Error())
		return nil, shabase64, cnt, err
	}

	return fh, shabase64, cnt, nil
}
