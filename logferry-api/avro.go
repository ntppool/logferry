package main

import (
	"net/http"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"

	kafconn "go.ntppool.org/common/kafka"
	"go.ntppool.org/common/logger"

	"github.com/abh/geodns/v3/querylog"
)

func postAvro(kaf *kafconn.Kafka) func(c echo.Context) error {
	log := logger.Setup()

	schema, err := querylog.AvroSchema()
	if err != nil {
		log.Error("avro schema setup", "err", err)
	}

	requestLimiter := make(chan bool, 20)

	return func(c echo.Context) error {

		ctx := c.Request().Context()
		log := logger.Setup()
		hostname := ctx.Value(CertificateKey).(string)

		if len(hostname) == 0 {
			return c.NoContent(http.StatusUnauthorized)
		}

		id := c.Request().Header.Get("Request-ID")
		log = log.With("host", hostname, "id", id)

		if len(requestLimiter) > 10 {
			log.Debug("rate limiting queue",
				"length", len(requestLimiter),
				"capacity", cap(requestLimiter),
			)
		}

		select {
		case requestLimiter <- true:
			defer func() {
				<-requestLimiter
			}()
		default:
			c.Response().Header().Add("Retry-After", "5")
			return c.String(http.StatusTooManyRequests, "try later")
		}

		writer, err := kaf.NewWriter("geodns-avro")
		if err != nil {
			log.Error("could not get kafka writer", "err", err)
			return c.NoContent(http.StatusInternalServerError)
		}
		writer.BatchTimeout = 500 * time.Millisecond
		writer.BatchSize = 5000

		dec, err := ocf.NewDecoder(c.Request().Body)
		if err != nil {
			return c.String(http.StatusInternalServerError, "could not get decoder")
		}
		defer c.Request().Body.Close()

		buffer := []kafka.Message{}

		count := 0

		for dec.HasNext() {
			e := querylog.Entry{}

			err := dec.Decode(&e)
			if err != nil {
				log.Info("decode error", "err", err)
				continue
			}

			e.Hostname = hostname

			// spew.Dump(e)

			d, err := avro.Marshal(schema, e)
			if err != nil {
				log.Warn("avro marshal error", "err", err)
				continue
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

		log.Info("writing to kafka", "count", len(buffer))

		err = writer.WriteMessages(ctx, buffer...)
		if err != nil {
			log.Error("kafka write", "err", err)
			return c.NoContent(http.StatusInternalServerError)
		}

		if dec.Error() != nil {
			log.Error("avro decoder error", "err", dec.Error())
		}

		stats := writer.Stats()
		log.Info("kafka write stats",
			"messages", stats.Messages,
			"errors", stats.Errors,
			"batches", stats.BatchBytes.Count,
			"total bytes", stats.BatchBytes.Sum,
			"total batch time", stats.BatchTime.Sum,
			"total wait time", stats.WaitTime.Sum,
			"avg queue time", stats.BatchQueueTime.Avg,
			"total queue time", stats.BatchQueueTime.Sum,
		)

		err = writer.Close()
		if err != nil {
			log.Error("kafka close error", "err", err)
			return c.NoContent(http.StatusInternalServerError)
		}

		// log.Info("accepted messages", "count", count)

		return c.NoContent(http.StatusAccepted)
	}
}
