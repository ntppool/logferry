package main

import (
	"net/http"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/tracing"

	"github.com/abh/geodns/v3/querylog"
)

func postAvro(writer *kafka.Writer) func(c echo.Context) error {
	log := logger.Setup()

	schema, err := querylog.AvroSchema()
	if err != nil {
		log.Error("avro schema setup", "err", err)
	}

	requestLimiter := make(chan bool, 20)

	return func(c echo.Context) error {
		log := logger.Setup()
		ctx := c.Request().Context()
		hostname := ctx.Value(CertificateKey).(string)
		ctx, span := tracing.Tracer().Start(
			ctx, "PostAvro",
			trace.WithAttributes(
				attribute.String("hostname", hostname),
			),
		)
		defer span.End()

		if len(hostname) == 0 {
			return c.NoContent(http.StatusUnauthorized)
		}

		id := c.Request().Header.Get("Request-ID")
		span.SetAttributes(attribute.String("Request-ID", id))
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
			span.SetAttributes(attribute.Bool("Rate-Limit", true))
			c.Request().Close = true
			c.Response().Header().Add("Retry-After", "5")
			return c.String(http.StatusTooManyRequests, "try later")
		}

		_, avroSpan := tracing.Tracer().Start(ctx, "PostAvro.ReadRequest")

		dec, err := ocf.NewDecoder(c.Request().Body)
		if err != nil {
			avroSpan.End()
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
		if dec.Error() != nil {
			avroSpan.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", dec.Error().Error())))
			log.Error("avro decoder error", "err", dec.Error())
		}
		avroSpan.End()

		_, kafkaSpan := tracing.Tracer().Start(ctx,
			"PostAvro.KafkaWrite",
			trace.WithAttributes(attribute.Int("count", len(buffer))),
		)
		defer kafkaSpan.End()
		// log.Info("writing to kafka", "count", len(buffer))

		err = writer.WriteMessages(ctx, buffer...)
		if err != nil {
			kafkaSpan.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", err.Error())))
			log.Error("kafka write", "err", err)
			return c.NoContent(http.StatusInternalServerError)
		}

		kafkaSpan.AddEvent("kafak accepted messages", trace.WithAttributes(attribute.Int("count", count)))
		log.Info("accepted messages", "count", count)

		return c.NoContent(http.StatusAccepted)
	}
}
