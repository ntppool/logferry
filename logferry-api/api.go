package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/sync/errgroup"

	"go.ntppool.org/common/apitls"
	"go.ntppool.org/common/health"
	kafconn "go.ntppool.org/common/kafka"
	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/version"
)

type APIServer struct {
	kaf  *kafconn.Kafka
	conf Config
}

func main() {

	log := logger.Setup()

	log.Info("Logferry API server", "version", version.Version())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
	}()

	conf, err := LoadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	apisrv, err := NewAPIServer(ctx, conf)
	if err != nil {
		log.Error("could not setup API server", "err", err)
		os.Exit(2)
	}

	healthSrv := health.NewServer(func(w http.ResponseWriter, r *http.Request) {
		err := apisrv.kaf.CheckPartitions()
		if err != nil {
			w.WriteHeader(502)
			w.Write([]byte("kafka error"))
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok!"))
	})

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		healthSrv.SetLogger(log.WithGroup("health"))
		return healthSrv.Listen(ctx, 4280)
	})

	g.Go(func() error {
		err = apisrv.run(ctx)
		if err != nil {
			log.Error("server error", "err", err)
			return err
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		log.Error("run error", "err", err)
	}

	cancel()
}

func NewAPIServer(ctx context.Context, conf Config) (*APIServer, error) {
	kaf, err := kafconn.NewKafka(ctx, kafconn.TLSSetup{
		Key:  conf.Kafka.Key,
		Cert: conf.Kafka.Cert,
		CA:   conf.Kafka.CA,
	})
	if err != nil {
		return nil, err
	}

	return &APIServer{
		kaf:  kaf,
		conf: conf,
	}, nil

}

func (apisrv *APIServer) run(ctx context.Context) error {
	log := logger.Setup()
	conf := apisrv.conf

	listen := ":4200"

	log.Info("Starting api server", "port", listen)

	caCertPool, err := apitls.CAPool()
	if err != nil {
		return err
	}

	cm, err := apitls.GetCertman(conf.TLS.Cert, conf.TLS.Key)
	if err != nil {
		return err
	}

	e := echo.New()

	// e.Use(slogecho.New(log.WithGroup("api")))
	e.Use(middleware.Recover())

	// Routes
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "logferry")
	})

	writer, err := apisrv.kaf.NewWriter("geodns-avro")
	if err != nil {
		return fmt.Errorf("could not get kafka writer: %w", err)
	}
	writer.BatchTimeout = 500 * time.Millisecond
	writer.BatchSize = 10000

	apiEcho := e.Group("/api/v1", apisrv.certificateMiddleware)
	apiEcho.POST("/geodns/avro", postAvro(writer))

	hsrv := http.Server{
		Addr:    listen,
		Handler: e,
		TLSConfig: &tls.Config{
			MinVersion:            tls.VersionTLS12,
			MaxVersion:            tls.VersionTLS12,
			ClientCAs:             caCertPool,
			ClientAuth:            tls.VerifyClientCertIfGiven,
			GetCertificate:        cm.GetCertificate,
			VerifyPeerCertificate: apisrv.verifyClient,
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		if err := hsrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			e.Logger.Infof("ListenAndServeTLS returned: %s", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		return err
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
	}

	return nil
}
