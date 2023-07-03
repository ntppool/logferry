package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"go.ntppool.org/common/apitls"
	"go.ntppool.org/common/health"
	kafconn "go.ntppool.org/common/kafka"
	"go.ntppool.org/common/logger"
)

type APIServer struct{}

func main() {

	log := logger.Setup()

	log.Info("Starting API server")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
	}()

	conf, err := LoadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	apisrv := &APIServer{}

	go health.HealthCheckListener(ctx, 4280, log.WithGroup("health"))

	err = apisrv.run(ctx, conf)
	if err != nil {
		logger.Setup().Error("server error", "err", err)
	}

	cancel()
}

func (apisrv *APIServer) run(ctx context.Context, conf Config) error {
	log := logger.Setup()

	log.Info("Starting labstack")

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

	kaf, err := kafconn.NewKafka(ctx, kafconn.TLSSetup{
		Key:  conf.Kafka.Key,
		Cert: conf.Kafka.Cert,
		CA:   conf.Kafka.CA,
	})
	if err != nil {
		return err
	}

	apiEcho := e.Group("/api/v1", apisrv.certificateMiddleware)
	apiEcho.POST("/geodns/avro", postAvro(kaf))

	hsrv := http.Server{
		Addr:    ":4200",
		Handler: e,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientCAs:  caCertPool,
			// ClientAuth:            tls.RequireAndVerifyClientCert,
			ClientAuth:            tls.VerifyClientCertIfGiven,
			GetCertificate:        cm.GetCertificate,
			VerifyPeerCertificate: apisrv.verifyClient,
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := hsrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		return err
	}

	return nil
}
