package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.ntppool.org/common/apitls"
	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/metricsserver"
)

func main() {
	log := logger.Setup()

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
	}()

	err := run(ctx)
	if err != nil {
		lerr := &logger.Error{}
		if errors.As(err, &lerr) {
			log.Error(lerr.Msg, lerr.Data...)
		} else {
			log.Error("runtime error", "err", err)
		}
	}

	cancel()
}

func run(ctx context.Context) error {
	// log := logger.Setup()

	keyFile := flag.String("key", "", "client key")
	certFile := flag.String("cert", "", "client certificate")
	path := flag.String("path", "", "path with avro files")
	upstreamAPI := flag.String("upstream", "", "log receiver URL")

	flag.Parse()

	// todo: unless upstream API isn't https
	if len(*keyFile) == 0 || len(*certFile) == 0 {
		return fmt.Errorf("-cert and -key are required")
	}

	cm, err := apitls.GetCertman(*certFile, *keyFile)
	if err != nil {
		return logger.NewError("certman setup", "err", err)
	}

	client, err := httpClient(cm)
	if err != nil {
		return logger.NewError("http setup", "err", err)
	}

	metricssrv := metricsserver.New()
	go metricssrv.ListenAndServe(ctx, 9097)

	metricssrv.Registry().MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: "logferry",
		}),
	)

	err = uploader(ctx, client, *path, *upstreamAPI, metricssrv.Registry())
	if err != nil {
		return err
	}

	return nil
}
