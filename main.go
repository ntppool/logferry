package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sync/errgroup"

	"go.ntppool.org/common/apitls"
	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/metricsserver"
	"go.ntppool.org/common/tracing"
	"go.ntppool.org/common/version"
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
	log := logger.Setup()

	keyFile := flag.String("key", "", "client key")
	certFile := flag.String("cert", "", "client certificate")
	path := flag.String("path", "", "path with avro files")
	upstreamAPI := flag.String("upstream", "", "log receiver URL")
	jsondump := flag.Bool("jsondump", false, "dump specified files to json")
	versionFlag := flag.Bool("version", false, "show version")

	flag.Parse()

	if *versionFlag {
		fmt.Printf("logferry %s\n", version.Version())
		os.Exit(0)
	}

	if *jsondump {
		dumpJSON(flag.Args())
		os.Exit(0)
	}

	log.Info("logferry", "version", version.Version())

	// todo: unless upstream API isn't https
	if len(*keyFile) == 0 || len(*certFile) == 0 {
		return fmt.Errorf("-cert and -key are required")
	}

	cm, err := apitls.GetCertman(*certFile, *keyFile)
	if err != nil {
		return logger.NewError("certman setup", "err", err)
	}

	capool, err := apitls.CAPool()
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		tpShutdown, err := tracing.InitTracer(ctx, &tracing.TracerConfig{
			ServiceName:         "logferry",
			Environment:         "prod",
			RootCAs:             capool,
			CertificateProvider: cm.GetClientCertificate,
		})
		if err != nil {
			return err
		}
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return tpShutdown(shutdownCtx)
	})

	metricssrv := metricsserver.New()

	g.Go(func() error {
		metricssrv.Registry().MustRegister(
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		version.RegisterMetric("logferry", metricssrv.Registry())
		return metricssrv.ListenAndServe(ctx, 9097)
	})

	g.Go(func() error {
		return directoryCleaner(ctx, *path)
	})

	g.Go(func() error {
		upstreamURL, err := url.Parse(*upstreamAPI)
		if err != nil {
			return err
		}

		upstreamURL.Path = "/"
		log.Info("upstream", "url", upstreamURL.String())

		clientManager, err := NewHTTPClientManager(cm, upstreamURL.String())
		if err != nil {
			return logger.NewError("http setup", "err", err)
		}
		return uploader(ctx, clientManager, *path, metricssrv.Registry())
	})

	return g.Wait()
}
