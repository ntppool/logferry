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

	tracing.InitTracer(ctx, &tracing.TracerConfig{
		ServiceName: "logferry",
		Environment: "prod",
	})

	log.Info("logferry", "version", version.Version())

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
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	version.RegisterMetric("logferry", metricssrv.Registry())

	err = uploader(ctx, client, *path, *upstreamAPI, metricssrv.Registry())
	if err != nil {
		return err
	}

	return nil
}
