package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"go.ntppool.org/common/apitls"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var clientCache *http.Client
var onehttpClient sync.Once

func httpClient(cm apitls.CertificateProvider) (*http.Client, error) {

	var clientErr error

	onehttpClient.Do(func() {
		capool, err := apitls.CAPool()
		if err != nil {
			clientErr = err
			return
		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify:   false,
			GetClientCertificate: cm.GetClientCertificate,
			RootCAs:              capool,
			MinVersion:           tls.VersionTLS12,
			MaxVersion:           tls.VersionTLS12,
		}

		// tlsConfig.BuildNameToCertificate()

		transport := otelhttp.NewTransport(&http.Transport{
			TLSClientConfig: tlsConfig,

			MaxIdleConns:    3,
			IdleConnTimeout: 15 * time.Second,

			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 5 * time.Second,
		},
			otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
				return otelhttptrace.NewClientTrace(ctx)
			}),
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
		)
		clientCache = &http.Client{
			Transport: transport,
			Timeout:   20 * time.Second,
		}
	})

	return clientCache, clientErr
}
