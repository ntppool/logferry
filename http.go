package main

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"

	"go.ntppool.org/common/apitls"
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
		transport := &http.Transport{
			TLSClientConfig: tlsConfig,

			MaxIdleConns:    3,
			IdleConnTimeout: 15 * time.Second,

			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		clientCache = &http.Client{
			Transport: transport,
			Timeout:   20 * time.Second,
		}
	})

	return clientCache, clientErr
}
