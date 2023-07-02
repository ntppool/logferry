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
		}

		// tlsConfig.BuildNameToCertificate()
		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
			MaxIdleConns:    10,
			IdleConnTimeout: 90 * time.Second,
		}
		clientCache = &http.Client{
			Transport: transport,
		}
	})

	return clientCache, clientErr
}
