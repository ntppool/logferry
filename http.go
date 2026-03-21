package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.ntppool.org/common/apitls"
	"go.ntppool.org/common/logger"
	"go.ntppool.org/common/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Default interval for SRV record refresh.
const defaultSRVTTL = 120 * time.Second

type httpClientManager struct {
	client *http.Client

	currentTimeout time.Duration
	tlsConfig      *tls.Config
	apiURL         string
	certProvider   apitls.CertificateProvider

	// SRV target management
	targetPool *TargetPool
	srvTTL     time.Duration
	stopCh     chan struct{}
}

func NewHTTPClientManager(certProvider apitls.CertificateProvider, apiURL string, metrics prometheus.Registerer) (*httpClientManager, error) {
	log := logger.Setup()

	capool, err := apitls.CAPool()
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify:   false,
		GetClientCertificate: certProvider.GetClientCertificate,
		RootCAs:              capool,
		MinVersion:           tls.VersionTLS13,
		// MaxVersion:           tls.VersionTLS13,
	}

	// Parse the upstream URL
	upstreamURL, err := url.Parse(apiURL)
	if err != nil {
		return nil, err
	}

	// Initialize SRV metrics and target pool
	srvMetrics := NewSRVMetrics(metrics)
	targetPool := NewTargetPool(upstreamURL, srvMetrics)

	cm := &httpClientManager{
		tlsConfig:    tlsConfig,
		apiURL:       apiURL,
		certProvider: certProvider,
		targetPool:   targetPool,
		srvTTL:       defaultSRVTTL,
		stopCh:       make(chan struct{}),
	}

	cm.client = &http.Client{}

	// set transport and timeouts
	cm.SetTimeout(60 * time.Second)

	// Initial SRV resolution
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if !targetPool.ResolveSRV(ctx) {
		log.Info("SRV resolution failed, using direct upstream")
	}
	cancel()

	// Start background goroutines
	go cm.runConfigLoop()
	go cm.runSRVRefresh()

	return cm, nil
}

// runConfigLoop periodically fetches config from the server.
func (cm *httpClientManager) runConfigLoop() {
	for {
		cm.getConfig(context.Background())

		select {
		case <-cm.stopCh:
			return
		case <-time.After(1 * time.Hour):
		}
	}
}

// runSRVRefresh periodically re-resolves SRV records based on TTL.
func (cm *httpClientManager) runSRVRefresh() {
	log := logger.Setup()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-time.After(cm.srvTTL):
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if !cm.targetPool.ResolveSRV(ctx) {
			log.Warn("SRV refresh failed or timed out")
		}
		cancel()
	}
}

func (cm *httpClientManager) SetTimeout(baseTimeout time.Duration) {
	if baseTimeout == cm.currentTimeout {
		return
	}

	cm.currentTimeout = baseTimeout

	baseTimeoutMs := baseTimeout.Milliseconds()

	responseHeaderTimeout := time.Duration(time.Millisecond * time.Duration(float64(baseTimeoutMs)*0.85))

	log := logger.Setup()
	log.Info("timeouts", "responseHeader", responseHeaderTimeout)

	tr := otelhttp.NewTransport(&http.Transport{
		TLSClientConfig: cm.tlsConfig,

		MaxIdleConns:    3,
		IdleConnTimeout: 15 * time.Second,

		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: 5 * time.Second,
	},
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
			return otelhttptrace.NewClientTrace(ctx)
		}),
	)

	cm.client.Transport = tr
	cm.client.Timeout = baseTimeout
}

func (cm *httpClientManager) Client() *http.Client {
	return cm.client
}

// APIBase returns the original upstream URL (for fallback/config purposes).
func (cm *httpClientManager) APIBase() string {
	return cm.apiURL
}

// GetTargetURL returns the URL for the current target to use for requests
// and a wait duration. When all targets are in backoff, wait indicates how
// long the caller should wait before attempting the request.
func (cm *httpClientManager) GetTargetURL() (string, time.Duration) {
	return cm.targetPool.SelectTarget()
}

// TargetPool returns the SRV target pool for health tracking.
func (cm *httpClientManager) TargetPool() *TargetPool {
	return cm.targetPool
}

// CertExpiry returns the NotAfter time of the current client certificate.
func (cm *httpClientManager) CertExpiry() (time.Time, error) {
	cert, err := cm.certProvider.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		return time.Time{}, err
	}
	if len(cert.Certificate) == 0 {
		return time.Time{}, fmt.Errorf("no certificate available")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return time.Time{}, err
	}
	return leaf.NotAfter, nil
}

// CertValid returns true if the current client certificate is not expired.
func (cm *httpClientManager) CertValid() bool {
	expiry, err := cm.CertExpiry()
	if err != nil {
		return false
	}
	return time.Now().Before(expiry)
}

// Stop shuts down background goroutines.
func (cm *httpClientManager) Stop() {
	close(cm.stopCh)
}

func (cm *httpClientManager) getConfig(ctx context.Context) error {
	ctx, span := tracing.Tracer().Start(ctx, "get_config")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	log := logger.Setup()

	configURL := cm.apiURL + "api/v1/config"
	log.Info("config", "url", configURL)

	req, err := http.NewRequestWithContext(ctx, "GET", configURL, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}

	resp, err := cm.Client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		conf := struct{ Timeout time.Duration }{}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil
		}
		err = json.Unmarshal(data, &conf)
		if err != nil {
			return err
		}
		cm.SetTimeout(conf.Timeout)
	} else {
		log.Warn("config status", "code", resp.StatusCode, "status", resp.Status)
	}
	return nil
}
