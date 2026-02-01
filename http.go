package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
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
		tlsConfig:  tlsConfig,
		apiURL:     apiURL,
		targetPool: targetPool,
		srvTTL:     defaultSRVTTL,
		stopCh:     make(chan struct{}),
	}

	cm.client = &http.Client{}

	// set transport and timeouts
	cm.SetTimeout(60 * time.Second)

	// Initial SRV resolution
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if targetPool.ResolveSRV(ctx) {
		log.Info("SRV resolution succeeded", "targets", targetPool.TargetCount())
	} else {
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
		if cm.targetPool.ResolveSRV(ctx) {
			log.Debug("SRV refresh succeeded", "targets", cm.targetPool.TargetCount())
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

// GetTargetURL returns the URL for the current target to use for requests.
// This implements SRV-based target selection with health tracking.
func (cm *httpClientManager) GetTargetURL() string {
	return cm.targetPool.SelectTarget()
}

// TargetPool returns the SRV target pool for health tracking.
func (cm *httpClientManager) TargetPool() *TargetPool {
	return cm.targetPool
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
