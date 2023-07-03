package main

import (
	"context"
	"crypto/x509"
	"strings"

	"github.com/labstack/echo/v4"
)

func (srv *APIServer) certificateMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		_, name := srv.getVerifiedCert(c.Request().TLS.VerifiedChains)
		ctx := context.WithValue(c.Request().Context(), CertificateKey, name)
		rctx := c.Request().WithContext(ctx)
		c.SetRequest(rctx)
		return next(c)
	}
}

func (srv *APIServer) getVerifiedCert(verifiedChains [][]*x509.Certificate) (*x509.Certificate, string) {
	for _, chain := range verifiedChains {
		for _, cert := range chain {
			for _, name := range cert.DNSNames {
				// log.Printf("verified cert, dns name: %+v", cert.DNSNames)
				// log.Printf("issuer: %s", cert.Issuer)
				if strings.HasSuffix(name, ".mon.ntppool.dev") {
					return cert, name
				}
				if strings.HasSuffix(name, ".ntppool.net") {
					return cert, name
				}
			}
		}
	}
	return nil, ""
}

func (srv *APIServer) verifyClient(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	cert, _ := srv.getVerifiedCert(verifiedChains)
	if cert != nil {
		return nil
	}
	return nil
	// return errors.New("no valid certificate found")
}
