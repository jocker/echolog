package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CAFile        string
	CertFile      string
	KeyFile       string
	ServerAddress string
	IsServer      bool
}

func SetupTLSConfig(cfg TLSConfig) (tlsConfig *tls.Config, err error) {
	tlsConfig = &tls.Config{}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return
		}
	}
	if cfg.CAFile != "" {
		var b []byte
		b, err = ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(b)
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q",
				cfg.CAFile,
			)
		}
		if cfg.IsServer {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return
}
