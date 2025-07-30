// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package helpers

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// HTTPHealthCheck returns a function for ginkgo "Eventually" poll it repeatedly to check whether a HTTP server is ready.
func HTTPHealthCheck(addr, cert string) func() error {
	return HTTPHealthCheckWithAuth(addr, cert, "", "")
}

// HTTPHealthCheckWithAuth returns a function for ginkgo "Eventually" poll it repeatedly to check whether a HTTP server is ready with Auth.
func HTTPHealthCheckWithAuth(addr, cert, username, password string) func() error {
	return func() error {
		client := resty.New()
		schema := "http"
		if cert != "" {
			// #nosec G402
			config := tls.Config{}
			cert, err := os.ReadFile(cert)
			if err != nil {
				return err
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(cert) {
				return errors.New("failed to add server's certificate")
			}
			config.RootCAs = certPool
			client.SetTLSClientConfig(&config)
			schema = "https"
		}
		req := client.R().SetHeader("Accept", "application/json")
		if username != "" {
			req.SetHeader("Authorization", auth.GenerateBasicAuthHeader(username, password))
		}
		resp, err := req.Get(fmt.Sprintf("%s://%s/api/healthz", schema, addr))
		if err != nil {
			time.Sleep(1 * time.Second)
			return err
		}
		l := logger.GetLogger("http-health")
		if resp.StatusCode() != 200 {
			l.Warn().Str("responded_status", resp.Status()).Msg("service unhealthy")
			time.Sleep(1 * time.Second)
			return errServiceUnhealthy
		}
		if e := l.Debug(); e.Enabled() {
			e.Stringer("response", resp).Msg("connected")
		}
		time.Sleep(500 * time.Millisecond)
		return nil
	}
}
