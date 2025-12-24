// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package integration_test

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const (
	defaultLocalhost = "localhost"
)

func TestFODCIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FODC Integration Test Suite")
}

var (
	banyanDBHTTPAddr string
	deferFunc        func()
	goods            []gleak.Goroutine
)

var _ = SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	// Start BanyanDB once for all tests
	var cleanup func()
	_, banyanDBHTTPAddr, cleanup = setup.Standalone(
		"--observability-modes=prometheus",
		"--observability-listener-addr=:2121",
	)
	deferFunc = cleanup

	// Wait for HTTP endpoint to be ready
	Eventually(helpers.HTTPHealthCheck(banyanDBHTTPAddr, ""), flags.EventuallyTimeout).Should(Succeed())

	// Wait for metrics endpoint to be ready
	host, _, splitErr := net.SplitHostPort(banyanDBHTTPAddr)
	if splitErr != nil {
		parts := strings.Split(banyanDBHTTPAddr, ":")
		if len(parts) > 0 {
			host = parts[0]
		} else {
			host = defaultLocalhost
		}
	}
	if host == "" {
		host = defaultLocalhost
	}
	metricsAddr := fmt.Sprintf("%s:2121", host)

	// Wait for metrics endpoint to be accessible
	Eventually(func() error {
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://%s/metrics", metricsAddr))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
		}
		return nil
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(Succeed())

	return []byte(banyanDBHTTPAddr)
}, func(address []byte) {
	banyanDBHTTPAddr = string(address)
})

var _ = SynchronizedAfterSuite(func() {}, func() {})

var _ = ReportAfterSuite("FODC Integration Test Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	}
})
