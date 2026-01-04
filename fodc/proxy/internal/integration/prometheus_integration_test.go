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
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/fodc/agent/testhelper"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	grpcproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	metricsproxy "github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

type promFixture struct {
	registry   *registry.AgentRegistry
	aggregator *metricsproxy.Aggregator
	service    *grpcproxy.FODCService
	grpcServer *grpcproxy.Server
	httpServer *api.Server
	testLogger *logger.Logger
	grpcAddr   string
	httpAddr   string
}

func newPromFixture() *promFixture {
	testLogger := logger.GetLogger("test", "prometheus")
	heartbeatTimeout := 5 * time.Second
	cleanupTimeout := 10 * time.Second
	heartbeatInterval := 2 * time.Second

	reg := registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
	agg := metricsproxy.NewAggregator(reg, nil, testLogger)
	svc := grpcproxy.NewFODCService(reg, agg, testLogger, heartbeatInterval)
	agg.SetGRPCService(svc)

	grpcListener, err := net.Listen("tcp", "localhost:0")
	Expect(err).NotTo(HaveOccurred())
	grpcAddr := grpcListener.Addr().String()
	_ = grpcListener.Close()
	grpcSrv := grpcproxy.NewServer(svc, grpcAddr, 4194304, testLogger)
	Expect(grpcSrv.Start()).To(Succeed())

	httpListener, err := net.Listen("tcp", "localhost:0")
	Expect(err).NotTo(HaveOccurred())
	httpAddr := httpListener.Addr().String()
	_ = httpListener.Close()
	httpSrv := api.NewServer(agg, reg, testLogger)
	Expect(httpSrv.Start(httpAddr, 10*time.Second, 10*time.Second)).To(Succeed())

	Eventually(func() error {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", httpAddr))
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

	return &promFixture{
		registry:   reg,
		aggregator: agg,
		service:    svc,
		grpcServer: grpcSrv,
		httpServer: httpSrv,
		grpcAddr:   grpcAddr,
		httpAddr:   httpAddr,
		testLogger: testLogger,
	}
}

func (p *promFixture) teardown() {
	if p.httpServer != nil {
		_ = p.httpServer.Stop()
	}
	if p.grpcServer != nil {
		p.grpcServer.Stop()
	}
	if p.registry != nil {
		p.registry.Stop()
	}
}

var _ = Describe("Prometheus Integration", func() {
	var (
		fixture *promFixture
		agent   *testhelper.ProxyClientWrapper
		ctx     context.Context
		cancel  context.CancelFunc
		fr      interface{}
	)

	BeforeEach(func() {
		fixture = newPromFixture()
		ctx, cancel = context.WithCancel(context.Background())
		fr = testhelper.NewFlightRecorder(4 * 1024 * 1024)
		agent = testhelper.NewProxyClientWrapper(
			fixture.grpcAddr,
			"192.168.10.10",
			9100,
			"liaison",
			map[string]string{"env": "prom-test"},
			2*time.Second,
			1*time.Second,
			fr,
			fixture.testLogger,
		)
		Expect(agent).NotTo(BeNil())
		Expect(agent.Connect(ctx)).To(Succeed())
		Expect(agent.StartRegistrationStream(ctx)).To(Succeed())
		Expect(agent.StartMetricsStream(ctx)).To(Succeed())
	})

	AfterEach(func() {
		if agent != nil {
			_ = agent.Disconnect()
		}
		if cancel != nil {
			cancel()
		}
		if fixture != nil {
			fixture.teardown()
		}
	})

	It("satisfies Prometheus scraping expectations", func() {
		Expect(testhelper.UpdateMetrics(fr, []testhelper.RawMetric{{
			Name:  "prometheus_metric_total",
			Value: 123.0,
			Desc:  "Mock Prometheus metric",
			Labels: []testhelper.Label{
				{Name: "component", Value: "worker"},
			},
		}})).To(Succeed())

		time.Sleep(200 * time.Millisecond)

		var promBody string
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics", fixture.httpAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			body, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return readErr
			}
			promBody = string(body)
			if promBody == "" {
				return fmt.Errorf("empty metrics response")
			}
			if !contains(promBody, "prometheus_metric_total") {
				return fmt.Errorf("metric missing from /metrics output")
			}
			return nil
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		Expect(contains(promBody, `node_role="liaison"`)).To(BeTrue())
		Expect(contains(promBody, `component="worker"`)).To(BeTrue())

		var metricsJSON []map[string]interface{}
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics-windows", fixture.httpAddr))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return json.NewDecoder(resp.Body).Decode(&metricsJSON)
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())

		Expect(len(metricsJSON)).To(BeNumerically(">=", 1))
		for _, metric := range metricsJSON {
			if metric["name"] == "prometheus_metric_total" {
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("liaison"))
				Expect(labels["component"]).To(Equal("worker"))
			}
		}

		start := time.Now()
		for i := 0; i < 5; i++ {
			resp, err := http.Get(fmt.Sprintf("http://%s/metrics", fixture.httpAddr))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			_ = resp.Body.Close()
		}
		duration := time.Since(start)
		Expect(duration).To(BeNumerically("<", 5*time.Second))

		Expect(fixture.aggregator.ActiveCollections()).To(Equal(0))
	})
})

func contains(body, substr string) bool {
	return strings.Contains(body, substr)
}
