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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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

var _ = Describe("Test Case 3: Agent Reconnection", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		flightRecorder    interface{}
		proxyClient       *testhelper.ProxyClientWrapper
		agentCtx          context.Context
		agentCancel       context.CancelFunc
		testLogger        *logger.Logger
		heartbeatInterval time.Duration
	)

	BeforeEach(func() {
		testLogger = logger.GetLogger("test", "integration")

		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval = 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAggregator, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)

		grpcListener, listenErr := net.Listen("tcp", "localhost:0")
		Expect(listenErr).NotTo(HaveOccurred())
		proxyGRPCAddr = grpcListener.Addr().String()
		_ = grpcListener.Close()
		grpcServer = grpcproxy.NewServer(grpcService, proxyGRPCAddr, 4194304, testLogger)
		Expect(grpcServer.Start()).To(Succeed())

		time.Sleep(100 * time.Millisecond)

		httpListener, httpListenErr := net.Listen("tcp", "localhost:0")
		Expect(httpListenErr).NotTo(HaveOccurred())
		proxyHTTPAddr = httpListener.Addr().String()
		_ = httpListener.Close()
		httpServer = api.NewServer(metricsAggregator, agentRegistry, testLogger)
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

		capacitySize := int64(10 * 1024 * 1024)
		flightRecorder = testhelper.NewFlightRecorder(capacitySize)

		proxyClient = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.1",
			8080,
			"datanode-hot",
			"test",
			[]string{"data"},
			map[string]string{"env": "test"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder,
			testLogger,
		)
		Expect(proxyClient).NotTo(BeNil())

		agentCtx, agentCancel = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		if agentCancel != nil {
			agentCancel()
		}
		if proxyClient != nil {
			_ = proxyClient.Disconnect()
		}
		if httpServer != nil {
			_ = httpServer.Stop()
		}
		if grpcServer != nil {
			grpcServer.Stop()
		}
		if agentRegistry != nil {
			agentRegistry.Stop()
		}
	})

	It("should handle agent disconnection and reconnection", func() {
		By("Connecting agent to proxy")
		connectErr := proxyClient.Connect(agentCtx)
		Expect(connectErr).NotTo(HaveOccurred())

		By("Starting registration stream")
		regErr := proxyClient.StartRegistrationStream(agentCtx)
		Expect(regErr).NotTo(HaveOccurred())

		By("Waiting for agent registration")
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))

		By("Starting metrics stream")
		metricsErr := proxyClient.StartMetricsStream(agentCtx)
		Expect(metricsErr).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to Flight Recorder")
		rawMetrics := []testhelper.RawMetric{
			{
				Name:   "metric_before_disconnect",
				Value:  42.5,
				Desc:   "Metric before disconnect",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr := testhelper.UpdateMetrics(flightRecorder, rawMetrics)
		Expect(updateErr).NotTo(HaveOccurred())

		time.Sleep(100 * time.Millisecond)

		By("Querying /metrics-windows endpoint before disconnection")
		var metricsListBefore []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsListBefore)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsListBefore) < 1 {
				return fmt.Errorf("expected at least 1 metric, got %d", len(metricsListBefore))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		foundMetricBefore := false
		for _, metricItem := range metricsListBefore {
			metric := metricItem
			name := metric["name"].(string)
			if name == "metric_before_disconnect" {
				foundMetricBefore = true
			}
		}
		Expect(foundMetricBefore).To(BeTrue(), "metric_before_disconnect should be found")

		By("Simulating agent disconnection")
		agentCancel()
		disconnectErr := proxyClient.Disconnect()
		Expect(disconnectErr).NotTo(HaveOccurred())

		By("Waiting for agent to be marked as offline")
		// Health check runs every heartbeatTimeout/2 (2.5s), and we need to wait for
		// the heartbeat timeout (5s) plus a bit more for the health check to run
		// Maximum wait: heartbeatTimeout (5s) + healthCheckInterval (2.5s) = 7.5s, use 15s for safety
		Eventually(func() string {
			agents := agentRegistry.ListAgents()
			if len(agents) == 0 {
				return "no_agents"
			}
			return string(agents[0].Status)
		}, 15*time.Second, 500*time.Millisecond).Should(Equal("unconnected"))

		By("Querying /metrics-windows endpoint after disconnection")
		var metricsListAfter []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsListAfter)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying Proxy handles missing agent gracefully")
		foundMetricAfter := false
		for _, metricItem := range metricsListAfter {
			metric := metricItem
			name := metric["name"].(string)
			if name == "metric_before_disconnect" {
				foundMetricAfter = true
			}
		}
		Expect(foundMetricAfter).To(BeFalse(), "metrics should not be returned from disconnected agent")

		By("Creating new agent client for reconnection")
		agentCtx, agentCancel = context.WithCancel(context.Background())
		// Create a new ProxyClientWrapper for reconnection to simulate a real agent reconnecting
		proxyClient = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.1",
			8080,
			"datanode-hot",
			"test",
			[]string{"data"},
			map[string]string{"env": "test"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder,
			testLogger,
		)
		Expect(proxyClient).NotTo(BeNil())

		By("Agent reconnects and re-registers")
		reconnectErr := proxyClient.Connect(agentCtx)
		Expect(reconnectErr).NotTo(HaveOccurred())

		reRegErr := proxyClient.StartRegistrationStream(agentCtx)
		Expect(reRegErr).NotTo(HaveOccurred())

		By("Waiting for agent to be marked as online")
		Eventually(func() string {
			agents := agentRegistry.ListAgents()
			if len(agents) == 0 {
				return ""
			}
			// Return the status of the most recent agent (should be the new one)
			for _, agent := range agents {
				if agent.Status == "online" {
					return "online"
				}
			}
			return string(agents[0].Status)
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal("online"))

		By("Waiting for old agent to be cleaned up from registry")
		// The old agent will be unregistered after cleanupTimeout (10s)
		// We may have 2 agents temporarily (old offline one + new online one)
		Eventually(func() int {
			agents := agentRegistry.ListAgents()
			onlineCount := 0
			for _, agent := range agents {
				if agent.Status == "online" {
					onlineCount++
				}
			}
			return onlineCount
		}, 12*time.Second, 500*time.Millisecond).Should(Equal(1))

		By("Starting metrics stream after reconnection")
		reMetricsErr := proxyClient.StartMetricsStream(agentCtx)
		Expect(reMetricsErr).NotTo(HaveOccurred())

		// Wait for metrics stream to be fully established
		time.Sleep(500 * time.Millisecond)

		By("Adding new metrics to Flight Recorder after reconnection")
		rawMetricsAfter := []testhelper.RawMetric{
			{
				Name:   "metric_after_reconnect",
				Value:  99.9,
				Desc:   "Metric after reconnect",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErrAfter := testhelper.UpdateMetrics(flightRecorder, rawMetricsAfter)
		Expect(updateErrAfter).NotTo(HaveOccurred())

		// Wait for metrics to be stored
		time.Sleep(500 * time.Millisecond)

		By("Querying /metrics-windows endpoint after reconnection")
		var metricsListReconnected []map[string]interface{}
		Eventually(func() error {
			client := &http.Client{
				Timeout: 10 * time.Second,
			}
			metricsResp, metricsHTTPErr := client.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return fmt.Errorf("HTTP request failed: %w", metricsHTTPErr)
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				bodyBytes := make([]byte, 1024)
				n, _ := metricsResp.Body.Read(bodyBytes)
				bodyMsg := string(bodyBytes[:n])
				return fmt.Errorf("unexpected status code: %d, body: %s", metricsResp.StatusCode, bodyMsg)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsListReconnected)
			if metricsDecodeErr != nil {
				return fmt.Errorf("failed to decode JSON: %w", metricsDecodeErr)
			}
			// Check if we have the new metric
			foundNewMetric := false
			for _, metricItem := range metricsListReconnected {
				metric := metricItem
				name := metric["name"].(string)
				if name == "metric_after_reconnect" {
					foundNewMetric = true
					break
				}
			}
			if !foundNewMetric {
				return fmt.Errorf("expected metric_after_reconnect, got %d metrics: %v", len(metricsListReconnected), metricsListReconnected)
			}
			return nil
		}, 20*time.Second, 1*time.Second).Should(Succeed())

		By("Verifying metrics collection resumes successfully")
		foundMetricReconnected := false
		for _, metricItem := range metricsListReconnected {
			metric := metricItem
			name := metric["name"].(string)
			if name == "metric_after_reconnect" {
				foundMetricReconnected = true
				data := metric["data"].([]interface{})
				Expect(len(data)).To(BeNumerically(">=", 1))
				dataPoint := data[0].(map[string]interface{})
				Expect(dataPoint["value"]).To(BeNumerically("==", 99.9))
			}
		}
		Expect(foundMetricReconnected).To(BeTrue(), "metric_after_reconnect should be found after reconnection")
	})
})
