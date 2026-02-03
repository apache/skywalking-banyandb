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

var _ = Describe("Test Case 1: Agent Registration and Metrics Flow", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		flightRecorder    interface{} // FlightRecorder from agent package (via testhelper)
		proxyClient       *testhelper.ProxyClientWrapper
		agentCtx          context.Context
		agentCancel       context.CancelFunc
	)

	BeforeEach(func() {
		// Initialize logger
		testLogger := logger.GetLogger("test", "integration")

		// Create registry and aggregator
		heartbeatTimeout := 5 * time.Second
		cleanupTimeout := 10 * time.Second
		heartbeatInterval := 2 * time.Second
		agentRegistry = registry.NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, 100)
		metricsAggregator = metricsproxy.NewAggregator(agentRegistry, nil, testLogger)
		grpcService = grpcproxy.NewFODCService(agentRegistry, metricsAggregator, nil, testLogger, heartbeatInterval)
		metricsAggregator.SetGRPCService(grpcService)

		// Start gRPC server on random port
		grpcListener, listenErr := net.Listen("tcp", "localhost:0")
		Expect(listenErr).NotTo(HaveOccurred())
		proxyGRPCAddr = grpcListener.Addr().String()
		_ = grpcListener.Close() // Close listener so port is available for server
		grpcServer = grpcproxy.NewServer(grpcService, proxyGRPCAddr, 4194304, testLogger)
		Expect(grpcServer.Start()).To(Succeed())

		// Wait a bit for gRPC server to be ready
		time.Sleep(100 * time.Millisecond)

		// Start HTTP server on random port
		httpListener, httpListenErr := net.Listen("tcp", "localhost:0")
		Expect(httpListenErr).NotTo(HaveOccurred())
		proxyHTTPAddr = httpListener.Addr().String()
		_ = httpListener.Close() // Close listener so port is available for server
		httpServer = api.NewServer(metricsAggregator, nil, agentRegistry, testLogger)
		// Start HTTP server using the address from the listener
		Expect(httpServer.Start(proxyHTTPAddr, 10*time.Second, 10*time.Second)).To(Succeed())

		// Wait for HTTP server to be ready
		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

		// Create Flight Recorder for mock agent
		capacitySize := int64(10 * 1024 * 1024) // 10MB
		flightRecorder = testhelper.NewFlightRecorder(capacitySize)

		// Create proxy client using testhelper wrapper
		proxyClient = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"datanode-hot",
			"127.0.0.1",
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

	It("should register agent, collect metrics, and verify endpoints", func() {
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

		By("Verifying agent is registered")
		agents := agentRegistry.ListAgents()
		Expect(len(agents)).To(Equal(1))
		Expect(agents[0].AgentIdentity.Role).To(Equal("datanode-hot"))
		Expect(agents[0].Status).To(Equal(registry.AgentStatusOnline))

		By("Starting metrics stream")
		metricsErr := proxyClient.StartMetricsStream(agentCtx)
		Expect(metricsErr).NotTo(HaveOccurred())

		// Wait a bit for metrics stream to be fully established
		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to Flight Recorder")
		rawMetrics := []testhelper.RawMetric{
			{
				Name:  "test_metric_1",
				Value: 42.5,
				Desc:  "Test metric 1 description",
				Labels: []testhelper.Label{
					{Name: "label1", Value: "value1"},
					{Name: "node_role", Value: "datanode-hot"},
					{Name: "pod_name", Value: "test"},
					{Name: "container_name", Value: "data"},
				},
			},
			{
				Name:  "test_metric_2",
				Value: 100.0,
				Desc:  "Test metric 2 description",
				Labels: []testhelper.Label{
					{Name: "label2", Value: "value2"},
					{Name: "node_role", Value: "datanode-hot"},
					{Name: "pod_name", Value: "test"},
					{Name: "container_name", Value: "data"},
				},
			},
		}
		// Use helper function to update metrics (avoids importing internal package)
		updateErr := testhelper.UpdateMetrics(flightRecorder, rawMetrics)
		Expect(updateErr).NotTo(HaveOccurred())

		// Wait a bit for metrics to be stored in Flight Recorder
		time.Sleep(100 * time.Millisecond)

		By("Querying /metrics-windows endpoint to trigger metrics collection")
		// The proxy will request metrics from the agent, which should retrieve from Flight Recorder
		// and send back. The aggregator waits up to 10 seconds for metrics.
		var metricsList []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			// The /metrics-windows endpoint returns an array directly, not an object
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			// Wait until we have at least 2 metrics (our test metrics)
			if len(metricsList) < 2 {
				return fmt.Errorf("expected at least 2 metrics, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying metrics contain expected values and node metadata")
		foundMetric1 := false
		foundMetric2 := false
		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			if name == "test_metric_1" {
				foundMetric1 = true
				// Verify data points exist and contain expected value
				data := metric["data"].([]interface{})
				Expect(len(data)).To(BeNumerically(">=", 1))
				dataPoint := data[0].(map[string]interface{})
				Expect(dataPoint["value"]).To(BeNumerically("==", 42.5))
				// Verify node metadata (top-level fields)
				Expect(metric["agent_id"]).NotTo(BeEmpty())
				Expect(metric["pod_name"]).To(Equal("test"))
				// Verify labels contain node_role
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("datanode-hot"))
				Expect(labels["pod_name"]).To(Equal("test"))
				Expect(labels["container_name"]).To(Equal("data"))
			}
			if name == "test_metric_2" {
				foundMetric2 = true
				// Verify data points exist and contain expected value
				data := metric["data"].([]interface{})
				Expect(len(data)).To(BeNumerically(">=", 1))
				dataPoint := data[0].(map[string]interface{})
				Expect(dataPoint["value"]).To(BeNumerically("==", 100.0))
				// Verify node metadata
				Expect(metric["agent_id"]).NotTo(BeEmpty())
				labels := metric["labels"].(map[string]interface{})
				Expect(labels["node_role"]).To(Equal("datanode-hot"))
				Expect(labels["pod_name"]).To(Equal("test"))
				Expect(labels["container_name"]).To(Equal("data"))
			}
		}
		Expect(foundMetric1).To(BeTrue(), "test_metric_1 should be found in response")
		Expect(foundMetric2).To(BeTrue(), "test_metric_2 should be found in response")

		By("Stopping agent")
		agentCancel()
		disconnectErr := proxyClient.Disconnect()
		Expect(disconnectErr).NotTo(HaveOccurred())

		By("Waiting for agent to be marked as offline")
		// Note: AgentStatusOffline is actually "unconnected" per registry constants
		// The health check runs every heartbeatTimeout/2 (2.5s), and we need to wait for
		// the heartbeat timeout (5s) plus a bit more for the health check to run
		// Maximum wait: heartbeatTimeout (5s) + healthCheckInterval (2.5s) = 7.5s, use 10s for safety
		Eventually(func() string {
			agents := agentRegistry.ListAgents()
			if len(agents) == 0 {
				return "unregistered"
			}
			return string(agents[0].Status)
		}, 10*time.Second, 500*time.Millisecond).Should(Equal("unconnected"))
	})
})
