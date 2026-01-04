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
	"net/url"
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

var _ = Describe("Test Case 2: Metrics Time Window", func() {
	var (
		proxyGRPCAddr     string
		proxyHTTPAddr     string
		grpcServer        *grpcproxy.Server
		httpServer        *api.Server
		agentRegistry     *registry.AgentRegistry
		metricsAggregator *metricsproxy.Aggregator
		grpcService       *grpcproxy.FODCService
		flightRecorder1   interface{}
		flightRecorder2   interface{}
		proxyClient1      *testhelper.ProxyClientWrapper
		proxyClient2      *testhelper.ProxyClientWrapper
		agentCtx1         context.Context
		agentCancel1      context.CancelFunc
		agentCtx2         context.Context
		agentCancel2      context.CancelFunc
	)

	BeforeEach(func() {
		testLogger := logger.GetLogger("test", "integration")

		// Use longer timeouts for this test suite since tests can take time
		// and agents need to stay alive during metric collection
		// The aggregator timeout can be up to (endTime - startTime) + 5 seconds
		// For time windows up to ~20 seconds, we need at least 25 seconds aggregator timeout
		// Health check runs every heartbeatTimeout/2, so we need heartbeatTimeout to be
		// at least 2x the maximum expected aggregator timeout to avoid premature cleanup
		heartbeatTimeout := 60 * time.Second
		cleanupTimeout := 120 * time.Second
		heartbeatInterval := 2 * time.Second
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
		// Use longer timeouts to accommodate aggregator's timeout calculation:
		// timeout = (endTime - startTime) + 5 seconds
		// For time windows up to ~20 seconds, we need at least 25 seconds write timeout
		Expect(httpServer.Start(proxyHTTPAddr, 30*time.Second, 30*time.Second)).To(Succeed())

		Eventually(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s/health", proxyHTTPAddr))
			if err != nil {
				return err
			}
			resp.Body.Close()
			return nil
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Succeed())

		capacitySize := int64(10 * 1024 * 1024)
		flightRecorder1 = testhelper.NewFlightRecorder(capacitySize)
		flightRecorder2 = testhelper.NewFlightRecorder(capacitySize)

		proxyClient1 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.1",
			8080,
			"datanode-hot",
			map[string]string{"env": "test"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder1,
			testLogger,
		)
		Expect(proxyClient1).NotTo(BeNil())

		proxyClient2 = testhelper.NewProxyClientWrapper(
			proxyGRPCAddr,
			"127.0.0.2",
			8081,
			"liaison",
			map[string]string{"env": "test"},
			heartbeatInterval,
			1*time.Second,
			flightRecorder2,
			testLogger,
		)
		Expect(proxyClient2).NotTo(BeNil())

		agentCtx1, agentCancel1 = context.WithCancel(context.Background())
		agentCtx2, agentCancel2 = context.WithCancel(context.Background())
	})

	AfterEach(func() {
		if agentCancel1 != nil {
			agentCancel1()
		}
		if agentCancel2 != nil {
			agentCancel2()
		}
		if proxyClient1 != nil {
			_ = proxyClient1.Disconnect()
		}
		if proxyClient2 != nil {
			_ = proxyClient2.Disconnect()
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

	It("should filter metrics by time window", func() {
		By("Connecting agents to proxy")
		connectErr1 := proxyClient1.Connect(agentCtx1)
		Expect(connectErr1).NotTo(HaveOccurred())
		connectErr2 := proxyClient2.Connect(agentCtx2)
		Expect(connectErr2).NotTo(HaveOccurred())

		By("Starting registration streams")
		regErr1 := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr1).NotTo(HaveOccurred())
		regErr2 := proxyClient2.StartRegistrationStream(agentCtx2)
		Expect(regErr2).NotTo(HaveOccurred())

		By("Waiting for agent registrations")
		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(2))

		By("Starting metrics streams")
		metricsErr1 := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr1).NotTo(HaveOccurred())
		metricsErr2 := proxyClient2.StartMetricsStream(agentCtx2)
		Expect(metricsErr2).NotTo(HaveOccurred())

		// Wait for metrics streams to be fully established
		time.Sleep(500 * time.Millisecond)

		By("Adding metrics at different times to Flight Recorders")
		metricName := "metric_time_window"

		// Add first metric value (this will be excluded from time window)
		rawMetrics1Time1 := []testhelper.RawMetric{
			{
				Name:   metricName,
				Value:  10.0,
				Desc:   "Time window metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr1 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1Time1)
		Expect(updateErr1).NotTo(HaveOccurred())

		// Wait 2 seconds to ensure clear separation between metric timestamps (different Unix seconds)
		time.Sleep(5 * time.Second)
		now := time.Now().UTC()
		startTime := time.Unix(now.Unix(), 0).UTC()

		// Add second metric value (this will be included in time window)
		rawMetrics1Time2 := []testhelper.RawMetric{
			{
				Name:   metricName,
				Value:  20.0,
				Desc:   "Time window metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr2 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1Time2)
		Expect(updateErr2).NotTo(HaveOccurred())

		// Wait a bit before adding third metric
		time.Sleep(1100 * time.Millisecond)

		rawMetrics1Time3 := []testhelper.RawMetric{
			{
				Name:   metricName,
				Value:  30.0,
				Desc:   "Time window metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr3 := testhelper.UpdateMetrics(flightRecorder1, rawMetrics1Time3)
		Expect(updateErr3).NotTo(HaveOccurred())

		By("Verifying timestamps length equals each metric buffer length")
		Expect(testhelper.ValidateMetricsBufferAlignment(flightRecorder1)).To(Succeed())

		rawMetrics2Time2 := []testhelper.RawMetric{
			{
				Name:   "metric_agent2",
				Value:  50.0,
				Desc:   "Agent 2 metric",
				Labels: []testhelper.Label{{Name: "label2", Value: "value2"}},
			},
		}
		updateErr4 := testhelper.UpdateMetrics(flightRecorder2, rawMetrics2Time2)
		Expect(updateErr4).NotTo(HaveOccurred())

		// Wait a bit after last metric to ensure it's stored
		time.Sleep(500 * time.Millisecond)
		// Use UTC to ensure consistent timezone handling
		endTime := time.Now().UTC().Add(1 * time.Second)

		// Wait a bit more to ensure all metrics are fully stored and ready
		time.Sleep(1 * time.Second)

		By("Verifying agents are still connected and online before metrics collection")
		Eventually(func() error {
			agents := agentRegistry.ListAgents()
			if len(agents) != 2 {
				return fmt.Errorf("expected 2 agents registered, got %d", len(agents))
			}
			onlineCount := 0
			for _, agent := range agents {
				if agent.Status == registry.AgentStatusOnline {
					onlineCount++
				}
			}
			if onlineCount != 2 {
				return fmt.Errorf("expected 2 online agents, got %d", onlineCount)
			}
			return nil
		}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

		By("First verifying metrics can be collected without time window")
		// First, verify that metrics can be collected at all
		var allMetricsList []map[string]interface{}
		Eventually(func() error {
			metricsResp, metricsHTTPErr := http.Get(fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr))
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", metricsResp.StatusCode)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&allMetricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(allMetricsList) < 1 {
				return fmt.Errorf("expected at least 1 metric without filter, got %d", len(allMetricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Querying /metrics-windows with time window filter")
		// Use startTime set to the start of the Unix second when second metric was stored
		// This ensures:
		//   - First metric (stored ~2 seconds before, different Unix second) is excluded
		//   - Second metric (stored at startTime's Unix second) is included (timestamp >= startTime)
		//   - Third metric and metric_agent2 (stored after startTime) are included
		startTimeStr := startTime.Format(time.RFC3339)
		endTimeStr := endTime.Format(time.RFC3339)

		var metricsList []map[string]interface{}
		Eventually(func() error {
			// First verify agents are still registered and online
			agents := agentRegistry.ListAgents()
			if len(agents) < 2 {
				return fmt.Errorf("expected 2 agents registered, got %d", len(agents))
			}
			// Verify agents are online (not offline due to heartbeat timeout)
			onlineCount := 0
			for _, agent := range agents {
				if agent.Status == registry.AgentStatusOnline {
					onlineCount++
				}
			}
			if onlineCount < 2 {
				return fmt.Errorf("expected 2 online agents, got %d (total agents: %d)", onlineCount, len(agents))
			}

			baseURL := fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr)
			reqURL, urlErr := url.Parse(baseURL)
			if urlErr != nil {
				return urlErr
			}
			query := reqURL.Query()
			query.Set("start_time", startTimeStr)
			query.Set("end_time", endTimeStr)
			reqURL.RawQuery = query.Encode()
			// Use longer timeout to match server write timeout and aggregator timeout
			// Aggregator timeout = (endTime - startTime) + 5 seconds
			httpClient := &http.Client{
				Timeout: 30 * time.Second,
			}
			metricsResp, metricsHTTPErr := httpClient.Get(reqURL.String())
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				bodyBytes := make([]byte, 1024)
				n, readErr := metricsResp.Body.Read(bodyBytes)
				bodyMsg := ""
				if readErr == nil && n > 0 {
					bodyMsg = string(bodyBytes[:n])
				}
				return fmt.Errorf("unexpected status code: %d, body: %s, url: %s", metricsResp.StatusCode, bodyMsg, reqURL.String())
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsList) < 2 {
				// Debug: log which metrics were returned and their details
				metricNames := make([]string, 0, len(metricsList))
				for _, m := range metricsList {
					if name, ok := m["name"].(string); ok {
						metricNames = append(metricNames, name)
					}
				}
				// Also check how many metrics we got without filter for comparison
				allCount := len(allMetricsList)
				return fmt.Errorf(
					"expected at least 2 metrics, got %d: %v (start_time: %s, end_time: %s, all_metrics_count: %d)",
					len(metricsList), metricNames, startTimeStr, endTimeStr, allCount,
				)
			}
			return nil
		}, 20*time.Second, 1*time.Second).Should(Succeed())

		By("Verifying metrics are filtered by time window")
		foundMetricTimeWindow := false
		foundMetricAgent2 := false
		foundOldValue := false
		foundMidValue := false
		foundRecentValue := false

		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			switch name {
			case metricName:
				foundMetricTimeWindow = true
				data, ok := metric["data"].([]interface{})
				Expect(ok).To(BeTrue(), "metric_time_window data should be an array")
				Expect(len(data)).To(Equal(2), "metric_time_window should contain 2 data points in time window")

				for _, dataPoint := range data {
					pointMap, ok := dataPoint.(map[string]interface{})
					Expect(ok).To(BeTrue(), "data point should be an object")
					value, ok := pointMap["value"].(float64)
					Expect(ok).To(BeTrue(), "data point value should be a number")

					if value == 10.0 {
						foundOldValue = true
					}
					if value == 20.0 {
						foundMidValue = true
					}
					if value == 30.0 {
						foundRecentValue = true
					}
				}
			case "metric_agent2":
				foundMetricAgent2 = true
			}
		}

		Expect(foundMetricTimeWindow).To(BeTrue(), "metric_time_window should be found in time window")
		Expect(foundMetricAgent2).To(BeTrue(), "metric_agent2 should be found in time window")
		Expect(foundOldValue).To(BeFalse(), "old value (10.0) should not be present in the time window")
		Expect(foundMidValue).To(BeTrue(), "mid value (20.0) should be present in the time window")
		Expect(foundRecentValue).To(BeTrue(), "recent value (30.0) should be present in the time window")
	})

	It("should handle empty time window gracefully", func() {
		By("Connecting agent to proxy")
		connectErr := proxyClient1.Connect(agentCtx1)
		Expect(connectErr).NotTo(HaveOccurred())

		regErr := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))

		metricsErr := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to Flight Recorder first")
		rawMetrics := []testhelper.RawMetric{
			{
				Name:   "metric_before_future",
				Value:  50.0,
				Desc:   "Metric before future window",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr := testhelper.UpdateMetrics(flightRecorder1, rawMetrics)
		Expect(updateErr).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Querying /metrics-windows with empty time window (future times)")
		futureStart := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
		futureEnd := time.Now().Add(2 * time.Hour).Format(time.RFC3339)

		var metricsList []map[string]interface{}
		Eventually(func() error {
			baseURL := fmt.Sprintf("http://%s/metrics-windows", proxyHTTPAddr)
			reqURL, urlErr := url.Parse(baseURL)
			if urlErr != nil {
				return urlErr
			}
			query := reqURL.Query()
			query.Set("start_time", futureStart)
			query.Set("end_time", futureEnd)
			reqURL.RawQuery = query.Encode()
			// Use longer timeout to match server write timeout
			httpClient := &http.Client{
				Timeout: 30 * time.Second,
			}
			metricsResp, metricsHTTPErr := httpClient.Get(reqURL.String())
			if metricsHTTPErr != nil {
				return metricsHTTPErr
			}
			defer metricsResp.Body.Close()
			if metricsResp.StatusCode != http.StatusOK {
				bodyBytes := make([]byte, 1024)
				n, readErr := metricsResp.Body.Read(bodyBytes)
				bodyMsg := ""
				if readErr == nil && n > 0 {
					bodyMsg = string(bodyBytes[:n])
				}
				return fmt.Errorf("unexpected status code: %d, body: %s", metricsResp.StatusCode, bodyMsg)
			}
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying empty result for future time window")
		// The agent filters metrics by timestamp, so future time window should return empty
		// However, if no metrics match the time window, the response might be empty array
		Expect(len(metricsList)).To(Equal(0), "should return empty metrics for future time window")
	})

	It("should handle full time window (no filter)", func() {
		By("Connecting agent to proxy")
		connectErr := proxyClient1.Connect(agentCtx1)
		Expect(connectErr).NotTo(HaveOccurred())

		regErr := proxyClient1.StartRegistrationStream(agentCtx1)
		Expect(regErr).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(agentRegistry.ListAgents())
		}, flags.EventuallyTimeout, 100*time.Millisecond).Should(Equal(1))

		metricsErr := proxyClient1.StartMetricsStream(agentCtx1)
		Expect(metricsErr).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Adding metrics to Flight Recorder")
		rawMetrics := []testhelper.RawMetric{
			{
				Name:   "metric_full_window",
				Value:  100.0,
				Desc:   "Full window metric",
				Labels: []testhelper.Label{{Name: "label1", Value: "value1"}},
			},
		}
		updateErr := testhelper.UpdateMetrics(flightRecorder1, rawMetrics)
		Expect(updateErr).NotTo(HaveOccurred())

		time.Sleep(200 * time.Millisecond)

		By("Querying /metrics-windows without time window parameters")
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
			metricsDecodeErr := json.NewDecoder(metricsResp.Body).Decode(&metricsList)
			if metricsDecodeErr != nil {
				return metricsDecodeErr
			}
			if len(metricsList) < 1 {
				return fmt.Errorf("expected at least 1 metric, got %d", len(metricsList))
			}
			return nil
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())

		By("Verifying metrics are returned without time filtering")
		foundMetricFullWindow := false
		for _, metricItem := range metricsList {
			metric := metricItem
			name := metric["name"].(string)
			if name == "metric_full_window" {
				foundMetricFullWindow = true
				data := metric["data"].([]interface{})
				Expect(len(data)).To(BeNumerically(">=", 1))
			}
		}
		Expect(foundMetricFullWindow).To(BeTrue(), "metric_full_window should be found without time filter")
	})
})
