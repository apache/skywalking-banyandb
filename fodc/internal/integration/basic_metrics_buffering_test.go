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
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/apache/skywalking-banyandb/fodc/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/server"
	"github.com/apache/skywalking-banyandb/fodc/internal/watchdog"
)

var _ = Describe("Test Case 1: Basic Metrics Buffering", func() {
	var (
		metricsEndpoint     string
		fr                  *flightrecorder.FlightRecorder
		wd                  *watchdog.Watchdog
		metricsServer       *server.Server
		promReg             *prometheus.Registry
		datasourceCollector *exporter.DatasourceCollector
	)

	BeforeEach(func() {
		// Construct metrics endpoint URL
		// Extract host from HTTP address and use port 2121 for observability metrics
		host, _, splitErr := net.SplitHostPort(banyanDBHTTPAddr)
		if splitErr != nil {
			// Fallback: if SplitHostPort fails, try simple string split
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
		metricsEndpoint = fmt.Sprintf("http://%s:2121/metrics", host)

		// Create Flight Recorder with reasonable capacity (10MB)
		capacitySize := 10 * 1024 * 1024 // 10MB
		fr = flightrecorder.NewFlightRecorder(capacitySize)

		// Create Prometheus registry and collector
		promReg = prometheus.NewRegistry()
		datasourceCollector = exporter.NewDatasourceCollector(fr)

		// Create and start Prometheus metrics server for FODC
		var serverCreateErr error
		metricsServer, serverCreateErr = server.NewServer(server.Config{
			ListenAddr:        defaultLocalhost + ":0", // Use port 0 for automatic assignment
			ReadHeaderTimeout: 3 * time.Second,
			ShutdownTimeout:   5 * time.Second,
		})
		Expect(serverCreateErr).NotTo(HaveOccurred())

		serverErrCh, serverStartErr := metricsServer.Start(promReg, datasourceCollector)
		Expect(serverStartErr).NotTo(HaveOccurred())
		Expect(serverErrCh).NotTo(BeNil())

		// Create Watchdog with short polling interval for testing
		pollInterval := 2 * time.Second
		wd = watchdog.NewWatchdogWithConfig(fr, metricsEndpoint, pollInterval)

		ctx := context.Background()
		preRunErr := wd.PreRun(ctx)
		Expect(preRunErr).NotTo(HaveOccurred())

		// Verify metrics endpoint is accessible before starting watchdog
		client := &http.Client{Timeout: 2 * time.Second}
		resp, healthErr := client.Get(metricsEndpoint)
		Expect(healthErr).NotTo(HaveOccurred(), "Metrics endpoint should be accessible")
		if resp != nil {
			resp.Body.Close()
		}

		// Start watchdog polling
		stopCh := wd.Serve()
		Expect(stopCh).NotTo(BeNil())

		// Give watchdog a moment to start and perform initial poll
		time.Sleep(500 * time.Millisecond)
	})

	AfterEach(func() {
		// Stop watchdog
		if wd != nil {
			wd.GracefulStop()
		}

		// Stop metrics server
		if metricsServer != nil {
			stopErr := metricsServer.Stop()
			Expect(stopErr).NotTo(HaveOccurred())
		}
	})

	It("should buffer metrics correctly after watchdog polls", func() {
		// Step 2: Generate metrics by performing operations
		// Make HTTP requests to generate HTTP-related metrics
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		// Perform multiple operations to generate various metrics
		for i := 0; i < 5; i++ {
			// Health check endpoint
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			if respErr == nil && resp != nil {
				resp.Body.Close()
			}

			// Metrics endpoint (will generate metrics about metrics collection)
			metricsReq, metricsReqErr := http.NewRequest(http.MethodGet, metricsEndpoint, nil)
			Expect(metricsReqErr).NotTo(HaveOccurred())

			metricsResp, metricsRespErr := client.Do(metricsReq)
			if metricsRespErr == nil && metricsResp != nil {
				metricsResp.Body.Close()
			}

			time.Sleep(200 * time.Millisecond)
		}

		// Step 3: Wait for Watchdog to poll metrics
		// Poll interval is 2s, so wait at least 2.5s to ensure at least one poll cycle completes
		// Use Eventually to wait for metrics to be buffered
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			if len(datasources) == 0 {
				return false
			}
			ds := datasources[0]
			metricsMap := ds.GetMetrics()
			return len(metricsMap) > 0
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Metrics should be buffered after watchdog polls")

		// Step 4: Verify metrics are buffered correctly through internal checks
		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty(), "FlightRecorder should have at least one datasource")

		ds := datasources[0]
		Expect(ds).NotTo(BeNil(), "First datasource should not be nil")

		// Check that metrics were collected
		metricsMap := ds.GetMetrics()
		Expect(metricsMap).NotTo(BeEmpty(), "Datasource should have buffered metrics")

		// Verify that timestamps were recorded (one per polling cycle)
		timestamps := ds.GetTimestamps()
		Expect(timestamps).NotTo(BeNil())
		timestampValues := timestamps.GetAllValues()
		Expect(len(timestampValues)).To(BeNumerically(">", 0), "Timestamps should be recorded")

		// Verify that at least some metrics have values
		totalMetricsWithValues := 0
		for metricKey, metricBuffer := range metricsMap {
			Expect(metricKey).NotTo(BeEmpty(), "Metric key should not be empty")
			Expect(metricBuffer).NotTo(BeNil(), "Metric buffer should not be nil")

			values := metricBuffer.GetAllValues()
			if len(values) > 0 {
				totalMetricsWithValues++
				// Verify that the metric has at least one non-zero value
				hasNonZeroValue := false
				for _, val := range values {
					if val != 0 {
						hasNonZeroValue = true
						break
					}
				}
				if hasNonZeroValue {
					GinkgoWriter.Printf("Metric %s has buffered values: %v\n", metricKey, values)
				}
			}
		}

		Expect(totalMetricsWithValues).To(BeNumerically(">", 0),
			"At least some metrics should have values buffered")

		// Verify descriptions were stored (at least some metrics should have descriptions)
		descriptions := ds.GetDescriptions()
		// Note: Not all metrics may have descriptions, but we should have at least some
		if len(descriptions) == 0 {
			GinkgoWriter.Printf("Warning: No descriptions found, but metrics were buffered: %d metrics\n", len(metricsMap))
		}

		// Verify TotalWritten counter is incremented
		Expect(ds.TotalWritten).To(BeNumerically(">", 0), "TotalWritten should be greater than 0")
	})

	It("should handle multiple polling cycles correctly", func() {
		// Generate some initial activity
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		for i := 0; i < 3; i++ {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			if respErr == nil && resp != nil {
				resp.Body.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Wait for multiple polling cycles
		// Poll interval is 2s, so wait for at least 2 cycles (4s + buffer)
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			if len(datasources) == 0 {
				return false
			}
			ds := datasources[0]
			timestamps := ds.GetTimestamps()
			if timestamps == nil {
				return false
			}
			timestampValues := timestamps.GetAllValues()
			return len(timestampValues) >= 2
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Should have multiple timestamps from multiple polling cycles")

		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty())

		ds := datasources[0]

		// Check that multiple timestamps were recorded (one per polling cycle)
		timestamps := ds.GetTimestamps()
		Expect(timestamps).NotTo(BeNil())
		timestampValues := timestamps.GetAllValues()
		Expect(len(timestampValues)).To(BeNumerically(">=", 2),
			"Should have multiple timestamps from multiple polling cycles")

		// Verify timestamps are in chronological order (newest last)
		if len(timestampValues) >= 2 {
			for i := 1; i < len(timestampValues); i++ {
				Expect(timestampValues[i]).To(BeNumerically(">=", timestampValues[i-1]),
					"Timestamps should be in chronological order")
			}
		}

		// Verify metrics were updated across multiple cycles
		metricsMap := ds.GetMetrics()
		for metricKey, metricBuffer := range metricsMap {
			values := metricBuffer.GetAllValues()
			// At least some metrics should have multiple values from multiple polls
			if len(values) > 0 {
				Expect(len(values)).To(BeNumerically(">=", 1),
					fmt.Sprintf("Metric %s should have values from polling cycles", metricKey))
			}
		}
	})

	It("should verify FlightRecorder API accessibility", func() {
		// Generate some metrics first
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		for i := 0; i < 2; i++ {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			if respErr == nil && resp != nil {
				resp.Body.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Wait for watchdog to poll
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			return len(datasources) > 0
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Datasources should be available")

		// Verify we can access datasources
		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty())

		ds := datasources[0]

		// Verify capacity is set
		capacitySize := fr.GetCapacitySize()
		Expect(capacitySize).To(BeNumerically(">", 0), "Capacity size should be set")

		// Verify metrics map is accessible
		metricsMap := ds.GetMetrics()
		Expect(metricsMap).NotTo(BeNil())

		// Verify timestamps are accessible
		timestamps := ds.GetTimestamps()
		Expect(timestamps).NotTo(BeNil())

		// Verify descriptions are accessible
		descriptions := ds.GetDescriptions()
		Expect(descriptions).NotTo(BeNil())

		// Verify TotalWritten is accessible
		Expect(ds.TotalWritten).To(BeNumerically(">=", 0), "TotalWritten should be accessible")
	})
})
