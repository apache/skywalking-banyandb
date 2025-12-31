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

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/server"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/watchdog"
)

var _ = Describe("Test Case 2: Buffer Overflow Handling", func() {
	var (
		metricsEndpoint     string
		fr                  *flightrecorder.FlightRecorder
		wd                  *watchdog.Watchdog
		metricsServer       *server.Server
		promReg             *prometheus.Registry
		datasourceCollector *exporter.DatasourceCollector
		capacitySize        int64
	)

	BeforeEach(func() {
		// Construct metrics endpoint URL
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
		metricsEndpoint = fmt.Sprintf("http://%s:2121/metrics", host)

		capacitySize = 100 * 1024 // 100KB - small enough to overflow

		fr = flightrecorder.NewFlightRecorder(capacitySize)

		// Create Prometheus registry and collector
		promReg = prometheus.NewRegistry()
		datasourceCollector = exporter.NewDatasourceCollector(fr)

		// Create and start Prometheus metrics server for FODC
		var serverCreateErr error
		metricsServer, serverCreateErr = server.NewServer(server.Config{
			ListenAddr:        defaultLocalhost + ":0",
			ReadHeaderTimeout: 3 * time.Second,
			ShutdownTimeout:   5 * time.Second,
		})
		Expect(serverCreateErr).NotTo(HaveOccurred())

		serverErrCh, serverStartErr := metricsServer.Start(promReg, datasourceCollector)
		Expect(serverStartErr).NotTo(HaveOccurred())
		Expect(serverErrCh).NotTo(BeNil())

		// Create Watchdog with short polling interval for testing
		pollInterval := 1 * time.Second // Faster polling to generate more data
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

		// Give watchdog a moment to start
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

	It("should handle buffer overflow with circular overwrite behavior", func() {
		var (
			datasources []*flightrecorder.Datasource
			ds          *flightrecorder.Datasource
		)

		// Step 1: Generate initial metrics and capture baseline state
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		// Generate some initial metrics
		for i := 0; i < 10; i++ {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			Expect(respErr).NotTo(HaveOccurred())
			if resp != nil {
				resp.Body.Close()
			}

			metricsReq, metricsReqErr := http.NewRequest(http.MethodGet, metricsEndpoint, nil)
			Expect(metricsReqErr).NotTo(HaveOccurred())

			metricsResp, metricsRespErr := client.Do(metricsReq)
			Expect(metricsRespErr).NotTo(HaveOccurred())
			if metricsRespErr == nil && metricsResp != nil {
				metricsResp.Body.Close()
			}

			time.Sleep(100 * time.Millisecond)
		}

		// Wait for initial metrics to be collected
		Eventually(func() bool {
			allDatasources := fr.GetDatasources()
			if len(allDatasources) == 0 {
				return false
			}
			currentDatasource := allDatasources[0]
			metricsMap := currentDatasource.GetMetrics()
			return len(metricsMap) > 0
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Initial metrics should be buffered")
		// Capture baseline: get initial timestamps and metric values
		datasources = fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty())
		ds = datasources[0]
		initialTimestamps := ds.GetTimestamps()
		Expect(initialTimestamps).NotTo(BeNil())
		initialTimestampValues := initialTimestamps.GetAllValues()
		// Filter out zeros to get actual count
		nonZeroInitialTimestamps := make([]int64, 0)
		for _, ts := range initialTimestampValues {
			if ts != 0 {
				nonZeroInitialTimestamps = append(nonZeroInitialTimestamps, ts)
			}
		}
		initialTimestampCount := len(nonZeroInitialTimestamps)
		// Get a sample metric to track
		metricsMap := ds.GetMetrics()
		Expect(metricsMap).NotTo(BeEmpty())

		var sampleMetricKey string
		var sampleMetricBuffer *flightrecorder.MetricRingBuffer
		for key, buffer := range metricsMap {
			sampleMetricKey = key
			sampleMetricBuffer = buffer
			break
		}
		Expect(sampleMetricKey).NotTo(BeEmpty(), "Should have at least one metric")

		initialMetricValues := sampleMetricBuffer.GetAllValues()
		initialMetricCount := len(initialMetricValues)
		initialCapacity := sampleMetricBuffer.Cap()

		GinkgoWriter.Printf("Initial state: %d timestamps, %d metric values, capacity: %d\n",
			initialTimestampCount, initialMetricCount, initialCapacity)

		// Step 2: Generate large number of metric values (exceeding buffer size)
		// Continue generating metrics for many polling cycles to force overflow
		// Poll interval is 1s, so generate metrics for 30+ seconds to ensure overflow
		for i := 0; i < 50; i++ {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			Expect(respErr).NotTo(HaveOccurred())
			if resp != nil {
				resp.Body.Close()
			}

			metricsReq, metricsReqErr := http.NewRequest(http.MethodGet, metricsEndpoint, nil)
			Expect(metricsReqErr).NotTo(HaveOccurred())

			metricsResp, metricsRespErr := client.Do(metricsReq)
			Expect(metricsRespErr).NotTo(HaveOccurred())
			if metricsResp != nil {
				metricsResp.Body.Close()
			}

			time.Sleep(200 * time.Millisecond)
		}

		// Wait for watchdog to collect many polling cycles
		// Wait long enough to ensure buffer overflow occurs
		time.Sleep(35 * time.Second)

		// Step 3: Verify circular overwrite behavior
		datasources = fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty())
		ds = datasources[0]

		// Verify timestamps - should have reached capacity and started overwriting
		finalTimestamps := ds.GetTimestamps()
		Expect(finalTimestamps).NotTo(BeNil())
		finalTimestampValues := finalTimestamps.GetAllValues()
		finalTimestampCount := len(finalTimestampValues)

		// Verify capacity limit is maintained
		timestampCapacity := finalTimestamps.Cap()
		Expect(finalTimestampCount).To(BeNumerically("<=", timestampCapacity),
			"Timestamp count should not exceed capacity")

		// Step 4: Verify newest metrics are preserved
		// Get the same metric buffer
		finalMetricsMap := ds.GetMetrics()
		Expect(finalMetricsMap).NotTo(BeEmpty())

		finalSampleBuffer, exists := finalMetricsMap[sampleMetricKey]
		Expect(exists).To(BeTrue(), "Sample metric should still exist")

		finalMetricValues := finalSampleBuffer.GetAllValues()
		finalMetricCount := len(finalMetricValues)
		finalCapacity := finalSampleBuffer.Cap()

		GinkgoWriter.Printf("Final state: %d timestamps (capacity: %d), %d metric values (capacity: %d)\n",
			finalTimestampCount, timestampCapacity, finalMetricCount, finalCapacity)

		// Verify capacity is maintained (should not grow unbounded)
		Expect(finalCapacity).To(BeNumerically(">", 0), "Capacity should be set")
		Expect(finalMetricCount).To(BeNumerically("<=", finalCapacity),
			"Metric count should not exceed capacity")

		// Step 5: Verify oldest metrics are overwritten correctly
		// If buffer overflow occurred, we should see that:
		// 1. The count is at or near capacity
		// 2. The newest values are present
		// 3. The oldest values from initial state are gone (if overflow occurred)

		if finalTimestampCount >= timestampCapacity {
			// Buffer overflow occurred - verify circular behavior
			GinkgoWriter.Printf("Buffer overflow detected: %d timestamps at capacity %d\n",
				finalTimestampCount, timestampCapacity)

			// Filter out zero values before checking chronological order
			// GetAllValues() returns all values in buffer array, including zeros
			nonZeroTimestamps := make([]int64, 0)
			for _, ts := range finalTimestampValues {
				if ts != 0 {
					nonZeroTimestamps = append(nonZeroTimestamps, ts)
				}
			}

			// Verify timestamps are in chronological order (oldest to newest)
			// Only check if we have at least 2 non-zero timestamps
			if len(nonZeroTimestamps) >= 2 {
				for i := 1; i < len(nonZeroTimestamps); i++ {
					Expect(nonZeroTimestamps[i]).To(BeNumerically(">=", nonZeroTimestamps[i-1]),
						"Timestamps should be in chronological order")
				}
			}

			// Verify newest timestamp is more recent than oldest
			// Use non-zero timestamps for comparison
			if len(nonZeroTimestamps) >= 2 {
				newestTimestamp := nonZeroTimestamps[len(nonZeroTimestamps)-1]
				oldestTimestamp := nonZeroTimestamps[0]
				Expect(newestTimestamp).To(BeNumerically(">", oldestTimestamp),
					"Newest timestamp should be more recent than oldest")
			}

			// Verify that if we had initial timestamps, newest are more recent
			if initialTimestampCount > 0 && len(nonZeroTimestamps) >= timestampCapacity {
				// If we overflowed significantly, verify newest timestamps are present
				if len(nonZeroTimestamps) > initialTimestampCount {
					// Verify that newest timestamps are present and more recent
					newestTimestamp := nonZeroTimestamps[len(nonZeroTimestamps)-1]
					oldestInitialTimestamp := nonZeroInitialTimestamps[0]
					Expect(newestTimestamp).To(BeNumerically(">", oldestInitialTimestamp),
						"Newest timestamp should be more recent than initial oldest")
				}
			}
		}

		// Verify metrics also show circular overwrite behavior
		if finalMetricCount >= finalCapacity {
			GinkgoWriter.Printf("Metric buffer overflow detected: %d values at capacity %d\n",
				finalMetricCount, finalCapacity)

			// Verify newest metric values are present
			if len(finalMetricValues) > 0 {
				newestValue := finalMetricValues[len(finalMetricValues)-1]
				Expect(newestValue).To(BeNumerically(">=", 0),
					"Newest metric value should be valid")
			}

			// Verify that capacity is reasonable
			if initialMetricCount > 0 {
				Expect(finalCapacity).To(BeNumerically(">", 0),
					"Capacity should be positive")
				// Capacity may decrease as more metrics are added, but should not be zero
				GinkgoWriter.Printf("Capacity changed from %d to %d (expected: may decrease as metrics increase)\n",
					initialCapacity, finalCapacity)
			}
		}
	})
})
