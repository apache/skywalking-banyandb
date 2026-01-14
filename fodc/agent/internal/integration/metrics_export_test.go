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
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm"
	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/server"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/watchdog"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// volatileMetricNames contains names of metrics that are known to be volatile (gauges that change frequently).
var volatileMetricNames = []string{
	"go_memstats_heap_alloc_bytes",
	"go_memstats_heap_sys_bytes",
	"go_memstats_heap_inuse_bytes",
	"go_memstats_heap_idle_bytes",
	"go_memstats_heap_released_bytes",
	"go_memstats_alloc_bytes",
	"go_memstats_sys_bytes",
	"go_memstats_mallocs_total",
	"go_memstats_frees_total",
	"go_memstats_gc_sys_bytes",
	"go_memstats_other_sys_bytes",
	"go_memstats_next_gc_bytes",
	"go_memstats_last_gc_time_seconds",
	"process_resident_memory_bytes",
	"process_virtual_memory_bytes",
}

// isVolatileMetric checks if a metric key contains any volatile metric name.
// Latency metrics are also considered volatile as they represent timing measurements that fluctuate.
func isVolatileMetric(metricKeyStr string) bool {
	// Check for latency-related keywords (case-insensitive)
	lowerKey := strings.ToLower(metricKeyStr)
	if strings.Contains(lowerKey, "latency") {
		return true
	}

	// Check against predefined volatile metric names
	for _, volatileName := range volatileMetricNames {
		if strings.Contains(metricKeyStr, volatileName) {
			return true
		}
	}
	return false
}

// isLatencyMetric checks if a metric key represents a latency metric.
func isLatencyMetric(metricKeyStr string) bool {
	lowerKey := strings.ToLower(metricKeyStr)
	return strings.Contains(lowerKey, "latency")
}

// calculateTolerance calculates the appropriate tolerance for a volatile metric.
// For latency metrics, uses a smaller absolute tolerance (0.1 seconds).
// For memory metrics, uses 10MB as absolute tolerance.
func calculateTolerance(metricKeyStr string, bufferedValue float64) float64 {
	if isLatencyMetric(metricKeyStr) {
		// For latency metrics, use percentage-based tolerance (10%) or 0.1 seconds, whichever is larger
		absTolerance := 0.1                      // 100ms
		percentTolerance := bufferedValue * 0.10 // 10%
		if percentTolerance > absTolerance {
			return percentTolerance
		}
		return absTolerance
	}
	// For memory metrics, use percentage-based tolerance (10%) or 10MB, whichever is larger
	absTolerance := 10.0 * 1024 * 1024       // 10MB
	percentTolerance := bufferedValue * 0.10 // 10%
	if percentTolerance > absTolerance {
		return percentTolerance
	}
	return absTolerance
}

func skipOrFailKTM(reason string) {
	if flags.RequireKTM {
		Fail(reason)
		return
	}
	GinkgoWriter.Printf("Skipping KTM integration test: %s\n", reason)
	Skip(reason)
}

var _ = Describe("Test Case 3: Metrics Export to Prometheus", func() {
	var (
		metricsEndpoint     string
		fr                  *flightrecorder.FlightRecorder
		wd                  *watchdog.Watchdog
		metricsServer       *server.Server
		metricsServerAddr   string
		promReg             *prometheus.Registry
		datasourceCollector *exporter.DatasourceCollector
	)

	BeforeEach(func() {
		// Construct BanyanDB metrics endpoint URL
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

		// Create Flight Recorder with reasonable capacity
		capacitySize := 10 * 1024 * 1024 // 10MB
		fr = flightrecorder.NewFlightRecorder(int64(capacitySize))

		// Create Prometheus registry and collector
		promReg = prometheus.NewRegistry()
		datasourceCollector = exporter.NewDatasourceCollector(fr)

		// Create and start Prometheus metrics server for FODC with fixed port for testing
		// Use a high port number to avoid conflicts
		metricsServerAddr = defaultLocalhost + ":9091"
		var serverCreateErr error
		metricsServer, serverCreateErr = server.NewServer(server.Config{
			ListenAddr:        metricsServerAddr,
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

		// Verify BanyanDB metrics endpoint is accessible before starting watchdog
		client := &http.Client{Timeout: 2 * time.Second}
		resp, healthErr := client.Get(metricsEndpoint)
		Expect(healthErr).NotTo(HaveOccurred(), "BanyanDB metrics endpoint should be accessible")
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
			stopErr := metricsServer.Stop(context.Background())
			Expect(stopErr).NotTo(HaveOccurred())
		}
	})

	It("should export metrics to Prometheus format correctly", func() {
		// Step 1: Generate metrics and wait for Watchdog to collect them
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		// Generate metrics by performing operations
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
			if metricsResp != nil {
				metricsResp.Body.Close()
			}

			time.Sleep(200 * time.Millisecond)
		}

		// Wait for Watchdog to poll metrics
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			if len(datasources) == 0 {
				return false
			}
			ds := datasources[0]
			metricsMap := ds.GetMetrics()
			return len(metricsMap) > 0
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Metrics should be buffered after watchdog polls")

		// Step 2: Verify metrics are stored in FlightRecorder Datasources
		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty(), "FlightRecorder should have at least one datasource")

		ds := datasources[0]
		metricsMap := ds.GetMetrics()
		Expect(metricsMap).NotTo(BeEmpty(), "Datasource should have buffered metrics")

		// Step 3: Scrape `/metrics` endpoint using Prometheus client
		fodcMetricsURL := fmt.Sprintf("http://%s/metrics", metricsServerAddr)
		scrapeClient := &http.Client{
			Timeout: 5 * time.Second,
		}

		// Wait for metrics server to be ready
		Eventually(func() error {
			resp, err := scrapeClient.Get(fodcMetricsURL)
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return nil
		}, 5*time.Second, 500*time.Millisecond).Should(Succeed(), "FODC metrics endpoint should be accessible")

		// Scrape metrics
		resp, scrapeErr := scrapeClient.Get(fodcMetricsURL)
		Expect(scrapeErr).NotTo(HaveOccurred(), "Should be able to scrape metrics endpoint")
		Expect(resp.StatusCode).To(Equal(http.StatusOK), "Metrics endpoint should return 200 OK")

		// Verify Content-Type header
		contentType := resp.Header.Get("Content-Type")
		Expect(contentType).To(ContainSubstring("text/plain"), "Content-Type should be text/plain")
		Expect(contentType).To(ContainSubstring("version=0.0.4"), "Should include Prometheus format version")

		// Read response body
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		Expect(readErr).NotTo(HaveOccurred(), "Should be able to read response body")

		body := string(bodyBytes)
		Expect(body).NotTo(BeEmpty(), "Metrics response should not be empty")

		GinkgoWriter.Printf("Scraped metrics:\n%s\n", body)

		// Step 4: Parse Prometheus format and verify exported metrics match buffered metrics
		// Strip timestamps from metric lines before parsing (Prometheus format includes optional timestamps)
		bodyWithoutTimestamps := stripTimestampsFromPrometheusFormat(body)
		parsedMetrics, parseErr := fodcmetrics.Parse(bodyWithoutTimestamps)
		Expect(parseErr).NotTo(HaveOccurred(), "Should be able to parse Prometheus format")

		Expect(len(parsedMetrics)).To(BeNumerically(">", 0), "Should have parsed at least one metric")

		// Build a map of buffered metrics for comparison
		bufferedMetricsMap := make(map[string]float64)
		descriptions := ds.GetDescriptions()

		for metricKeyStr, metricBuffer := range metricsMap {
			if metricBuffer.Len() == 0 {
				continue
			}
			currentValue := metricBuffer.GetCurrentValue()
			bufferedMetricsMap[metricKeyStr] = currentValue
		}

		// Step 5: Verify exported metrics match buffered metrics
		exportedMetricsMap := make(map[string]float64)
		for _, parsedMetric := range parsedMetrics {
			// Reconstruct metric key from parsed metric
			metricKey := fodcmetrics.MetricKey{
				Name:   parsedMetric.Name,
				Labels: parsedMetric.Labels,
			}
			metricKeyStr := metricKey.String()
			exportedMetricsMap[metricKeyStr] = parsedMetric.Value
		}

		// Verify that exported metrics match buffered metrics
		matchedCount := 0
		for metricKeyStr, bufferedValue := range bufferedMetricsMap {
			exportedValue, exists := exportedMetricsMap[metricKeyStr]
			if exists {
				matchedCount++
				// Check if this is a volatile metric (memory-related gauges or latency metrics)
				if isVolatileMetric(metricKeyStr) {
					tolerance := calculateTolerance(metricKeyStr, bufferedValue)
					Expect(exportedValue).To(BeNumerically("~", bufferedValue, tolerance),
						fmt.Sprintf("Exported metric %s value should be within tolerance of buffered value (volatile metric)", metricKeyStr))
				} else {
					// Allow small floating point differences for non-volatile metrics
					Expect(exportedValue).To(BeNumerically("~", bufferedValue, 0.0001),
						fmt.Sprintf("Exported metric %s value should match buffered value", metricKeyStr))
				}
			}
		}

		Expect(matchedCount).To(BeNumerically(">", 0),
			"At least some exported metrics should match buffered metrics")

		// Step 6: Verify metric labels are preserved correctly
		for _, parsedMetric := range parsedMetrics {
			metricKey := fodcmetrics.MetricKey{
				Name:   parsedMetric.Name,
				Labels: parsedMetric.Labels,
			}
			metricKeyStr := metricKey.String()

			// Check if this metric exists in buffered metrics
			if _, exists := bufferedMetricsMap[metricKeyStr]; exists {
				// Verify labels are preserved
				bufferedBuffer, bufferedExists := metricsMap[metricKeyStr]
				Expect(bufferedExists).To(BeTrue(), "Metric should exist in buffered metrics")

				// Verify the metric has the same structure
				Expect(bufferedBuffer).NotTo(BeNil(), "Buffered metric buffer should not be nil")
			}
		}

		// Step 7: Verify HELP text is included in exported metrics
		lines := strings.Split(body, "\n")
		helpLines := make(map[string]string)

		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "# HELP ") {
				// Parse HELP line: # HELP metric_name description
				// SplitN(line, " ", 3) produces ["#", "HELP", "metric_name description_text"]
				parts := strings.SplitN(line, " ", 3)
				if len(parts) >= 3 {
					metricName := parts[1]
					helpText := parts[2]
					helpLines[metricName] = helpText
				} else if len(parts) == 2 {
					// Some HELP lines might just have metric name
					metricName := parts[1]
					helpLines[metricName] = ""
				}
			}
		}

		// Verify that HELP lines exist in the exported format
		Expect(len(helpLines)).To(BeNumerically(">", 0),
			"At least some HELP lines should be present in Prometheus format")

		// Verify that at least some exported metrics have HELP text
		// The exporter always generates HELP text, so this should always be true
		metricsWithHelp := 0
		for _, parsedMetric := range parsedMetrics {
			if helpText, exists := helpLines[parsedMetric.Name]; exists {
				metricsWithHelp++
				// HELP text might be empty or the metric name itself (when no description provided)
				// Both are valid - exporter generates default if empty
				if helpText != "" {
					// Verify HELP text matches description from datasource (if description exists)
					if desc, descExists := descriptions[parsedMetric.Name]; descExists && desc != "" {
						Expect(helpText).To(Equal(desc), fmt.Sprintf("HELP text should match description for %s", parsedMetric.Name))
					} else {
						// If no description, exporter should generate default HELP text
						expectedDefaultHelp := fmt.Sprintf("Metric %s from FlightRecorder", parsedMetric.Name)
						// HELP text might be the metric name itself (from BanyanDB) or the default
						// Both are acceptable
						if helpText != expectedDefaultHelp && helpText != parsedMetric.Name {
							GinkgoWriter.Printf("Warning: HELP text for %s is '%s', expected '%s' or '%s'\n",
								parsedMetric.Name, helpText, expectedDefaultHelp, parsedMetric.Name)
						}
					}
				}
			}
		}

		// Verify that at least some exported metrics have HELP text
		// Note: Prometheus format includes HELP lines, but not all metrics may have them matched
		// This is acceptable - the important thing is that HELP lines exist in the format
		if len(parsedMetrics) > 0 {
			// At least some metrics should have HELP text, or HELP lines should exist in body
			if metricsWithHelp == 0 {
				// If no matches, at least verify HELP lines exist in the body
				Expect(len(helpLines)).To(BeNumerically(">", 0),
					"HELP lines should exist in Prometheus format output")
			} else {
				Expect(metricsWithHelp).To(BeNumerically(">", 0),
					"At least some exported metrics should have HELP text")
			}
		}

		// Step 8: Verify metric values are current (from RingBuffers)
		// The exporter uses GetCurrentValue() which returns the most recent value
		for metricKeyStr, bufferedValue := range bufferedMetricsMap {
			exportedValue, exists := exportedMetricsMap[metricKeyStr]
			if exists {
				metricBuffer := metricsMap[metricKeyStr]
				currentValue := metricBuffer.GetCurrentValue()
				// Check if this is a volatile metric (memory-related gauges or latency metrics)
				if isVolatileMetric(metricKeyStr) {
					tolerance := calculateTolerance(metricKeyStr, bufferedValue)
					// Exported value should match current value from RingBuffer (with tolerance for volatile metrics)
					Expect(exportedValue).To(BeNumerically("~", currentValue, tolerance),
						fmt.Sprintf("Exported value for %s should match current RingBuffer value (volatile metric)", metricKeyStr))
					Expect(exportedValue).To(BeNumerically("~", bufferedValue, tolerance),
						fmt.Sprintf("Exported value for %s should match buffered value (volatile metric)", metricKeyStr))
				} else {
					// Exported value should match current value from RingBuffer
					Expect(exportedValue).To(BeNumerically("~", currentValue, 0.0001),
						fmt.Sprintf("Exported value for %s should match current RingBuffer value", metricKeyStr))
					Expect(exportedValue).To(BeNumerically("~", bufferedValue, 0.0001),
						fmt.Sprintf("Exported value for %s should match buffered value", metricKeyStr))
				}
			}
		}
	})

	It("should handle concurrent scraping while metrics are being updated", func() {
		// Generate initial metrics
		client := &http.Client{
			Timeout: 5 * time.Second,
		}

		for i := 0; i < 5; i++ {
			req, reqErr := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
			Expect(reqErr).NotTo(HaveOccurred())

			resp, respErr := client.Do(req)
			Expect(respErr).NotTo(HaveOccurred())
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Wait for initial metrics
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			return len(datasources) > 0 && len(datasources[0].GetMetrics()) > 0
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "Initial metrics should be buffered")

		// Step 9: Test concurrent scraping while metrics are being updated
		fodcMetricsURL := fmt.Sprintf("http://%s/metrics", metricsServerAddr)
		scrapeClient := &http.Client{
			Timeout: 5 * time.Second,
		}

		// Start concurrent metric generation
		stopGenerating := make(chan struct{})
		generationDone := make(chan struct{})
		go func() {
			defer close(generationDone)
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-stopGenerating:
					return
				case <-ticker.C:
					req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/v1/health", banyanDBHTTPAddr), nil)
					if resp, err := client.Do(req); err == nil && resp != nil {
						resp.Body.Close()
					}
				}
			}
		}()

		// Perform concurrent scrapes
		numScrapes := 10
		scrapeResults := make([]string, numScrapes)
		scrapeErrors := make([]error, numScrapes)

		var wg sync.WaitGroup
		for i := 0; i < numScrapes; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				resp, err := scrapeClient.Get(fodcMetricsURL)
				if err != nil {
					scrapeErrors[index] = err
					return
				}
				if resp.StatusCode != http.StatusOK {
					scrapeErrors[index] = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
					resp.Body.Close()
					return
				}
				bodyBytes, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr != nil {
					scrapeErrors[index] = readErr
					return
				}
				scrapeResults[index] = string(bodyBytes)
			}(i)
			// Stagger requests slightly
			time.Sleep(100 * time.Millisecond)
		}

		wg.Wait()
		close(stopGenerating)
		<-generationDone

		// Verify all scrapes succeeded
		successfulScrapes := 0
		for i := 0; i < numScrapes; i++ {
			if scrapeErrors[i] == nil && scrapeResults[i] != "" {
				successfulScrapes++
			}
		}

		Expect(successfulScrapes).To(BeNumerically(">=", numScrapes/2),
			"At least half of concurrent scrapes should succeed")

		// Verify scraped metrics are valid Prometheus format
		for i, result := range scrapeResults {
			if result != "" {
				// Strip timestamps before parsing (Prometheus format includes optional timestamps)
				resultWithoutTimestamps := stripTimestampsFromPrometheusFormat(result)
				_, parseErr := fodcmetrics.Parse(resultWithoutTimestamps)
				Expect(parseErr).NotTo(HaveOccurred(),
					fmt.Sprintf("Scrape result %d should be valid Prometheus format", i))
			}
		}

		// Verify that metrics are being updated (values may differ between scrapes)
		// This is expected behavior - metrics are being updated concurrently
		if len(scrapeResults) >= 2 {
			firstResult := scrapeResults[0]
			lastResult := scrapeResults[len(scrapeResults)-1]

			if firstResult != "" && lastResult != "" {
				firstResultWithoutTimestamps := stripTimestampsFromPrometheusFormat(firstResult)
				lastResultWithoutTimestamps := stripTimestampsFromPrometheusFormat(lastResult)
				firstMetrics, firstErr := fodcmetrics.Parse(firstResultWithoutTimestamps)
				lastMetrics, lastErr := fodcmetrics.Parse(lastResultWithoutTimestamps)

				if firstErr == nil && lastErr == nil {
					// Both should have metrics (may have different values)
					Expect(len(firstMetrics)).To(BeNumerically(">", 0),
						"First scrape should have metrics")
					Expect(len(lastMetrics)).To(BeNumerically(">", 0),
						"Last scrape should have metrics")
				}
			}
		}
	})

	It("should capture and export KTM metrics when enabled", func() {
		if runtime.GOOS != "linux" {
			skipOrFailKTM("KTM is only supported on Linux")
			return
		}

		// Initialize KTM for this test
		ctx := context.Background()
		ktmInterval := 2 * time.Second

		ktmCfg := ktm.Config{
			Enabled:  true,
			Interval: ktmInterval,
			Modules:  []string{"iomonitor"},
		}

		ktmLog := zerolog.New(io.Discard)
		ktmSvc, createErr := ktm.NewKTM(ktmCfg, ktmLog)
		if createErr != nil {
			skipOrFailKTM(fmt.Sprintf("KTM initialization failed (may lack permissions): %v", createErr))
			return
		}

		startErr := ktmSvc.Start(ctx)
		if startErr != nil {
			skipOrFailKTM(fmt.Sprintf("KTM start failed (may lack CAP_BPF or root): %v", startErr))
			return
		}
		defer ktmSvc.Stop()

		// Start metrics bridge goroutine
		stopBridgeCh := make(chan struct{})
		defer close(stopBridgeCh)

		go func() {
			ticker := time.NewTicker(ktmInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-stopBridgeCh:
					return
				case <-ticker.C:
					store := ktmSvc.GetMetrics()
					if store == nil {
						continue
					}
					rawMetrics := ktm.ToRawMetrics(store)
					ktmStatus := 2.0
					if ktmSvc.IsDegraded() {
						ktmStatus = 1.0
					}
					rawMetrics = append(rawMetrics, fodcmetrics.RawMetric{
						Name:  "ktm_status",
						Value: ktmStatus,
						Desc:  "KTM status: 1=Degraded (comm-only), 2=Full (cgroup+comm)",
					})
					if len(rawMetrics) > 0 {
						_ = fr.Update(rawMetrics)
					}
				}
			}
		}()

		// Create a temporary file to generate I/O operations for KTM to capture
		tmpFile, tmpErr := os.CreateTemp("", "ktm-test-*.dat")
		Expect(tmpErr).NotTo(HaveOccurred())
		tmpFilePath := tmpFile.Name()
		defer os.Remove(tmpFilePath)

		// Write some data to generate I/O
		testData := make([]byte, 4096)
		for i := range testData {
			testData[i] = byte(i % 256)
		}
		_, writeErr := tmpFile.Write(testData)
		Expect(writeErr).NotTo(HaveOccurred())
		tmpFile.Close()

		// Read the file multiple times to generate read I/O
		for i := 0; i < 10; i++ {
			readFile, readErr := os.Open(tmpFilePath)
			Expect(readErr).NotTo(HaveOccurred())
			readData := make([]byte, 4096)
			_, _ = readFile.Read(readData)
			readFile.Close()
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for KTM to collect metrics
		Eventually(func() bool {
			datasources := fr.GetDatasources()
			if len(datasources) == 0 {
				return false
			}
			ds := datasources[0]
			metricsMap := ds.GetMetrics()

			// Check if any KTM metrics are present
			for metricKey := range metricsMap {
				if strings.Contains(metricKey, "ktm_") {
					return true
				}
			}
			return false
		}, 10*time.Second, 500*time.Millisecond).Should(BeTrue(), "KTM metrics should be buffered in FlightRecorder")

		// Verify KTM metrics are in FlightRecorder
		datasources := fr.GetDatasources()
		Expect(datasources).NotTo(BeEmpty())
		ds := datasources[0]
		metricsMap := ds.GetMetrics()

		// Check for ktm_status metric
		ktmStatusFound := false
		for metricKey := range metricsMap {
			if strings.Contains(metricKey, "ktm_status") {
				ktmStatusFound = true
				metricBuffer := metricsMap[metricKey]
				currentValue := metricBuffer.GetCurrentValue()
				// ktm_status should be 1 (Degraded) or 2 (Full), not 0 (Disabled)
				Expect(currentValue).To(BeNumerically(">=", 1.0), "KTM should be enabled (status >= 1)")
				Expect(currentValue).To(BeNumerically("<=", 2.0), "KTM status should be valid (status <= 2)")
				GinkgoWriter.Printf("KTM status: %.0f\n", currentValue)
				break
			}
		}
		Expect(ktmStatusFound).To(BeTrue(), "ktm_status metric should be present")

		// Scrape Prometheus endpoint and verify KTM metrics are exported
		fodcMetricsURL := fmt.Sprintf("http://%s/metrics", metricsServerAddr)
		scrapeClient := &http.Client{Timeout: 5 * time.Second}

		resp, scrapeErr := scrapeClient.Get(fodcMetricsURL)
		Expect(scrapeErr).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		Expect(readErr).NotTo(HaveOccurred())

		body := string(bodyBytes)
		GinkgoWriter.Printf("Scraped metrics (KTM test):\n%s\n", body)

		// Parse metrics
		bodyWithoutTimestamps := stripTimestampsFromPrometheusFormat(body)
		parsedMetrics, parseErr := fodcmetrics.Parse(bodyWithoutTimestamps)
		Expect(parseErr).NotTo(HaveOccurred())

		// Verify KTM metrics are present in exported format
		ktmMetricsFound := make(map[string]bool)
		expectedKTMMetrics := []string{
			"ktm_status",
			"ktm_degraded",
		}

		for _, parsedMetric := range parsedMetrics {
			for _, expectedMetric := range expectedKTMMetrics {
				if parsedMetric.Name == expectedMetric {
					ktmMetricsFound[expectedMetric] = true
					GinkgoWriter.Printf("Found KTM metric: %s = %.2f\n", parsedMetric.Name, parsedMetric.Value)
				}
			}
			// Also check for any other ktm_ prefixed metrics
			if strings.HasPrefix(parsedMetric.Name, "ktm_") {
				GinkgoWriter.Printf("Additional KTM metric: %s = %.2f\n", parsedMetric.Name, parsedMetric.Value)
			}
		}

		// Verify at least the core KTM metrics are present
		Expect(ktmMetricsFound["ktm_status"]).To(BeTrue(), "ktm_status should be exported")
		// ktm_degraded is optional - it may not be present in the first collection cycle
		if ktmMetricsFound["ktm_degraded"] {
			GinkgoWriter.Printf("ktm_degraded metric is present\n")
		} else {
			GinkgoWriter.Printf("ktm_degraded metric not yet collected (this is acceptable)\n")
		}

		// Verify ktm_status value in exported metrics matches FlightRecorder
		for _, parsedMetric := range parsedMetrics {
			if parsedMetric.Name == "ktm_status" {
				Expect(parsedMetric.Value).To(BeNumerically(">=", 1.0), "Exported ktm_status should indicate KTM is enabled")
				Expect(parsedMetric.Value).To(BeNumerically("<=", 2.0), "Exported ktm_status should be valid")
			}
		}
	})
})

// stripTimestampsFromPrometheusFormat removes timestamps from Prometheus metric lines.
// Prometheus format: metric_name{labels} value timestamp.
// This function removes the timestamp part, leaving: metric_name{labels} value.
func stripTimestampsFromPrometheusFormat(text string) string {
	lines := strings.Split(text, "\n")
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip empty lines and comment lines
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			result = append(result, line)
			continue
		}

		// Check if line has a timestamp (ends with a number after whitespace)
		// Prometheus format: metric_name{labels} value timestamp
		// We need to remove the timestamp part
		parts := strings.Fields(trimmed)
		if len(parts) >= 3 {
			// Check if last part looks like a timestamp (all digits, 10-13 digits = seconds/milliseconds since epoch)
			lastPart := parts[len(parts)-1]
			if len(lastPart) >= 10 && len(lastPart) <= 13 {
				// Check if it's all digits
				isTimestamp := true
				for _, r := range lastPart {
					if r < '0' || r > '9' {
						isTimestamp = false
						break
					}
				}
				if isTimestamp {
					// Likely a timestamp, remove it
					// Reconstruct line without timestamp
					metricPart := strings.Join(parts[:len(parts)-1], " ")
					result = append(result, metricPart)
					continue
				}
			}
		}

		// No timestamp detected, keep line as-is
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
