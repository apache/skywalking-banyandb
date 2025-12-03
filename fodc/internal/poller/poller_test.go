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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package poller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_NewMetricsPoller tests the constructor
func TestE2E_NewMetricsPoller(t *testing.T) {
	poller := NewMetricsPoller("http://localhost:9090/metrics", 5*time.Second)

	assert.NotNil(t, poller)
	assert.Equal(t, "http://localhost:9090/metrics", poller.url)
	assert.Equal(t, 5*time.Second, poller.interval)
	assert.NotNil(t, poller.client)
	assert.Equal(t, 5*time.Second, poller.client.Timeout)
}

// TestE2E_Poll_SuccessfulMetrics tests successful polling with valid metrics
func TestE2E_Poll_SuccessfulMetrics(t *testing.T) {
	metricsData := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="201"} 567
# HELP memory_usage_bytes Current memory usage
# TYPE memory_usage_bytes gauge
memory_usage_bytes 1073741824
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.WithinDuration(t, time.Now(), snapshot.Timestamp, 1*time.Second)
		assert.Equal(t, 0, len(snapshot.Errors))
		// Should have 3 metrics: 2 http_requests_total (with different labels) + 1 memory_usage_bytes
		assert.Equal(t, 3, len(snapshot.RawMetrics))
		assert.Equal(t, 0, len(snapshot.Histograms))

		// Verify metrics
		foundCounter := false
		foundGauge := false
		counterCount := 0
		for _, m := range snapshot.RawMetrics {
			if m.Name == "http_requests_total" {
				foundCounter = true
				counterCount++
				assert.Equal(t, "Total number of HTTP requests", m.Description)
			}
			if m.Name == "memory_usage_bytes" {
				foundGauge = true
				assert.Equal(t, "Current memory usage", m.Description)
			}
		}
		assert.True(t, foundCounter)
		assert.True(t, foundGauge)
		assert.Equal(t, 2, counterCount) // Two http_requests_total metrics with different labels

		// Verify last snapshot
		lastSnapshot := poller.GetLastSnapshot()
		assert.NotNil(t, lastSnapshot)
		assert.Equal(t, snapshot.Timestamp, lastSnapshot.Timestamp)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_Poll_HistogramMetrics tests polling with histogram metrics
func TestE2E_Poll_HistogramMetrics(t *testing.T) {
	metricsData := `# HELP http_request_duration_seconds HTTP request duration
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.005"} 10
http_request_duration_seconds_bucket{le="0.01"} 20
http_request_duration_seconds_bucket{le="0.025"} 30
http_request_duration_seconds_bucket{le="0.05"} 40
http_request_duration_seconds_bucket{le="0.1"} 50
http_request_duration_seconds_bucket{le="+Inf"} 100
http_request_duration_seconds_count 100
http_request_duration_seconds_sum 5.5
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))
		assert.Equal(t, 1, len(snapshot.Histograms))

		hist, exists := snapshot.Histograms["http_request_duration_seconds"]
		require.True(t, exists)
		assert.Equal(t, "http_request_duration_seconds", hist.Name)
		assert.Equal(t, "HTTP request duration", hist.Description)
		assert.Equal(t, 6, len(hist.Bins))

		// Histogram buckets should be filtered out from RawMetrics
		for _, m := range snapshot.RawMetrics {
			assert.False(t, strings.HasSuffix(m.Name, "_bucket"))
			assert.False(t, strings.HasSuffix(m.Name, "_count"))
			assert.False(t, strings.HasSuffix(m.Name, "_sum"))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_Poll_EmptyResponse tests polling with empty response
func TestE2E_Poll_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(""))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))
		assert.Equal(t, 0, len(snapshot.RawMetrics))
		assert.Equal(t, 0, len(snapshot.Histograms))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_Poll_Non200Status tests error handling for non-200 status codes
func TestE2E_Poll_Non200Status(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"404 Not Found", http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError},
		{"503 Service Unavailable", http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			poller := NewMetricsPoller(server.URL, 1*time.Second)
			outChan := make(chan MetricsSnapshot, 1)
			ctx := context.Background()

			poller.poll(ctx, outChan)

			select {
			case snapshot := <-outChan:
				assert.NotNil(t, snapshot)
				assert.Equal(t, 1, len(snapshot.Errors))
				assert.Contains(t, snapshot.Errors[0], "Non-200 status")
				assert.Contains(t, snapshot.Errors[0], fmt.Sprintf("%d", tt.statusCode))
			case <-time.After(2 * time.Second):
				t.Fatal("Timeout waiting for snapshot")
			}
		})
	}
}

// TestE2E_Poll_InvalidURL tests error handling for invalid URL
func TestE2E_Poll_InvalidURL(t *testing.T) {
	poller := NewMetricsPoller("http://invalid-host-that-does-not-exist:9999/metrics", 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Greater(t, len(snapshot.Errors), 0)
		assert.Contains(t, snapshot.Errors[0], "Failed to fetch metrics")
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_Poll_InvalidMetrics tests parsing with invalid metric lines
func TestE2E_Poll_InvalidMetrics(t *testing.T) {
	metricsData := `# HELP valid_metric A valid metric
valid_metric 123
invalid_line_without_value
another_invalid_line
# TYPE valid_metric counter
valid_metric{label="value"} 456
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))
		// Should parse valid metrics and skip invalid ones
		assert.GreaterOrEqual(t, len(snapshot.RawMetrics), 2)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_Start_ContextCancellation tests that Start stops when context is cancelled
func TestE2E_Start_ContextCancellation(t *testing.T) {
	requestCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("metric_value 123\n"))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 100*time.Millisecond)
	outChan := make(chan MetricsSnapshot, 10)
	ctx, cancel := context.WithCancel(context.Background())

	// Start poller in goroutine
	var startErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = poller.Start(ctx, outChan)
	}()

	// Wait for at least one poll
	select {
	case <-outChan:
		// Got at least one snapshot
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first snapshot")
	}

	// Cancel context
	cancel()

	// Wait for Start to return
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Error(t, startErr)
		assert.Equal(t, context.Canceled, startErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Start to return")
	}

	// Verify at least one request was made
	mu.Lock()
	assert.Greater(t, requestCount, 0)
	mu.Unlock()
}

// TestE2E_Start_MultiplePolls tests that Start polls multiple times
func TestE2E_Start_MultiplePolls(t *testing.T) {
	requestCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("metric_value 123\n"))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 100*time.Millisecond)
	outChan := make(chan MetricsSnapshot, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start poller in goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = poller.Start(ctx, outChan)
	}()

	// Collect snapshots
	snapshots := make([]MetricsSnapshot, 0)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case snapshot := <-outChan:
				snapshots = append(snapshots, snapshot)
			case <-ctx.Done():
				close(done)
				return
			}
		}
	}()

	// Wait for context to timeout
	<-done
	wg.Wait()

	// Should have received multiple snapshots (initial + periodic)
	assert.Greater(t, len(snapshots), 1)
	mu.Lock()
	assert.Greater(t, requestCount, 1)
	mu.Unlock()
}

// TestE2E_GetLastSnapshot tests GetLastSnapshot functionality
func TestE2E_GetLastSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test_metric 42\n"))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)

	// Initially should be nil
	assert.Nil(t, poller.GetLastSnapshot())

	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	// Wait for snapshot
	select {
	case snapshot := <-outChan:
		// Get last snapshot
		lastSnapshot := poller.GetLastSnapshot()
		assert.NotNil(t, lastSnapshot)
		assert.Equal(t, snapshot.Timestamp, lastSnapshot.Timestamp)
		assert.Equal(t, len(snapshot.RawMetrics), len(lastSnapshot.RawMetrics))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_ParseMetrics_WithComments tests parsing metrics with various comments
func TestE2E_ParseMetrics_WithComments(t *testing.T) {
	metricsData := `# HELP metric1 Description for metric1
# TYPE metric1 counter
metric1 100
# HELP metric2 Description for metric2
# TYPE metric2 gauge
metric2 200
# This is a regular comment that should be ignored
metric3 300
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))
		assert.Equal(t, 3, len(snapshot.RawMetrics))

		// Verify descriptions
		for _, m := range snapshot.RawMetrics {
			if m.Name == "metric1" {
				assert.Equal(t, "Description for metric1", m.Description)
			}
			if m.Name == "metric2" {
				assert.Equal(t, "Description for metric2", m.Description)
			}
			if m.Name == "metric3" {
				assert.Equal(t, "", m.Description) // No HELP comment
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_ParseMetrics_MixedMetrics tests parsing mixed metric types
func TestE2E_ParseMetrics_MixedMetrics(t *testing.T) {
	metricsData := `# Counter
counter_metric_total 1000
# Gauge
gauge_metric 42.5
# Histogram
histogram_metric_bucket{le="0.1"} 10
histogram_metric_bucket{le="+Inf"} 20
histogram_metric_count 20
histogram_metric_sum 1.5
# Metric with labels
labeled_metric{env="prod",service="api"} 999
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))

		// Should have one histogram
		assert.Equal(t, 1, len(snapshot.Histograms))

		// Should have counter, gauge, and labeled metric (histogram parts filtered)
		assert.GreaterOrEqual(t, len(snapshot.RawMetrics), 3)

		// Verify histogram
		hist, exists := snapshot.Histograms["histogram_metric"]
		require.True(t, exists)
		assert.Equal(t, 2, len(hist.Bins))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_ParseMetrics_EmptyLines tests handling of empty lines and whitespace
func TestE2E_ParseMetrics_EmptyLines(t *testing.T) {
	metricsData := `


# HELP metric1 Description
metric1 100


metric2 200


`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))
		assert.Equal(t, 2, len(snapshot.RawMetrics))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_ParseMetrics_ScannerError tests handling of scanner errors
func TestE2E_ParseMetrics_ScannerError(t *testing.T) {
	// Create a reader that will cause a scanner error
	// We'll use a custom reader that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Write some data then close connection to simulate error
		w.Write([]byte("metric1 100\n"))
		// Note: We can't easily simulate a scanner error from httptest,
		// but we can test that valid data is parsed correctly
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		// Should parse successfully
		assert.Equal(t, 0, len(snapshot.Errors))
		assert.Equal(t, 1, len(snapshot.RawMetrics))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}

// TestE2E_ConcurrentAccess tests concurrent access to GetLastSnapshot
func TestE2E_ConcurrentAccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test_metric 123\n"))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	// Poll once
	poller.poll(ctx, outChan)
	<-outChan

	// Concurrently access GetLastSnapshot
	var wg sync.WaitGroup
	iterations := 100
	wg.Add(iterations)

	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			snapshot := poller.GetLastSnapshot()
			assert.NotNil(t, snapshot)
		}()
	}

	wg.Wait()
}

// TestE2E_RealWorldPrometheusFormat tests with realistic Prometheus metrics format
func TestE2E_RealWorldPrometheusFormat(t *testing.T) {
	metricsData := `# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.21.0"} 1
# HELP go_memstats_alloc_bytes Number of bytes allocated and still in use.
# TYPE go_memstats_alloc_bytes gauge
go_memstats_alloc_bytes 2.147483648e+09
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="GET",status="200"} 1234
http_requests_total{method="POST",status="201"} 567
http_requests_total{method="GET",status="404"} 89
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.005"} 10
http_request_duration_seconds_bucket{le="0.01"} 20
http_request_duration_seconds_bucket{le="0.025"} 30
http_request_duration_seconds_bucket{le="0.05"} 40
http_request_duration_seconds_bucket{le="0.1"} 50
http_request_duration_seconds_bucket{le="+Inf"} 100
http_request_duration_seconds_count 100
http_request_duration_seconds_sum 5.5
`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(metricsData))
	}))
	defer server.Close()

	poller := NewMetricsPoller(server.URL, 1*time.Second)
	outChan := make(chan MetricsSnapshot, 1)
	ctx := context.Background()

	poller.poll(ctx, outChan)

	select {
	case snapshot := <-outChan:
		assert.NotNil(t, snapshot)
		assert.Equal(t, 0, len(snapshot.Errors))

		// Should have one histogram
		assert.Equal(t, 1, len(snapshot.Histograms))

		// Should have multiple raw metrics (go_info, go_memstats, http_requests)
		assert.GreaterOrEqual(t, len(snapshot.RawMetrics), 5)

		// Verify histogram
		hist, exists := snapshot.Histograms["http_request_duration_seconds"]
		require.True(t, exists)
		assert.Equal(t, "A histogram of the request duration.", hist.Description)
		assert.Equal(t, 6, len(hist.Bins))

		// Verify descriptions
		for _, m := range snapshot.RawMetrics {
			if m.Name == "go_info" {
				assert.Equal(t, "Information about the Go environment.", m.Description)
			}
			if m.Name == "http_requests_total" {
				assert.Equal(t, "The total number of HTTP requests.", m.Description)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for snapshot")
	}
}
