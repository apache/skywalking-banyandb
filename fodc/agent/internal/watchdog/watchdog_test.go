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

package watchdog

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

const testMetricsCPUUsage = "cpu_usage 75.5"

// mockMetricsRecorder is a mock implementation of MetricsRecorder for testing.
type mockMetricsRecorder struct {
	metrics         [][]metrics.RawMetric
	updateErrors    []error
	updateCallCount int
	mu              sync.Mutex
}

// Update records metrics from a polling cycle.
func (m *mockMetricsRecorder) Update(rawMetrics []metrics.RawMetric) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateCallCount++
	m.metrics = append(m.metrics, rawMetrics)

	if len(m.updateErrors) > 0 {
		err := m.updateErrors[0]
		m.updateErrors = m.updateErrors[1:]
		return err
	}

	return nil
}

// GetMetrics returns all recorded metrics.
func (m *mockMetricsRecorder) GetMetrics() [][]metrics.RawMetric {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([][]metrics.RawMetric, len(m.metrics))
	copy(result, m.metrics)
	return result
}

// GetUpdateCallCount returns the number of times Update was called.
func (m *mockMetricsRecorder) GetUpdateCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.updateCallCount
}

// SetUpdateErrors sets errors to return on Update calls.
func (m *mockMetricsRecorder) SetUpdateErrors(errors []error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateErrors = errors
}

func TestNewWatchdogWithConfig(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	url := "http://localhost:2121/metrics"
	interval := 10 * time.Second

	wd := NewWatchdogWithConfig(recorder, url, interval)

	assert.NotNil(t, wd)
	assert.Equal(t, recorder, wd.recorder)
	assert.Equal(t, url, wd.url)
	assert.Equal(t, interval, wd.interval)
	assert.NotNil(t, wd.ctx)
	assert.NotNil(t, wd.cancel)
	assert.Equal(t, initialBackoff, wd.retryBackoff)
}

func TestWatchdog_Name(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:2121/metrics", 10*time.Second)

	assert.Equal(t, "watchdog", wd.Name())
}

func TestWatchdog_PreRun(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:2121/metrics", 10*time.Second)

	ctx := context.Background()
	err := wd.PreRun(ctx)

	require.NoError(t, err)
	assert.NotNil(t, wd.log)
	assert.NotNil(t, wd.client)
	assert.Equal(t, 5*time.Second, wd.client.Timeout)
}

func TestWatchdog_PollMetrics_Success(t *testing.T) {
	metricsText := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET"} 100
` + testMetricsCPUUsage

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	rawMetrics, err := wd.pollMetrics(ctx)

	require.NoError(t, err)
	assert.Len(t, rawMetrics, 2)
	assert.Equal(t, "http_requests_total", rawMetrics[0].Name)
	assert.Equal(t, 100.0, rawMetrics[0].Value)
	assert.Equal(t, "cpu_usage", rawMetrics[1].Name)
	assert.Equal(t, 75.5, rawMetrics[1].Value)
}

func TestWatchdog_PollMetrics_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	rawMetrics, err := wd.pollMetrics(ctx)

	assert.Error(t, err)
	assert.Nil(t, rawMetrics)
	assert.Contains(t, err.Error(), "failed after")
}

func TestWatchdog_PollMetrics_ConnectionFailure(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:99999/metrics", 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	rawMetrics, err := wd.pollMetrics(ctx)

	assert.Error(t, err)
	assert.Nil(t, rawMetrics)
	assert.Contains(t, err.Error(), "failed after")
}

func TestWatchdog_PollMetrics_InvalidMetricsFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("invalid metric format"))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	rawMetrics, err := wd.pollMetrics(ctx)

	// Invalid format causes parsing error after retries
	assert.Error(t, err)
	assert.Nil(t, rawMetrics)
	assert.Contains(t, err.Error(), "failed after")
}

func TestWatchdog_PollMetrics_RetryWithExponentialBackoff(t *testing.T) {
	var attemptCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt := atomic.AddInt32(&attemptCount, 1)
		if attempt < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(testMetricsCPUUsage))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	startTime := time.Now()
	rawMetrics, err := wd.pollMetrics(ctx)
	duration := time.Since(startTime)

	require.NoError(t, err)
	assert.Len(t, rawMetrics, 1)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attemptCount))

	// Verify exponential backoff was applied (should take at least initialBackoff * 2)
	minExpectedDuration := initialBackoff * 2
	assert.GreaterOrEqual(t, duration, minExpectedDuration, "Expected exponential backoff delay")
}

func TestWatchdog_PollMetrics_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(testMetricsCPUUsage))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	// Cancel context before polling
	cancel()

	rawMetrics, err := wd.pollMetrics(ctx)

	assert.Error(t, err)
	assert.Nil(t, rawMetrics)
	assert.Equal(t, context.Canceled, err)
}

func TestWatchdog_PollAndForward_Success(t *testing.T) {
	metricsText := `# HELP http_requests_total Total number of HTTP requests
http_requests_total{method="GET"} 100`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	require.NoError(t, err)
	assert.Equal(t, 1, recorder.GetUpdateCallCount())
	allMetrics := recorder.GetMetrics()
	assert.Len(t, allMetrics, 1)
	assert.Len(t, allMetrics[0], 1)
	assert.Equal(t, "http_requests_total", allMetrics[0][0].Name)
}

func TestWatchdog_PollAndForward_NoRecorder(t *testing.T) {
	metricsText := testMetricsCPUUsage

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	wd := NewWatchdogWithConfig(nil, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	require.NoError(t, err)
}

func TestWatchdog_PollAndForward_RecorderError(t *testing.T) {
	metricsText := testMetricsCPUUsage

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	recorder.SetUpdateErrors([]error{fmt.Errorf("recorder error")})

	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to forward metrics to recorder")
}

func TestWatchdog_PollAndForward_PollError(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:99999/metrics", 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to poll metrics")
}

func TestWatchdog_ExponentialBackoff(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:2121/metrics", 10*time.Second)

	// Test exponential backoff calculation
	backoff1 := wd.exponentialBackoff(initialBackoff)
	assert.Equal(t, initialBackoff*2, backoff1)

	backoff2 := wd.exponentialBackoff(backoff1)
	assert.Equal(t, initialBackoff*4, backoff2)

	// Test max backoff limit
	largeBackoff := maxBackoff
	backoff3 := wd.exponentialBackoff(largeBackoff)
	assert.Equal(t, maxBackoff, backoff3)

	// Test exceeding max backoff
	exceedBackoff := maxBackoff * 2
	backoff4 := wd.exponentialBackoff(exceedBackoff)
	assert.Equal(t, maxBackoff, backoff4)
}

func TestWatchdog_Serve_StartStop(t *testing.T) {
	metricsText := testMetricsCPUUsage

	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 100*time.Millisecond)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	stopCh := wd.Serve()

	// Wait for at least one poll
	time.Sleep(150 * time.Millisecond)

	// Stop the watchdog
	wd.GracefulStop()

	// Wait for stop channel to close
	select {
	case <-stopCh:
		// Channel closed, good
	case <-time.After(1 * time.Second):
		t.Fatal("Stop channel not closed within timeout")
	}

	// Verify at least one request was made
	assert.GreaterOrEqual(t, atomic.LoadInt32(&requestCount), int32(1))
	assert.GreaterOrEqual(t, recorder.GetUpdateCallCount(), 1)
}

func TestWatchdog_Serve_PollingIntervalAccuracy(t *testing.T) {
	metricsText := testMetricsCPUUsage

	var requestTimes []time.Time
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestTimes = append(requestTimes, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	interval := 200 * time.Millisecond
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, interval)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	stopCh := wd.Serve()

	// Wait for multiple polling cycles
	time.Sleep(650 * time.Millisecond)

	wd.GracefulStop()

	select {
	case <-stopCh:
	case <-time.After(1 * time.Second):
		t.Fatal("Stop channel not closed within timeout")
	}

	mu.Lock()
	requestCount := len(requestTimes)
	mu.Unlock()

	// Should have at least 3 requests (initial + 2 intervals)
	assert.GreaterOrEqual(t, requestCount, 3)

	// Verify intervals are approximately correct (allow 50ms tolerance)
	mu.Lock()
	if len(requestTimes) >= 2 {
		for i := 1; i < len(requestTimes); i++ {
			actualInterval := requestTimes[i].Sub(requestTimes[i-1])
			// Allow 50ms tolerance for timing variations
			assert.InDelta(t, interval.Milliseconds(), actualInterval.Milliseconds(), 50,
				"Polling interval should be approximately %v", interval)
		}
	}
	mu.Unlock()
}

func TestWatchdog_Serve_AlreadyRunning(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:2121/metrics", 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	stopCh1 := wd.Serve()
	assert.NotNil(t, stopCh1)

	// Try to start again - should return immediately closed channel
	stopCh2 := wd.Serve()
	assert.NotNil(t, stopCh2)

	// Verify second channel is already closed
	select {
	case <-stopCh2:
		// Channel already closed, good
	default:
		t.Fatal("Second stop channel should be immediately closed")
	}

	wd.GracefulStop()

	select {
	case <-stopCh1:
	case <-time.After(1 * time.Second):
		t.Fatal("First stop channel not closed within timeout")
	}
}

func TestWatchdog_GracefulStop_NotRunning(t *testing.T) {
	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, "http://localhost:2121/metrics", 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	// Stop without starting should not panic
	wd.GracefulStop()

	// Should be able to stop multiple times without issue
	wd.GracefulStop()
}

func TestWatchdog_HTTPClientConnectionReuse(t *testing.T) {
	metricsText := testMetricsCPUUsage

	var connectionHeaders []string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connectionHeaders = append(connectionHeaders, r.Header.Get("Connection"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 50*time.Millisecond)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	stopCh := wd.Serve()

	// Wait for multiple polls
	time.Sleep(200 * time.Millisecond)

	wd.GracefulStop()

	select {
	case <-stopCh:
	case <-time.After(1 * time.Second):
		t.Fatal("Stop channel not closed within timeout")
	}
	// Verify multiple requests were made (connection reuse is handled by http.Transport)
	mu.Lock()
	headerCount := len(connectionHeaders)
	mu.Unlock()
	assert.GreaterOrEqual(t, headerCount, 2)
}

func TestWatchdog_MetricsForwardingToFlightRecorder(t *testing.T) {
	metricsText := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET"} 100
http_requests_total{method="POST"} 200
` + testMetricsCPUUsage

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	fr := flightrecorder.NewFlightRecorder(1000000)
	wd := NewWatchdogWithConfig(fr, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	require.NoError(t, err)

	// Verify metrics were forwarded to FlightRecorder
	datasources := fr.GetDatasources()
	assert.Len(t, datasources, 1)
	assert.NotNil(t, datasources[0])
}

func TestWatchdog_RetryBackoffResetOnSuccess(t *testing.T) {
	metricsText := testMetricsCPUUsage

	var attemptCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt := atomic.AddInt32(&attemptCount, 1)
		if attempt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsText))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	// First poll will retry and succeed
	_, err := wd.pollMetrics(ctx)
	require.NoError(t, err)

	// Verify backoff was reset to initial value
	wd.mu.RLock()
	currentBackoff := wd.retryBackoff
	wd.mu.RUnlock()

	assert.Equal(t, initialBackoff, currentBackoff, "Backoff should be reset to initial value after success")
}

func TestWatchdog_EmptyMetricsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(""))
	}))
	defer server.Close()

	recorder := &mockMetricsRecorder{}
	wd := NewWatchdogWithConfig(recorder, server.URL, 10*time.Second)

	ctx := context.Background()
	preRunErr := wd.PreRun(ctx)
	require.NoError(t, preRunErr)

	err := wd.pollAndForward()

	require.NoError(t, err)
	assert.Equal(t, 1, recorder.GetUpdateCallCount())
	allMetrics := recorder.GetMetrics()
	assert.Len(t, allMetrics, 1)
	assert.Len(t, allMetrics[0], 0) // Empty metrics array
}
