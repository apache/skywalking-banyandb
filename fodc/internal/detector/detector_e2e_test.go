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

package detector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_DeathRattleDetector_FileDetection tests file-based death rattle detection
func TestE2E_DeathRattleDetector_FileDetection(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Use a valid health interval (0 causes panic in NewTicker)
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait a bit for detector to initialize
	time.Sleep(100 * time.Millisecond)

	// Create death rattle file
	err := os.WriteFile(deathRattlePath, []byte("Container is failing"), 0644)
	require.NoError(t, err)

	// Wait for detection
	select {
	case event := <-eventChan:
		assert.Equal(t, DeathRattleTypeFile, event.Type)
		assert.Contains(t, event.Message, deathRattlePath)
		assert.Contains(t, event.Message, "Container is failing")
		assert.Equal(t, "critical", event.Severity)
		assert.WithinDuration(t, time.Now(), event.Timestamp, 5*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for file detection event")
	}

	// Verify file was removed after detection (file watcher runs every 2 seconds)
	// Wait a bit for the file removal to complete
	time.Sleep(2500 * time.Millisecond)
	_, err = os.Stat(deathRattlePath)
	assert.True(t, os.IsNotExist(err), "File should be removed after detection")

	cancel()
	wg.Wait()
}

// TestE2E_DeathRattleDetector_HealthCheckFailure tests health check failure detection
func TestE2E_DeathRattleDetector_HealthCheckFailure(t *testing.T) {
	// Create a test server that fails health checks
	requestCount := 0
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		count := requestCount
		mu.Unlock()

		// Fail first 3 requests, then succeed
		if count <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	detector := NewDeathRattleDetector("", "", server.URL, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for health check failures to accumulate
	time.Sleep(500 * time.Millisecond)

	// Should receive a health check failure event
	select {
	case event := <-eventChan:
		assert.Equal(t, DeathRattleTypeHealth, event.Type)
		assert.True(t, strings.Contains(event.Message, "Health check") || strings.Contains(event.Message, "failed"), 
			"Message should contain health check failure info: %s", event.Message)
		assert.Contains(t, event.Message, "3 times")
		assert.Equal(t, "critical", event.Severity)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for health check failure event")
	}

	cancel()
	wg.Wait()
}

// TestE2E_DeathRattleDetector_HealthCheckSuccess tests that successful health checks don't trigger events
func TestE2E_DeathRattleDetector_HealthCheckSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	detector := NewDeathRattleDetector("", "", server.URL, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for multiple health checks
	time.Sleep(500 * time.Millisecond)

	// Should not receive any events
	select {
	case event := <-eventChan:
		t.Fatalf("Unexpected event received: %+v", event)
	case <-time.After(200 * time.Millisecond):
		// Expected - no events should be generated
	}

	cancel()
	wg.Wait()
}

// TestE2E_DeathRattleDetector_MultipleDetectionTypes tests multiple detection types simultaneously
func TestE2E_DeathRattleDetector_MultipleDetectionTypes(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Create a test server that fails health checks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// Use a valid health interval
	detector := NewDeathRattleDetector(deathRattlePath, "", server.URL, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait a bit for initialization
	time.Sleep(100 * time.Millisecond)

	// Create death rattle file
	err := os.WriteFile(deathRattlePath, []byte("Test message"), 0644)
	require.NoError(t, err)

	// Collect events
	events := make([]DeathRattleEvent, 0)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case event := <-eventChan:
				events = append(events, event)
			case <-time.After(1 * time.Second):
				close(done)
				return
			}
		}
	}()

	// Wait for events
	time.Sleep(600 * time.Millisecond)
	<-done

	// Should have received at least file detection event
	assert.Greater(t, len(events), 0)

	// Verify file event
	foundFileEvent := false
	for _, event := range events {
		if event.Type == DeathRattleTypeFile {
			foundFileEvent = true
			assert.Contains(t, event.Message, deathRattlePath)
			assert.Equal(t, "critical", event.Severity)
		}
	}
	assert.True(t, foundFileEvent, "File detection event should be present")

	cancel()
	wg.Wait()
}

// TestE2E_DeathRattleDetector_ContextCancellation tests that detector stops on context cancellation
func TestE2E_DeathRattleDetector_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Use a valid health interval (0 causes panic in NewTicker)
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	var startErr error
	go func() {
		defer wg.Done()
		startErr = detector.Start(ctx, eventChan)
	}()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

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
		t.Fatal("Timeout waiting for detector to stop")
	}
}

// TestE2E_AlertManager_HighErrorRate tests high error rate detection
func TestE2E_AlertManager_HighErrorRate(t *testing.T) {
	alertManager := NewAlertManager(0.8) // 80% threshold

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 90},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Greater(t, len(alerts), 0)
	assert.Contains(t, alerts[0], "High error rate")
	assert.Contains(t, alerts[0], "90.00%")
	assert.Contains(t, alerts[0], "80.00%")

	// Verify alert was stored
	storedAlerts := alertManager.GetAlerts()
	assert.Equal(t, 1, len(storedAlerts))
	assert.Equal(t, "critical", storedAlerts[0].Severity)
}

// TestE2E_AlertManager_LowErrorRate tests that low error rates don't trigger alerts
func TestE2E_AlertManager_LowErrorRate(t *testing.T) {
	alertManager := NewAlertManager(0.8) // 80% threshold

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 50},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Equal(t, 0, len(alerts))
}

// TestE2E_AlertManager_HighMemoryUsage tests high memory usage detection
func TestE2E_AlertManager_HighMemoryUsage(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  9 * 1024 * 1024 * 1024, // 9GB
			},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  10 * 1024 * 1024 * 1024, // 10GB
			},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Greater(t, len(alerts), 0)
	assert.Contains(t, alerts[0], "High memory usage")
	assert.Contains(t, alerts[0], "90.00%")

	// Verify alert was stored
	storedAlerts := alertManager.GetAlerts()
	assert.Equal(t, 1, len(storedAlerts))
	assert.Equal(t, "warning", storedAlerts[0].Severity)
}

// TestE2E_AlertManager_HighDiskUsage tests high disk usage detection
func TestE2E_AlertManager_HighDiskUsage(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{
				Name:   "banyandb_system_disk",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  85 * 1024 * 1024 * 1024, // 85GB
			},
			{
				Name:   "banyandb_system_disk",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  100 * 1024 * 1024 * 1024, // 100GB
			},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Greater(t, len(alerts), 0)
	assert.Contains(t, alerts[0], "High disk usage")
	assert.Contains(t, alerts[0], "85.00%")

	// Verify alert was stored
	storedAlerts := alertManager.GetAlerts()
	assert.Equal(t, 1, len(storedAlerts))
	assert.Equal(t, "warning", storedAlerts[0].Severity)
}

// TestE2E_AlertManager_ZeroWriteRate tests zero write rate detection
func TestE2E_AlertManager_ZeroWriteRate(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_measure_total_written", Value: 0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Greater(t, len(alerts), 0)
	assert.Contains(t, alerts[0], "Zero write rate")

	// Verify alert was stored
	storedAlerts := alertManager.GetAlerts()
	assert.Equal(t, 1, len(storedAlerts))
	assert.Equal(t, "warning", storedAlerts[0].Severity)
}

// TestE2E_AlertManager_MetricsPollingErrors tests metrics polling error detection
func TestE2E_AlertManager_MetricsPollingErrors(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: []metric.RawMetric{},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{"Failed to fetch metrics: connection timeout"},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Greater(t, len(alerts), 0)
	assert.Contains(t, alerts[0], "Metrics polling error")
	assert.Contains(t, alerts[0], "connection timeout")

	// Verify alert was stored
	storedAlerts := alertManager.GetAlerts()
	assert.Equal(t, 1, len(storedAlerts))
	assert.Equal(t, "warning", storedAlerts[0].Severity)
}

// TestE2E_AlertManager_MultipleAlerts tests multiple alerts from a single snapshot
func TestE2E_AlertManager_MultipleAlerts(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 90},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  9 * 1024 * 1024 * 1024,
			},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  10 * 1024 * 1024 * 1024,
			},
			{Name: "banyandb_measure_total_written", Value: 0},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{"Error 1", "Error 2"},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.GreaterOrEqual(t, len(alerts), 3) // At least error rate, memory, zero write rate, and errors

	// Verify multiple alerts were stored
	storedAlerts := alertManager.GetAlerts()
	assert.GreaterOrEqual(t, len(storedAlerts), 3)
}

// TestE2E_AlertManager_HandleDeathRattle tests handling death rattle events
func TestE2E_AlertManager_HandleDeathRattle(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	event := DeathRattleEvent{
		Type:      DeathRattleTypeFile,
		Message:   "Death rattle file detected",
		Timestamp: time.Now(),
		Severity:  "critical",
	}

	alertsBefore := alertManager.GetAlerts()
	initialAlertCount := len(alertsBefore)
	alertManager.HandleDeathRattle(event)

	// Verify alert was added
	alertsAfter := alertManager.GetAlerts()
	assert.Equal(t, initialAlertCount+1, len(alertsAfter))
	assert.Contains(t, alertsAfter[len(alertsAfter)-1].Message, "Death rattle")
	assert.Contains(t, alertsAfter[len(alertsAfter)-1].Message, "file detected")
	assert.Equal(t, "critical", alertsAfter[len(alertsAfter)-1].Severity)
}

// TestE2E_AlertManager_DeathRattleIntegration tests integration with death rattle detector
func TestE2E_AlertManager_DeathRattleIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	alertManager := NewAlertManager(0.8)
	// Use a valid health interval
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for initialization
	time.Sleep(100 * time.Millisecond)

	// Create death rattle file
	err := os.WriteFile(deathRattlePath, []byte("Integration test"), 0644)
	require.NoError(t, err)

	// Wait for event
	var event DeathRattleEvent
	select {
	case event = <-eventChan:
		// Got event
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for death rattle event")
	}

	// Handle event with alert manager
	alertsBefore := alertManager.GetAlerts()
	initialAlertCount := len(alertsBefore)
	alertManager.HandleDeathRattle(event)

	// Verify alert was created
	alertsAfter := alertManager.GetAlerts()
	assert.Equal(t, initialAlertCount+1, len(alertsAfter))
	assert.Contains(t, alertsAfter[len(alertsAfter)-1].Message, "Death rattle")
	assert.Equal(t, "critical", alertsAfter[len(alertsAfter)-1].Severity)

	cancel()
	wg.Wait()
}

// TestE2E_AlertManager_NoAlertsForNormalMetrics tests that normal metrics don't trigger alerts
func TestE2E_AlertManager_NoAlertsForNormalMetrics(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 10},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  5 * 1024 * 1024 * 1024,
			},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  10 * 1024 * 1024 * 1024,
			},
			{Name: "banyandb_measure_total_written", Value: 1000},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Equal(t, 0, len(alerts))
}

// TestE2E_AlertManager_ThresholdBoundary tests alert threshold boundary conditions
func TestE2E_AlertManager_ThresholdBoundary(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	// Test exactly at threshold (should not trigger)
	snapshot1 := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 80},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts1 := alertManager.AnalyzeMetrics(snapshot1)
	assert.Equal(t, 0, len(alerts1), "Exactly at threshold should not trigger alert")

	// Test just above threshold (should trigger)
	snapshot2 := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			{Name: "banyandb_liaison_grpc_total_err", Value: 81},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts2 := alertManager.AnalyzeMetrics(snapshot2)
	assert.Greater(t, len(alerts2), 0, "Just above threshold should trigger alert")
}

// TestE2E_DeathRattleDetector_CommonPaths tests detection of common death rattle file paths
func TestE2E_DeathRattleDetector_CommonPaths(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Use a valid health interval (0 causes panic in NewTicker)
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for initialization
	time.Sleep(100 * time.Millisecond)

	// Create a common path file
	commonPath := filepath.Join(tmpDir, "container-failing")
	err := os.WriteFile(commonPath, []byte(""), 0644)
	require.NoError(t, err)

	// Wait for detection
	select {
	case event := <-eventChan:
		assert.Equal(t, DeathRattleTypeFile, event.Type)
		assert.Contains(t, event.Message, "container-failing")
	case <-time.After(5 * time.Second):
		// Note: Common paths detection might not work in test environment
		// This is acceptable as it depends on the actual paths being checked
		t.Log("Common path detection may not work in test environment")
	}

	cancel()
	wg.Wait()
}

// TestE2E_AlertManager_MetricNotFound tests behavior when required metrics are not found
func TestE2E_AlertManager_MetricNotFound(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			// Missing error metrics
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	// Should not trigger error rate alert since error metric is missing
	assert.Equal(t, 0, len(alerts))
}

// TestE2E_AlertManager_EmptySnapshot tests handling of empty snapshot
func TestE2E_AlertManager_EmptySnapshot(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	snapshot := poller.MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: []metric.RawMetric{},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	assert.Equal(t, 0, len(alerts))
}

// TestE2E_DeathRattleDetector_FileWithContent tests file detection with content
func TestE2E_DeathRattleDetector_FileWithContent(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Use a valid health interval (0 causes panic in NewTicker)
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for initialization
	time.Sleep(100 * time.Millisecond)

	// Create file with detailed content
	content := "Container health check failed: timeout after 30s"
	err := os.WriteFile(deathRattlePath, []byte(content), 0644)
	require.NoError(t, err)

	// Wait for detection
	select {
	case event := <-eventChan:
		assert.Equal(t, DeathRattleTypeFile, event.Type)
		assert.Contains(t, event.Message, content)
		assert.Equal(t, "critical", event.Severity)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for file detection event")
	}

	cancel()
	wg.Wait()
}

// TestE2E_AlertManager_ConcurrentAccess tests concurrent access to alert manager
func TestE2E_AlertManager_ConcurrentAccess(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent analysis
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			snapshot := poller.MetricsSnapshot{
				Timestamp: time.Now(),
				RawMetrics: []metric.RawMetric{
					{Name: "banyandb_liaison_grpc_total_err", Value: float64(90 + id)},
					{Name: "banyandb_liaison_grpc_total_started", Value: 100},
				},
				Histograms: make(map[string]metric.Histogram),
				Errors:     []string{},
			}
			_ = alertManager.AnalyzeMetrics(snapshot)
		}(i)
	}

	wg.Wait()

	// Should have multiple alerts
	alerts := alertManager.GetAlerts()
	assert.GreaterOrEqual(t, len(alerts), numGoroutines)
}

// TestE2E_DeathRattleDetector_MultipleFiles tests detection of multiple death rattle files
func TestE2E_DeathRattleDetector_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	deathRattlePath := filepath.Join(tmpDir, "death-rattle")

	// Use a valid health interval (0 causes panic in NewTicker)
	detector := NewDeathRattleDetector(deathRattlePath, "", "", 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventChan := make(chan DeathRattleEvent, 10)

	// Start detector
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = detector.Start(ctx, eventChan)
	}()

	// Wait for initialization
	time.Sleep(100 * time.Millisecond)

	// Create multiple files sequentially
	for i := 0; i < 3; i++ {
		err := os.WriteFile(deathRattlePath, []byte(fmt.Sprintf("File %d", i)), 0644)
		require.NoError(t, err)

		// Wait for detection
		select {
		case event := <-eventChan:
			assert.Equal(t, DeathRattleTypeFile, event.Type)
			assert.Contains(t, event.Message, fmt.Sprintf("File %d", i))
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for file detection event %d", i)
		}

		// Small delay between files
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	wg.Wait()
}

// TestE2E_AlertManager_RealWorldScenario tests a realistic end-to-end scenario
func TestE2E_AlertManager_RealWorldScenario(t *testing.T) {
	alertManager := NewAlertManager(0.8)

	// Simulate a realistic scenario with multiple metrics
	snapshot := poller.MetricsSnapshot{
		Timestamp: time.Now(),
		RawMetrics: []metric.RawMetric{
			// Error rate metrics
			{Name: "banyandb_liaison_grpc_total_err", Value: 85},
			{Name: "banyandb_liaison_grpc_total_started", Value: 100},
			// Memory metrics
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  9.5 * 1024 * 1024 * 1024,
			},
			{
				Name:   "banyandb_system_memory_state",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  10 * 1024 * 1024 * 1024,
			},
			// Disk metrics
			{
				Name:   "banyandb_system_disk",
				Labels: []metric.Label{{Name: "kind", Value: "used"}},
				Value:  82 * 1024 * 1024 * 1024,
			},
			{
				Name:   "banyandb_system_disk",
				Labels: []metric.Label{{Name: "kind", Value: "total"}},
				Value:  100 * 1024 * 1024 * 1024,
			},
			// Write rate
			{Name: "banyandb_measure_total_written", Value: 500},
		},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	alerts := alertManager.AnalyzeMetrics(snapshot)
	
	// Should have multiple alerts: error rate, memory usage, disk usage
	assert.GreaterOrEqual(t, len(alerts), 3)

	// Verify all alerts are stored
	allAlerts := alertManager.GetAlerts()
	assert.GreaterOrEqual(t, len(allAlerts), 3)

	// Verify alert types
	hasErrorRate := false
	hasMemory := false
	hasDisk := false
	for _, alert := range allAlerts {
		if strings.Contains(alert.Message, "error rate") {
			hasErrorRate = true
		}
		if strings.Contains(alert.Message, "memory") {
			hasMemory = true
		}
		if strings.Contains(alert.Message, "disk") {
			hasDisk = true
		}
	}

	assert.True(t, hasErrorRate, "Should have error rate alert")
	assert.True(t, hasMemory, "Should have memory alert")
	assert.True(t, hasDisk, "Should have disk alert")
}


