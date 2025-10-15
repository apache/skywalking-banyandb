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

package tracestreaming

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/cgroups"
)

// MemorySnapshot represents a point-in-time memory measurement.
type MemorySnapshot struct {
	Timestamp    time.Time
	HeapAlloc    uint64
	HeapSys      uint64
	HeapInuse    uint64
	HeapIdle     uint64
	RSS          uint64
	NumGC        uint32
	GCPauseNs    uint64
	NumGoroutine int
}

// MemoryMonitor tracks memory usage over time.
type MemoryMonitor struct {
	pprofURL     string
	profileDir   string
	snapshots    []MemorySnapshot
	pollInterval time.Duration
	mu           sync.RWMutex
}

// NewMemoryMonitor creates a new memory monitor.
func NewMemoryMonitor(pollInterval time.Duration, pprofURL string, profileDir string) *MemoryMonitor {
	return &MemoryMonitor{
		snapshots:    make([]MemorySnapshot, 0),
		pollInterval: pollInterval,
		pprofURL:     pprofURL,
		profileDir:   profileDir,
	}
}

// Start begins monitoring memory usage.
func (m *MemoryMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	// Create profile directory if it doesn't exist
	if m.profileDir != "" {
		_ = os.MkdirAll(m.profileDir, 0o755)
	}

	// Capture initial snapshot
	m.captureSnapshot()

	// Start periodic heap profiling (every 30 seconds)
	profileTicker := time.NewTicker(30 * time.Second)
	defer profileTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.captureSnapshot()
		case <-profileTicker.C:
			if m.pprofURL != "" && m.profileDir != "" {
				_ = m.captureHeapProfile()
			}
		}
	}
}

// captureSnapshot captures a memory snapshot.
func (m *MemoryMonitor) captureSnapshot() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Get memory limit from cgroups if available
	// We use HeapSys as a proxy for RSS in containerized environments
	rss := memStats.HeapSys
	if limit, err := cgroups.MemoryLimit(); err == nil && limit > 0 {
		// In containers, use sys memory as RSS approximation
		rss = memStats.Sys
	}

	snapshot := MemorySnapshot{
		Timestamp:    time.Now(),
		HeapAlloc:    memStats.HeapAlloc,
		HeapSys:      memStats.HeapSys,
		HeapInuse:    memStats.HeapInuse,
		HeapIdle:     memStats.HeapIdle,
		RSS:          rss,
		NumGC:        memStats.NumGC,
		GCPauseNs:    memStats.PauseNs[(memStats.NumGC+255)%256],
		NumGoroutine: runtime.NumGoroutine(),
	}

	m.mu.Lock()
	m.snapshots = append(m.snapshots, snapshot)
	m.mu.Unlock()
}

// captureHeapProfile captures a heap profile via pprof HTTP endpoint.
func (m *MemoryMonitor) captureHeapProfile() error {
	url := fmt.Sprintf("%s/debug/pprof/heap", m.pprofURL)
	resp, err := http.Get(url) // #nosec G107
	if err != nil {
		return fmt.Errorf("failed to get heap profile: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heap profile returned status %d", resp.StatusCode)
	}

	// Save profile to file
	filename := filepath.Join(m.profileDir, fmt.Sprintf("heap_%d.pprof", time.Now().Unix()))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create profile file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("failed to write profile: %w", err)
	}

	return nil
}

// CaptureCPUProfile captures a CPU profile for the specified duration.
func (m *MemoryMonitor) CaptureCPUProfile(duration time.Duration) error {
	if m.pprofURL == "" || m.profileDir == "" {
		return fmt.Errorf("pprof URL or profile directory not configured")
	}

	url := fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", m.pprofURL, int(duration.Seconds()))
	resp, err := http.Get(url) // #nosec G107
	if err != nil {
		return fmt.Errorf("failed to get CPU profile: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("CPU profile returned status %d", resp.StatusCode)
	}

	// Save profile to file
	filename := filepath.Join(m.profileDir, fmt.Sprintf("cpu_%d.pprof", time.Now().Unix()))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create profile file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("failed to write profile: %w", err)
	}

	return nil
}

// GetSnapshots returns all captured snapshots.
func (m *MemoryMonitor) GetSnapshots() []MemorySnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]MemorySnapshot, len(m.snapshots))
	copy(result, m.snapshots)
	return result
}

// GetPeakMemory returns the peak memory usage.
func (m *MemoryMonitor) GetPeakMemory() (heapAlloc uint64, rss uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, snapshot := range m.snapshots {
		if snapshot.HeapAlloc > heapAlloc {
			heapAlloc = snapshot.HeapAlloc
		}
		if snapshot.RSS > rss {
			rss = snapshot.RSS
		}
	}

	return heapAlloc, rss
}

// ExportToCSV exports memory snapshots to a CSV file.
func (m *MemoryMonitor) ExportToCSV(filename string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"Timestamp",
		"HeapAlloc(MB)",
		"HeapSys(MB)",
		"HeapInuse(MB)",
		"HeapIdle(MB)",
		"RSS(MB)",
		"NumGC",
		"GCPauseMs",
		"NumGoroutine",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data
	for _, snapshot := range m.snapshots {
		record := []string{
			snapshot.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.2f", float64(snapshot.HeapAlloc)/1024/1024),
			fmt.Sprintf("%.2f", float64(snapshot.HeapSys)/1024/1024),
			fmt.Sprintf("%.2f", float64(snapshot.HeapInuse)/1024/1024),
			fmt.Sprintf("%.2f", float64(snapshot.HeapIdle)/1024/1024),
			fmt.Sprintf("%.2f", float64(snapshot.RSS)/1024/1024),
			fmt.Sprintf("%d", snapshot.NumGC),
			fmt.Sprintf("%.3f", float64(snapshot.GCPauseNs)/1e6),
			fmt.Sprintf("%d", snapshot.NumGoroutine),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	return nil
}

// MemoryStats provides summary statistics.
type MemoryStats struct {
	PeakHeapAlloc uint64
	PeakRSS       uint64
	AvgHeapAlloc  uint64
	AvgRSS        uint64
	MaxGoroutines int
	TotalGC       uint32
}

// GetStats returns summary statistics.
func (m *MemoryMonitor) GetStats() MemoryStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.snapshots) == 0 {
		return MemoryStats{}
	}

	stats := MemoryStats{}
	var totalHeap uint64
	var totalRSS uint64

	for _, snapshot := range m.snapshots {
		if snapshot.HeapAlloc > stats.PeakHeapAlloc {
			stats.PeakHeapAlloc = snapshot.HeapAlloc
		}
		if snapshot.RSS > stats.PeakRSS {
			stats.PeakRSS = snapshot.RSS
		}
		if snapshot.NumGoroutine > stats.MaxGoroutines {
			stats.MaxGoroutines = snapshot.NumGoroutine
		}
		if snapshot.NumGC > stats.TotalGC {
			stats.TotalGC = snapshot.NumGC
		}
		totalHeap += snapshot.HeapAlloc
		totalRSS += snapshot.RSS
	}

	stats.AvgHeapAlloc = totalHeap / uint64(len(m.snapshots))
	stats.AvgRSS = totalRSS / uint64(len(m.snapshots))

	return stats
}
