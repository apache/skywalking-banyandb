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

//go:build linux

package collector

import (
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"

	loader "github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
)

// IOMonitorModule collects I/O, cache, and memory statistics from eBPF.
type IOMonitorModule struct {
	logger     *zap.Logger
	loader     *loader.Loader
	objs       *generated.IomonitorObjects
	name       string
	cgroupPath string
}

// NewIOMonitorModule creates a new I/O monitoring module.
func NewIOMonitorModule(logger *zap.Logger, ebpfCfg EBPFConfig) (*IOMonitorModule, error) {
	ebpfLoader, err := loader.NewLoader()
	if err != nil {
		return nil, fmt.Errorf("failed to create eBPF loader: %w", err)
	}

	return &IOMonitorModule{
		name:       "iomonitor",
		logger:     logger,
		loader:     ebpfLoader,
		cgroupPath: ebpfCfg.CgroupPath,
	}, nil
}

// Name returns the module name.
func (m *IOMonitorModule) Name() string {
	return m.name
}

// Start loads and attaches the eBPF programs.
func (m *IOMonitorModule) Start() error {
	m.logger.Info("Starting I/O monitor module")

	// Configure cgroup filtering if requested.
	if m.cgroupPath != "" {
		m.logger.Info("Enabling cgroup filter for eBPF programs",
			zap.String("cgroup_path", m.cgroupPath))
		m.loader.SetCgroupPath(m.cgroupPath)
	}

	// Load eBPF programs
	if err := m.loader.LoadPrograms(); err != nil {
		return fmt.Errorf("failed to load eBPF programs: %w", err)
	}

	if err := m.loader.ConfigureFilters("banyand"); err != nil {
		m.logger.Warn("Failed to configure filters", zap.Error(err))
	}

	// Attach to tracepoints/kprobes
	if err := m.loader.AttachTracepoints(); err != nil {
		return fmt.Errorf("failed to attach tracepoints: %w", err)
	}

	// Get loaded objects
	m.objs = m.loader.GetObjects()
	if m.objs == nil {
		return fmt.Errorf("failed to get eBPF objects")
	}

	go m.refreshAllowedPIDsLoop()

	m.logger.Info("I/O monitor module started successfully")
	return nil
}

// Stop cleans up the module.
func (m *IOMonitorModule) Stop() error {
	m.logger.Info("Stopping I/O monitor module")

	if m.loader != nil {
		if err := m.loader.Close(); err != nil {
			m.logger.Error("Failed to close eBPF loader", zap.Error(err))
		}
	}

	return nil
}

// Collect gathers all metrics from eBPF maps.
func (m *IOMonitorModule) Collect() (*metrics.MetricSet, error) {
	if m.objs == nil {
		return nil, fmt.Errorf("eBPF objects not initialized")
	}

	ms := metrics.NewMetricSet()

	// Collect metrics (cumulative)
	m.collectMetrics(ms)

	return ms, nil
}

// collectMetrics collects metrics from all maps without clearing them.
func (m *IOMonitorModule) collectMetrics(ms *metrics.MetricSet) {
	if err := m.collectFadviseStats(ms); err != nil {
		m.logger.Debug("Failed to collect fadvise stats", zap.Error(err))
	}

	if err := m.collectCacheStats(ms); err != nil {
		m.logger.Debug("Failed to collect cache stats", zap.Error(err))
	}

	if err := m.collectMemoryStats(ms); err != nil {
		m.logger.Debug("Failed to collect memory stats", zap.Error(err))
	}

	if err := m.collectReadLatencyStats(ms); err != nil {
		m.logger.Debug("Failed to collect read latency stats", zap.Error(err))
	}
}


// Standard collection methods.
func (m *IOMonitorModule) collectFadviseStats(ms *metrics.MetricSet) error {
	// Same as before but without deletion
	var pid uint32
	var stats generated.IomonitorFadviseStatsT
	iter := m.objs.FadviseStatsMap.Iterate()

	var totalCalls uint64
	var dontneed uint64

	for iter.Next(&pid, &stats) {
		totalCalls += stats.TotalCalls
		dontneed += stats.AdviceDontneed
	}

	// Add metrics without clearing
	ms.AddCounter("ebpf_fadvise_calls_total", float64(totalCalls), nil)
	ms.AddCounter("ebpf_fadvise_dontneed_total", float64(dontneed), nil)

	return iter.Err()
}

func (m *IOMonitorModule) collectCacheStats(ms *metrics.MetricSet) error {
	var pid uint32
	var stats generated.IomonitorCacheStatsT
	iter := m.objs.CacheStatsMap.Iterate()

	var lookups uint64
	var adds uint64
	var deletes uint64

	for iter.Next(&pid, &stats) {
		lookups += stats.Lookups
		adds += stats.Adds
		deletes += stats.Deletes
	}

	ms.AddCounter("ebpf_cache_lookups_total", float64(lookups), nil)
	ms.AddCounter("ebpf_cache_fills_total", float64(adds), nil)
	ms.AddCounter("ebpf_cache_deletes_total", float64(deletes), nil)

	return iter.Err()
}

func (m *IOMonitorModule) collectMemoryStats(ms *metrics.MetricSet) error {
	// LRU shrink stats
	var key uint32
	var shrinkCounters generated.IomonitorShrinkCountersT

	if err := m.objs.ShrinkStatsMap.Lookup(key, &shrinkCounters); err == nil {
		ms.AddCounter("ebpf_memory_lru_pages_scanned_total", float64(shrinkCounters.NrScannedTotal), nil)
		ms.AddCounter("ebpf_memory_lru_pages_reclaimed_total", float64(shrinkCounters.NrReclaimedTotal), nil)
		ms.AddCounter("ebpf_memory_lru_shrink_events_total", float64(shrinkCounters.EventsTotal), nil)
	}

	// Direct reclaim stats
	var reclaimKey uint32
	var reclaimCounters generated.IomonitorReclaimCountersT
	if err := m.objs.ReclaimCountersMap.Lookup(reclaimKey, &reclaimCounters); err == nil {
		ms.AddCounter("ebpf_memory_direct_reclaim_begin_total", float64(reclaimCounters.DirectReclaimBeginTotal), nil)
	}

	return nil
}

func (m *IOMonitorModule) collectReadLatencyStats(ms *metrics.MetricSet) error {
	var pid uint32
	var stats generated.IomonitorReadLatencyStatsT
	iter := m.objs.ReadLatencyStatsMap.Iterate()

	var aggBuckets [32]uint64
	var totalSumNs uint64
	var totalCount uint64
	var totalBytes uint64

	for iter.Next(&pid, &stats) {
		totalCount += stats.Count
		totalSumNs += stats.SumLatencyNs
		totalBytes += stats.ReadBytesTotal
		for i, v := range stats.Buckets {
			aggBuckets[i] += v
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

	// Convert to cumulative map for Prometheus
	promBuckets := make(map[float64]uint64)
	var cumulative uint64

	for i := 0; i < 32; i++ {
		count := aggBuckets[i]
		cumulative += count

		// Upper bound in seconds
		// Bucket i upper bound: 2^(i+1) microseconds
		upperBoundUs := math.Pow(2, float64(i+1))
		upperBoundSec := upperBoundUs / 1e6

		promBuckets[upperBoundSec] = cumulative
	}
	// Add +Inf bucket
	promBuckets[math.Inf(1)] = cumulative

	ms.AddHistogram("ebpf_read_latency_seconds", promBuckets, float64(totalSumNs)/1e9, totalCount, nil)
	ms.AddCounter("ebpf_read_bytes_total", float64(totalBytes), nil)

	return nil
}

func (m *IOMonitorModule) refreshAllowedPIDsLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := m.loader.RefreshAllowedPIDs("banyand"); err != nil {
			m.logger.Debug("Refresh allowed pids failed", zap.Error(err))
		}
	}
}
