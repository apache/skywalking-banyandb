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

package iomonitor

import (
	"fmt"
	"math"

	"go.uber.org/zap"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/metrics"
)

type module struct {
	logger     *zap.Logger
	loader     *ebpf.EnhancedLoader
	objs       *generated.IomonitorObjects
	name       string
	cgroupPath string
}

func newModule(logger *zap.Logger, ebpfCfg EBPFConfig) (*module, error) {
	ebpfLoader, err := ebpf.NewEnhancedLoader(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create eBPF loader: %w", err)
	}

	return &module{
		name:       "iomonitor",
		logger:     logger,
		loader:     ebpfLoader,
		cgroupPath: ebpfCfg.CgroupPath,
	}, nil
}

// Name returns the module name.
func (m *module) Name() string {
	return m.name
}

// Start loads and attaches the eBPF programs.
func (m *module) Start() error {
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

	// Attach to tracepoints/kprobes
	if err := m.loader.AttachPrograms(); err != nil {
		return fmt.Errorf("failed to attach programs: %w", err)
	}

	// Get loaded objects
	m.objs = m.loader.GetObjects()
	if m.objs == nil {
		return fmt.Errorf("failed to get eBPF objects")
	}

	m.logger.Info("I/O monitor module started successfully")
	return nil
}

// Stop cleans up the module.
func (m *module) Stop() error {
	m.logger.Info("Stopping I/O monitor module")

	if m.loader != nil {
		if err := m.loader.Close(); err != nil {
			m.logger.Error("Failed to close eBPF loader", zap.Error(err))
		}
	}

	return nil
}

// Degraded returns whether the module is running in degraded mode.
func (m *module) Degraded() bool {
	if m.loader == nil {
		return false
	}
	return m.loader.Degraded()
}

// Collect gathers all metrics from eBPF maps.
func (m *module) Collect() (*metrics.MetricSet, error) {
	if m.objs == nil {
		return nil, fmt.Errorf("eBPF objects not initialized")
	}

	ms := metrics.NewMetricSet()

	// Collect metrics (cumulative)
	m.collectMetrics(ms)
	m.addDegradedMetric(ms)

	return ms, nil
}

func (m *module) addDegradedMetric(ms *metrics.MetricSet) {
	degraded := 0.0
	if m.loader != nil && m.loader.Degraded() {
		degraded = 1.0
	}
	ms.AddGauge("ktm_degraded", degraded, nil)
}

// collectMetrics collects metrics from all maps without clearing them.
func (m *module) collectMetrics(ms *metrics.MetricSet) {
	if err := m.collectFadviseStats(ms); err != nil {
		m.logger.Debug("Failed to collect fadvise stats", zap.Error(err))
	}

	if err := m.collectCacheStats(ms); err != nil {
		m.logger.Debug("Failed to collect cache stats", zap.Error(err))
	}

	m.collectMemoryStats(ms)

	if err := m.collectReadLatencyStats(ms); err != nil {
		m.logger.Debug("Failed to collect read latency stats", zap.Error(err))
	}

	if err := m.collectPreadLatencyStats(ms); err != nil {
		m.logger.Debug("Failed to collect pread latency stats", zap.Error(err))
	}
}

func (m *module) collectFadviseStats(ms *metrics.MetricSet) error {
	var pid uint32
	var perCPUStats []generated.IomonitorFadviseStatsT
	iter := m.objs.FadviseStatsMap.Iterate()

	var totalCalls uint64
	var dontneed uint64

	for iter.Next(&pid, &perCPUStats) {
		// Fold per-CPU values
		for _, cpuStats := range perCPUStats {
			totalCalls += cpuStats.TotalCalls
			dontneed += cpuStats.AdviceDontneed
		}
	}

	ms.AddCounter("ktm_fadvise_calls_total", float64(totalCalls), nil)
	ms.AddCounter("ktm_fadvise_dontneed_total", float64(dontneed), nil)

	return iter.Err()
}

func (m *module) collectCacheStats(ms *metrics.MetricSet) error {
	var pid uint32
	var perCPUStats []generated.IomonitorCacheStatsT
	iter := m.objs.CacheStatsMap.Iterate()

	var lookups uint64
	var adds uint64
	var deletes uint64

	for iter.Next(&pid, &perCPUStats) {
		// Fold per-CPU values
		for _, cpuStats := range perCPUStats {
			lookups += cpuStats.Lookups
			adds += cpuStats.Adds
			deletes += cpuStats.Deletes
		}
	}

	ms.AddCounter("ktm_cache_lookups_total", float64(lookups), nil)
	ms.AddCounter("ktm_cache_fills_total", float64(adds), nil)
	ms.AddCounter("ktm_cache_deletes_total", float64(deletes), nil)

	return iter.Err()
}

func (m *module) collectMemoryStats(ms *metrics.MetricSet) {
	// LRU shrink stats (per-CPU)
	var key uint32
	var perCPUShrink []generated.IomonitorShrinkCountersT

	if err := m.objs.ShrinkStatsMap.Lookup(key, &perCPUShrink); err == nil {
		var totalScanned, totalReclaimed, totalEvents uint64
		for _, cpuCounters := range perCPUShrink {
			totalScanned += cpuCounters.NrScannedTotal
			totalReclaimed += cpuCounters.NrReclaimedTotal
			totalEvents += cpuCounters.EventsTotal
		}
		ms.AddCounter("ktm_memory_lru_pages_scanned_total", float64(totalScanned), nil)
		ms.AddCounter("ktm_memory_lru_pages_reclaimed_total", float64(totalReclaimed), nil)
		ms.AddCounter("ktm_memory_lru_shrink_events_total", float64(totalEvents), nil)
	}

	// Direct reclaim stats (per-CPU)
	var reclaimKey uint32
	var perCPUReclaim []generated.IomonitorReclaimCountersT
	if err := m.objs.ReclaimCountersMap.Lookup(reclaimKey, &perCPUReclaim); err == nil {
		var totalBegin uint64
		for _, cpuCounters := range perCPUReclaim {
			totalBegin += cpuCounters.DirectReclaimBeginTotal
		}
		ms.AddCounter("ktm_memory_direct_reclaim_begin_total", float64(totalBegin), nil)
	}
}

func (m *module) collectReadLatencyStats(ms *metrics.MetricSet) error {
	var pid uint32
	var perCPUStats []generated.IomonitorReadLatencyStatsT
	iter := m.objs.ReadLatencyStatsMap.Iterate()

	var aggBuckets [32]uint64
	var totalSumNs uint64
	var totalCount uint64
	var totalBytes uint64

	for iter.Next(&pid, &perCPUStats) {
		// Fold per-CPU values
		for _, cpuStats := range perCPUStats {
			totalCount += cpuStats.Count
			totalSumNs += cpuStats.SumLatencyNs
			totalBytes += cpuStats.ReadBytesTotal
			for i, v := range cpuStats.Buckets {
				aggBuckets[i] += v
			}
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
		// Bucket i upper bound: 2^i microseconds
		upperBoundUs := math.Pow(2, float64(i))
		upperBoundSec := upperBoundUs / 1e6

		promBuckets[upperBoundSec] = cumulative
	}
	// Add +Inf bucket
	promBuckets[math.Inf(1)] = cumulative

	ms.AddHistogram("ktm_sys_read_latency_seconds", promBuckets, float64(totalSumNs)/1e9, totalCount, nil)
	ms.AddCounter("ktm_sys_read_bytes_total", float64(totalBytes), nil)

	return nil
}

func (m *module) collectPreadLatencyStats(ms *metrics.MetricSet) error {
	var pid uint32
	var perCPUStats []generated.IomonitorReadLatencyStatsT
	iter := m.objs.PreadLatencyStatsMap.Iterate()

	var aggBuckets [32]uint64
	var totalSumNs uint64
	var totalCount uint64
	var totalBytes uint64

	for iter.Next(&pid, &perCPUStats) {
		// Fold per-CPU values
		for _, cpuStats := range perCPUStats {
			totalCount += cpuStats.Count
			totalSumNs += cpuStats.SumLatencyNs
			totalBytes += cpuStats.ReadBytesTotal
			for i, v := range cpuStats.Buckets {
				aggBuckets[i] += v
			}
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
		// Bucket i upper bound: 2^i microseconds
		upperBoundUs := math.Pow(2, float64(i))
		upperBoundSec := upperBoundUs / 1e6

		promBuckets[upperBoundSec] = cumulative
	}
	// Add +Inf bucket
	promBuckets[math.Inf(1)] = cumulative

	ms.AddHistogram("ktm_sys_pread_latency_seconds", promBuckets, float64(totalSumNs)/1e9, totalCount, nil)
	ms.AddCounter("ktm_sys_pread_bytes_total", float64(totalBytes), nil)

	return nil
}
