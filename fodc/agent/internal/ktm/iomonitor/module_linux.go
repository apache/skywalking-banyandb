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

//go:build linux && (amd64 || arm64 || 386)

package iomonitor

import (
	"fmt"
	"math"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor/ebpf/generated"
	fodcmetrics "github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
)

type module struct {
	logger     zerolog.Logger
	loader     *ebpf.EnhancedLoader
	objs       *generated.IomonitorObjects
	name       string
	cgroupPath string
}

func newModule(log zerolog.Logger, ebpfCfg EBPFConfig) (*module, error) {
	ebpfLoader, err := ebpf.NewEnhancedLoader(log)
	if err != nil {
		return nil, fmt.Errorf("failed to create eBPF loader: %w", err)
	}

	return &module{
		name:       "iomonitor",
		logger:     log,
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
	m.logger.Info().Msg("Starting I/O monitor module")

	// Configure cgroup filtering if requested.
	if m.cgroupPath != "" {
		m.logger.Info().Str("cgroup_path", m.cgroupPath).Msg("Enabling cgroup filter for eBPF programs")
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

	m.logger.Info().Msg("I/O monitor module started successfully")
	return nil
}

// Stop cleans up the module.
func (m *module) Stop() error {
	m.logger.Info().Msg("Stopping I/O monitor module")

	if m.loader != nil {
		if err := m.loader.Close(); err != nil {
			m.logger.Error().Err(err).Msg("Failed to close eBPF loader")
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
func (m *module) Collect() ([]fodcmetrics.RawMetric, error) {
	if m.objs == nil {
		return nil, fmt.Errorf("eBPF objects not initialized")
	}

	rawMetrics := make([]fodcmetrics.RawMetric, 0)

	// Collect metrics (cumulative)
	m.collectMetrics(&rawMetrics)
	m.addDegradedMetric(&rawMetrics)

	return rawMetrics, nil
}

func (m *module) addDegradedMetric(rawMetrics *[]fodcmetrics.RawMetric) {
	degraded := 0.0
	if m.loader != nil && m.loader.Degraded() {
		degraded = 1.0
	}
	*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
		Name:  "ktm_degraded",
		Value: degraded,
		Desc:  "KTM degraded mode indicator: 0=normal, 1=degraded",
	})
}

// collectMetrics collects metrics from all maps without clearing them.
func (m *module) collectMetrics(rawMetrics *[]fodcmetrics.RawMetric) {
	if err := m.collectFadviseStats(rawMetrics); err != nil {
		m.logger.Debug().Err(err).Msg("Failed to collect fadvise stats")
	}

	if err := m.collectCacheStats(rawMetrics); err != nil {
		m.logger.Debug().Err(err).Msg("Failed to collect cache stats")
	}

	m.collectMemoryStats(rawMetrics)

	if err := m.collectReadLatencyStats(rawMetrics); err != nil {
		m.logger.Debug().Err(err).Msg("Failed to collect read latency stats")
	}

	if err := m.collectPreadLatencyStats(rawMetrics); err != nil {
		m.logger.Debug().Err(err).Msg("Failed to collect pread latency stats")
	}
}

func (m *module) collectFadviseStats(rawMetrics *[]fodcmetrics.RawMetric) error {
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

	*rawMetrics = append(*rawMetrics,
		fodcmetrics.RawMetric{
			Name:  "ktm_fadvise_calls_total",
			Value: float64(totalCalls),
			Desc:  "Total number of fadvise calls",
		},
		fodcmetrics.RawMetric{
			Name:  "ktm_fadvise_dontneed_total",
			Value: float64(dontneed),
			Desc:  "Total number of fadvise DONTNEED calls",
		},
	)

	return iter.Err()
}

func (m *module) collectCacheStats(rawMetrics *[]fodcmetrics.RawMetric) error {
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

	*rawMetrics = append(*rawMetrics,
		fodcmetrics.RawMetric{
			Name:  "ktm_cache_lookups_total",
			Value: float64(lookups),
			Desc:  "Total number of page cache lookups",
		},
		fodcmetrics.RawMetric{
			Name:  "ktm_cache_fills_total",
			Value: float64(adds),
			Desc:  "Total number of page cache fills",
		},
		fodcmetrics.RawMetric{
			Name:  "ktm_cache_deletes_total",
			Value: float64(deletes),
			Desc:  "Total number of page cache deletions",
		},
	)

	return iter.Err()
}

func (m *module) collectMemoryStats(rawMetrics *[]fodcmetrics.RawMetric) {
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
		*rawMetrics = append(*rawMetrics,
			fodcmetrics.RawMetric{
				Name:  "ktm_memory_lru_pages_scanned_total",
				Value: float64(totalScanned),
				Desc:  "Total number of LRU pages scanned",
			},
			fodcmetrics.RawMetric{
				Name:  "ktm_memory_lru_pages_reclaimed_total",
				Value: float64(totalReclaimed),
				Desc:  "Total number of LRU pages reclaimed",
			},
			fodcmetrics.RawMetric{
				Name:  "ktm_memory_lru_shrink_events_total",
				Value: float64(totalEvents),
				Desc:  "Total number of LRU shrink events",
			},
		)
	}

	// Direct reclaim stats (per-CPU)
	var reclaimKey uint32
	var perCPUReclaim []generated.IomonitorReclaimCountersT
	if err := m.objs.ReclaimCountersMap.Lookup(reclaimKey, &perCPUReclaim); err == nil {
		var totalBegin uint64
		for _, cpuCounters := range perCPUReclaim {
			totalBegin += cpuCounters.DirectReclaimBeginTotal
		}
		*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
			Name:  "ktm_memory_direct_reclaim_begin_total",
			Value: float64(totalBegin),
			Desc:  "Total number of direct reclaim begin events",
		})
	}
}

func (m *module) collectReadLatencyStats(rawMetrics *[]fodcmetrics.RawMetric) error {
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

	// Add histogram sum and count
	*rawMetrics = append(*rawMetrics,
		fodcmetrics.RawMetric{
			Name:  "ktm_sys_read_latency_seconds_sum",
			Value: float64(totalSumNs) / 1e9,
			Desc:  "Total sum of read() latency in seconds",
		},
		fodcmetrics.RawMetric{
			Name:  "ktm_sys_read_latency_seconds_count",
			Value: float64(totalCount),
			Desc:  "Total count of read() calls",
		},
	)

	// Add histogram buckets
	var cumulative uint64
	for i := 0; i < 32; i++ {
		count := aggBuckets[i]
		cumulative += count

		// Upper bound in seconds
		// Bucket i upper bound: 2^i microseconds
		upperBoundUs := math.Pow(2, float64(i))
		upperBoundSec := upperBoundUs / 1e6

		*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
			Name:  "ktm_sys_read_latency_seconds_bucket",
			Value: float64(cumulative),
			Labels: []fodcmetrics.Label{
				{Name: "le", Value: fmt.Sprintf("%g", upperBoundSec)},
			},
			Desc: "Histogram buckets for read() latency",
		})
	}
	// Add +Inf bucket
	*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
		Name:  "ktm_sys_read_latency_seconds_bucket",
		Value: float64(cumulative),
		Labels: []fodcmetrics.Label{
			{Name: "le", Value: "+Inf"},
		},
		Desc: "Histogram buckets for read() latency",
	})

	*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
		Name:  "ktm_sys_read_bytes_total",
		Value: float64(totalBytes),
		Desc:  "Total bytes read via read() syscall",
	})

	return nil
}

func (m *module) collectPreadLatencyStats(rawMetrics *[]fodcmetrics.RawMetric) error {
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

	// Add histogram sum and count
	*rawMetrics = append(*rawMetrics,
		fodcmetrics.RawMetric{
			Name:  "ktm_sys_pread_latency_seconds_sum",
			Value: float64(totalSumNs) / 1e9,
			Desc:  "Total sum of pread() latency in seconds",
		},
		fodcmetrics.RawMetric{
			Name:  "ktm_sys_pread_latency_seconds_count",
			Value: float64(totalCount),
			Desc:  "Total count of pread() calls",
		},
	)

	// Add histogram buckets
	var cumulative uint64
	for i := 0; i < 32; i++ {
		count := aggBuckets[i]
		cumulative += count

		// Upper bound in seconds
		// Bucket i upper bound: 2^i microseconds
		upperBoundUs := math.Pow(2, float64(i))
		upperBoundSec := upperBoundUs / 1e6

		*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
			Name:  "ktm_sys_pread_latency_seconds_bucket",
			Value: float64(cumulative),
			Labels: []fodcmetrics.Label{
				{Name: "le", Value: fmt.Sprintf("%g", upperBoundSec)},
			},
			Desc: "Histogram buckets for pread() latency",
		})
	}
	// Add +Inf bucket
	*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
		Name:  "ktm_sys_pread_latency_seconds_bucket",
		Value: float64(cumulative),
		Labels: []fodcmetrics.Label{
			{Name: "le", Value: "+Inf"},
		},
		Desc: "Histogram buckets for pread() latency",
	})

	*rawMetrics = append(*rawMetrics, fodcmetrics.RawMetric{
		Name:  "ktm_sys_pread_bytes_total",
		Value: float64(totalBytes),
		Desc:  "Total bytes read via pread() syscall",
	})

	return nil
}
