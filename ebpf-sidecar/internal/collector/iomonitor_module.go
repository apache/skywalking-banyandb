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
	"time"

	"github.com/cilium/ebpf"
	"go.uber.org/zap"

	loader "github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/ebpf"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/ebpf/generated"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/metrics"
)

// CleanupStrategy defines how to manage eBPF map memory
type CleanupStrategy string

const (
	// ClearAfterRead deletes entries after reading (best for counters)
	ClearAfterRead CleanupStrategy = "clear_after_read"
	// KeepRecent only keeps entries updated in last N minutes
	KeepRecent CleanupStrategy = "keep_recent"
	// NoCleanup never cleans up (for debugging)
	NoCleanup CleanupStrategy = "no_cleanup"
)

// IOMonitorModule collects I/O, cache, and memory statistics from eBPF
type IOMonitorModule struct {
	name            string
	logger          *zap.Logger
	loader          *loader.Loader
	objs            *generated.IomonitorObjects
	cleanupStrategy CleanupStrategy
	cleanupInterval time.Duration
	lastCleanup     time.Time
	
	// Track PIDs to detect stale entries
	activePIDs      map[uint32]time.Time
	staleThreshold  time.Duration
}

// NewIOMonitorModule creates a new I/O monitoring module with cleanup
func NewIOMonitorModule(logger *zap.Logger) (*IOMonitorModule, error) {
	ebpfLoader, err := loader.NewLoader()
	if err != nil {
		return nil, fmt.Errorf("failed to create eBPF loader: %w", err)
	}

	return &IOMonitorModule{
		name:            "iomonitor",
		logger:          logger,
		loader:          ebpfLoader,
		cleanupStrategy: ClearAfterRead, // Default strategy
		cleanupInterval: 60 * time.Second,
		lastCleanup:     time.Now(),
		activePIDs:      make(map[uint32]time.Time),
		staleThreshold:  5 * time.Minute,
	}, nil
}

// SetCleanupStrategy configures the cleanup behavior
func (m *IOMonitorModule) SetCleanupStrategy(strategy CleanupStrategy, interval time.Duration) {
	m.cleanupStrategy = strategy
	m.cleanupInterval = interval
	m.logger.Info("Set cleanup strategy",
		zap.String("strategy", string(strategy)),
		zap.Duration("interval", interval))
}

// Name returns the module name
func (m *IOMonitorModule) Name() string {
	return m.name
}

// Start loads and attaches the eBPF programs
func (m *IOMonitorModule) Start() error {
	m.logger.Info("Starting I/O monitor module")
	
	// Load eBPF programs
	if err := m.loader.LoadPrograms(); err != nil {
		return fmt.Errorf("failed to load eBPF programs: %w", err)
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
	
	m.logger.Info("I/O monitor module started successfully")
	return nil
}

// Stop cleans up the module
func (m *IOMonitorModule) Stop() error {
	m.logger.Info("Stopping I/O monitor module")
	
	// Final cleanup of all maps
	if m.objs != nil {
		m.cleanupAllMaps()
	}
	
	if m.loader != nil {
		if err := m.loader.Close(); err != nil {
			m.logger.Error("Failed to close eBPF loader", zap.Error(err))
		}
	}
	
	return nil
}

// Collect gathers all metrics from eBPF maps with cleanup
func (m *IOMonitorModule) Collect() (*metrics.MetricSet, error) {
	if m.objs == nil {
		return nil, fmt.Errorf("eBPF objects not initialized")
	}

	ms := metrics.NewMetricSet()
	
	// Collect metrics based on strategy
	switch m.cleanupStrategy {
	case ClearAfterRead:
		// Collect and immediately clear
		m.collectWithClear(ms)
	case KeepRecent:
		// Collect and cleanup stale entries
		m.collectWithStaleCleanup(ms)
	case NoCleanup:
		// Just collect, no cleanup
		m.collectNoCleanup(ms)
	}
	
	// Periodic maintenance cleanup
	if time.Since(m.lastCleanup) > m.cleanupInterval {
		m.performMaintenanceCleanup()
		m.lastCleanup = time.Now()
	}
	
	return ms, nil
}

// collectWithClear collects metrics and clears maps (best for Prometheus)
func (m *IOMonitorModule) collectWithClear(ms *metrics.MetricSet) {
	// Collect fadvise stats
	m.collectAndClearFadviseStats(ms)
	
	// Collect cache stats
	m.collectAndClearCacheStats(ms)
	
	// Collect memory stats
	m.collectAndClearMemoryStats(ms)
}

// collectAndClearFadviseStats collects fadvise metrics and clears the map
func (m *IOMonitorModule) collectAndClearFadviseStats(ms *metrics.MetricSet) {
	var pid uint32
	var stats generated.IomonitorFadviseStatsT
	iter := m.objs.FadviseStatsMap.Iterate()
	
	// Aggregate stats
	var totalCalls, successCalls uint64
	adviceCounts := make(map[string]uint64)
	
	// Collect PIDs to delete
	var pidsToDelete []uint32
	
	for iter.Next(&pid, &stats) {
		totalCalls += stats.TotalCalls
		successCalls += stats.SuccessCalls
		adviceCounts["dontneed"] += stats.AdviceDontneed
		adviceCounts["sequential"] += stats.AdviceSequential
		adviceCounts["normal"] += stats.AdviceNormal
		adviceCounts["random"] += stats.AdviceRandom
		adviceCounts["willneed"] += stats.AdviceWillneed
		adviceCounts["noreuse"] += stats.AdviceNoreuse
		
		// Mark for deletion
		pidsToDelete = append(pidsToDelete, pid)
		
		// Track active PID
		m.activePIDs[pid] = time.Now()
	}
	
	if err := iter.Err(); err != nil {
		m.logger.Warn("Failed to iterate fadvise stats", zap.Error(err))
		return
	}
	
	// Add metrics
	ms.AddCounter("ebpf_fadvise_calls_total", float64(totalCalls), nil)
	ms.AddCounter("ebpf_fadvise_success_total", float64(successCalls), nil)
	
	for advice, count := range adviceCounts {
		ms.AddCounter("ebpf_fadvise_advice_total", float64(count), map[string]string{
			"advice": advice,
		})
	}
	
	if totalCalls > 0 {
		successRate := float64(successCalls) / float64(totalCalls) * 100
		ms.AddGauge("ebpf_fadvise_success_rate_percent", successRate, nil)
	}
	
	// Clear the map entries
	for _, p := range pidsToDelete {
		if err := m.objs.FadviseStatsMap.Delete(p); err != nil && err != ebpf.ErrKeyNotExist {
			m.logger.Debug("Failed to delete fadvise stats entry",
				zap.Uint32("pid", p),
				zap.Error(err))
		}
	}
	
	m.logger.Debug("Collected and cleared fadvise stats",
		zap.Int("entries_cleared", len(pidsToDelete)),
		zap.Uint64("total_calls", totalCalls))
}

// collectAndClearCacheStats collects cache metrics and clears the map
func (m *IOMonitorModule) collectAndClearCacheStats(ms *metrics.MetricSet) {
	var pid uint32
	var stats generated.IomonitorCacheStatsT
	iter := m.objs.CacheStatsMap.Iterate()
	
	var totalReadAttempts, cacheMisses, readBatchCalls, pageCacheAdds uint64
	var pidsToDelete []uint32
	
	for iter.Next(&pid, &stats) {
		totalReadAttempts += stats.TotalReadAttempts
		cacheMisses += stats.CacheMisses
		readBatchCalls += stats.ReadBatchCalls
		pageCacheAdds += stats.PageCacheAdds
		
		pidsToDelete = append(pidsToDelete, pid)
		m.activePIDs[pid] = time.Now()
	}
	
	if err := iter.Err(); err != nil {
		m.logger.Warn("Failed to iterate cache stats", zap.Error(err))
		return
	}
	
	// Add metrics
	ms.AddCounter("ebpf_cache_read_attempts_total", float64(totalReadAttempts), nil)
	ms.AddCounter("ebpf_cache_misses_total", float64(cacheMisses), nil)
	ms.AddCounter("ebpf_page_cache_adds_total", float64(pageCacheAdds), nil)
	
	// Calculate rates
	if totalReadAttempts > 0 {
		missRate := float64(cacheMisses) / float64(totalReadAttempts) * 100
		hitRate := 100 - missRate
		
		ms.AddGauge("ebpf_cache_hit_rate_percent", hitRate, nil)
		ms.AddGauge("ebpf_cache_miss_rate_percent", missRate, nil)
	}
	
	// Clear entries
	for _, p := range pidsToDelete {
		if err := m.objs.CacheStatsMap.Delete(p); err != nil && err != ebpf.ErrKeyNotExist {
			m.logger.Debug("Failed to delete cache stats entry",
				zap.Uint32("pid", p),
				zap.Error(err))
		}
	}
	
	m.logger.Debug("Collected and cleared cache stats",
		zap.Int("entries_cleared", len(pidsToDelete)),
		zap.Uint64("cache_misses", cacheMisses))
}

// collectAndClearMemoryStats collects memory metrics and clears maps
func (m *IOMonitorModule) collectAndClearMemoryStats(ms *metrics.MetricSet) {
	// LRU shrink stats (single entry)
	var key uint32 = 0
	var shrinkInfo generated.IomonitorLruShrinkInfoT
	
	if err := m.objs.ShrinkStatsMap.Lookup(key, &shrinkInfo); err == nil {
		ms.AddGauge("ebpf_memory_lru_pages_scanned", float64(shrinkInfo.NrScanned), nil)
		ms.AddGauge("ebpf_memory_lru_pages_reclaimed", float64(shrinkInfo.NrReclaimed), nil)
		
		if shrinkInfo.NrScanned > 0 {
			efficiency := float64(shrinkInfo.NrReclaimed) / float64(shrinkInfo.NrScanned) * 100
			ms.AddGauge("ebpf_memory_reclaim_efficiency_percent", efficiency, nil)
		}
		
		// Clear after read
		m.objs.ShrinkStatsMap.Delete(key)
	}
	
	// Direct reclaim map
	var reclaimPid uint32
	var reclaimInfo generated.IomonitorReclaimInfoT
	iter := m.objs.DirectReclaimMap.Iterate()
	
	var pidsToDelete []uint32
	directReclaimCount := 0
	
	for iter.Next(&reclaimPid, &reclaimInfo) {
		directReclaimCount++
		pidsToDelete = append(pidsToDelete, reclaimPid)
	}
	
	ms.AddGauge("ebpf_memory_direct_reclaim_processes", float64(directReclaimCount), nil)
	
	// Clear direct reclaim entries
	for _, p := range pidsToDelete {
		if err := m.objs.DirectReclaimMap.Delete(p); err != nil && err != ebpf.ErrKeyNotExist {
			m.logger.Debug("Failed to delete reclaim entry",
				zap.Uint32("pid", p),
				zap.Error(err))
		}
	}
}

// collectWithStaleCleanup collects metrics and removes stale entries
func (m *IOMonitorModule) collectWithStaleCleanup(ms *metrics.MetricSet) {
	// Similar to collectNoCleanup but tracks PIDs
	m.collectNoCleanup(ms)
	
	// Clean up stale PIDs (not seen in last 5 minutes)
	now := time.Now()
	var stalePIDs []uint32
	
	for pid, lastSeen := range m.activePIDs {
		if now.Sub(lastSeen) > m.staleThreshold {
			stalePIDs = append(stalePIDs, pid)
		}
	}
	
	// Remove stale entries from all maps
	for _, pid := range stalePIDs {
		m.objs.FadviseStatsMap.Delete(pid)
		m.objs.CacheStatsMap.Delete(pid)
		m.objs.DirectReclaimMap.Delete(pid)
		delete(m.activePIDs, pid)
	}
	
	if len(stalePIDs) > 0 {
		m.logger.Info("Cleaned up stale PIDs",
			zap.Int("count", len(stalePIDs)))
	}
}

// collectNoCleanup just collects without any cleanup
func (m *IOMonitorModule) collectNoCleanup(ms *metrics.MetricSet) {
	// Collect without clearing
	if err := m.collectFadviseStats(ms); err != nil {
		m.logger.Warn("Failed to collect fadvise stats", zap.Error(err))
	}
	
	if err := m.collectCacheStats(ms); err != nil {
		m.logger.Warn("Failed to collect cache stats", zap.Error(err))
	}
	
	if err := m.collectMemoryStats(ms); err != nil {
		m.logger.Warn("Failed to collect memory stats", zap.Error(err))
	}
}

// Standard collection methods (no cleanup)
func (m *IOMonitorModule) collectFadviseStats(ms *metrics.MetricSet) error {
	// Same as before but without deletion
	var pid uint32
	var stats generated.IomonitorFadviseStatsT
	iter := m.objs.FadviseStatsMap.Iterate()
	
	var totalCalls, successCalls uint64
	adviceCounts := make(map[string]uint64)
	
	for iter.Next(&pid, &stats) {
		totalCalls += stats.TotalCalls
		successCalls += stats.SuccessCalls
		adviceCounts["dontneed"] += stats.AdviceDontneed
		adviceCounts["sequential"] += stats.AdviceSequential
		adviceCounts["normal"] += stats.AdviceNormal
		adviceCounts["random"] += stats.AdviceRandom
		adviceCounts["willneed"] += stats.AdviceWillneed
		adviceCounts["noreuse"] += stats.AdviceNoreuse
		
		m.activePIDs[pid] = time.Now()
	}
	
	// Add metrics without clearing
	ms.AddCounter("ebpf_fadvise_calls_total", float64(totalCalls), nil)
	ms.AddCounter("ebpf_fadvise_success_total", float64(successCalls), nil)
	
	for advice, count := range adviceCounts {
		ms.AddCounter("ebpf_fadvise_advice_total", float64(count), map[string]string{
			"advice": advice,
		})
	}
	
	return iter.Err()
}

func (m *IOMonitorModule) collectCacheStats(ms *metrics.MetricSet) error {
	var pid uint32
	var stats generated.IomonitorCacheStatsT
	iter := m.objs.CacheStatsMap.Iterate()
	
	var totalReadAttempts, cacheMisses uint64
	
	for iter.Next(&pid, &stats) {
		totalReadAttempts += stats.TotalReadAttempts
		cacheMisses += stats.CacheMisses
		m.activePIDs[pid] = time.Now()
	}
	
	ms.AddCounter("ebpf_cache_read_attempts_total", float64(totalReadAttempts), nil)
	ms.AddCounter("ebpf_cache_misses_total", float64(cacheMisses), nil)
	
	if totalReadAttempts > 0 {
		missRate := float64(cacheMisses) / float64(totalReadAttempts) * 100
		ms.AddGauge("ebpf_cache_miss_rate_percent", missRate, nil)
	}
	
	return iter.Err()
}

func (m *IOMonitorModule) collectMemoryStats(ms *metrics.MetricSet) error {
	// Similar implementation without cleanup
	return nil
}

// performMaintenanceCleanup does periodic maintenance
func (m *IOMonitorModule) performMaintenanceCleanup() {
	// Log map sizes for monitoring
	fadviseCount := 0
	cacheCount := 0
	
	iter := m.objs.FadviseStatsMap.Iterate()
	for iter.Next(nil, nil) {
		fadviseCount++
	}
	
	iter = m.objs.CacheStatsMap.Iterate()
	for iter.Next(nil, nil) {
		cacheCount++
	}
	
	m.logger.Info("eBPF map sizes",
		zap.Int("fadvise_entries", fadviseCount),
		zap.Int("cache_entries", cacheCount),
		zap.Int("tracked_pids", len(m.activePIDs)))
	
	// Warn if maps are getting too large
	if fadviseCount > 1000 || cacheCount > 1000 {
		m.logger.Warn("eBPF maps are large, consider more aggressive cleanup",
			zap.Int("fadvise_entries", fadviseCount),
			zap.Int("cache_entries", cacheCount))
	}
}

// cleanupAllMaps removes all entries from all maps
func (m *IOMonitorModule) cleanupAllMaps() {
	m.logger.Info("Performing final cleanup of all eBPF maps")
	
	// Clear all fadvise stats
	var pid uint32
	iter := m.objs.FadviseStatsMap.Iterate()
	for iter.Next(&pid, nil) {
		m.objs.FadviseStatsMap.Delete(pid)
	}
	
	// Clear all cache stats
	iter = m.objs.CacheStatsMap.Iterate()
	for iter.Next(&pid, nil) {
		m.objs.CacheStatsMap.Delete(pid)
	}
	
	// Clear direct reclaim map
	iter = m.objs.DirectReclaimMap.Iterate()
	for iter.Next(&pid, nil) {
		m.objs.DirectReclaimMap.Delete(pid)
	}
	
	// Clear shrink stats
	var key uint32 = 0
	m.objs.ShrinkStatsMap.Delete(key)
}