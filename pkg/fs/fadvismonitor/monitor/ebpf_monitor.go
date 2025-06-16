// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.
//go:build linux
// +build linux

// Package monitor provides functionality for monitoring file access advice and memory reclaim.
package monitor

import (
	"bytes"
	"fmt"
	"os"
	"unsafe"

	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"

	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/bpf"
)

// Monitor represents a monitoring session for fadvise and memory reclaim operations.
type Monitor struct {
	objs        *bpf.BpfObjects
	tpEnter     link.Link
	tpExit      link.Link
	tpLru       link.Link
	tpCacheRead link.Link
	tpCacheAdd  link.Link
	useKprobe   bool // flag to indicate if using kprobe fallback
	kprobeLinks []link.Link
}

// MonitorOptions configures the monitor behavior.
type MonitorOptions struct {
	// ForceKprobe determines whether to bypass tracepoints entirely and only use kprobes.
	ForceKprobe bool
	// EnableFallback determines whether to fall back to kprobes if tracepoint attachment fails.
	EnableFallback bool
}

// Validate ensures that the MonitorOptions are not contradictory.
func (opts *MonitorOptions) Validate() error {
	if opts.ForceKprobe && opts.EnableFallback {
		return fmt.Errorf("ForceKprobe=true does not require EnableFallback")
	}
	return nil
}

// ChooseDefaultOptions automatically selects MonitorOptions based on kernel capabilities:
//   - If BTF/CO-RE is available and kernel version ≥ 5.4, it returns ForceKprobe=false, EnableFallback=true.
//   - Otherwise, it returns ForceKprobe=true, EnableFallback=false.
func ChooseDefaultOptions() MonitorOptions {
	if ShouldUseCORE() {
		return MonitorOptions{
			ForceKprobe:    false,
			EnableFallback: true,
		}
	}
	return MonitorOptions{
		ForceKprobe:    true,
		EnableFallback: false,
	}
}

// NewMonitor creates a new monitor instance in “auto” mode: try tracepoints first, then fall back to kprobes.
func NewMonitor() (*Monitor, error) {
	opts := ChooseDefaultOptions()
	return NewMonitorWithOptions(opts)
}

// NewTracepointOnlyMonitor creates a monitor instance that only uses tracepoints (no fallback).
func NewTracepointOnlyMonitor() (*Monitor, error) {
	return NewMonitorWithOptions(MonitorOptions{
		ForceKprobe:    false,
		EnableFallback: false,
	})
}

// NewKprobeOnlyMonitor creates a monitor instance that only uses kprobes (bypassing tracepoint).
func NewKprobeOnlyMonitor() (*Monitor, error) {
	return NewMonitorWithOptions(MonitorOptions{
		ForceKprobe:    true,
		EnableFallback: false,
	})
}

// NewMonitorWithOptions creates a new monitor with the specified options.
func NewMonitorWithOptions(opts MonitorOptions) (*Monitor, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	// Remove memlock limit so that eBPF programs can be loaded.
	if err := rlimit.RemoveMemlock(); err != nil {
		fmt.Printf("warning: RemoveMemlock failed: %v\n", err)
	}

	// Load BPF objects (CO-RE or precompiled).
	objs := &bpf.BpfObjects{}
	if err := bpf.LoadBpfObjects(objs, nil); err != nil {
		return nil, fmt.Errorf("load bpf objects failed: %w", err)
	}

	// 1. If ForceKprobe is set, attach kprobes immediately.
	if opts.ForceKprobe {
		mon, err := attachKprobes(objs)
		if err != nil {
			objs.Close()
			return nil, fmt.Errorf("forced kprobe attach failed: %w", err)
		}
		mon.useKprobe = true
		return mon, nil
	}

	// 2. Otherwise, attempt to attach tracepoints.
	mon, err := attachTracepoints(objs)
	if err == nil {
		mon.useKprobe = false
		return mon, nil
	}

	// 3. If tracepoint attach fails and fallback is allowed, reload BPF and use kprobes.
	if opts.EnableFallback {
		mon.Close()
		objs2 := &bpf.BpfObjects{}
		if err2 := bpf.LoadBpfObjects(objs2, nil); err2 != nil {
			return nil, fmt.Errorf("reload bpf objects for kprobe fallback failed: %w", err2)
		}
		mon2, err2 := attachKprobes(objs2)
		if err2 != nil {
			objs2.Close()
			return nil, fmt.Errorf("tracepoint error: %v; kprobe error: %v", err, err2)
		}
		mon2.useKprobe = true
		return mon2, nil
	}

	// 4. If tracepoint attach fails and fallback is not allowed, error out.
	mon.Close()
	return nil, fmt.Errorf("tracepoint attach failed (fallback disabled): %w", err)
}

// attachTracepoints attempts to attach all required tracepoints.
// On any failure, it closes any already-attached links and returns an error.
func attachTracepoints(objs *bpf.BpfObjects) (*Monitor, error) {
	m := &Monitor{objs: objs}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		return nil, fmt.Errorf("attach tracepoint sys_enter_fadvise64 failed: %w", err)
	}
	m.tpEnter = tpEnter

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		tpEnter.Close()
		return nil, fmt.Errorf("attach tracepoint sys_exit_fadvise64 failed: %w", err)
	}
	m.tpExit = tpExit

	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		return nil, fmt.Errorf("attach tracepoint mm_vmscan_lru_shrink_inactive failed: %w", err)
	}
	m.tpLru = tpLru

	// Cache read tracepoint
	tpCacheRead, err := link.Tracepoint("filemap", "filemap_get_read_batch", objs.TraceFilemapGetReadBatch, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		tpLru.Close()
		return nil, fmt.Errorf("attach tracepoint filemap_get_read_batch failed: %w", err)
	}
	m.tpCacheRead = tpCacheRead

	// Cache add tracepoint
	tpCacheAdd, err := link.Tracepoint("filemap", "mm_filemap_add_to_page_cache", objs.TraceMmFilemapAddToPageCache, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		tpLru.Close()
		tpCacheRead.Close()
		return nil, fmt.Errorf("attach tracepoint mm_filemap_add_to_page_cache failed: %w", err)
	}
	m.tpCacheAdd = tpCacheAdd

	return m, nil
}

// attachKprobes attempts to attach all required kprobes using kernel-version-specific function names.
// It consults getFadviseFunctionNames() and getFilemapFunctionNames() to try multiple symbol candidates.
// On any failure to attach the essential probes, it closes attached links and returns an error.
func attachKprobes(objs *bpf.BpfObjects) (*Monitor, error) {
	m := &Monitor{objs: objs}
	var err error

	// 1. Attach fadvise enter + exit using symbol candidates.
	fadviseFuncs := getFadviseFunctionNames()
	var kpEnter, kpExit link.Link
	found := false
	for _, funcName := range fadviseFuncs {
		kpEnter, err = link.Kprobe(funcName, objs.KprobeKsysFadvise6464, nil)
		if err != nil {
			continue
		}
		kpExit, err = link.Kretprobe(funcName, objs.KretprobeKsysFadvise6464, nil)
		if err != nil {
			kpEnter.Close()
			continue
		}
		// Successfully attached both enter and exit
		m.kprobeLinks = append(m.kprobeLinks, kpEnter, kpExit)
		found = true
		break
	}
	if !found {
		m.Close()
		return nil, fmt.Errorf("failed to attach kprobe for fadvise on any symbol: %v", err)
	}

	// 2. Attach filemap_read kprobe.
	readFuncs, addFuncs := getFilemapFunctionNames()

	found = false
	for _, funcName := range readFuncs {
		kp, err := link.Kprobe(funcName, objs.KprobeFilemapGetReadBatch, nil)
		if err != nil {
			continue
		}
		m.kprobeLinks = append(m.kprobeLinks, kp)
		found = true
		break
	}
	if !found {
		// Only a warning, not fatal—cache read may be optional
		fmt.Fprintf(os.Stderr, "warning: failed to attach cache-read kprobe on any symbol\n")
	}

	// 3. Attach filemap_add kprobe.
	found = false
	for _, funcName := range addFuncs {
		kp, err := link.Kprobe(funcName, objs.KprobeAddToPageCacheLru, nil)
		if err != nil {
			continue
		}
		m.kprobeLinks = append(m.kprobeLinks, kp)
		found = true
		break
	}
	if !found {
		// Only a warning, not fatal—cache-add may be optional
		fmt.Fprintf(os.Stderr, "warning: failed to attach cache-add kprobe on any symbol\n")
	}

	m.useKprobe = true
	return m, nil
}

// Close cleans up resources used by the monitor.
func (m *Monitor) Close() {
	if m.tpEnter != nil {
		m.tpEnter.Close()
	}
	if m.tpExit != nil {
		m.tpExit.Close()
	}
	if m.tpLru != nil {
		m.tpLru.Close()
	}
	if m.tpCacheRead != nil {
		m.tpCacheRead.Close()
	}
	if m.tpCacheAdd != nil {
		m.tpCacheAdd.Close()
	}
	for _, l := range m.kprobeLinks {
		if l != nil {
			l.Close()
		}
	}
	if m.objs != nil {
		m.objs.Close()
	}
}

// IsUsingKprobe returns true if the monitor is using kprobe fallback instead of tracepoints.
func (m *Monitor) IsUsingKprobe() bool {
	return m.useKprobe
}

// ReadCounts returns a map of process IDs to fadvise call counts.
func (m *Monitor) ReadCounts() (map[uint32]uint64, error) {
	out := make(map[uint32]uint64)
	iter := m.objs.FadviseStatsMap.Iterate()
	var pid uint32
	var stats bpf.BpfFadviseStatsT
	for iter.Next(&pid, &stats) {
		// Use TotalCalls (not AdviceDontneed) for full count
		out[pid] = stats.TotalCalls
	}
	return out, iter.Err()
}

// ReadShrinkStats returns a map of process IDs to LRU shrink statistics.
func (m *Monitor) ReadShrinkStats() (map[uint32]bpf.BpfLruShrinkInfoT, error) {
	out := make(map[uint32]bpf.BpfLruShrinkInfoT)
	iter := m.objs.ShrinkStatsMap.Iterate()
	var pid uint32
	var val bpf.BpfLruShrinkInfoT
	for iter.Next(&pid, &val) {
		out[pid] = val
	}
	return out, iter.Err()
}

// ReclaimInfo stores information about memory reclaim events.
type ReclaimInfo struct {
	Comm string
	PID  uint32
}

// ReadDirectReclaimStats returns information about direct memory reclaim events.
func (m *Monitor) ReadDirectReclaimStats() (map[uint32]ReclaimInfo, error) {
	if m.objs == nil || m.objs.DirectReclaimMap == nil {
		return nil, fmt.Errorf("DirectReclaimMap not initialized")
	}

	result := make(map[uint32]ReclaimInfo)
	var key uint32
	var value bpf.BpfReclaimInfoT

	iter := m.objs.DirectReclaimMap.Iterate()
	for iter.Next(&key, &value) {
		comm := unsafe.Slice((*byte)(unsafe.Pointer(&value.Comm[0])), len(value.Comm))
		result[key] = ReclaimInfo{
			PID:  key,
			Comm: string(bytes.Trim(comm, "\x00")),
		}
	}
	return result, iter.Err()
}

// CacheStats represents cache hit/miss statistics.
type CacheStats struct {
	CacheHits      uint64  // ReadBatchCalls - CacheMisses
	CacheMisses    uint64  // from mm_filemap_add_to_page_cache
	ReadBatchCalls uint64  // from filemap_get_read_batch
	PageCacheAdds  uint64  // same as CacheMisses
	HitRatio       float64 // CacheHits / ReadBatchCalls
}

// ReadCacheStats returns cache hit/miss statistics for all processes.
func (m *Monitor) ReadCacheStats() (map[uint32]CacheStats, error) {
	if m.objs == nil || m.objs.CacheStatsMap == nil {
		return nil, fmt.Errorf("CacheStatsMap not initialized")
	}

	result := make(map[uint32]CacheStats)
	var key uint32
	var value bpf.BpfCacheStatsT

	iter := m.objs.CacheStatsMap.Iterate()
	for iter.Next(&key, &value) {
		cacheHits := uint64(0)
		if value.ReadBatchCalls > value.CacheMisses {
			cacheHits = value.ReadBatchCalls - value.CacheMisses
		}
		hitRatio := float64(0)
		if value.ReadBatchCalls > 0 {
			hitRatio = float64(cacheHits) / float64(value.ReadBatchCalls)
		}
		result[key] = CacheStats{
			CacheHits:      cacheHits,
			CacheMisses:    value.CacheMisses,
			ReadBatchCalls: value.ReadBatchCalls,
			PageCacheAdds:  value.PageCacheAdds,
			HitRatio:       hitRatio,
		}
	}
	return result, iter.Err()
}

// CacheEvent represents a cache-related event.
type CacheEvent struct {
	PID       uint32
	Comm      string
	PageCount uint64
	Timestamp uint64
}

// ReadLastCacheEvent returns the most recent cache event.
func (m *Monitor) ReadLastCacheEvent() (*CacheEvent, error) {
	if m.objs == nil || m.objs.CacheEventMap == nil {
		return nil, fmt.Errorf("CacheEventMap not initialized")
	}

	var key uint32 = 0
	var value bpf.BpfCacheEventT

	err := m.objs.CacheEventMap.Lookup(&key, &value)
	if err != nil {
		return nil, err
	}

	comm := unsafe.Slice((*byte)(unsafe.Pointer(&value.Comm[0])), len(value.Comm))
	return &CacheEvent{
		PID:       value.Pid,
		Comm:      string(bytes.Trim(comm, "\x00")),
		PageCount: value.PageCount,
		Timestamp: value.Timestamp,
	}, nil
}
