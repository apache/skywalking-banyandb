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
// +build linux

// bpf package provides functions to load and manage eBPF programs.
package bpf

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cilium/ebpf/link"
)

type FadviseStats struct {
	PID            uint32
	SuccessCalls   uint64
	AdviceDontneed uint64
}

// LruShrinkStats contains statistics about LRU shrink operations.
// Fields are ordered by size for optimal memory layout.
type LruShrinkStats struct {
	CallerComm  string
	NrScanned   uint64
	NrReclaimed uint64
	CallerPid   uint32
}

func RunFadvise() (func(), error) {
	objs := BpfObjects{}
	if err := LoadBpfObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attaching enter tracepoint: %w", err)
	}
	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		tpEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("attaching exit tracepoint: %w", err)
	}
	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		objs.Close()
		return nil, fmt.Errorf("attaching lru_shrink tracepoint: %w", err)
	}

	cleanup := func() {
		tpEnter.Close()
		tpExit.Close()
		tpLru.Close()
		objs.Close()
	}
	return cleanup, nil
}

func GetFadviseStats(objs *BpfObjects) ([]FadviseStats, error) {
	if objs == nil || objs.FadviseStatsMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	var stats []FadviseStats
	var key uint32
	var value BpfFadviseStatsT

	iter := objs.FadviseStatsMap.Iterate()
	for iter.Next(&key, &value) {
		stats = append(stats, FadviseStats{
			PID:            key,
			SuccessCalls:   value.SuccessCalls,
			AdviceDontneed: value.AdviceDontneed,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterating map: %w", err)
	}

	return stats, nil
}

func GetLruShrinkStats(objs *BpfObjects) (*LruShrinkStats, error) {
	if objs == nil || objs.ShrinkStatsMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	var key uint32 = 0
	var value BpfLruShrinkInfoT

	if err := objs.ShrinkStatsMap.Lookup(&key, &value); err != nil {
		return nil, fmt.Errorf("failed to lookup shrink stats: %w", err)
	}

	stats := &LruShrinkStats{
		NrScanned:   value.NrScanned,
		NrReclaimed: value.NrReclaimed,
		CallerPid:   value.CallerPid,
		CallerComm:  int8SliceToString(value.CallerComm[:]),
	}

	return stats, nil
}

func int8SliceToString(s []int8) string {
	b := make([]byte, len(s))
	for i, v := range s {
		if v == 0 {
			return string(b[:i])
		}
		b[i] = byte(v)
	}
	return string(b)
}

func MonitorFadviseWithTimeout(duration time.Duration) ([]FadviseStats, error) {
	objs := BpfObjects{}
	if err := LoadBpfObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}
	defer objs.Close()

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching enter tracepoint: %w", err)
	}
	defer tpEnter.Close()

	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching exit tracepoint: %w", err)
	}
	defer tpExit.Close()

	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		return nil, fmt.Errorf("attaching lru_shrink tracepoint: %w", err)
	}
	defer tpLru.Close()

	time.Sleep(duration)
	return GetFadviseStats(&objs)
}

type DirectReclaimInfo struct {
	Comm string
	PID  uint32
}

func int8SliceToByte(s []int8) []byte {
	b := make([]byte, len(s))
	for i, v := range s {
		b[i] = byte(v)
	}
	return b
}

func GetDirectReclaimStats(objs *BpfObjects) (map[uint32]DirectReclaimInfo, error) {
	if objs == nil || objs.DirectReclaimMap == nil {
		return nil, fmt.Errorf("BPF objects or map not initialized")
	}

	result := make(map[uint32]DirectReclaimInfo)
	var key uint32
	var value BpfReclaimInfoT

	iter := objs.DirectReclaimMap.Iterate()
	for iter.Next(&key, &value) {
		result[key] = DirectReclaimInfo{
			PID:  key,
			Comm: string(bytes.Trim(int8SliceToByte(value.Comm[:]), "\x00")),
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterating direct reclaim map: %w", err)
	}

	return result, nil
}
