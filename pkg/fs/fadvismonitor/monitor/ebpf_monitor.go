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
	"unsafe"

	"github.com/cilium/ebpf/link"

	"github.com/apache/skywalking-banyandb/pkg/fs/fadvismonitor/bpf"
)

type Monitor struct {
	objs    *bpf.BpfObjects
	tpEnter link.Link
	tpExit  link.Link
	tpLru   link.Link
}

func NewMonitor() (*Monitor, error) {
	objs := &bpf.BpfObjects{}
	if err := bpf.LoadBpfObjects(objs, nil); err != nil {
		return nil, fmt.Errorf("load bpf objs: %w", err)
	}

	tpEnter, err := link.Tracepoint("syscalls", "sys_enter_fadvise64", objs.TraceEnterFadvise64, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attach enter tp: %w", err)
	}
	tpExit, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceExitFadvise64, nil)
	if err != nil {
		tpEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("attach exit tp: %w", err)
	}
	tpLru, err := link.Tracepoint("vmscan", "mm_vmscan_lru_shrink_inactive", objs.TraceLruShrinkInactive, nil)
	if err != nil {
		tpEnter.Close()
		tpExit.Close()
		objs.Close()
		return nil, fmt.Errorf("attach lru tp: %w", err)
	}

	return &Monitor{
		objs:    objs,
		tpEnter: tpEnter,
		tpExit:  tpExit,
		tpLru:   tpLru,
	}, nil
}

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
	if m.objs != nil {
		m.objs.Close()
	}
}

func (m *Monitor) ReadCounts() (map[uint32]uint64, error) {
	out := make(map[uint32]uint64)
	iter := m.objs.FadviseStatsMap.Iterate()
	var pid uint32
	var stats bpf.BpfFadviseStatsT
	for iter.Next(&pid, &stats) {
		out[pid] = stats.AdviceDontneed
	}
	return out, iter.Err()
}

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

type ReclaimInfo struct {
	Comm string
	PID  uint32
}

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
