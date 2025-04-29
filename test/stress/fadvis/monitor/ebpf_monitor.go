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

package monitor

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/test/stress/fadvis/bpf"
	"github.com/cilium/ebpf/link"
)

type Monitor struct {
	objs *bpf.FadviseObjects
	tp   link.Link
}

func NewMonitor() (*Monitor, error) {
	// 1. 加载 BPF 对象
	objs := &bpf.FadviseObjects{}
	if err := bpf.LoadFadviseObjects(objs, nil); err != nil {
		return nil, fmt.Errorf("load bpf objs: %w", err)
	}
	// 2. 附到 exit tracepoint
	tp, err := link.Tracepoint("syscalls", "sys_exit_fadvise64", objs.TraceFadviseExit, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attach tp: %w", err)
	}
	return &Monitor{objs: objs, tp: tp}, nil
}

func (m *Monitor) Close() {
	if m.tp != nil {
		m.tp.Close()
	}
	if m.objs != nil {
		m.objs.Close()
	}
}

// ReadCounts 批量读取每个 PID 的调用次数
func (m *Monitor) ReadCounts() (map[uint32]uint64, error) {
	out := make(map[uint32]uint64)
	iter := m.objs.FadviseCalls.Iterate()
	var pid uint32
	var cnt uint64
	for iter.Next(&pid, &cnt) {
		out[pid] = cnt
	}
	return out, iter.Err()
}
