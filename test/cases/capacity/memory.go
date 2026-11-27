// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package capacity

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"sync/atomic"
	"time"
)

const (
	heapObjectsMetric = "/memory/classes/heap/objects:bytes"
	// measureGCPercent keeps the heap close to what is live while a query runs, so its peak is not mostly garbage.
	measureGCPercent = 1
	measureRepeats   = 5
	sampleInterval   = 500 * time.Microsecond
)

// queryAllowance is what a query may spend of a query pool: an eighth of the pool, less the fixed admission overhead.
func queryAllowance(poolBytes uint64) uint64 {
	const fixedOverhead = 1 << 20
	if poolBytes/8 <= fixedOverhead {
		return 0
	}
	return poolBytes/8 - fixedOverhead
}

func heapObjects(sample []metrics.Sample) uint64 {
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// MeasureHeap runs fn once, with the garbage collector set tight, and returns how far the heap of the process grew above
// where it stood before, at its peak while fn ran. The nodes of an in-process cluster and the client share the heap, so the
// result is the memory of all of them.
func MeasureHeap(fn func()) uint64 {
	previous := debug.SetGCPercent(measureGCPercent)
	defer debug.SetGCPercent(previous)
	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	sample := []metrics.Sample{{Name: heapObjectsMetric}}
	base := heapObjects(sample)
	var peak atomic.Uint64
	peak.Store(base)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		reader := []metrics.Sample{{Name: heapObjectsMetric}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			if value := heapObjects(reader); value > peak.Load() {
				peak.Store(value)
			}
			time.Sleep(sampleInterval)
		}
	}()
	fn()
	close(stop)
	<-done
	if peak.Load() < base {
		return 0
	}
	return peak.Load() - base
}

// MedianHeap is the median of MeasureHeap over measureRepeats runs of fn.
func MedianHeap(fn func()) uint64 {
	growth := make([]uint64, 0, measureRepeats)
	for i := 0; i < measureRepeats; i++ {
		growth = append(growth, MeasureHeap(fn))
	}
	sort.Slice(growth, func(i, j int) bool { return growth[i] < growth[j] })
	return growth[len(growth)/2]
}
