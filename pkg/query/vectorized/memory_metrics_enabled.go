//go:build querymetrics

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

package vectorized

import "sync/atomic"

// QueryMetricsEnabled reports whether test query counters are compiled in.
const QueryMetricsEnabled = true

var (
	liveReservedBytes atomic.Int64
	peakReservedBytes atomic.Int64
	leaseChargedBytes atomic.Uint64
)

func recordMemoryReservation(bytes int64) {
	live := liveReservedBytes.Add(bytes)
	for peak := peakReservedBytes.Load(); live > peak; peak = peakReservedBytes.Load() {
		if peakReservedBytes.CompareAndSwap(peak, live) {
			break
		}
	}
}

func recordMemoryRelease(bytes int64) { liveReservedBytes.Add(-bytes) }
func recordLeaseCharge(bytes uint64)  { leaseChargedBytes.Add(bytes) }

// QueryMemoryStats returns process-wide test memory accounting counters.
func QueryMemoryStats() MemoryCounters {
	return MemoryCounters{liveReservedBytes.Load(), peakReservedBytes.Load(), leaseChargedBytes.Load()}
}

// ResetQueryMemoryPeak starts an isolated benchmark observation when no reservation is live.
func ResetQueryMemoryPeak() bool {
	if liveReservedBytes.Load() != 0 {
		return false
	}
	peakReservedBytes.Store(0)
	return true
}
