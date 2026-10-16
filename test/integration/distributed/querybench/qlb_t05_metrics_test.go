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

package querybench

import (
	"fmt"

	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
)

type qlbT05MetricsSnapshot struct {
	execution qlbT03MetricsSnapshot
	cache     liaisongrpc.PlanCacheStats
}

func qlbT05CacheSnapshot() liaisongrpc.PlanCacheStats {
	return liaisongrpc.BydbQLPlanCacheStats()
}

func qlbT05MetricsStart() (qlbT05MetricsSnapshot, error) {
	execution, err := qlbT03MetricsStart()
	if err != nil {
		return qlbT05MetricsSnapshot{}, fmt.Errorf("T05 metrics: %w", err)
	}
	return qlbT05MetricsSnapshot{execution: execution, cache: qlbT05CacheSnapshot()}, nil
}

func qlbT05MetricsFinish(before qlbT05MetricsSnapshot, shape string, samples, rows int) (map[string]uint64, []string, error) {
	executionName := "ql_plain_legacy"
	if shape == "typed_wide" {
		executionName = "ql_repeated_projection"
	}
	metrics, _, err := qlbT03MetricsFinish(before.execution, executionName, samples, rows)
	if err != nil {
		return nil, nil, err
	}
	cache := qlbT05CacheSnapshot()
	metrics["plan_cache_hits"] = cache.Hits - before.cache.Hits
	metrics["plan_cache_misses"] = cache.Misses - before.cache.Misses
	metrics["plan_cache_compiles"] = cache.Compiles - before.cache.Compiles
	metrics["plan_cache_evictions"] = cache.Evictions - before.cache.Evictions
	if cache.Entries < 0 || cache.EntryBytes < 0 || cache.RetainedBytes < 0 || cache.RetiredBytes < 0 || cache.InFlight < 0 ||
		cache.CompileReservedBytes < 0 || cache.Waiters < 0 || cache.UncachedBytes < 0 {
		return nil, nil, fmt.Errorf("T05 cache returned negative resource counters")
	}
	metrics["plan_cache_entries"] = uint64(cache.Entries)
	metrics["plan_cache_entry_bytes"] = uint64(cache.EntryBytes)
	metrics["plan_cache_retained_bytes"] = uint64(cache.RetainedBytes)
	metrics["plan_cache_retired_bytes"] = uint64(cache.RetiredBytes)
	metrics["plan_cache_inflight"] = uint64(cache.InFlight)
	metrics["plan_cache_compile_reserved_bytes"] = uint64(cache.CompileReservedBytes)
	metrics["plan_cache_waiters"] = uint64(cache.Waiters)
	metrics["plan_cache_uncached_bytes"] = uint64(cache.UncachedBytes)
	return metrics, []string{"storage_physical_scanned_rows", "total_parent_lease_charges"}, nil
}
