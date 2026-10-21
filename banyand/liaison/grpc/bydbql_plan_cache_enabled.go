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

package grpc

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

var compiledCacheObservation struct {
	caches map[*compiledPlanCache]struct{}
	sync.Mutex
	stats PlanCacheStats
}
var compiledCacheHooks atomic.Pointer[PlanCacheTestHooks]

// BydbQLPlanCacheStats returns process-wide instrumented compilation/cache observations.
func BydbQLPlanCacheStats() PlanCacheStats {
	compiledCacheObservation.Lock()
	defer compiledCacheObservation.Unlock()
	return compiledCacheObservation.stats
}

func recordPlanCacheStats(before, after PlanCacheStats) {
	compiledCacheObservation.Lock()
	defer compiledCacheObservation.Unlock()
	target := &compiledCacheObservation.stats
	target.Hits += after.Hits - before.Hits
	target.Misses += after.Misses - before.Misses
	target.Compiles += after.Compiles - before.Compiles
	target.Stale += after.Stale - before.Stale
	target.Bypass += after.Bypass - before.Bypass
	target.Oversize += after.Oversize - before.Oversize
	target.Coalesced += after.Coalesced - before.Coalesced
	target.Errors += after.Errors - before.Errors
	target.Evictions += after.Evictions - before.Evictions
	target.CanceledCompiles += after.CanceledCompiles - before.CanceledCompiles
	target.Entries += after.Entries - before.Entries
	target.EntryBytes += after.EntryBytes - before.EntryBytes
	target.RetainedBytes += after.RetainedBytes - before.RetainedBytes
	target.RetiredBytes += after.RetiredBytes - before.RetiredBytes
	target.InFlight += after.InFlight - before.InFlight
	target.CompileReservedBytes += after.CompileReservedBytes - before.CompileReservedBytes
	target.Waiters += after.Waiters - before.Waiters
	target.UncachedBytes += after.UncachedBytes - before.UncachedBytes
}

func registerCompiledPlanCache(cache *compiledPlanCache) {
	compiledCacheObservation.Lock()
	defer compiledCacheObservation.Unlock()
	if compiledCacheObservation.caches == nil {
		compiledCacheObservation.caches = make(map[*compiledPlanCache]struct{})
	}
	compiledCacheObservation.caches[cache] = struct{}{}
}

func unregisterCompiledPlanCache(cache *compiledPlanCache) {
	compiledCacheObservation.Lock()
	delete(compiledCacheObservation.caches, cache)
	compiledCacheObservation.Unlock()
}

func activeCompiledPlanCaches() []*compiledPlanCache {
	compiledCacheObservation.Lock()
	defer compiledCacheObservation.Unlock()
	result := make([]*compiledPlanCache, 0, len(compiledCacheObservation.caches))
	for cache := range compiledCacheObservation.caches {
		result = append(result, cache)
	}
	return result
}

func bydbQLPlanCacheHooks() PlanCacheTestHooks {
	if hooks := compiledCacheHooks.Load(); hooks != nil {
		return *hooks
	}
	return PlanCacheTestHooks{}
}

// SetBydbQLPlanCacheHooksForTest installs serialized hooks around real cache work.
func SetBydbQLPlanCacheHooksForTest(hooks PlanCacheTestHooks) func() {
	old := compiledCacheHooks.Swap(&hooks)
	return func() { compiledCacheHooks.Store(old) }
}

// ClearBydbQLPlanCachesForTest evicts lookups while preserving live plan handles.
func ClearBydbQLPlanCachesForTest() {
	for _, cache := range activeCompiledPlanCaches() {
		cache.clear()
	}
}

// ContextWithPlanCachePrincipalForTest supplies an authenticated test principal.
// Authorization still evaluates the actual request snapshot and lowered resources.
func ContextWithPlanCachePrincipalForTest(ctx context.Context, principal auth.Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// BumpBydbQLMeasureGenerationForTest delivers a local watch replacement, including
// same/lower revisions, without changing persisted registry schemas.
func BumpBydbQLMeasureGenerationForTest(group, name string, revision *int64) func() {
	type savedMeasure struct {
		repo    *entityRepo
		measure *databasev1.Measure
	}
	saved := make([]savedMeasure, 0)
	seen := make(map[*entityRepo]bool)
	for _, cache := range activeCompiledPlanCaches() {
		if cache.source == nil || cache.source.measures == nil || seen[cache.source.measures] {
			continue
		}
		repo := cache.source.measures
		seen[repo] = true
		measure, ok := repo.getMeasure(identity{group: group, name: name})
		if !ok {
			continue
		}
		saved = append(saved, savedMeasure{repo: repo, measure: measure})
		replacement := proto.Clone(measure).(*databasev1.Measure)
		if revision != nil {
			replacement.Metadata.ModRevision = *revision
		}
		repo.OnAddOrUpdate(measureAddEventForPlanCache(replacement))
	}
	return func() {
		for _, item := range saved {
			item.repo.OnAddOrUpdate(measureAddEventForPlanCache(item.measure))
		}
	}
}

// WithholdBydbQLMeasureSchemaForTest simulates a missing local schema object while
// retaining its locator. QL must read the real registry and bypass plan publication.
func WithholdBydbQLMeasureSchemaForTest(group, name string) func() {
	type savedMeasure struct {
		repo    *entityRepo
		measure *databasev1.Measure
	}
	saved := make([]savedMeasure, 0)
	seen := make(map[*entityRepo]bool)
	for _, cache := range activeCompiledPlanCaches() {
		if cache.source == nil || cache.source.measures == nil || seen[cache.source.measures] {
			continue
		}
		repo := cache.source.measures
		seen[repo] = true
		repo.Lock()
		id := identity{group: group, name: name}
		if measure, found := repo.measureMap[id]; found {
			saved = append(saved, savedMeasure{repo: repo, measure: measure})
			delete(repo.measureMap, id)
		}
		repo.Unlock()
	}
	return func() {
		for _, item := range saved {
			item.repo.OnAddOrUpdate(measureAddEventForPlanCache(item.measure))
		}
	}
}

func measureAddEventForPlanCache(measure *databasev1.Measure) schema.Metadata {
	return schema.Metadata{TypeMeta: schema.TypeMeta{Kind: schema.KindMeasure}, Spec: measure}
}
