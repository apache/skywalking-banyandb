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

package trace

import (
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// localMergeGraceRegistry stores per-group merge_grace in nanoseconds as set by
// reconcilePipeline. A zero value means "use option.mergeGraceDefault".
var localMergeGraceRegistry = struct {
	m  map[string]int64
	mu sync.RWMutex
}{m: make(map[string]int64)}

// setMergeGraceForGroup stores graceNs for group. graceNs == 0 removes the entry.
func setMergeGraceForGroup(group string, graceNs int64) {
	localMergeGraceRegistry.mu.Lock()
	if graceNs > 0 {
		localMergeGraceRegistry.m[group] = graceNs
	} else {
		delete(localMergeGraceRegistry.m, group)
	}
	localMergeGraceRegistry.mu.Unlock()
}

// lookupMergeGrace returns the per-group merge_grace in nanoseconds, or 0 if
// no per-group grace is configured (caller falls back to mergeGraceDefault).
func lookupMergeGrace(group string) int64 {
	localMergeGraceRegistry.mu.RLock()
	defer localMergeGraceRegistry.mu.RUnlock()
	return localMergeGraceRegistry.m[group]
}

// namedSampler pairs a sampler instance with its stable identity and config hash.
// name is the plugin name within the group's plugins list; configHash is the hash
// of the marshaled SamplerPlugin.config bytes, used by the config-aware loader cache.
type namedSampler struct {
	sampler    sdk.Sampler
	name       string
	configHash string
}

// localSamplerRegistry maps group → ordered named samplers for in-process, flag-gated
// activation of the in-merge filter hook. It is consulted ONLY when
// option.nativePipelineEnabled is true. The dynamic watch path (KindGroup
// OnAddOrUpdate) populates this registry at runtime; unit tests may register
// samplers directly for fast in-process coverage.
var localSamplerRegistry = struct {
	m  map[string][]namedSampler
	mu sync.RWMutex
}{m: make(map[string][]namedSampler)}

// replaceSamplersForGroup atomically swaps the entire sampler set for group.
// The caller builds the new set off-lock; this function copies the slice and
// stores it under a single Lock. Close is never called on retired instances (R4).
func replaceSamplersForGroup(group string, set []namedSampler) {
	cp := make([]namedSampler, len(set))
	copy(cp, set)
	localSamplerRegistry.mu.Lock()
	localSamplerRegistry.m[group] = cp
	localSamplerRegistry.mu.Unlock()
}

// removeSamplersForGroup atomically removes the entire sampler set for group.
// Close is never called on retired instances (R4).
func removeSamplersForGroup(group string) {
	localSamplerRegistry.mu.Lock()
	delete(localSamplerRegistry.m, group)
	localSamplerRegistry.mu.Unlock()
}

// registerSampler registers s under group and returns a deregister closure.
// Callers must invoke the returned function (e.g., via t.Cleanup) to remove
// the sampler when the test ends.
func registerSampler(group string, s sdk.Sampler) func() {
	localSamplerRegistry.mu.Lock()
	localSamplerRegistry.m[group] = append(localSamplerRegistry.m[group], namedSampler{sampler: s})
	localSamplerRegistry.mu.Unlock()
	return func() {
		localSamplerRegistry.mu.Lock()
		defer localSamplerRegistry.mu.Unlock()
		samplers := localSamplerRegistry.m[group]
		for idx, existing := range samplers {
			if existing.sampler == s {
				localSamplerRegistry.m[group] = append(samplers[:idx], samplers[idx+1:]...)
				break
			}
		}
		if len(localSamplerRegistry.m[group]) == 0 {
			delete(localSamplerRegistry.m, group)
		}
	}
}

// nameHash is the stable identity of one registered sampler: its plugin name and
// the hash of its marshaled config. reconcilePipeline compares the desired list
// against currentSamplerIdentity to skip redundant re-applies.
type nameHash struct {
	name       string
	configHash string
}

// currentSamplerIdentity returns the (name, configHash) identity of group's
// registered samplers in declared order, or nil if none.
func currentSamplerIdentity(group string) []nameHash {
	localSamplerRegistry.mu.RLock()
	defer localSamplerRegistry.mu.RUnlock()
	ns := localSamplerRegistry.m[group]
	if len(ns) == 0 {
		return nil
	}
	out := make([]nameHash, len(ns))
	for idx, item := range ns {
		out[idx] = nameHash{name: item.name, configHash: item.configHash}
	}
	return out
}

// lookupSamplers returns a snapshot of the registered samplers for group in declared
// order, or nil if none. The snapshot is safe for the caller to hold across a merge
// without being torn by concurrent registry mutations.
func lookupSamplers(group string) []sdk.Sampler {
	localSamplerRegistry.mu.RLock()
	defer localSamplerRegistry.mu.RUnlock()
	ns := localSamplerRegistry.m[group]
	if len(ns) == 0 {
		return nil
	}
	out := make([]sdk.Sampler, len(ns))
	for idx, item := range ns {
		out[idx] = item.sampler
	}
	return out
}
