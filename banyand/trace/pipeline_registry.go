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

// Finalization-sampling threshold defaults (see .omc/plans/trace-finalize-sampling.md
// DD2). They bound whether a cooled segment warrants another finalize round.
const (
	// finalizeFloorBytesDefault is the absolute minimum newly-arrived uncompressed
	// span bytes since the last round that justifies re-finalizing a segment. Below
	// this a trickle of late writes is left unsampled (an accepted miss).
	finalizeFloorBytesDefault uint64 = 8 << 20 // 8 MiB
	// finalizeRatioDefault gates large segments: re-finalize only when new unsampled
	// bytes reach this fraction of the segment's total, so a proportionally-tiny
	// addition to a big segment does not trigger a full rewrite.
	finalizeRatioDefault = 0.10
	// finalizeMaxRoundsDefault is the hard per-segment lifetime cap on finalize
	// rounds; after this the segment is marked terminal and never re-scanned.
	finalizeMaxRoundsDefault = 8
)

// localFinalizeGraceRegistry stores per-group finalize_grace in nanoseconds as set
// by reconcilePipeline. A zero value means "use option.finalizeGraceDefault". It
// mirrors localMergeGraceRegistry.
var localFinalizeGraceRegistry = struct {
	m  map[string]int64
	mu sync.RWMutex
}{m: make(map[string]int64)}

// setFinalizeGraceForGroup stores graceNs for group. graceNs == 0 removes the entry.
func setFinalizeGraceForGroup(group string, graceNs int64) {
	localFinalizeGraceRegistry.mu.Lock()
	if graceNs > 0 {
		localFinalizeGraceRegistry.m[group] = graceNs
	} else {
		delete(localFinalizeGraceRegistry.m, group)
	}
	localFinalizeGraceRegistry.mu.Unlock()
}

// lookupFinalizeGrace returns the per-group finalize_grace in nanoseconds, or 0 if
// none is configured (caller falls back to option.finalizeGraceDefault).
func lookupFinalizeGrace(group string) int64 {
	localFinalizeGraceRegistry.mu.RLock()
	defer localFinalizeGraceRegistry.mu.RUnlock()
	return localFinalizeGraceRegistry.m[group]
}

// finalizeConfig holds the per-group finalize threshold knobs. A zero value is
// never stored directly; lookupFinalizeConfig fills unset fields with defaults.
type finalizeConfig struct {
	floorBytes uint64  // absolute floor; 0 => finalizeFloorBytesDefault
	ratio      float64 // fraction of segment total; <=0 => finalizeRatioDefault
	cooldownNs int64   // min ns between rounds; 0 => the group's finalize_grace
	maxRounds  int     // hard lifetime cap; <=0 => finalizeMaxRoundsDefault
}

// localFinalizeConfigRegistry stores per-group finalize threshold overrides. The
// proto carries no override fields today (v1), so reconcilePipeline stores defaults;
// tests may inject custom values via setFinalizeConfigForGroup.
var localFinalizeConfigRegistry = struct {
	m  map[string]finalizeConfig
	mu sync.RWMutex
}{m: make(map[string]finalizeConfig)}

// listFinalizeGroups returns the groups that have finalization sampling enabled (a
// finalize config entry exists). The background finalize scanner iterates these.
func listFinalizeGroups() []string {
	localFinalizeConfigRegistry.mu.RLock()
	defer localFinalizeConfigRegistry.mu.RUnlock()
	if len(localFinalizeConfigRegistry.m) == 0 {
		return nil
	}
	groups := make([]string, 0, len(localFinalizeConfigRegistry.m))
	for group := range localFinalizeConfigRegistry.m {
		groups = append(groups, group)
	}
	return groups
}

// setFinalizeConfigForGroup stores cfg for group. A nil cfg removes the entry.
func setFinalizeConfigForGroup(group string, cfg *finalizeConfig) {
	localFinalizeConfigRegistry.mu.Lock()
	if cfg != nil {
		localFinalizeConfigRegistry.m[group] = *cfg
	} else {
		delete(localFinalizeConfigRegistry.m, group)
	}
	localFinalizeConfigRegistry.mu.Unlock()
}

// lookupFinalizeConfig returns the group's finalize threshold config with any unset
// field filled from the package defaults. graceNs is the resolved finalize_grace for
// the group and is used as the cooldown when no explicit cooldown is configured.
func lookupFinalizeConfig(group string, graceNs int64) finalizeConfig {
	localFinalizeConfigRegistry.mu.RLock()
	cfg := localFinalizeConfigRegistry.m[group]
	localFinalizeConfigRegistry.mu.RUnlock()
	if cfg.floorBytes == 0 {
		cfg.floorBytes = finalizeFloorBytesDefault
	}
	if cfg.ratio <= 0 {
		cfg.ratio = finalizeRatioDefault
	}
	if cfg.cooldownNs <= 0 {
		cfg.cooldownNs = graceNs
	}
	if cfg.maxRounds <= 0 {
		cfg.maxRounds = finalizeMaxRoundsDefault
	}
	return cfg
}

// namedSampler pairs a sampler instance with its stable identity. The identity is the
// plugin name plus everything that selects which code runs: the config hash AND the
// plugin reference (path, symbol, ABI version). reconcilePipeline compares this identity
// to decide whether a re-apply is redundant, so all fields that change the loaded plugin
// must be captured here — otherwise a path/symbol/ABI swap under an unchanged config JSON
// would be skipped and keep the stale plugin.
type namedSampler struct {
	sampler    sdk.Sampler
	name       string
	configHash string
	path       string
	symbol     string
	abiVersion uint32
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

// nameHash is the stable identity of one registered sampler: its plugin name, config
// hash, and plugin reference (path, symbol, ABI version). reconcilePipeline compares the
// desired list against currentSamplerIdentity to skip redundant re-applies; including the
// plugin reference ensures a path/symbol/ABI change (even with an unchanged config JSON)
// triggers a rebuild.
type nameHash struct {
	name       string
	configHash string
	path       string
	symbol     string
	abiVersion uint32
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
		out[idx] = nameHash{
			name:       item.name,
			configHash: item.configHash,
			path:       item.path,
			symbol:     item.symbol,
			abiVersion: item.abiVersion,
		}
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
