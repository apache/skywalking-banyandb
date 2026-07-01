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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// stubSampler is a minimal sdk.Sampler for registry unit tests.
type stubSampler struct{ id string }

func (s *stubSampler) Kind() sdk.Kind                                { return sdk.KindSampler }
func (s *stubSampler) Project() sdk.Projection                       { return sdk.Projection{} }
func (s *stubSampler) Decide(_ *sdk.TraceBatch) (sdk.Verdict, error) { return sdk.Verdict{}, nil }
func (s *stubSampler) Close() error                                  { return nil }

// cleanRegistry resets the shared registry and restores it after the test,
// so individual tests do not bleed state into each other.
func cleanRegistry(t *testing.T) {
	t.Helper()
	localSamplerRegistry.mu.Lock()
	saved := localSamplerRegistry.m
	localSamplerRegistry.m = make(map[string][]namedSampler)
	localSamplerRegistry.mu.Unlock()
	t.Cleanup(func() {
		localSamplerRegistry.mu.Lock()
		localSamplerRegistry.m = saved
		localSamplerRegistry.mu.Unlock()
	})
}

func TestRegistryReplaceThenLookup(t *testing.T) {
	cleanRegistry(t)
	const group = "test-group"

	s1 := &stubSampler{id: "a"}
	s2 := &stubSampler{id: "b"}

	set := []namedSampler{
		{name: "plugin-a", sampler: s1},
		{name: "plugin-b", sampler: s2},
	}
	replaceSamplersForGroup(group, set)

	got := lookupSamplers(group)
	require.Equal(t, []sdk.Sampler{s1, s2}, got, "lookupSamplers must return the new set in declared order")

	// Mutating the returned snapshot must not affect the registry.
	got[0] = &stubSampler{id: "z"}
	require.Equal(t, []sdk.Sampler{s1, s2}, lookupSamplers(group), "snapshot is independent of registry")
}

func TestRegistryReplaceOverwritesPreviousSet(t *testing.T) {
	cleanRegistry(t)
	const group = "test-group"

	s1 := &stubSampler{id: "old"}
	replaceSamplersForGroup(group, []namedSampler{{name: "old", sampler: s1}})

	s2 := &stubSampler{id: "new"}
	replaceSamplersForGroup(group, []namedSampler{{name: "new", sampler: s2}})

	got := lookupSamplers(group)
	require.Equal(t, []sdk.Sampler{s2}, got, "replace must overwrite the previous set entirely")
}

func TestRegistryRemoveThenLookupReturnsNil(t *testing.T) {
	cleanRegistry(t)
	const group = "test-group"

	s1 := &stubSampler{id: "a"}
	replaceSamplersForGroup(group, []namedSampler{{name: "plugin-a", sampler: s1}})
	require.NotNil(t, lookupSamplers(group))

	removeSamplersForGroup(group)
	require.Nil(t, lookupSamplers(group), "lookupSamplers must return nil after remove")
}

func TestRegistryRemoveNonExistentGroupIsNoop(t *testing.T) {
	cleanRegistry(t)
	// Must not panic.
	removeSamplersForGroup("no-such-group")
	require.Nil(t, lookupSamplers("no-such-group"))
}

func TestRegistryRegisterAndDeregister(t *testing.T) {
	cleanRegistry(t)
	const group = "test-group"

	s1 := &stubSampler{id: "a"}
	s2 := &stubSampler{id: "b"}

	dereg1 := registerSampler(group, s1)
	registerSampler(group, s2)

	got := lookupSamplers(group)
	require.Equal(t, []sdk.Sampler{s1, s2}, got)

	// Deregister s1; s2 must remain.
	dereg1()
	got = lookupSamplers(group)
	require.Equal(t, []sdk.Sampler{s2}, got, "deregister closure must remove only the target sampler")
}

// TestRegistryConcurrency is a -race-safe concurrency test: many goroutines
// call replace/remove/lookupSamplers concurrently. The test asserts no data
// race (enforced by -race) and that the final state is deterministic.
func TestRegistryConcurrency(t *testing.T) {
	cleanRegistry(t)
	const group = "race-group"
	const workers = 20
	const iters = 50

	s := &stubSampler{id: "x"}
	set := []namedSampler{{name: "plugin-x", sampler: s}}

	var wg sync.WaitGroup
	wg.Add(workers * 3)

	// Replace workers.
	for range workers {
		go func() {
			defer wg.Done()
			for range iters {
				replaceSamplersForGroup(group, set)
			}
		}()
	}

	// Remove workers.
	for range workers {
		go func() {
			defer wg.Done()
			for range iters {
				removeSamplersForGroup(group)
			}
		}()
	}

	// Reader workers.
	for range workers {
		go func() {
			defer wg.Done()
			for range iters {
				_ = lookupSamplers(group)
			}
		}()
	}

	wg.Wait()

	// After all removes and replaces, the registry contains either the last
	// replace or nothing — both are valid; what matters is no race and that
	// lookupSamplers never returns a torn slice.
	result := lookupSamplers(group)
	require.True(t, result == nil || len(result) == 1, "final state must be nil or a single-element slice, got %v", result)
}
