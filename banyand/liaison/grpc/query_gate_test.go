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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// staticLocatorRevision returns a getLocatorRevision function backed by a fixed map.
// Groups present in revs return (rev, true); absent groups return (0, false).
func staticLocatorRevision(revs map[string]int64) func(string, string) (int64, bool) {
	return func(_, group string) (int64, bool) {
		rev, ok := revs[group]
		return rev, ok
	}
}

// advancingLocatorRevision returns a getLocatorRevision function that starts at
// initialRev for the given group and atomically advances to targetRev after delay.
func advancingLocatorRevision(group string, initialRev, targetRev int64, delay time.Duration) func(string, string) (int64, bool) {
	var mu sync.RWMutex
	current := initialRev
	go func() {
		time.Sleep(delay)
		mu.Lock()
		current = targetRev
		mu.Unlock()
	}()
	return func(_, g string) (int64, bool) {
		if g != group {
			return 0, false
		}
		mu.RLock()
		defer mu.RUnlock()
		return current, true
	}
}

// TestCheckQueryGate_EmptyMap_SkipsGateEntirely verifies that a nil or empty
// groupModRevisions map causes checkQueryGate to return (nil, false) without
// consulting the locator at all.
func TestCheckQueryGate_EmptyMap_SkipsGateEntirely(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1", "g2"},
		"s",
		nil,
		staticLocatorRevision(map[string]int64{"g1": 100}),
		50*time.Millisecond,
	)
	assert.Nil(t, statuses)
	assert.False(t, shortCircuit)
}

// TestCheckQueryGate_ZeroRevision_SkipsGroup verifies that a group present in the
// map with clientRev == 0 is skipped — it does not appear in the returned statuses.
func TestCheckQueryGate_ZeroRevision_SkipsGroup(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 0},
		staticLocatorRevision(map[string]int64{"g1": 100}),
		50*time.Millisecond,
	)
	assert.Nil(t, statuses)
	assert.False(t, shortCircuit)
}

// TestCheckQueryGate_Equal_ReturnsSucceedAndNoShortCircuit verifies that a client
// revision equal to the cached revision is accepted immediately with STATUS_SUCCEED
// and does not trigger short-circuit.
func TestCheckQueryGate_Equal_ReturnsSucceedAndNoShortCircuit(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 100},
		staticLocatorRevision(map[string]int64{"g1": 100}),
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, statuses["g1"])
	assert.False(t, shortCircuit)
}

// TestCheckQueryGate_ClientLower_ReturnsExpiredSchemaAndShortCircuits verifies that
// a client revision below the cached revision is rejected immediately with
// STATUS_EXPIRED_SCHEMA and the short-circuit flag is set.
func TestCheckQueryGate_ClientLower_ReturnsExpiredSchemaAndShortCircuits(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 50},
		staticLocatorRevision(map[string]int64{"g1": 100}),
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, statuses["g1"])
	assert.True(t, shortCircuit)
}

// TestCheckQueryGate_ClientHigher_WaitsAndReturnsNotAppliedOnTimeout verifies that
// a client revision above the cached revision causes checkQueryGate to wait up to
// maxWait and then return STATUS_SCHEMA_NOT_APPLIED with shortCircuit=true when the
// cache never catches up.
func TestCheckQueryGate_ClientHigher_WaitsAndReturnsNotAppliedOnTimeout(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 99999},
		staticLocatorRevision(map[string]int64{"g1": 100}),
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, statuses["g1"])
	assert.True(t, shortCircuit)
}

// TestCheckQueryGate_ClientHigher_SucceedsWhenCacheCatchesUp verifies that a client
// revision above the cache produces STATUS_SUCCEED with shortCircuit=false once the
// cache advances to the required revision within maxWait.
func TestCheckQueryGate_ClientHigher_SucceedsWhenCacheCatchesUp(t *testing.T) {
	getter := advancingLocatorRevision("g1", 100, 200, 20*time.Millisecond)
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 200},
		getter,
		500*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, statuses["g1"])
	assert.False(t, shortCircuit)
}

// TestCheckQueryGate_GroupNotInLocator_ReturnsNotFoundAndShortCircuits verifies that
// a group present in the request map but absent from the locator cache yields
// STATUS_NOT_FOUND and triggers short-circuit.
func TestCheckQueryGate_GroupNotInLocator_ReturnsNotFoundAndShortCircuits(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1"},
		"s",
		map[string]int64{"g1": 100},
		staticLocatorRevision(map[string]int64{}), // g1 absent from locator cache
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, statuses["g1"])
	assert.True(t, shortCircuit)
}

// TestCheckQueryGate_MixedGroups_ShortCircuitsWhenAnyGroupFails verifies that when
// multiple groups are gated and at least one fails (stale revision), the result map
// contains per-group statuses for all gated groups and shortCircuit is true.
func TestCheckQueryGate_MixedGroups_ShortCircuitsWhenAnyGroupFails(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1", "g2"},
		"s",
		map[string]int64{"g1": 50, "g2": 100},
		staticLocatorRevision(map[string]int64{"g1": 100, "g2": 100}),
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, statuses["g1"])
	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, statuses["g2"])
	assert.True(t, shortCircuit)
}

// TestCheckQueryGate_PartialCoverage_OnlyGatesGroupsInRevisionMap verifies that
// groups listed in the groups slice but absent from groupModRevisions are not gated
// and do not appear in the returned statuses map.
func TestCheckQueryGate_PartialCoverage_OnlyGatesGroupsInRevisionMap(t *testing.T) {
	statuses, shortCircuit := checkQueryGate(
		[]string{"g1", "g2"},
		"s",
		map[string]int64{"g1": 100}, // only g1 gated; g2 absent from map
		staticLocatorRevision(map[string]int64{"g1": 100, "g2": 200}),
		50*time.Millisecond,
	)
	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, statuses["g1"])
	_, g2Present := statuses["g2"]
	assert.False(t, g2Present, "g2 must not appear in statuses when absent from groupModRevisions")
	assert.False(t, shortCircuit)
}
