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
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
)

// Mock cache helpers follow.

// staticBarrierCache implements barrierCacheReader with fixed data — no advancing.
type staticBarrierCache struct {
	keys           map[string]int64
	maxModRevision int64
}

func (c *staticBarrierCache) GetMaxModRevision() int64 { return c.maxModRevision }
func (c *staticBarrierCache) GetKeyModRevision(propID string) (int64, bool) {
	rev, ok := c.keys[propID]
	return rev, ok
}

// advancingBarrierCache implements barrierCacheReader where individual fields can
// be updated concurrently (used to simulate cache catch-up).
type advancingBarrierCache struct {
	keys   map[string]int64
	maxRev int64
	mu     sync.RWMutex
}

func newAdvancingCache(maxRev int64, keys map[string]int64) *advancingBarrierCache {
	copied := make(map[string]int64, len(keys))
	maps.Copy(copied, keys)
	return &advancingBarrierCache{maxRev: maxRev, keys: copied}
}

func (c *advancingBarrierCache) GetMaxModRevision() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxRev
}

func (c *advancingBarrierCache) GetKeyModRevision(propID string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rev, ok := c.keys[propID]
	return rev, ok
}

func (c *advancingBarrierCache) setMaxRev(rev int64) {
	c.mu.Lock()
	c.maxRev = rev
	c.mu.Unlock()
}

func (c *advancingBarrierCache) addKey(propID string, rev int64) {
	c.mu.Lock()
	c.keys[propID] = rev
	c.mu.Unlock()
}

func (c *advancingBarrierCache) removeKey(propID string) {
	c.mu.Lock()
	delete(c.keys, propID)
	c.mu.Unlock()
}

// schemaKey is a convenience constructor.
func schemaKey(kind, group, name string) *schemav1.SchemaKey {
	return &schemav1.SchemaKey{Kind: kind, Group: group, Name: name}
}

// AwaitRevisionApplied tests follow.

// TestBarrier_AwaitRevisionApplied_AlreadyAtRevision verifies that when the
// cache is already at or above the requested min_revision, Applied=true is
// returned immediately without any polling.
func TestBarrier_AwaitRevisionApplied_AlreadyAtRevision(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{maxModRevision: 100} })
	resp, rpcErr := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// TestBarrier_AwaitRevisionApplied_NeverReaches_ReturnsFalse verifies that when
// the cache never advances to the required revision, Applied=false is returned
// after the timeout elapses.
func TestBarrier_AwaitRevisionApplied_NeverReaches_ReturnsFalse(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{maxModRevision: 50} })
	resp, rpcErr := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 99999,
		Timeout:     durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.False(t, resp.GetApplied())
}

// TestBarrier_AwaitRevisionApplied_CacheAdvances_ReturnsTrue verifies that when
// the cache advances to the required revision within the timeout window, Applied=true
// is returned as soon as the advancement is detected.
func TestBarrier_AwaitRevisionApplied_CacheAdvances_ReturnsTrue(t *testing.T) {
	cache := newAdvancingCache(50, nil)
	svc := newBarrierService(func() barrierCacheReader { return cache })
	go func() {
		time.Sleep(20 * time.Millisecond)
		cache.setMaxRev(200)
	}()
	resp, rpcErr := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 200,
		Timeout:     durationpb.New(500 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// TestBarrier_AwaitRevisionApplied_ContextCancelled_ReturnsFalse verifies that
// when the caller's context is canceled before the revision is reached, the RPC
// returns Applied=false without error.
func TestBarrier_AwaitRevisionApplied_ContextCancelled_ReturnsFalse(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{maxModRevision: 1} })
	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	resp, rpcErr := svc.AwaitRevisionApplied(cancelCtx, &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 99999,
		Timeout:     durationpb.New(500 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.False(t, resp.GetApplied())
}

// AwaitSchemaApplied tests follow.

// TestBarrier_AwaitSchemaApplied_AllKeysPresent_ReturnsTrue verifies that when
// all requested schema keys are already in the cache, Applied=true is returned immediately.
func TestBarrier_AwaitSchemaApplied_AllKeysPresent_ReturnsTrue(t *testing.T) {
	cache := &staticBarrierCache{keys: map[string]int64{"stream_g/s": 10}}
	svc := newBarrierService(func() barrierCacheReader { return cache })
	resp, rpcErr := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// TestBarrier_AwaitSchemaApplied_MissingKey_TimesOut_ReturnsFalse verifies that
// when a required key is absent from the cache, Applied=false is returned with
// the missing key in the Laggards list after the timeout.
func TestBarrier_AwaitSchemaApplied_MissingKey_TimesOut_ReturnsFalse(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{keys: map[string]int64{}} })
	resp, rpcErr := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.False(t, resp.GetApplied())
	require.Len(t, resp.GetLaggards(), 1)
	assert.Len(t, resp.GetLaggards()[0].GetMissingKeys(), 1)
}

// TestBarrier_AwaitSchemaApplied_TooManyKeys_ReturnsInvalidArgument verifies that
// requests with more than barrierMaxKeys entries are rejected with InvalidArgument.
func TestBarrier_AwaitSchemaApplied_TooManyKeys_ReturnsInvalidArgument(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{keys: map[string]int64{}} })
	keys := make([]*schemav1.SchemaKey, barrierMaxKeys+1)
	for i := range keys {
		keys[i] = schemaKey("stream", "g", "s")
	}
	_, rpcErr := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    keys,
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.Error(t, rpcErr)
	st, _ := status.FromError(rpcErr)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// TestBarrier_AwaitSchemaApplied_KeyAppearsInTime_ReturnsTrue verifies that when
// the missing key appears in the cache before the timeout, Applied=true is returned.
func TestBarrier_AwaitSchemaApplied_KeyAppearsInTime_ReturnsTrue(t *testing.T) {
	cache := newAdvancingCache(0, map[string]int64{})
	svc := newBarrierService(func() barrierCacheReader { return cache })
	go func() {
		time.Sleep(20 * time.Millisecond)
		cache.addKey("stream_g/s", 10)
	}()
	resp, rpcErr := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(500 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// AwaitSchemaDeleted tests follow.

// TestBarrier_AwaitSchemaDeleted_AllKeysAbsent_ReturnsTrue verifies that when all
// requested keys are already absent from the cache, Applied=true is returned immediately.
func TestBarrier_AwaitSchemaDeleted_AllKeysAbsent_ReturnsTrue(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{keys: map[string]int64{}} })
	resp, rpcErr := svc.AwaitSchemaDeleted(context.Background(), &schemav1.AwaitSchemaDeletedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// TestBarrier_AwaitSchemaDeleted_KeyPresent_TimesOut_ReturnsFalse verifies that
// when a key is still in the cache at timeout, Applied=false is returned with the
// key in the Laggards list.
func TestBarrier_AwaitSchemaDeleted_KeyPresent_TimesOut_ReturnsFalse(t *testing.T) {
	cache := &staticBarrierCache{keys: map[string]int64{"stream_g/s": 10}}
	svc := newBarrierService(func() barrierCacheReader { return cache })
	resp, rpcErr := svc.AwaitSchemaDeleted(context.Background(), &schemav1.AwaitSchemaDeletedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.False(t, resp.GetApplied())
	require.Len(t, resp.GetLaggards(), 1)
	assert.Len(t, resp.GetLaggards()[0].GetStillPresentKeys(), 1)
}

// TestBarrier_AwaitSchemaDeleted_TooManyKeys_ReturnsInvalidArgument verifies
// that requests with more than barrierMaxKeys entries are rejected.
func TestBarrier_AwaitSchemaDeleted_TooManyKeys_ReturnsInvalidArgument(t *testing.T) {
	svc := newBarrierService(func() barrierCacheReader { return &staticBarrierCache{keys: map[string]int64{}} })
	keys := make([]*schemav1.SchemaKey, barrierMaxKeys+1)
	for i := range keys {
		keys[i] = schemaKey("stream", "g", "s")
	}
	_, rpcErr := svc.AwaitSchemaDeleted(context.Background(), &schemav1.AwaitSchemaDeletedRequest{
		Keys:    keys,
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.Error(t, rpcErr)
	st, _ := status.FromError(rpcErr)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// TestBarrier_AwaitSchemaDeleted_KeyRemovedInTime_ReturnsTrue verifies that when
// the key is removed from the cache within the timeout, Applied=true is returned.
func TestBarrier_AwaitSchemaDeleted_KeyRemovedInTime_ReturnsTrue(t *testing.T) {
	cache := newAdvancingCache(0, map[string]int64{"stream_g/s": 10})
	svc := newBarrierService(func() barrierCacheReader { return cache })
	go func() {
		time.Sleep(20 * time.Millisecond)
		cache.removeKey("stream_g/s")
	}()
	resp, rpcErr := svc.AwaitSchemaDeleted(context.Background(), &schemav1.AwaitSchemaDeletedRequest{
		Keys:    []*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		Timeout: durationpb.New(500 * time.Millisecond),
	})
	require.NoError(t, rpcErr)
	assert.True(t, resp.GetApplied())
}

// Pure function tests follow.

// TestBarrierBackoff_GrowsByFactor verifies the backoff grows by barrierGrowthFactor each step.
func TestBarrierBackoff_GrowsByFactor(t *testing.T) {
	d := barrierInitInterval // 10ms
	next := barrierBackoff(d)
	expected := time.Duration(float64(d) * barrierGrowthFactor)
	assert.Equal(t, expected, next)
}

// TestBarrierBackoff_CapsAtMaxInterval verifies the backoff never exceeds barrierMaxInterval.
func TestBarrierBackoff_CapsAtMaxInterval(t *testing.T) {
	// Feed a very large duration and verify it is capped.
	result := barrierBackoff(10 * time.Second)
	assert.Equal(t, barrierMaxInterval, result)
}

// TestBarrierDeadlineDuration_NilInput_ReturnsDefault verifies that a nil timeout proto
// returns barrierDefaultTimeout — large enough to cover normal schema propagation rather
// than the poll-interval cap.
func TestBarrierDeadlineDuration_NilInput_ReturnsDefault(t *testing.T) {
	assert.Equal(t, barrierDefaultTimeout, barrierDeadlineDuration(nil))
}

// TestBarrierDeadlineDuration_ZeroInput_ReturnsDefault verifies that a zero duration
// proto returns barrierDefaultTimeout for the same reason as the nil case.
func TestBarrierDeadlineDuration_ZeroInput_ReturnsDefault(t *testing.T) {
	assert.Equal(t, barrierDefaultTimeout, barrierDeadlineDuration(durationpb.New(0)))
}

// TestBarrierDeadlineDuration_PositiveInput_ReturnsExact verifies that a positive
// timeout is passed through unchanged.
func TestBarrierDeadlineDuration_PositiveInput_ReturnsExact(t *testing.T) {
	want := 2 * time.Second
	assert.Equal(t, want, barrierDeadlineDuration(durationpb.New(want)))
}

// TestSchemaKeyToPropID_Stream verifies that a stream key is encoded as
// "stream_<group>/<name>".
func TestSchemaKeyToPropID_Stream(t *testing.T) {
	key := schemaKey("stream", "my-group", "my-stream")
	assert.Equal(t, "stream_my-group/my-stream", schemaKeyToPropID(key))
}

// TestSchemaKeyToPropID_Measure verifies that a measure key is encoded as
// "measure_<group>/<name>".
func TestSchemaKeyToPropID_Measure(t *testing.T) {
	key := schemaKey("measure", "grp", "m1")
	assert.Equal(t, "measure_grp/m1", schemaKeyToPropID(key))
}

// TestSchemaKeyToPropID_Group verifies that a group key (no group field) is
// encoded as "group_<name>".
func TestSchemaKeyToPropID_Group(t *testing.T) {
	key := schemaKey("group", "", "g1")
	assert.Equal(t, "group_g1", schemaKeyToPropID(key))
}

// TestProtoKindToString_CompoundKinds verifies that underscore_case proto kind strings
// are normalised to their schema.Kind.String() equivalents.
func TestProtoKindToString_CompoundKinds(t *testing.T) {
	cases := []struct {
		proto string
		want  string
	}{
		{"index_rule", "indexRule"},
		{"index_rule_binding", "indexRuleBinding"},
		{"top_n_aggregation", "topNAggregation"},
	}
	for _, tc := range cases {
		t.Run(tc.proto, func(t *testing.T) {
			assert.Equal(t, tc.want, protoKindToString(tc.proto))
		})
	}
}

// TestProtoKindToString_SimpleKinds verifies that simple kinds (stream, measure,
// trace, group, property) pass through unchanged.
func TestProtoKindToString_SimpleKinds(t *testing.T) {
	for _, kind := range []string{"stream", "measure", "trace", "group", "property", "node"} {
		t.Run(kind, func(t *testing.T) {
			assert.Equal(t, kind, protoKindToString(kind))
		})
	}
}

// TestBarrier_CollectMissingKeys_PartialPresence verifies that only absent or
// below-minRevision keys appear in the missing list.
func TestBarrier_CollectMissingKeys_PartialPresence(t *testing.T) {
	cache := &staticBarrierCache{
		keys: map[string]int64{
			"stream_g/s1": 10, // present, above minRev 5
			// "stream_g/s2" absent
		},
	}
	svc := newBarrierService(func() barrierCacheReader { return cache })
	keys := []*schemav1.SchemaKey{
		schemaKey("stream", "g", "s1"),
		schemaKey("stream", "g", "s2"),
	}
	missing := svc.collectMissingKeys(keys, []int64{5, 1})
	require.Len(t, missing, 1)
	assert.Equal(t, "s2", missing[0].GetName())
}

// TestBarrier_CollectMissingKeys_BelowMinRevision verifies that a key present in
// the cache but below the required minRevision is reported as missing.
func TestBarrier_CollectMissingKeys_BelowMinRevision(t *testing.T) {
	cache := &staticBarrierCache{keys: map[string]int64{"stream_g/s": 5}} // present at rev 5, but minRev = 10
	svc := newBarrierService(func() barrierCacheReader { return cache })
	missing := svc.collectMissingKeys(
		[]*schemav1.SchemaKey{schemaKey("stream", "g", "s")},
		[]int64{10},
	)
	require.Len(t, missing, 1, "key at rev 5 with minRev 10 must be reported as missing")
}

// TestBarrier_CollectPresentKeys_FiltersAbsent verifies that only keys still
// present in the cache are returned.
func TestBarrier_CollectPresentKeys_FiltersAbsent(t *testing.T) {
	cache := &staticBarrierCache{
		keys: map[string]int64{
			"stream_g/s1": 10,
			// "stream_g/s2" absent
		},
	}
	svc := newBarrierService(func() barrierCacheReader { return cache })
	keys := []*schemav1.SchemaKey{
		schemaKey("stream", "g", "s1"),
		schemaKey("stream", "g", "s2"),
	}
	present := svc.collectPresentKeys(keys)
	require.Len(t, present, 1)
	assert.Equal(t, "s1", present[0].GetName())
}
