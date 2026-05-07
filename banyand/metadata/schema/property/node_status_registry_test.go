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

package property

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	metaschema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

// fakeRevRepo emulates a per-service pkg/schema.schemaRepo for the registry
// pointer-identity tests.
type fakeRevRepo struct {
	resources map[metaschema.Kind]map[string]int64
}

func newFakeRevRepo() *fakeRevRepo {
	return &fakeRevRepo{
		resources: make(map[metaschema.Kind]map[string]int64),
	}
}

// put records (kind, group, name, rev) in the fake. group is parameterized
// even though every current caller passes "g1" so the helper stays usable
// when future tests exercise cross-group routing.
//
//nolint:unparam // group is generic by design; test fixtures happen to share "g1".
func (f *fakeRevRepo) put(kind metaschema.Kind, group, name string, rev int64) {
	if f.resources[kind] == nil {
		f.resources[kind] = make(map[string]int64)
	}
	f.resources[kind][group+"/"+name] = rev
}

func (f *fakeRevRepo) ResourceRevision(kind metaschema.Kind, group, name string) (int64, bool) {
	if m := f.resources[kind]; m != nil {
		if rev, ok := m[group+"/"+name]; ok {
			return rev, true
		}
	}
	return 0, false
}

func (f *fakeRevRepo) IsAbsent(kind metaschema.Kind, group, name string) bool {
	_, ok := f.ResourceRevision(kind, group, name)
	return !ok
}

// TestExecutor_ResolvesGroupsViaSharedSchemaRepo asserts the cluster barrier's
// per-node probe reads the same schemaRepo instance the executor consults via
// LoadGroup. Pointer identity is the load-bearing Phase 2 §Step 2.5 invariant:
// the registry returns a positive answer for (KindGroup, group, name) only
// when the same repo holds the group, so the barrier and the executor cannot
// disagree on the same (group, ModRevision) pair.
func TestExecutor_ResolvesGroupsViaSharedSchemaRepo(t *testing.T) {
	const targetRev int64 = 42
	repo := newFakeRevRepo()
	repo.put(metaschema.KindGroup, "g1", "g1", targetRev)
	repo.put(metaschema.KindMeasure, "g1", "m1", targetRev)

	reg := registry.NewNodeRepoRegistry()
	registry.MaybeRegister(reg,
		metaschema.KindGroup|metaschema.KindMeasure|metaschema.KindIndexRule|metaschema.KindIndexRuleBinding,
		repo,
	)

	srv := NewNodeSchemaStatusServerWithRegistry(
		func() *schemaCache { return nil },
		func() *registry.NodeRepoRegistry { return reg },
	)

	revResp, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "group", Group: "g1", Name: "g1"},
			{Kind: "measure", Group: "g1", Name: "m1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, revResp.GetRevisions(), 2)
	for _, kr := range revResp.GetRevisions() {
		assert.True(t, kr.GetPresent(), "%s present in registry-routed lookup", kr.GetKey().GetName())
		assert.Equal(t, targetRev, kr.GetModRevision())
	}

	// Pointer identity: the value returned to the barrier and the value the
	// executor would resolve via LoadGroup are read from the SAME repo
	// pointer. Verifying via fake means changing the fake's internal state
	// is observable from both lookups (no parallel cache to drift).
	repo.put(metaschema.KindGroup, "g1", "g1", targetRev+1)
	revResp2, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "group", Group: "g1", Name: "g1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, revResp2.GetRevisions(), 1)
	assert.Equal(t, targetRev+1, revResp2.GetRevisions()[0].GetModRevision(),
		"a repo-side mutation is immediately visible through the registry — same pointer")
}

// TestNodeStatus_GetMaxRevision_ReadsCacheOnly asserts the new GetMaxRevision
// contract: the response equals the schemaCache's notifiedModRevision and is
// independent of the registry. The cache observes every catalog's events, so
// it is the correct global watermark — symmetric with the receiving liaison's
// selfName probe at barrier_cluster.go:354-360. Per-key executor-cache gating
// stays on the registry via GetKeyRevisions / GetAbsentKeys.
func TestNodeStatus_GetMaxRevision_ReadsCacheOnly(t *testing.T) {
	c := newSchemaCache()
	c.notifiedModRevision = 100

	// A registered repo with per-catalog state must NOT shift GetMaxRevision —
	// the cache value alone wins.
	repo := newFakeRevRepo()
	repo.put(metaschema.KindMeasure, "g1", "m1", 50)
	reg := registry.NewNodeRepoRegistry()
	registry.MaybeRegister(reg, metaschema.KindMeasure, repo)

	srv := NewNodeSchemaStatusServerWithRegistry(
		func() *schemaCache { return c },
		func() *registry.NodeRepoRegistry { return reg },
	)

	resp, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(100), resp.GetMaxModRevision(),
		"GetMaxRevision = schemaCache.notifiedModRevision; registry contributes nothing")
}

// TestNodeStatus_GetMaxRevision_NilCacheReturnsZero pins the boundary case:
// when the cache provider returns nil, the response carries 0 regardless of
// what the registry holds.
func TestNodeStatus_GetMaxRevision_NilCacheReturnsZero(t *testing.T) {
	repo := newFakeRevRepo()
	repo.put(metaschema.KindStream, "g1", "s1", 60)
	reg := registry.NewNodeRepoRegistry()
	registry.MaybeRegister(reg, metaschema.KindStream, repo)

	srv := NewNodeSchemaStatusServerWithRegistry(
		func() *schemaCache { return nil },
		func() *registry.NodeRepoRegistry { return reg },
	)

	resp, err := srv.GetMaxRevision(context.Background(), &clusterv1.GetMaxRevisionRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.GetMaxModRevision())
}

// TestNodeStatus_KindRouting_TopNFallsBackToSchemaCache asserts that kinds
// the registry does NOT track (TopNAggregation, Property) fall through to the
// schemaCache lookup unchanged. The Phase 2 §Step 2.5 invariant covers only
// the kinds schemaRepo holds; TopN/Property barrier reads remain on the
// schemaCache.notifiedModRevision watermark.
func TestNodeStatus_KindRouting_TopNFallsBackToSchemaCache(t *testing.T) {
	idTopN, entryTopN := makeEntry(metaschema.KindTopNAggregation, "topn1", 33)
	c := newSchemaCache()
	c.entries[idTopN] = entryTopN
	c.notifiedModRevision = 33

	repo := newFakeRevRepo()
	repo.put(metaschema.KindMeasure, "g1", "m1", 33)
	reg := registry.NewNodeRepoRegistry()
	registry.MaybeRegister(reg, metaschema.KindMeasure, repo)

	srv := NewNodeSchemaStatusServerWithRegistry(
		func() *schemaCache { return c },
		func() *registry.NodeRepoRegistry { return reg },
	)

	resp, err := srv.GetKeyRevisions(context.Background(), &clusterv1.GetKeyRevisionsRequest{
		Keys: []*schemav1.SchemaKey{
			{Kind: "top_n_aggregation", Group: testGroup, Name: "topn1"},
			{Kind: "measure", Group: "g1", Name: "m1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetRevisions(), 2)

	assert.True(t, resp.Revisions[0].GetPresent(), "TopN routes through schemaCache and is present at watermark")
	assert.Equal(t, int64(33), resp.Revisions[0].GetModRevision())

	assert.True(t, resp.Revisions[1].GetPresent(), "measure routes through registry and is present in repo")
	assert.Equal(t, int64(33), resp.Revisions[1].GetModRevision())
}
