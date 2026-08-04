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

package sub

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/schema/consistency"
)

type stubStreamRegistry struct {
	schema.Stream
	streams []*databasev1.Stream
}

func (s *stubStreamRegistry) ListStream(_ context.Context, _ schema.ListOpt) ([]*databasev1.Stream, error) {
	return s.streams, nil
}

type stubIndexRuleRegistry struct {
	schema.IndexRule
	err   error
	rules []*databasev1.IndexRule
}

func (s *stubIndexRuleRegistry) ListIndexRule(_ context.Context, _ schema.ListOpt) ([]*databasev1.IndexRule, error) {
	return s.rules, s.err
}

type stubBindingRegistry struct {
	schema.IndexRuleBinding
	bindings []*databasev1.IndexRuleBinding
}

func (s *stubBindingRegistry) ListIndexRuleBinding(
	_ context.Context, _ schema.ListOpt,
) ([]*databasev1.IndexRuleBinding, error) {
	return s.bindings, nil
}

type stubNodeRegistry struct {
	byRole map[databasev1.Role][]*databasev1.Node
	err    error
	schema.Node
}

func (s *stubNodeRegistry) ListNode(_ context.Context, role databasev1.Role) ([]*databasev1.Node, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.byRole[role], nil
}

// consistencyRepo serves the registry reads buildRegistryFingerprints performs.
type consistencyRepo struct {
	*mockMetadataRepo
	streamRegistry  *stubStreamRegistry
	ruleRegistry    *stubIndexRuleRegistry
	bindingRegistry *stubBindingRegistry
	nodeRegistry    *stubNodeRegistry
	liaisonInfo     []*databasev1.LiaisonInfo
}

func (r *consistencyRepo) StreamRegistry() schema.Stream { return r.streamRegistry }

func (r *consistencyRepo) IndexRuleRegistry() schema.IndexRule { return r.ruleRegistry }

func (r *consistencyRepo) IndexRuleBindingRegistry() schema.IndexRuleBinding {
	return r.bindingRegistry
}

func (r *consistencyRepo) NodeRegistry() schema.Node { return r.nodeRegistry }

func (r *consistencyRepo) CollectLiaisonInfo(
	_ context.Context, _ string, _ bool,
) ([]*databasev1.LiaisonInfo, error) {
	return r.liaisonInfo, nil
}

func testGroup() *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "g"},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
	}
}

func testStream() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo"},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"service_id"}},
	}
}

func newConsistencyServer(t *testing.T, nodeCount int) *server {
	t.Helper()
	nodes := make([]*databasev1.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, &databasev1.Node{
			Metadata: &commonv1.Metadata{Name: "data-1"},
		})
	}
	repo := &consistencyRepo{
		mockMetadataRepo: &mockMetadataRepo{
			groupRegistry: &mockGroupRegistry{groups: []*commonv1.Group{testGroup()}},
		},
		streamRegistry:  &stubStreamRegistry{streams: []*databasev1.Stream{testStream()}},
		ruleRegistry:    &stubIndexRuleRegistry{},
		bindingRegistry: &stubBindingRegistry{},
		nodeRegistry: &stubNodeRegistry{byRole: map[databasev1.Role][]*databasev1.Node{
			databasev1.Role_ROLE_UNSPECIFIED: nil,
			databasev1.Role_ROLE_META:        nil,
			databasev1.Role_ROLE_DATA:        nodes,
			databasev1.Role_ROLE_LIAISON:     nil,
		}},
	}
	s := &server{
		log:           logger.GetLogger("test-consistency"),
		metadataRepo:  repo,
		schemaChecker: consistency.NewChecker(),
		registrySem:   make(chan struct{}, registryReadConcurrency),
	}
	return s
}

// healthyDataInfo builds what a node in agreement with the registry would report.
func healthyDataInfo(t *testing.T) []*databasev1.DataInfo {
	t.Helper()
	groupFP, err := consistency.Fingerprint(testGroup(), nil)
	require.NoError(t, err)
	streamFP, err := consistency.Fingerprint(testStream(), nil)
	require.NoError(t, err)
	return []*databasev1.DataInfo{{
		Node: &databasev1.Node{Metadata: &commonv1.Metadata{Name: "data-1"}},
		SchemaObjects: []*databasev1.ObjectSchemaState{
			{
				Kind: schema.KindGroup.String(), Name: "g",
				CacheFingerprint: groupFP, RuntimeFingerprint: groupFP,
			},
			{
				Kind: schema.KindStream.String(), Name: "foo",
				CacheFingerprint: streamFP, RuntimeFingerprint: streamFP,
			},
		},
	}}
}

func TestCheckSchemaConsistency_HealthyClusterIsConsistent(t *testing.T) {
	s := newConsistencyServer(t, 1)

	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), nil)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, got.GetStatus(),
		"a node whose fingerprints match the registry must not be reported")
	assert.Empty(t, got.GetIssues())
}

func TestCheckSchemaConsistency_StaleNodeReportedAfterTwoCycles(t *testing.T) {
	s := newConsistencyServer(t, 1)
	stale := healthyDataInfo(t)
	stale[0].SchemaObjects[1].CacheFingerprint = 42
	stale[0].SchemaObjects[1].RuntimeFingerprint = 42

	first, err := s.checkSchemaConsistency(context.Background(), testGroup(), stale, nil)
	require.NoError(t, err)
	second, err := s.checkSchemaConsistency(context.Background(), testGroup(), stale, nil)
	require.NoError(t, err)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, first.GetStatus(),
		"one round of disagreement may still be propagation in flight")
	require.Len(t, second.GetIssues(), 1)
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE, second.GetIssues()[0].GetType())
	assert.Equal(t, "foo", second.GetIssues()[0].GetName())
}

func TestCheckSchemaConsistency_NilCheckerYieldsNoVerdict(t *testing.T) {
	// Existing lifecycle tests construct a server without a checker; that path
	// must stay silent rather than claim consistency it never established.
	s := newConsistencyServer(t, 1)
	s.schemaChecker = nil

	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), nil)

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestCheckSchemaConsistency_MissingNodeIsUnknown(t *testing.T) {
	// Two data nodes on the roster but only one answered.
	s := newConsistencyServer(t, 2)

	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), nil)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus())
}

func TestInspectAll_AttachesSchemaConsistency(t *testing.T) {
	// Guards the wiring: checkSchemaConsistency being correct is useless if
	// inspectGroup never puts its verdict on the response.
	s := newConsistencyServer(t, 1)
	repo, ok := s.metadataRepo.(*consistencyRepo)
	require.True(t, ok)
	repo.dataInfoMap = map[string][]*databasev1.DataInfo{"g": healthyDataInfo(t)}

	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetGroups(), 1)
	verdict := resp.GetGroups()[0].GetSchemaConsistency()
	require.NotNil(t, verdict, "every inspected non-property group must carry a verdict")
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, verdict.GetStatus())
	// Schema now lives only in schema_consistency.objects; inspectGroup must strip
	// the per-node copies from data_info so schema is not duplicated on the wire.
	require.NotEmpty(t, verdict.GetObjects(), "the object table must carry the schema")
	for _, di := range resp.GetGroups()[0].GetDataInfo() {
		assert.Empty(t, di.GetSchemaObjects(), "data_info schema must be cleared after the check")
	}
}

func TestInspectAll_KeepsSchemaObjectsWhenNoVerdict(t *testing.T) {
	// When no verdict is produced (disabled checker, property group, or a
	// registry-build failure) the per-node schema was folded nowhere, so
	// inspectGroup must NOT strip it -- otherwise the fingerprints vanish.
	s := newConsistencyServer(t, 1)
	s.schemaChecker = nil // no verdict will be produced
	repo, ok := s.metadataRepo.(*consistencyRepo)
	require.True(t, ok)
	repo.dataInfoMap = map[string][]*databasev1.DataInfo{"g": healthyDataInfo(t)}

	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetGroups(), 1)
	assert.Nil(t, resp.GetGroups()[0].GetSchemaConsistency(), "a disabled checker yields no verdict")
	require.NotEmpty(t, resp.GetGroups()[0].GetDataInfo())
	assert.NotEmpty(t, resp.GetGroups()[0].GetDataInfo()[0].GetSchemaObjects(),
		"without a verdict the schema must stay on data_info, not be dropped")
}

func TestCheckSchemaConsistency_IgnoresDegradedEmptyNodeEntry(t *testing.T) {
	// A collection broadcast can return a degraded shell with no node identity
	// and no schema. Counting it would fabricate a MISSING_IN_CACHE divergence
	// against every registry object and pin the verdict at UNKNOWN forever; it
	// must be skipped so the healthy nodes read CONSISTENT.
	s := newConsistencyServer(t, 1)
	degraded := []*databasev1.LiaisonInfo{{Node: nil, SchemaObjects: nil}}

	// Two rounds so the checker's consecutive-cycle suppression clears.
	_, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), degraded)
	require.NoError(t, err)
	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), degraded)
	require.NoError(t, err)

	require.NotNil(t, got)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, got.GetStatus(),
		"a degraded empty-node broadcast shell must not force UNKNOWN")
	assert.Empty(t, got.GetIssues())
}

func TestCheckSchemaConsistency_KeepsUnnamedResponderWithObjects(t *testing.T) {
	// A responder whose objects arrived but whose identity stamp did not must
	// still count toward the quorum; dropping it would shrink the quorum into a
	// spurious UNKNOWN. The roster expects two nodes, so the data node plus an
	// unnamed liaison that reported healthy objects must reach CONSISTENT.
	s := newConsistencyServer(t, 2)
	groupFP, err := consistency.Fingerprint(testGroup(), nil)
	require.NoError(t, err)
	streamFP, err := consistency.Fingerprint(testStream(), nil)
	require.NoError(t, err)
	unnamed := []*databasev1.LiaisonInfo{{
		Node: nil, // identity missing, but real objects present
		SchemaObjects: []*databasev1.ObjectSchemaState{
			{Kind: schema.KindGroup.String(), Name: "g", CacheFingerprint: groupFP, RuntimeFingerprint: groupFP},
			{Kind: schema.KindStream.String(), Name: "foo", CacheFingerprint: streamFP, RuntimeFingerprint: streamFP},
		},
	}}

	_, err = s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), unnamed)
	require.NoError(t, err)
	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), unnamed)
	require.NoError(t, err)

	require.NotNil(t, got)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, got.GetStatus(),
		"an unnamed responder carrying real objects must count toward the quorum, not be dropped")
	assert.Empty(t, got.GetIssues())
}

func TestCheckSchemaConsistency_SkipsPropertyGroups(t *testing.T) {
	// PROPERTY groups are skipped by the whole inspection pipeline and no node
	// schemaRepo tracks them, so checking them would emit UNKNOWN every round.
	s := newConsistencyServer(t, 1)
	propertyGroup := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "g"},
		Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
	}

	got, err := s.checkSchemaConsistency(context.Background(), propertyGroup, nil, nil)

	require.NoError(t, err)
	assert.Nil(t, got, "a property group must carry no verdict at all")
}

func TestCheckSchemaConsistency_RegistryBuildFailureReturnsError(t *testing.T) {
	// When the registry read fails the verdict cannot be formed: the check must
	// return no verdict AND a non-nil error so the caller surfaces why, instead
	// of swallowing it into a log line.
	s := newConsistencyServer(t, 1)
	repo, ok := s.metadataRepo.(*consistencyRepo)
	require.True(t, ok)
	repo.ruleRegistry.err = errors.New("registry unavailable")

	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "registry unavailable")
	assert.Nil(t, got, "no verdict can be formed when the registry read failed")
}

func TestCheckSchemaConsistency_RosterFailureReturnsUnknownAndError(t *testing.T) {
	// A roster read failure is not fatal to the check -- the verdict degrades to
	// UNKNOWN via a forced shortfall -- but the error must still be returned so
	// the operator can see why the group never reaches CONSISTENT.
	s := newConsistencyServer(t, 1)
	repo, ok := s.metadataRepo.(*consistencyRepo)
	require.True(t, ok)
	repo.nodeRegistry.err = errors.New("node registry unavailable")

	got, err := s.checkSchemaConsistency(context.Background(), testGroup(), healthyDataInfo(t), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "node registry unavailable")
	require.NotNil(t, got, "a roster failure still yields a degraded verdict")
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus())
}

func TestInspectAll_SurfacesSchemaConsistencyError(t *testing.T) {
	// The registry-build failure must reach the response as an info.Errors entry,
	// not vanish into a warning. The schema also stays on data_info because no
	// verdict folded it into schema_consistency.objects.
	s := newConsistencyServer(t, 1)
	repo, ok := s.metadataRepo.(*consistencyRepo)
	require.True(t, ok)
	repo.dataInfoMap = map[string][]*databasev1.DataInfo{"g": healthyDataInfo(t)}
	repo.ruleRegistry.err = errors.New("registry unavailable")

	resp, err := s.InspectAll(context.Background(), &fodcv1.InspectAllRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetGroups(), 1)
	g := resp.GetGroups()[0]
	assert.Nil(t, g.GetSchemaConsistency(), "a registry-build failure yields no verdict")
	require.NotEmpty(t, g.GetErrors())
	last := g.GetErrors()[len(g.GetErrors())-1]
	assert.Contains(t, last, schemaConsistencyErrorPrefix)
	assert.Contains(t, last, "registry unavailable")
	require.NotEmpty(t, g.GetDataInfo())
	assert.NotEmpty(t, g.GetDataInfo()[0].GetSchemaObjects(),
		"without a verdict the schema must stay on data_info")
}

func TestRegistryBindingLookup_FiltersByCatalogNotValidityWindow(t *testing.T) {
	// An expired binding must still be returned: the node-side cache applies it,
	// so filtering here would report a divergence that does not exist.
	expired := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b1"},
		Subject:  &databasev1.Subject{Name: "foo", Catalog: commonv1.Catalog_CATALOG_STREAM},
		Rules:    []string{"r1"},
	}
	otherCatalog := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b2"},
		Subject:  &databasev1.Subject{Name: "foo", Catalog: commonv1.Catalog_CATALOG_MEASURE},
		Rules:    []string{"r2"},
	}
	l := registryBindingLookup{
		bindings: []*databasev1.IndexRuleBinding{expired, otherCatalog},
		catalog:  commonv1.Catalog_CATALOG_STREAM,
	}

	got := l.BindingsOf("g", "foo")

	require.Len(t, got, 1)
	assert.Equal(t, "b1", got[0].GetMetadata().GetName())
}
