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

package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	pkglogger "github.com/apache/skywalking-banyandb/pkg/logger"
)

// testLogger returns a logger suitable for unit tests in this package.
func testLogger() *pkglogger.Logger {
	_ = pkglogger.Init(pkglogger.Logging{Env: "dev", Level: "warn"})
	return pkglogger.GetLogger("test", "schema-cache")
}

// noopIndexListener satisfies IndexListener and discards all index updates.
type noopIndexListener struct{}

func (noopIndexListener) OnIndexUpdate(_ []*databasev1.IndexRule) {}

// stubSupplier satisfies ResourceSchemaSupplier for tests that only need
// storeResource to succeed without real DB or metadata interactions.
type stubSupplier struct{}

func (stubSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (stubSupplier) OpenResource(_ Resource) (IndexListener, error) {
	return noopIndexListener{}, nil
}

// newTestSchemaRepo creates a minimal schemaRepo suitable for unit tests.
// The zero values of all sync.Map and atomic fields are ready to use.
func newTestSchemaRepo() *schemaRepo {
	return &schemaRepo{resourceSchemaSupplier: stubSupplier{}}
}

// buildMeasure returns a *databasev1.Measure with the specified name and mod_revision.
// Group is fixed to "g" because every test in this file uses the same group; varying
// it would only add noise without changing what's being asserted.
func buildMeasure(name string, modRevision int64) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: "g", Name: name, ModRevision: modRevision},
	}
}

// TestDeleteResource_RevisionGuard_RejectsStaleDelete verifies that an EventDelete
// carrying a DeleteRevision lower than the stored resource's mod_revision is ignored,
// leaving the cache entry intact (A7).
func TestDeleteResource_RevisionGuard_RejectsStaleDelete(t *testing.T) {
	sr := newTestSchemaRepo()
	require.NoError(t, sr.storeResource(buildMeasure("m", 100)))

	sr.deleteResource(MetadataEvent{
		Metadata:       buildMeasure("m", 100),
		DeleteRevision: 50,
		Typ:            EventDelete,
		Kind:           EventKindResource,
	})

	_, present := sr.resourceMap.Load("g/m")
	assert.True(t, present, "stale delete (DeleteRevision=50 < stored rev=100) must not remove the entry")
}

// TestDeleteResource_RevisionGuard_AcceptsFreshDelete verifies that an EventDelete
// carrying a DeleteRevision greater than the stored resource's mod_revision removes
// the cache entry (A7).
func TestDeleteResource_RevisionGuard_AcceptsFreshDelete(t *testing.T) {
	sr := newTestSchemaRepo()
	require.NoError(t, sr.storeResource(buildMeasure("m", 100)))

	sr.deleteResource(MetadataEvent{
		Metadata:       buildMeasure("m", 100),
		DeleteRevision: 150,
		Typ:            EventDelete,
		Kind:           EventKindResource,
	})

	_, present := sr.resourceMap.Load("g/m")
	assert.False(t, present, "fresh delete (DeleteRevision=150 >= stored rev=100) must remove the entry")
}

// TestLatestModRevision_Monotonic verifies that LatestModRevision advances monotonically
// with ascending stores and does not regress when a lower revision is applied (A8).
func TestLatestModRevision_Monotonic(t *testing.T) {
	sr := newTestSchemaRepo()
	assert.Equal(t, int64(0), sr.LatestModRevision(), "watermark must start at zero")

	require.NoError(t, sr.storeResource(buildMeasure("m1", 10)))
	assert.Equal(t, int64(10), sr.LatestModRevision())

	require.NoError(t, sr.storeResource(buildMeasure("m2", 30)))
	assert.Equal(t, int64(30), sr.LatestModRevision())

	// A new key at a lower revision must not regress the watermark.
	require.NoError(t, sr.storeResource(buildMeasure("m3", 20)))
	assert.Equal(t, int64(30), sr.LatestModRevision(), "stale store (rev=20) must not lower the watermark from 30")
}

// TestLatestModRevision_AcrossKinds verifies that LatestModRevision tracks the global
// maximum across Resource, IndexRule, and IndexRuleBinding stores (A8).
func TestLatestModRevision_AcrossKinds(t *testing.T) {
	sr := newTestSchemaRepo()

	require.NoError(t, sr.storeResource(buildMeasure("m1", 10)))
	assert.Equal(t, int64(10), sr.LatestModRevision())

	sr.storeIndexRule(&databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g", Name: "idx1", ModRevision: 50},
		Tags:     []string{"tag1"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	})
	assert.Equal(t, int64(50), sr.LatestModRevision(), "IndexRule store (rev=50) must advance the watermark")

	sr.storeIndexRuleBinding(&databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "irb1", ModRevision: 40},
		Rules:    []string{"idx1"},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m1"},
	})
	assert.Equal(t, int64(50), sr.LatestModRevision(), "IndexRuleBinding at rev=40 must not lower the watermark from 50")
}

// TestResourceRevision_PresentAndAbsent verifies that ResourceRevision returns the
// stored mod_revision and ok=true for a present resource, and (0, false) for an absent one (A8).
func TestResourceRevision_PresentAndAbsent(t *testing.T) {
	sr := newTestSchemaRepo()
	require.NoError(t, sr.storeResource(buildMeasure("m", 77)))

	rev, ok := sr.ResourceRevision(schema.KindMeasure, "g", "m")
	assert.True(t, ok)
	assert.Equal(t, int64(77), rev)

	rev, ok = sr.ResourceRevision(schema.KindMeasure, "g", "missing")
	assert.False(t, ok)
	assert.Equal(t, int64(0), rev)
}

// TestIsAbsent_AfterDelete verifies that IsAbsent returns false while the resource
// is cached and true after a valid delete removes it from the cache (A7, A8).
func TestIsAbsent_AfterDelete(t *testing.T) {
	sr := newTestSchemaRepo()
	require.NoError(t, sr.storeResource(buildMeasure("m", 100)))

	assert.False(t, sr.IsAbsent(schema.KindMeasure, "g", "m"), "resource must not be absent immediately after store")

	sr.deleteResource(MetadataEvent{
		Metadata:       buildMeasure("m", 100),
		DeleteRevision: 100,
		Typ:            EventDelete,
		Kind:           EventKindResource,
	})

	assert.True(t, sr.IsAbsent(schema.KindMeasure, "g", "m"), "resource must be absent after a valid delete")
}

// fakeDB satisfies pkg/schema.DB for unit tests that only exercise group caching.
type fakeDB struct{}

func (fakeDB) Close() error                           { return nil }
func (fakeDB) UpdateOptions(_ *commonv1.ResourceOpts) {}
func (fakeDB) Drop() error                            { return nil }

// fakeResourceSupplier is a programmable ResourceSupplier whose OpenDB returns
// a configurable error or DB. openCalls counts invocations so retry tests can
// assert that initBySchema actually re-attempted OpenDB.
type fakeResourceSupplier struct {
	openErr   error
	openCalls int
}

func (s *fakeResourceSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (s *fakeResourceSupplier) OpenResource(_ Resource) (IndexListener, error) {
	return noopIndexListener{}, nil
}

func (s *fakeResourceSupplier) OpenDB(_ *commonv1.Group) (DB, error) {
	s.openCalls++
	if s.openErr != nil {
		return nil, s.openErr
	}
	return fakeDB{}, nil
}

func (s *fakeResourceSupplier) ResolveResourceOpts(groupSchema *commonv1.Group) *commonv1.ResourceOpts {
	return groupSchema.GetResourceOpts()
}

func buildGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
	}
}

// TestInitBySchema_FailedOpenDB_LeavesGroupUninit asserts that when OpenDB
// fails, the group is left in an un-initialized state so a later retry can
// reattempt OpenDB. Previously the schema was stored before OpenDB ran, which
// caused isInit() to return true even though db was nil.
func TestInitBySchema_FailedOpenDB_LeavesGroupUninit(t *testing.T) {
	openErr := errors.New("boom")
	supplier := &fakeResourceSupplier{openErr: openErr}
	g := newGroup(nil, nil, supplier)

	err := g.initBySchema(buildGroup("g1"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, openErr))
	assert.False(t, g.isInit(), "group must not be initialized after a failed OpenDB")
	assert.Nil(t, g.GetSchema(), "groupSchema must not be stored after a failed OpenDB")
	assert.Nil(t, g.SupplyTSDB(), "SupplyTSDB must be nil after a failed OpenDB")
}

// TestInitBySchema_PortableGroupOK asserts that a portable group (nil
// resourceSupplier) stores the schema and is considered initialized without
// invoking OpenDB.
func TestInitBySchema_PortableGroupOK(t *testing.T) {
	g := newGroup(nil, nil, nil)
	require.NoError(t, g.initBySchema(buildGroup("portable")))
	assert.True(t, g.isInit(), "portable group must be initialized after initBySchema")
	assert.Nil(t, g.SupplyTSDB(), "portable group must have no underlying tsdb")
}

// TestInitBySchema_SuccessStoresSchema asserts the success path stores schema
// after OpenDB returns.
func TestInitBySchema_SuccessStoresSchema(t *testing.T) {
	supplier := &fakeResourceSupplier{}
	g := newGroup(nil, nil, supplier)
	require.NoError(t, g.initBySchema(buildGroup("g1")))
	assert.True(t, g.isInit())
	assert.Equal(t, 1, supplier.openCalls)
}

// TestInitGroup_RetryAfterFailedOpenDB asserts that initGroup retries OpenDB on
// the second pass when the first attempt left the group un-initialized — the
// fix for the silent-fail bug where a half-broken group survived in the cache.
func TestInitGroup_RetryAfterFailedOpenDB(t *testing.T) {
	openErr := errors.New("transient")
	supplier := &fakeResourceSupplier{openErr: openErr}
	repo := &schemaRepo{
		l:                testLogger(),
		resourceSupplier: supplier,
	}

	gs := buildGroup("g1")
	_, firstErr := repo.initGroup(gs)
	require.Error(t, firstErr)
	require.Equal(t, 1, supplier.openCalls)

	cached, ok := repo.getGroup("g1")
	require.True(t, ok)
	require.False(t, cached.isInit())

	supplier.openErr = nil
	g, secondErr := repo.initGroup(gs)
	require.NoError(t, secondErr)
	require.NotNil(t, g)
	assert.True(t, g.isInit())
	assert.Equal(t, 2, supplier.openCalls, "OpenDB must be re-attempted when the cached group is not initialized")
}

// TestInitGroup_AlreadyInit_NoReopen asserts that an already-initialized group
// is returned without reinvoking OpenDB.
func TestInitGroup_AlreadyInit_NoReopen(t *testing.T) {
	supplier := &fakeResourceSupplier{}
	repo := &schemaRepo{
		l:                testLogger(),
		resourceSupplier: supplier,
	}
	gs := buildGroup("g1")
	_, err := repo.initGroup(gs)
	require.NoError(t, err)
	require.Equal(t, 1, supplier.openCalls)
	_, err = repo.initGroup(gs)
	require.NoError(t, err)
	assert.Equal(t, 1, supplier.openCalls, "already-initialized group must not re-invoke OpenDB")
}

// recordingListener captures the rules from the most recent OnIndexUpdate so a test can
// assert whether a delete re-indexed the resource.
type recordingListener struct {
	rules *[]*databasev1.IndexRule
}

func (r recordingListener) OnIndexUpdate(rules []*databasev1.IndexRule) { *r.rules = rules }

type recordingSupplier struct {
	lis recordingListener
}

func (recordingSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (s recordingSupplier) OpenResource(_ Resource) (IndexListener, error) { return s.lis, nil }

// TestDeleteIndexRule_ReindexesBoundResource asserts that deleting an index rule
// re-indexes resources bound to it so the deleted rule is dropped from their in-memory
// index. The add path already refreshed; the delete path previously left it stale.
func TestDeleteIndexRule_ReindexesBoundResource(t *testing.T) {
	var applied []*databasev1.IndexRule
	sr := &schemaRepo{
		l:                      testLogger(),
		resourceSchemaSupplier: recordingSupplier{lis: recordingListener{rules: &applied}},
	}
	require.NoError(t, sr.storeResource(buildMeasure("m", 1)))
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Group: "g", Name: "r", ModRevision: 1}}
	sr.storeIndexRule(rule)
	sr.storeIndexRuleBinding(&databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b", ModRevision: 1},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"},
		Rules:    []string{"r"},
	})
	require.Len(t, applied, 1, "binding must apply the rule to the resource")

	require.NoError(t, sr.processEvent(context.Background(), MetadataEvent{
		Typ: EventDelete, Kind: EventKindIndexRule, Metadata: rule,
	}))
	require.Empty(t, applied, "deleting the index rule must re-index the resource without it")
}

// TestDeleteIndexRuleBinding_ReindexesSubject asserts that deleting a binding re-indexes
// the subject so the now-unbound rule stops being applied.
func TestDeleteIndexRuleBinding_ReindexesSubject(t *testing.T) {
	var applied []*databasev1.IndexRule
	sr := &schemaRepo{
		l:                      testLogger(),
		resourceSchemaSupplier: recordingSupplier{lis: recordingListener{rules: &applied}},
	}
	require.NoError(t, sr.storeResource(buildMeasure("m", 1)))
	sr.storeIndexRule(&databasev1.IndexRule{Metadata: &commonv1.Metadata{Group: "g", Name: "r", ModRevision: 1}})
	binding := &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b", ModRevision: 1},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"},
		Rules:    []string{"r"},
	}
	sr.storeIndexRuleBinding(binding)
	require.Len(t, applied, 1)

	require.NoError(t, sr.processEvent(context.Background(), MetadataEvent{
		Typ: EventDelete, Kind: EventKindIndexRuleBinding, Metadata: binding,
	}))
	require.Empty(t, applied, "deleting the binding must re-index the subject without the unbound rule")
}

// TestDeleteGroup_PurgesChildCaches asserts that deleting a group purges its child
// resource/index-rule/binding entries so nothing dangles after the group is gone.
func TestDeleteGroup_PurgesChildCaches(t *testing.T) {
	sr := newTestSchemaRepo()
	// A portable (nil supplier) group so deleteGroup proceeds past its lookup and
	// close() is a no-op.
	sr.groupMap.Store("g", newGroup(nil, nil, nil))
	require.NoError(t, sr.storeResource(buildMeasure("m", 1)))
	sr.storeIndexRule(&databasev1.IndexRule{Metadata: &commonv1.Metadata{Group: "g", Name: "r", ModRevision: 1}})
	sr.storeIndexRuleBinding(&databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b", ModRevision: 1},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: "m"},
		Rules:    []string{"r"},
	})

	require.NoError(t, sr.deleteGroup(&commonv1.Metadata{Name: "g"}))

	_, hasResource := sr.resourceMap.Load("g/m")
	_, hasRule := sr.indexRuleMap.Load("g/r")
	_, hasForward := sr.bindingForwardMap.Load("g/m")
	_, hasBackward := sr.bindingBackwardMap.Load("g/r")
	require.False(t, hasResource, "resource entry must be purged")
	require.False(t, hasRule, "index rule entry must be purged")
	require.False(t, hasForward, "forward binding entry must be purged")
	require.False(t, hasBackward, "backward binding entry must be purged")
}
