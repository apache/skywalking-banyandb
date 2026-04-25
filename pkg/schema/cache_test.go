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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

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
