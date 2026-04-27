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

package property_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestCreate_StampsCreatedAt verifies that CreateMeasure stamps created_at equal to
// updated_at when the caller does not supply a created_at value (Step 1.3 / A3).
func TestCreate_StampsCreatedAt(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	_, createErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, createErr)

	got, getErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getErr)
	require.NotNil(t, got.GetCreatedAt(), "created_at must be set after CreateMeasure")
	require.NotNil(t, got.GetUpdatedAt(), "updated_at must be set after CreateMeasure")
	assert.Equal(t, got.GetUpdatedAt().AsTime().UnixNano(), got.GetCreatedAt().AsTime().UnixNano(),
		"created_at must equal updated_at on initial create")
}

// TestUpdate_PreservesCreatedAt verifies that UpdateMeasure preserves the original
// created_at while advancing updated_at (Step 1.3 / A3).
func TestUpdate_PreservesCreatedAt(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	_, createErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, createErr)

	initial, getInitialErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getInitialErr)
	createdAt := initial.GetCreatedAt()
	require.NotNil(t, createdAt, "created_at must be set after CreateMeasure")

	// Advance time so the new updated_at is strictly greater than created_at.
	time.Sleep(2 * time.Millisecond)

	// Add a new field — a real change that must pass through the equality checker.
	initial.Fields = append(initial.Fields, &databasev1.FieldSpec{
		Name:              "count",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
	})
	_, updateErr := reg.UpdateMeasure(ctx, initial)
	require.NoError(t, updateErr)

	updated, getUpdatedErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getUpdatedErr)
	require.NotNil(t, updated.GetCreatedAt(), "created_at must still be set after UpdateMeasure")
	assert.Equal(t, createdAt.AsTime().UnixNano(), updated.GetCreatedAt().AsTime().UnixNano(),
		"created_at must be unchanged by UpdateMeasure")
	assert.Greater(t, updated.GetUpdatedAt().AsTime().UnixNano(), updated.GetCreatedAt().AsTime().UnixNano(),
		"updated_at must advance beyond created_at after an update")
}

// TestValidateMeasureUpdate_EntityChangeFails verifies that UpdateMeasure rejects
// an entity change and returns an error containing "entity", while leaving the
// stored schema's ModRevision unchanged (Step 1.3 / validate).
func TestValidateMeasureUpdate_EntityChangeFails(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	_, createErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, createErr)

	got, getErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getErr)
	originalModRev := got.GetMetadata().GetModRevision()

	// Add "endpoint" to the tag family so validate.Measure accepts the new entity,
	// but validateMeasureUpdate rejects it (entity definition changed).
	got.TagFamilies[0].Tags = append(got.TagFamilies[0].Tags, &databasev1.TagSpec{
		Name: "endpoint",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	})
	got.Entity = &databasev1.Entity{TagNames: []string{"service_name", "endpoint"}}
	_, updateErr := reg.UpdateMeasure(ctx, got)
	require.Error(t, updateErr, "UpdateMeasure must fail when entity changes")
	assert.Contains(t, updateErr.Error(), "entity", "error must mention 'entity'")

	// The stored schema must be completely unchanged.
	after, getAfterErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getAfterErr)
	assert.Equal(t, originalModRev, after.GetMetadata().GetModRevision(),
		"ModRevision must be unchanged after a rejected entity-change update")
	assert.Equal(t, []string{"service_name"}, after.GetEntity().GetTagNames(),
		"entity must be unchanged after a rejected update")
}
