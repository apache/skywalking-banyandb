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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
)

// TestCreate_AfterTombstone_Reject_WhenLeq verifies that CreateMeasure returns
// codes.InvalidArgument when a tombstone exists with a delete_time that is greater
// than or equal to the create's updated_at (Step 1.3 / A4).
//
// Strategy: inject a far-future delete_time via the raw schema management RPC
// so that time.Now() is always before the tombstone, forcing the rejection.
func TestCreate_AfterTombstone_Reject_WhenLeq(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	rawMgmt := rawMgmtClient(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	_, createErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, createErr)

	// Delete the measure with a far-future UpdateAt so any realtime CreateMeasure
	// (which stamps UpdatedAt = time.Now()) will be before the tombstone.
	farFuture := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, deleteSchemaErr := rawMgmt.DeleteSchema(ctx, &schemav1.DeleteSchemaRequest{
		Delete: &propertyv1.DeleteRequest{
			Group: schema.SchemaGroup,
			Name:  schema.KindMeasure.String(),
			Id:    property.BuildPropertyID(schema.KindMeasure, &commonv1.Metadata{Group: "test-group", Name: "test-measure"}),
		},
		UpdateAt: timestamppb.New(farFuture),
	})
	require.NoError(t, deleteSchemaErr)

	// Attempt to re-create: CreateMeasure stamps UpdatedAt = time.Now() < farFuture → rejected.
	_, recreateErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.Error(t, recreateErr, "CreateMeasure must fail when updated_at <= tombstone delete_time")
	st, ok := status.FromError(recreateErr)
	require.True(t, ok, "error must be a gRPC status error")
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "updated_at_before_tombstone")
}

// TestCreate_AfterTombstone_Accept_WhenGreater verifies that re-creating a schema
// after deletion succeeds when the new updated_at is after the tombstone's delete_time
// (Step 1.3 / A4).
func TestCreate_AfterTombstone_Accept_WhenGreater(t *testing.T) {
	addr := startTestSchemaServer(t)
	reg := newTestRegistry(t, addr)
	ctx := context.Background()
	createTestGroup(t, reg)

	_, createErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, createErr)

	// Normal deletion — tombstone delete_time ≈ time.Now().
	deleted, _, deleteErr := reg.DeleteMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, deleteErr)
	assert.True(t, deleted)

	// Small gap ensures the new UpdatedAt (time.Now()) is strictly > deleteTime.
	time.Sleep(2 * time.Millisecond)

	// Re-create: CreateMeasure stamps UpdatedAt = time.Now() > deleteTime → accepted.
	_, recreateErr := reg.CreateMeasure(ctx, testMeasure("test-group"))
	require.NoError(t, recreateErr, "re-create after delete must succeed when updated_at > tombstone delete_time")

	got, getErr := reg.GetMeasure(ctx, &commonv1.Metadata{Group: "test-group", Name: "test-measure"})
	require.NoError(t, getErr)
	assert.Equal(t, "test-measure", got.GetMetadata().GetName())
}
