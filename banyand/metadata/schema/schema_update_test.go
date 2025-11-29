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

package schema_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	testUpdateGroup  = "test-update"
	testUpdateStream = "test-stream"
)

func createTestGroup(t *testing.T, registry schema.Registry) {
	req := require.New(t)
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: testUpdateGroup,
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  7,
			},
		},
	}
	err := registry.CreateGroup(context.TODO(), group)
	req.NoError(err)
}

func createBaseStream() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:  testUpdateStream,
			Group: testUpdateGroup,
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "searchable",
				Tags: []*databasev1.TagSpec{
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
			},
			{
				Name: "data",
				Tags: []*databasev1.TagSpec{
					{Name: "data_binary", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				},
			},
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id"},
		},
	}
}

func Test_Stream_Update_AppendTag_ShouldSucceed(t *testing.T) {
	registry, closer := initServerAndRegister(t)
	defer closer()

	req := require.New(t)
	createTestGroup(t, registry)

	stream := createBaseStream()
	_, err := registry.CreateStream(context.Background(), stream)
	req.NoError(err)

	createdStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)

	updatedStream := createBaseStream()
	updatedStream.Metadata.ModRevision = createdStream.Metadata.ModRevision
	updatedStream.TagFamilies[0].Tags = append(updatedStream.TagFamilies[0].Tags,
		&databasev1.TagSpec{Name: "new_tag", Type: databasev1.TagType_TAG_TYPE_STRING})
	_, err = registry.UpdateStream(context.Background(), updatedStream)
	req.NoError(err, "should allow appending new tag to existing tag family")

	resultStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)
	req.Len(resultStream.TagFamilies[0].Tags, 4)
	req.Equal("new_tag", resultStream.TagFamilies[0].Tags[3].Name)
}

func Test_Stream_Update_ChangeEntity_ShouldFail(t *testing.T) {
	registry, closer := initServerAndRegister(t)
	defer closer()

	req := require.New(t)
	createTestGroup(t, registry)

	stream := createBaseStream()
	_, err := registry.CreateStream(context.Background(), stream)
	req.NoError(err)

	createdStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)

	updatedStream := createBaseStream()
	updatedStream.Metadata.ModRevision = createdStream.Metadata.ModRevision
	updatedStream.Entity = &databasev1.Entity{
		TagNames: []string{"trace_id"},
	}
	_, err = registry.UpdateStream(context.Background(), updatedStream)
	req.Error(err, "should not allow changing entity")
	req.Contains(err.Error(), "entity is different")
}

func Test_Stream_Update_DeleteEntityTag_ShouldFail(t *testing.T) {
	registry, closer := initServerAndRegister(t)
	defer closer()

	req := require.New(t)
	createTestGroup(t, registry)

	stream := createBaseStream()
	_, err := registry.CreateStream(context.Background(), stream)
	req.NoError(err)

	createdStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)

	updatedStream := createBaseStream()
	updatedStream.Metadata.ModRevision = createdStream.Metadata.ModRevision
	updatedStream.TagFamilies[0].Tags = []*databasev1.TagSpec{
		{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
	}
	_, err = registry.UpdateStream(context.Background(), updatedStream)
	req.Error(err, "should not allow deleting entity tag")
	req.Contains(err.Error(), "cannot delete entity tag")
}

func Test_Stream_Update_ChangeTagType_ShouldFail(t *testing.T) {
	registry, closer := initServerAndRegister(t)
	defer closer()

	req := require.New(t)
	createTestGroup(t, registry)

	stream := createBaseStream()
	_, err := registry.CreateStream(context.Background(), stream)
	req.NoError(err)

	createdStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)

	updatedStream := createBaseStream()
	updatedStream.Metadata.ModRevision = createdStream.Metadata.ModRevision
	updatedStream.TagFamilies[0].Tags[2] = &databasev1.TagSpec{
		Name: "duration",
		Type: databasev1.TagType_TAG_TYPE_STRING,
	}
	_, err = registry.UpdateStream(context.Background(), updatedStream)
	req.Error(err, "should not allow changing tag type")
	req.Contains(err.Error(), "is different")
}

func Test_Stream_Update_DeleteNonEntityTag_ShouldSucceed(t *testing.T) {
	registry, closer := initServerAndRegister(t)
	defer closer()

	req := require.New(t)
	createTestGroup(t, registry)

	stream := createBaseStream()
	_, err := registry.CreateStream(context.Background(), stream)
	req.NoError(err)

	createdStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)

	updatedStream := createBaseStream()
	updatedStream.Metadata.ModRevision = createdStream.Metadata.ModRevision
	updatedStream.TagFamilies[0].Tags = []*databasev1.TagSpec{
		{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	_, err = registry.UpdateStream(context.Background(), updatedStream)
	req.NoError(err, "should allow deleting non-entity tag")

	resultStream, err := registry.GetStream(context.Background(), stream.Metadata)
	req.NoError(err)
	req.Len(resultStream.TagFamilies[0].Tags, 2)
	req.Equal("service_id", resultStream.TagFamilies[0].Tags[0].Name)
	req.Equal("trace_id", resultStream.TagFamilies[0].Tags[1].Name)
}
