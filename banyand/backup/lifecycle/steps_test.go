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

package lifecycle

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	mock_node "github.com/apache/skywalking-banyandb/pkg/node/mock"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// MockStreamQueryResult is a mock implementation of model.StreamQueryResult.
type MockStreamQueryResult struct {
	results []*model.StreamResult
	index   int
}

func (m *MockStreamQueryResult) Pull(_ context.Context) *model.StreamResult {
	if m.index >= len(m.results) {
		return nil
	}
	result := m.results[m.index]
	m.index++
	return result
}

func (m *MockStreamQueryResult) Release() {}

// MockMeasureQueryResult is a mock implementation of model.MeasureQueryResult.
type MockMeasureQueryResult struct {
	results []*model.MeasureResult
	index   int
}

func (m *MockMeasureQueryResult) Pull() *model.MeasureResult {
	if m.index >= len(m.results) {
		return nil
	}
	result := m.results[m.index]
	m.index++
	return result
}

func (m *MockMeasureQueryResult) Release() {}

func TestMigrateStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := queue.NewMockClient(ctrl)
	mockBatchPublisher := queue.NewMockBatchPublisher(ctrl)
	mockSelector := mock_node.NewMockSelector(ctrl)

	l := logger.GetLogger("test")

	ctx := context.Background()
	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:  "test-stream",
			Group: "test-group",
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id", "service_instance_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "service_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "service_instance_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "trace_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
	}

	shardNum := uint32(2)
	replicas := uint32(0)

	// Create stream result data with expected timestamps
	timestamp1 := int64(1672531200000000000) // First timestamp in nanoseconds
	timestamp2 := int64(1672531260000000000) // Second timestamp in nanoseconds
	elementID1 := uint64(1001)
	elementID2 := uint64(1002)

	streamResult := &model.StreamResult{
		Timestamps: []int64{timestamp1, timestamp2},
		ElementIDs: []uint64{elementID1, elementID2},
		TagFamilies: []model.TagFamily{
			{
				Name: "default",
				Tags: []model.Tag{
					{
						Name: "service_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-2"}}},
						},
					},
					{
						Name: "service_instance_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-2"}}},
						},
					},
					{
						Name: "trace_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-2"}}},
						},
					},
				},
			},
		},
		SIDs: []common.SeriesID{101, 102},
	}

	queryResult := &MockStreamQueryResult{
		results: []*model.StreamResult{streamResult},
	}

	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).Return(mockBatchPublisher)
	mockBatchPublisher.EXPECT().Close().Return(nil, nil)

	mockSelector.EXPECT().Pick(stream.Metadata.Group, stream.Metadata.Name, gomock.Any(), gomock.Any()).Return("node-1", nil).Times(2)

	// Expected element IDs encoded as base64 strings
	expectedElementID1 := base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(elementID1))
	expectedElementID2 := base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(elementID2))

	// Use a counter to check the correct element for each call
	callCount := 0
	mockBatchPublisher.EXPECT().
		Publish(gomock.Any(), data.TopicStreamWrite, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ bus.Topic, msg bus.Message) (string, error) {
			assert.Equal(t, "node-1", msg.Node())

			req, ok := msg.Data().(*streamv1.InternalWriteRequest)
			assert.True(t, ok)
			assert.NotNil(t, req.Request)
			assert.Equal(t, stream.Metadata, req.Request.Metadata)

			element := req.Request.Element
			assert.NotNil(t, element)

			if callCount == 0 {
				assert.Equal(t, expectedElementID1, element.ElementId)
				expectedTime := time.Unix(0, timestamp1)
				assert.Equal(t, expectedTime.UnixNano(), element.Timestamp.AsTime().UnixNano())
			} else {
				assert.Equal(t, expectedElementID2, element.ElementId)
				expectedTime := time.Unix(0, timestamp2)
				assert.Equal(t, expectedTime.UnixNano(), element.Timestamp.AsTime().UnixNano())
			}

			assert.Equal(t, 1, len(element.TagFamilies))

			tagFamily := element.TagFamilies[0]
			assert.Equal(t, 3, len(tagFamily.Tags))

			if callCount == 0 {
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-1"}}, tagFamily.Tags[0].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-1"}}, tagFamily.Tags[1].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-1"}}, tagFamily.Tags[2].Value)
			} else {
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-2"}}, tagFamily.Tags[0].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-2"}}, tagFamily.Tags[1].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-2"}}, tagFamily.Tags[2].Value)
			}

			callCount++
			return "", nil
		}).Times(2)

	migrateStream(ctx, stream, queryResult, shardNum, replicas, mockSelector, mockClient, l)

	assert.Equal(t, 1, queryResult.index)
	assert.Equal(t, 2, callCount, "Expected exactly 2 elements to be processed")
}

func TestMigrateMeasure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := queue.NewMockClient(ctrl)
	mockBatchPublisher := queue.NewMockBatchPublisher(ctrl)
	mockSelector := mock_node.NewMockSelector(ctrl)

	l := logger.GetLogger("test")

	ctx := context.Background()
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  "test-measure",
			Group: "test-group",
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id", "service_instance_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "service_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "service_instance_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "endpoint_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name: "latency",

				FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			},
			{
				Name:      "status_code",
				FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			},
		},
	}

	shardNum := uint32(2)
	replicas := uint32(0)

	// Create measure result data with expected timestamps
	timestamp1 := int64(1672531200000000000) // First timestamp in nanoseconds
	timestamp2 := int64(1672531260000000000) // Second timestamp in nanoseconds

	measureResult := &model.MeasureResult{
		Timestamps: []int64{timestamp1, timestamp2},
		Versions:   []int64{101, 102},
		TagFamilies: []model.TagFamily{
			{
				Name: "default",
				Tags: []model.Tag{
					{
						Name: "service_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-2"}}},
						},
					},
					{
						Name: "service_instance_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-2"}}},
						},
					},
					{
						Name: "endpoint_id",
						Values: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "endpoint-1"}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "endpoint-2"}}},
						},
					},
				},
			},
		},
		Fields: []model.Field{
			{
				Name: "latency",
				Values: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 200}}},
				},
			},
			{
				Name: "status_code",
				Values: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 200}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 500}}},
				},
			},
		},
		SID: 101,
	}

	queryResult := &MockMeasureQueryResult{
		results: []*model.MeasureResult{measureResult},
	}

	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).Return(mockBatchPublisher)
	mockBatchPublisher.EXPECT().Close().Return(nil, nil)

	mockSelector.EXPECT().Pick(measure.Metadata.Group, measure.Metadata.Name, gomock.Any(), gomock.Any()).
		Return("node-1", nil).Times(2)

	// Use a counter to check the correct element for each call
	callCount := 0
	mockBatchPublisher.EXPECT().
		Publish(gomock.Any(), data.TopicMeasureWrite, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ bus.Topic, msg bus.Message) (string, error) {
			assert.Equal(t, "node-1", msg.Node())

			req, ok := msg.Data().(*measurev1.InternalWriteRequest)
			assert.True(t, ok)
			assert.NotNil(t, req.Request)
			assert.Equal(t, measure.Metadata, req.Request.Metadata)

			dataPoint := req.Request.DataPoint
			assert.NotNil(t, dataPoint)

			if callCount == 0 {
				expectedTime := time.Unix(0, timestamp1)
				assert.Equal(t, expectedTime.UnixNano(), dataPoint.Timestamp.AsTime().UnixNano())
			} else {
				expectedTime := time.Unix(0, timestamp2)
				assert.Equal(t, expectedTime.UnixNano(), dataPoint.Timestamp.AsTime().UnixNano())
			}

			assert.Equal(t, 1, len(dataPoint.TagFamilies))
			tagFamily := dataPoint.TagFamilies[0]
			assert.Equal(t, 3, len(tagFamily.Tags))

			if callCount == 0 {
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-1"}}, tagFamily.Tags[0].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-1"}}, tagFamily.Tags[1].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "endpoint-1"}}, tagFamily.Tags[2].Value)

				assert.Equal(t, 2, len(dataPoint.Fields))
				assert.Equal(t, &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 100}}, dataPoint.Fields[0].Value)
				assert.Equal(t, &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 200}}, dataPoint.Fields[1].Value)
			} else {
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-2"}}, tagFamily.Tags[0].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-2"}}, tagFamily.Tags[1].Value)
				assert.Equal(t, &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "endpoint-2"}}, tagFamily.Tags[2].Value)

				assert.Equal(t, 2, len(dataPoint.Fields))
				assert.Equal(t, &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 200}}, dataPoint.Fields[0].Value)
				assert.Equal(t, &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 500}}, dataPoint.Fields[1].Value)
			}

			callCount++
			return "", nil
		}).Times(2)

	migrateMeasure(ctx, measure, queryResult, shardNum, replicas, mockSelector, mockClient, l)

	assert.Equal(t, 1, queryResult.index)
	assert.Equal(t, 2, callCount, "Expected exactly 2 elements to be processed")
}

func TestParseGroup(t *testing.T) {
	l := logger.GetLogger("test")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		group          *commonv1.Group
		nodeLabels     map[string]string
		name           string
		errorMessage   string
		nodes          []*databasev1.Node
		expectShard    uint32
		expectReplicas uint32
		expectError    bool
		expectResult   bool
	}{
		{
			name: "no resource opts",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: nil,
			},
			nodeLabels:   map[string]string{"env": "test"},
			nodes:        []*databasev1.Node{},
			expectError:  true,
			errorMessage: "no resource opts in group test-group",
		},
		{
			name: "no stages",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: nil,
				},
			},
			nodeLabels:   map[string]string{"env": "test"},
			nodes:        []*databasev1.Node{},
			expectError:  true,
			errorMessage: "no stages in group test-group",
		},
		{
			name: "node matches current stage and there is next stage",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: []*commonv1.LifecycleStage{
						{
							Name:         "hot",
							NodeSelector: "env=hot",
							ShardNum:     2,
						},
						{
							Name:         "warm",
							NodeSelector: "env=warm",
							ShardNum:     4,
						},
					},
				},
			},
			nodeLabels: map[string]string{"env": "hot"},
			nodes: []*databasev1.Node{
				{
					Metadata: &commonv1.Metadata{
						Name: "node1",
					},
					Labels: map[string]string{"env": "warm"},
				},
			},
			expectResult: true,
			expectShard:  4,
		},
		{
			name: "node matches no stage, use first stage",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: []*commonv1.LifecycleStage{
						{
							Name:         "hot",
							NodeSelector: "env=hot",
							ShardNum:     2,
						},
						{
							Name:         "warm",
							NodeSelector: "env=warm",
							ShardNum:     4,
						},
					},
				},
			},
			nodeLabels: map[string]string{"env": "unknown"},
			nodes: []*databasev1.Node{
				{
					Metadata: &commonv1.Metadata{
						Name: "node1",
					},
					Labels: map[string]string{"env": "hot"},
				},
			},
			expectResult: true,
			expectShard:  2,
		},
		{
			name: "node matches last stage, no next stage",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: []*commonv1.LifecycleStage{
						{
							Name:         "hot",
							NodeSelector: "env=hot",
							ShardNum:     2,
						},
						{
							Name:         "warm",
							NodeSelector: "env=warm",
							ShardNum:     4,
						},
					},
				},
			},
			nodeLabels:   map[string]string{"env": "warm"},
			nodes:        []*databasev1.Node{},
			expectResult: false,
		},
		{
			name: "next stage selector has no matching nodes",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: []*commonv1.LifecycleStage{
						{
							Name:         "hot",
							NodeSelector: "env=hot",
							ShardNum:     2,
						},
						{
							Name:         "warm",
							NodeSelector: "env=warm",
							ShardNum:     4,
						},
					},
				},
			},
			nodeLabels: map[string]string{"env": "hot"},
			nodes: []*databasev1.Node{
				{
					Metadata: &commonv1.Metadata{
						Name: "node1",
					},
					Labels: map[string]string{"env": "cold"},
				},
			},
			expectError:  true,
			errorMessage: "no nodes matched",
		},
		{
			name: "invalid node selector format",
			group: &commonv1.Group{
				Metadata: &commonv1.Metadata{
					Name: "test-group",
				},
				ResourceOpts: &commonv1.ResourceOpts{
					Stages: []*commonv1.LifecycleStage{
						{
							Name:         "hot",
							NodeSelector: "invalid-selector",
							ShardNum:     2,
						},
					},
				},
			},
			nodeLabels:   map[string]string{"env": "hot"},
			nodes:        []*databasev1.Node{},
			expectError:  true,
			errorMessage: "no nodes matched",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := metadata.NewMockRepo(ctrl)
			mockRepo.EXPECT().RegisterHandler("", schema.KindGroup, gomock.Any()).MaxTimes(1)
			shardNum, replicas, selector, client, err := parseGroup(context.Background(), tt.group, tt.nodeLabels, tt.nodes, l, mockRepo)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			if !tt.expectResult {
				require.Nil(t, err)
				require.Nil(t, selector)
				require.Nil(t, client)
				require.Equal(t, uint32(0), shardNum)
				require.Equal(t, uint32(0), replicas)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, selector)
			require.NotNil(t, client)
			require.Equal(t, tt.expectShard, shardNum)
			require.Equal(t, tt.expectReplicas, replicas)
		})
	}
}
