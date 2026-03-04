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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type mockPropertyApplier struct {
	applyFn func(ctx context.Context, req *propertyv1.ApplyRequest) (*propertyv1.ApplyResponse, error)
	queryFn func(ctx context.Context, req *propertyv1.QueryRequest) (*propertyv1.QueryResponse, error)
}

func (m *mockPropertyApplier) Apply(ctx context.Context, req *propertyv1.ApplyRequest) (*propertyv1.ApplyResponse, error) {
	return m.applyFn(ctx, req)
}

func (m *mockPropertyApplier) Query(ctx context.Context, req *propertyv1.QueryRequest) (*propertyv1.QueryResponse, error) {
	return m.queryFn(ctx, req)
}

// stubIndexRuleBinding implements schema.IndexRuleBinding returning empty results.
type stubIndexRuleBinding struct{}

func (s *stubIndexRuleBinding) GetIndexRuleBinding(_ context.Context, _ *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	return nil, nil
}

func (s *stubIndexRuleBinding) ListIndexRuleBinding(_ context.Context, _ schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	return nil, nil
}

func (s *stubIndexRuleBinding) CreateIndexRuleBinding(_ context.Context, _ *databasev1.IndexRuleBinding) error {
	return nil
}

func (s *stubIndexRuleBinding) UpdateIndexRuleBinding(_ context.Context, _ *databasev1.IndexRuleBinding) error {
	return nil
}

func (s *stubIndexRuleBinding) DeleteIndexRuleBinding(_ context.Context, _ *commonv1.Metadata) (bool, error) {
	return true, nil
}

// stubIndexRule implements schema.IndexRule returning empty results.
type stubIndexRule struct{}

func (s *stubIndexRule) GetIndexRule(_ context.Context, _ *commonv1.Metadata) (*databasev1.IndexRule, error) {
	return nil, nil
}

func (s *stubIndexRule) ListIndexRule(_ context.Context, _ schema.ListOpt) ([]*databasev1.IndexRule, error) {
	return nil, nil
}

func (s *stubIndexRule) CreateIndexRule(_ context.Context, _ *databasev1.IndexRule) error {
	return nil
}

func (s *stubIndexRule) UpdateIndexRule(_ context.Context, _ *databasev1.IndexRule) error {
	return nil
}

func (s *stubIndexRule) DeleteIndexRule(_ context.Context, _ *commonv1.Metadata) (bool, error) {
	return true, nil
}

func TestHasNonEmptyResources(t *testing.T) {
	tests := []struct {
		name     string
		infos    []*databasev1.DataInfo
		expected bool
	}{
		{
			name:     "all zero sizes",
			infos:    []*databasev1.DataInfo{{DataSizeBytes: 0}, {DataSizeBytes: 0}},
			expected: false,
		},
		{
			name:     "one non-zero size",
			infos:    []*databasev1.DataInfo{{DataSizeBytes: 0}, {DataSizeBytes: 1024}},
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := metadata.NewMockRepo(ctrl)
			mockRepo.EXPECT().CollectDataInfo(gomock.Any(), "test-group").Return(tt.infos, nil)

			m := &groupDeletionTaskManager{schemaRegistry: mockRepo}
			hasResources, checkErr := m.hasNonEmptyResources(context.Background(), "test-group")
			require.NoError(t, checkErr)
			assert.Equal(t, tt.expected, hasResources)
		})
	}
}

func TestDeletion(t *testing.T) {
	t.Run("duplicate prevention", func(t *testing.T) {
		m := &groupDeletionTaskManager{}
		m.tasks.Store("existing-group", true)

		err := m.startDeletion(context.Background(), "existing-group")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already in progress")
	})

	t.Run("deletion", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const group = "test-group"
		gr := &groupRepo{
			log:          logger.GetLogger("test"),
			resourceOpts: make(map[string]*commonv1.ResourceOpts),
			inflight:     make(map[string]*groupInflight),
		}
		require.NoError(t, gr.acquireRequest(group))

		var (
			mu       sync.Mutex
			lastTask = &databasev1.GroupDeletionTask{}
		)
		propApplier := &mockPropertyApplier{
			applyFn: func(_ context.Context, req *propertyv1.ApplyRequest) (*propertyv1.ApplyResponse, error) {
				for _, tag := range req.Property.Tags {
					if tag.Key == taskDataTagName {
						var task databasev1.GroupDeletionTask
						if unmarshalErr := proto.Unmarshal(tag.Value.GetBinaryData(), &task); unmarshalErr == nil {
							mu.Lock()
							lastTask = proto.Clone(&task).(*databasev1.GroupDeletionTask)
							mu.Unlock()
						}
					}
				}
				return &propertyv1.ApplyResponse{}, nil
			},
			queryFn: func(_ context.Context, _ *propertyv1.QueryRequest) (*propertyv1.QueryResponse, error) {
				mu.Lock()
				cloned := proto.Clone(lastTask).(*databasev1.GroupDeletionTask)
				mu.Unlock()
				taskData, _ := proto.Marshal(cloned)
				return &propertyv1.QueryResponse{
					Properties: []*propertyv1.Property{{
						Id: group,
						Tags: []*modelv1.Tag{{
							Key:   taskDataTagName,
							Value: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: taskData}},
						}},
					}},
				}, nil
			},
		}

		mockRepo := metadata.NewMockRepo(ctrl)
		mockRepo.EXPECT().CollectDataInfo(gomock.Any(), group).Return([]*databasev1.DataInfo{{DataSizeBytes: 512}}, nil)
		mockRepo.EXPECT().IndexRuleBindingRegistry().Return(&stubIndexRuleBinding{})
		mockRepo.EXPECT().IndexRuleRegistry().Return(&stubIndexRule{})

		mockProperty := schema.NewMockProperty(ctrl)
		mockProperty.EXPECT().ListProperty(gomock.Any(), schema.ListOpt{Group: group}).Return(nil, nil)
		mockRepo.EXPECT().PropertyRegistry().Return(mockProperty)

		mockStream := schema.NewMockStream(ctrl)
		mockStream.EXPECT().ListStream(gomock.Any(), schema.ListOpt{Group: group}).Return(nil, nil)
		mockRepo.EXPECT().StreamRegistry().Return(mockStream)

		mockMeasure := schema.NewMockMeasure(ctrl)
		mockMeasure.EXPECT().ListMeasure(gomock.Any(), schema.ListOpt{Group: group}).Return(nil, nil)
		mockRepo.EXPECT().MeasureRegistry().Return(mockMeasure)

		mockTrace := schema.NewMockTrace(ctrl)
		mockTrace.EXPECT().ListTrace(gomock.Any(), schema.ListOpt{Group: group}).Return(nil, nil)
		mockRepo.EXPECT().TraceRegistry().Return(mockTrace)

		mockTopN := schema.NewMockTopNAggregation(ctrl)
		mockTopN.EXPECT().ListTopNAggregation(gomock.Any(), schema.ListOpt{Group: group}).Return(nil, nil)
		mockRepo.EXPECT().TopNAggregationRegistry().Return(mockTopN)

		mockGroup := schema.NewMockGroup(ctrl)
		mockGroup.EXPECT().DeleteGroup(gomock.Any(), group).DoAndReturn(
			func(_ context.Context, g string) (bool, error) {
				go func() {
					time.Sleep(10 * time.Millisecond)
					gr.OnDelete(schema.Metadata{
						TypeMeta: schema.TypeMeta{Kind: schema.KindGroup},
						Spec: &commonv1.Group{
							Metadata:     &commonv1.Metadata{Name: g},
							ResourceOpts: &commonv1.ResourceOpts{ShardNum: 1},
							Catalog:      commonv1.Catalog_CATALOG_STREAM,
						},
					})
				}()
				return true, nil
			},
		)
		mockRepo.EXPECT().GroupRegistry().Return(mockGroup)

		m := &groupDeletionTaskManager{
			schemaRegistry: mockRepo,
			propServer:     propApplier,
			groupRepo:      gr,
			log:            logger.GetLogger("test"),
		}
		require.NoError(t, m.startDeletion(context.Background(), group))
		require.Eventually(t, func() bool {
			acquireErr := gr.acquireRequest(group)
			if acquireErr == nil {
				gr.releaseRequest(group)
				return false
			}
			return errors.Is(acquireErr, errGroupPendingDeletion)
		}, 2*time.Second, 10*time.Millisecond)

		pendingTask, queryErr := m.getDeletionTask(context.Background(), group)
		require.NoError(t, queryErr)
		assert.Equal(t, databasev1.GroupDeletionTask_PHASE_PENDING, pendingTask.GetCurrentPhase())
		assert.Equal(t, int64(512), pendingTask.GetTotalDataSizeBytes())

		gr.releaseRequest(group)
		require.Eventually(t, func() bool {
			statusTask, statusErr := m.getDeletionTask(context.Background(), group)
			return statusErr == nil && statusTask.GetCurrentPhase() == databasev1.GroupDeletionTask_PHASE_COMPLETED
		}, 5*time.Second, 20*time.Millisecond)

		finalTask, finalErr := m.getDeletionTask(context.Background(), group)
		require.NoError(t, finalErr)
		assert.Equal(t, databasev1.GroupDeletionTask_PHASE_COMPLETED, finalTask.GetCurrentPhase())
		assert.Equal(t, "group deleted successfully", finalTask.GetMessage())
	})
}
