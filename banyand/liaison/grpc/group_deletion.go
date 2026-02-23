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
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	internalDeletionTaskGroup        = "_deletion_task"
	internalDeletionTaskPropertyName = "deletion_task"
	taskDataTagName                  = "task_data"
)

type groupDeletionTaskManager struct {
	schemaRegistry metadata.Repo
	propServer     *propertyServer
	log            *logger.Logger
	groupRepo      *groupRepo
	tasks          sync.Map
}

func newGroupDeletionTaskManager(schemaRegistry metadata.Repo, propServer *propertyServer, gr *groupRepo, l *logger.Logger) *groupDeletionTaskManager {
	return &groupDeletionTaskManager{
		schemaRegistry: schemaRegistry,
		propServer:     propServer,
		groupRepo:      gr,
		log:            l,
	}
}

func (m *groupDeletionTaskManager) initPropertyStorage(ctx context.Context) error {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: internalDeletionTaskGroup,
		},
		Catalog: commonv1.Catalog_CATALOG_PROPERTY,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
		},
	}
	_, getGroupErr := m.schemaRegistry.GroupRegistry().GetGroup(ctx, internalDeletionTaskGroup)
	if getGroupErr != nil {
		if createErr := m.schemaRegistry.GroupRegistry().CreateGroup(ctx, group); createErr != nil {
			return fmt.Errorf("failed to create internal deletion task group: %w", createErr)
		}
	}
	propSchema := &databasev1.Property{
		Metadata: &commonv1.Metadata{
			Group: internalDeletionTaskGroup,
			Name:  internalDeletionTaskPropertyName,
		},
		Tags: []*databasev1.TagSpec{
			{
				Name: taskDataTagName,
				Type: databasev1.TagType_TAG_TYPE_DATA_BINARY,
			},
		},
	}
	_, getPropErr := m.schemaRegistry.PropertyRegistry().GetProperty(ctx, propSchema.Metadata)
	if getPropErr != nil {
		if createErr := m.schemaRegistry.PropertyRegistry().CreateProperty(ctx, propSchema); createErr != nil {
			return fmt.Errorf("failed to create internal deletion task property schema: %w", createErr)
		}
	}
	return nil
}

func (m *groupDeletionTaskManager) startDeletion(ctx context.Context, group string) error {
	if _, loaded := m.tasks.LoadOrStore(group, true); loaded {
		return fmt.Errorf("deletion task for group %s is already in progress", group)
	}
	task := &databasev1.GroupDeletionTask{
		CurrentPhase:  databasev1.GroupDeletionTask_PHASE_PENDING,
		TotalCounts:   make(map[string]int32),
		DeletedCounts: make(map[string]int32),
		CreatedAt:     timestamppb.Now(),
	}
	dataInfo, dataErr := m.schemaRegistry.CollectDataInfo(ctx, group)
	if dataErr != nil {
		m.tasks.Delete(group)
		return fmt.Errorf("failed to collect data info for group %s: %w", group, dataErr)
	}
	var totalDataSize int64
	for _, di := range dataInfo {
		totalDataSize += di.GetDataSizeBytes()
	}
	task.TotalDataSizeBytes = totalDataSize
	if saveErr := m.saveDeletionTask(ctx, group, task); saveErr != nil {
		m.tasks.Delete(group)
		return fmt.Errorf("failed to save initial deletion task for group %s: %w", group, saveErr)
	}
	go m.executeDeletion(ctx, group, task)
	return nil
}

func (m *groupDeletionTaskManager) executeDeletion(ctx context.Context, group string, task *databasev1.GroupDeletionTask) {
	defer m.tasks.Delete(group)
	opt := schema.ListOpt{Group: group}

	task.Message = "waiting for in-flight requests to complete"
	m.saveProgress(ctx, group, task)
	done := m.groupRepo.startPendingDeletion(group)
	defer m.groupRepo.clearPendingDeletion(group)
	<-done

	task.CurrentPhase = databasev1.GroupDeletionTask_PHASE_IN_PROGRESS

	type deletionStep struct {
		fn      func() error
		message string
	}
	steps := []deletionStep{
		{func() error { return m.deleteIndexRuleBindings(ctx, opt, task) }, "deleting index rule bindings"},
		{func() error { return m.deleteIndexRules(ctx, opt, task) }, "deleting index rules"},
		{func() error { return m.deleteProperties(ctx, opt, task) }, "deleting properties"},
		{func() error { return m.deleteStreams(ctx, opt, task) }, "deleting streams"},
		{func() error { return m.deleteMeasures(ctx, opt, task) }, "deleting measures"},
		{func() error { return m.deleteTraces(ctx, opt, task) }, "deleting traces"},
		{func() error { return m.deleteTopNAggregations(ctx, opt, task) }, "deleting topN aggregations"},
	}
	for _, step := range steps {
		if stepErr := m.runStep(ctx, group, task, step.message, step.fn); stepErr != nil {
			return
		}
	}

	task.Message = "deleting group and data files"
	_, deleteGroupErr := m.schemaRegistry.GroupRegistry().DeleteGroup(ctx, group)
	if deleteGroupErr != nil {
		m.failTask(ctx, group, task, fmt.Sprintf("failed to delete group: %v", deleteGroupErr))
		return
	}
	<-m.groupRepo.awaitDeleted(group)
	task.CurrentPhase = databasev1.GroupDeletionTask_PHASE_COMPLETED
	task.Message = "group deleted successfully"
	m.saveProgress(ctx, group, task)
}

func (m *groupDeletionTaskManager) deleteIndexRuleBindings(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	bindings, listErr := m.schemaRegistry.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["index_rule_binding"] = int32(len(bindings))
	for _, binding := range bindings {
		if _, deleteErr := m.schemaRegistry.IndexRuleBindingRegistry().DeleteIndexRuleBinding(ctx, binding.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("index rule binding %s: %w", binding.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["index_rule_binding"] = task.TotalCounts["index_rule_binding"]
	return nil
}

func (m *groupDeletionTaskManager) deleteIndexRules(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	indexRules, listErr := m.schemaRegistry.IndexRuleRegistry().ListIndexRule(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["index_rule"] = int32(len(indexRules))
	for _, rule := range indexRules {
		if _, deleteErr := m.schemaRegistry.IndexRuleRegistry().DeleteIndexRule(ctx, rule.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("index rule %s: %w", rule.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["index_rule"] = task.TotalCounts["index_rule"]
	return nil
}

func (m *groupDeletionTaskManager) deleteProperties(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	properties, listErr := m.schemaRegistry.PropertyRegistry().ListProperty(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["property"] = int32(len(properties))
	for _, prop := range properties {
		if _, deleteErr := m.schemaRegistry.PropertyRegistry().DeleteProperty(ctx, prop.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("property %s: %w", prop.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["property"] = task.TotalCounts["property"]
	return nil
}

func (m *groupDeletionTaskManager) deleteStreams(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	streams, listErr := m.schemaRegistry.StreamRegistry().ListStream(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["stream"] = int32(len(streams))
	for _, stream := range streams {
		if _, deleteErr := m.schemaRegistry.StreamRegistry().DeleteStream(ctx, stream.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("stream %s: %w", stream.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["stream"] = task.TotalCounts["stream"]
	return nil
}

func (m *groupDeletionTaskManager) deleteMeasures(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	measures, listErr := m.schemaRegistry.MeasureRegistry().ListMeasure(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["measure"] = int32(len(measures))
	for _, measure := range measures {
		if _, deleteErr := m.schemaRegistry.MeasureRegistry().DeleteMeasure(ctx, measure.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("measure %s: %w", measure.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["measure"] = task.TotalCounts["measure"]
	return nil
}

func (m *groupDeletionTaskManager) deleteTraces(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	traces, listErr := m.schemaRegistry.TraceRegistry().ListTrace(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["trace"] = int32(len(traces))
	for _, trace := range traces {
		if _, deleteErr := m.schemaRegistry.TraceRegistry().DeleteTrace(ctx, trace.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("trace %s: %w", trace.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["trace"] = task.TotalCounts["trace"]
	return nil
}

func (m *groupDeletionTaskManager) deleteTopNAggregations(
	ctx context.Context, opt schema.ListOpt, task *databasev1.GroupDeletionTask,
) error {
	topNAggs, listErr := m.schemaRegistry.TopNAggregationRegistry().ListTopNAggregation(ctx, opt)
	if listErr != nil {
		return listErr
	}
	task.TotalCounts["topn_aggregation"] = int32(len(topNAggs))
	for _, agg := range topNAggs {
		if _, deleteErr := m.schemaRegistry.TopNAggregationRegistry().DeleteTopNAggregation(ctx, agg.GetMetadata()); deleteErr != nil {
			return fmt.Errorf("topN aggregation %s: %w", agg.GetMetadata().GetName(), deleteErr)
		}
	}
	task.DeletedCounts["topn_aggregation"] = task.TotalCounts["topn_aggregation"]
	return nil
}

func (m *groupDeletionTaskManager) runStep(
	ctx context.Context, group string, task *databasev1.GroupDeletionTask,
	message string, fn func() error,
) error {
	task.Message = message
	if stepErr := fn(); stepErr != nil {
		m.failTask(ctx, group, task, fmt.Sprintf("%s failed: %v", message, stepErr))
		return stepErr
	}
	m.saveProgress(ctx, group, task)
	return nil
}

func (m *groupDeletionTaskManager) saveProgress(ctx context.Context, group string, task *databasev1.GroupDeletionTask) {
	snapshot := proto.Clone(task).(*databasev1.GroupDeletionTask)
	if saveErr := m.saveDeletionTask(ctx, group, snapshot); saveErr != nil {
		m.log.Error().Err(saveErr).Str("group", group).Msg("failed to save deletion progress")
	}
}

func (m *groupDeletionTaskManager) failTask(
	ctx context.Context, group string, task *databasev1.GroupDeletionTask, msg string,
) {
	task.CurrentPhase = databasev1.GroupDeletionTask_PHASE_FAILED
	task.Message = msg
	m.log.Error().Str("group", group).Msg(msg)
	m.saveProgress(ctx, group, task)
}

func (m *groupDeletionTaskManager) saveDeletionTask(ctx context.Context, group string, task *databasev1.GroupDeletionTask) error {
	taskData, marshalErr := proto.Marshal(task)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal deletion task: %w", marshalErr)
	}
	_, applyErr := m.propServer.Apply(ctx, &propertyv1.ApplyRequest{
		Property: &propertyv1.Property{
			Metadata: &commonv1.Metadata{
				Group: internalDeletionTaskGroup,
				Name:  internalDeletionTaskPropertyName,
			},
			Id: group,
			Tags: []*modelv1.Tag{
				{
					Key:   taskDataTagName,
					Value: &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: taskData}},
				},
			},
		},
		Strategy: propertyv1.ApplyRequest_STRATEGY_REPLACE,
	})
	if applyErr != nil {
		return fmt.Errorf("failed to save deletion task property: %w", applyErr)
	}
	return nil
}

func (m *groupDeletionTaskManager) getDeletionTask(ctx context.Context, group string) (*databasev1.GroupDeletionTask, error) {
	resp, queryErr := m.propServer.Query(ctx, &propertyv1.QueryRequest{
		Groups: []string{internalDeletionTaskGroup},
		Name:   internalDeletionTaskPropertyName,
		Ids:    []string{group},
		Limit:  1,
	})
	if queryErr != nil {
		return nil, fmt.Errorf("failed to query deletion task property: %w", queryErr)
	}
	if len(resp.Properties) == 0 {
		return nil, fmt.Errorf("deletion task for group %s not found", group)
	}
	for _, tag := range resp.Properties[0].Tags {
		if tag.Key == taskDataTagName {
			binaryData := tag.Value.GetBinaryData()
			if binaryData == nil {
				return nil, fmt.Errorf("deletion task for group %s has no binary data", group)
			}
			var task databasev1.GroupDeletionTask
			if unmarshalErr := proto.Unmarshal(binaryData, &task); unmarshalErr != nil {
				return nil, fmt.Errorf("failed to unmarshal deletion task: %w", unmarshalErr)
			}
			return &task, nil
		}
	}
	return nil, fmt.Errorf("deletion task for group %s has no task_data tag", group)
}

func (m *groupDeletionTaskManager) hasNonEmptyResources(ctx context.Context, group string) (bool, error) {
	dataInfo, dataErr := m.schemaRegistry.CollectDataInfo(ctx, group)
	if dataErr != nil {
		return false, fmt.Errorf("failed to collect data info: %w", dataErr)
	}
	for _, di := range dataInfo {
		if di.GetDataSizeBytes() > 0 {
			return true, nil
		}
	}
	return false, nil
}
