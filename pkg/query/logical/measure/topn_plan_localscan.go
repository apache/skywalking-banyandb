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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedLocalScan)(nil)

type unresolvedLocalScan struct {
	startTime        time.Time
	endTime          time.Time
	metadata         *commonv1.Metadata
	conditions       []*modelv1.Condition
	projectionTags   [][]*logical.Tag
	projectionFields []*logical.Field
	sort             modelv1.Sort
}

func (uls *unresolvedLocalScan) Analyze(s logical.Schema) (logical.Plan, error) {
	var projTagsRefs [][]*logical.TagRef
	projTags := make([]model.TagProjection, len(uls.projectionTags))
	if len(uls.projectionTags) > 0 {
		for i := range uls.projectionTags {
			for _, tag := range uls.projectionTags[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
			}
		}
		var err error
		projTagsRefs, err = s.CreateTagRef(uls.projectionTags...)
		if err != nil {
			return nil, err
		}
	}

	var projFieldRefs []*logical.FieldRef
	var projField []string
	if len(uls.projectionFields) > 0 {
		for i := range uls.projectionFields {
			projField = append(projField, uls.projectionFields[i].Name)
		}
		var err error
		projFieldRefs, err = s.CreateFieldRef(uls.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	entity, err := uls.locateEntity(s.EntityList())
	if err != nil {
		return nil, err
	}

	return &localScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uls.startTime, uls.endTime),
		schema:               s,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		projectionTags:       projTags,
		projectionFields:     projField,
		metadata:             uls.metadata,
		entity:               entity,
		l:                    logger.GetLogger("topn", "measure", uls.metadata.Group, uls.metadata.Name, "local-index"),
	}, nil
}

func (uls *unresolvedLocalScan) locateEntity(entityList []string) ([]*modelv1.TagValue, error) {
	entityMap := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, tagName := range entityList {
		entityMap[tagName] = idx
		// allow to make fuzzy search with partial conditions
		entity[idx] = pbv1.AnyTagValue
	}
	for _, pairQuery := range uls.conditions {
		if pairQuery.GetOp() != modelv1.Condition_BINARY_OP_EQ {
			return nil, errors.Errorf("tag belongs to the entity only supports EQ operation in condition(%v)", pairQuery)
		}
		if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
			entity[entityIdx] = pairQuery.Value
			switch pairQuery.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str, *modelv1.TagValue_Int, *modelv1.TagValue_Null:
				entity[entityIdx] = pairQuery.Value
			default:
				return nil, errors.New("unsupported condition tag type for entity")
			}
			continue
		}
		return nil, errors.New("only groupBy tag name is supported")
	}

	return entity, nil
}

func local(startTime, endTime time.Time, metadata *commonv1.Metadata, projectionTags [][]*logical.Tag,
	projectionFields []*logical.Field, conditions []*modelv1.Condition, sort modelv1.Sort,
) logical.UnresolvedPlan {
	return &unresolvedLocalScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		conditions:       conditions,
		sort:             sort,
	}
}

var _ logical.Plan = (*localScan)(nil)

type localScan struct {
	schema               logical.Schema
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	projectionTags       []model.TagProjection
	projectionFields     []string
	entity               []*modelv1.TagValue
	sort                 modelv1.Sort
}

func (i *localScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	ec := executor.FromMeasureExecutionContext(ctx)
	result, err := ec.Query(ctx, model.MeasureQueryOptions{
		Name:            i.metadata.GetName(),
		TimeRange:       &i.timeRange,
		Entities:        [][]*modelv1.TagValue{i.entity},
		Order:           &model.OrderBy{Sort: i.sort},
		TagProjection:   i.projectionTags,
		FieldProjection: i.projectionFields,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query measure: %w", err)
	}
	return &resultMIterator{
		result: result,
	}, nil
}

func (i *localScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s}; projection=%s; sort=%s;",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.sort)
}

func (i *localScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localScan) Schema() logical.Schema {
	if len(i.projectionTagsRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagsRefs...).ProjFields(i.projectionFieldsRefs...)
}
