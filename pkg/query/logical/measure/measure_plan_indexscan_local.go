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

package measure

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ logical.UnresolvedPlan = (*unresolvedIndexScan)(nil)

type unresolvedIndexScan struct {
	startTime        time.Time
	endTime          time.Time
	metadata         *commonv1.Metadata
	criteria         *modelv1.Criteria
	projectionTags   [][]*logical.Tag
	projectionFields []*logical.Field
	groupByEntity    bool
}

func (uis *unresolvedIndexScan) Analyze(s logical.Schema) (logical.Plan, error) {
	projTags := make([]pbv1.TagProjection, len(uis.projectionTags))
	var projTagsRefs [][]*logical.TagRef
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
			}
		}
		var err error
		projTagsRefs, err = s.CreateTagRef(uis.projectionTags...)
		if err != nil {
			return nil, err
		}
	}

	var projField []string
	var projFieldRefs []*logical.FieldRef
	if len(uis.projectionFields) > 0 {
		for i := range uis.projectionFields {
			projField = append(projField, uis.projectionFields[i].Name)
		}
		var err error
		projFieldRefs, err = s.CreateFieldRef(uis.projectionFields...)
		if err != nil {
			return nil, err
		}
	}

	entityList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		// fill AnyEntry by default
		entity[idx] = pbv1.AnyTagValue
	}
	filter, entities, err := logical.BuildLocalFilter(uis.criteria, s, entityMap, entity, true)
	if err != nil {
		return nil, err
	}

	return &localIndexScan{
		timeRange:            timestamp.NewInclusiveTimeRange(uis.startTime, uis.endTime),
		schema:               s,
		projectionTags:       projTags,
		projectionFields:     projField,
		projectionTagsRefs:   projTagsRefs,
		projectionFieldsRefs: projFieldRefs,
		metadata:             uis.metadata,
		filter:               filter,
		entities:             entities,
		groupByEntity:        uis.groupByEntity,
		uis:                  uis,
		l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
	}, nil
}

var (
	_ logical.Plan          = (*localIndexScan)(nil)
	_ logical.Sorter        = (*localIndexScan)(nil)
	_ logical.VolumeLimiter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	filter               index.Filter
	schema               logical.Schema
	uis                  *unresolvedIndexScan
	order                *logical.OrderBy
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTags       []pbv1.TagProjection
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             [][]*modelv1.TagValue
	projectionFields     []string
	maxDataPointsSize    int
	groupByEntity        bool
}

func (i *localIndexScan) Limit(max int) {
	i.maxDataPointsSize = max
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	var orderBy *pbv1.OrderBy
	orderByType := pbv1.OrderByTypeTime
	if i.order != nil {
		if i.order.Index != nil {
			orderByType = pbv1.OrderByTypeIndex
		}
		orderBy = &pbv1.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	if i.groupByEntity {
		orderByType = pbv1.OrderByTypeSeries
	}
	ec := executor.FromMeasureExecutionContext(ctx)
	result, err := ec.Query(ctx, pbv1.MeasureQueryOptions{
		Name:            i.metadata.GetName(),
		TimeRange:       &i.timeRange,
		Entities:        i.entities,
		Filter:          i.filter,
		OrderByType:     orderByType,
		Order:           orderBy,
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

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; order=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.order, i.maxDataPointsSize)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	if len(i.projectionTagsRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagsRefs...).ProjFields(i.projectionFieldsRefs...)
}

func indexScan(startTime, endTime time.Time, metadata *commonv1.Metadata, projectionTags [][]*logical.Tag,
	projectionFields []*logical.Field, groupByEntity bool, criteria *modelv1.Criteria,
) logical.UnresolvedPlan {
	return &unresolvedIndexScan{
		startTime:        startTime,
		endTime:          endTime,
		metadata:         metadata,
		projectionTags:   projectionTags,
		projectionFields: projectionFields,
		groupByEntity:    groupByEntity,
		criteria:         criteria,
	}
}

type resultMIterator struct {
	result  pbv1.MeasureQueryResult
	current []*measurev1.DataPoint
	i       int
}

func (ei *resultMIterator) Next() bool {
	ei.i++
	if ei.i < len(ei.current) {
		return true
	}

	r := ei.result.Pull()
	if r == nil {
		return false
	}
	ei.current = ei.current[:0]
	ei.i = 0
	for i := range r.Timestamps {
		dp := &measurev1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[i])),
		}

		for _, tf := range r.TagFamilies {
			tagFamily := &modelv1.TagFamily{
				Name: tf.Name,
			}
			dp.TagFamilies = append(dp.TagFamilies, tagFamily)
			for _, t := range tf.Tags {
				tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
					Key:   t.Name,
					Value: t.Values[i],
				})
			}
		}
		for _, f := range r.Fields {
			dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
				Name:  f.Name,
				Value: f.Values[i],
			})
		}
		ei.current = append(ei.current, dp)
	}

	return true
}

func (ei *resultMIterator) Current() []*measurev1.DataPoint {
	return []*measurev1.DataPoint{ei.current[ei.i]}
}

func (ei *resultMIterator) Close() error {
	ei.result.Release()
	return nil
}
