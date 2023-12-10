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
	var projTags []pbv1.TagProjection
	var projTagsRefs [][]*logical.TagRef
	if len(uis.projectionTags) > 0 {
		for i := range uis.projectionTags {
			for _, tag := range uis.projectionTags[i] {
				projTags = append(projTags, pbv1.TagProjection{
					Family: tag.GetFamilyName(),
					Name:   tag.GetTagName(),
				})
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
		l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
	}, nil
}

var (
	_ logical.Plan          = (*localIndexScan)(nil)
	_ logical.Sorter        = (*localIndexScan)(nil)
	_ logical.VolumeLimiter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	schema               logical.Schema
	filter               index.Filter
	order                *logical.OrderBy
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTags       []pbv1.TagProjection
	projectionFields     []string
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             [][]*modelv1.TagValue
	groupByEntity        bool
	maxDataPointsSize    int
}

func (i *localIndexScan) Limit(max int) {
	i.maxDataPointsSize = max
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	var orderBy *pbv1.OrderBy
	if i.order.Index != nil {
		orderBy = &pbv1.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	} else if i.groupByEntity {
		orderBy = &pbv1.OrderBy{
			Sort: i.order.Sort,
		}
	}
	ec := executor.FromMeasureExecutionContext(ctx)
	var results []pbv1.MeasureQueryResult
	for _, e := range i.entities {
		result, err := ec.Query(ctx, pbv1.MeasureQueryOptions{
			Name:            i.metadata.GetName(),
			TimeRange:       &i.timeRange,
			Entity:          e,
			Filter:          i.filter,
			Order:           orderBy,
			TagProjection:   i.projectionTags,
			FieldProjection: i.projectionFields,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query measure: %w", err)
		}

		results = append(results, result)
	}
	if len(results) == 0 {
		return dummyIter, nil
	}
	return &resultMIterator{
		results: results,
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
	results []pbv1.MeasureQueryResult
	current []*measurev1.DataPoint
	index   int
}

func (ei *resultMIterator) Next() bool {
	if ei.index >= len(ei.results) {
		return false
	}

	r := ei.results[ei.index].Pull()
	if r == nil {
		ei.index++
		return ei.Next()
	}
	for i := range r.Timestamps {
		dp := &measurev1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(0, int64(r.Timestamps[i]))),
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
	}

	return true
}

func (ei *resultMIterator) Current() []*measurev1.DataPoint {
	return ei.current
}

func (ei *resultMIterator) Close() error {
	for _, result := range ei.results {
		result.Release()
	}
	return nil
}

var dummyIter = dummyMIterator{}

type dummyMIterator struct{}

func (ei dummyMIterator) Next() bool {
	return false
}

func (ei dummyMIterator) Current() []*measurev1.DataPoint {
	return nil
}

func (ei dummyMIterator) Close() error {
	return nil
}
