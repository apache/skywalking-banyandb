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
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

//For the time being, the mock implements an aggregation function of its own

type Number interface {
	~float64
}

func FromFieldValue[N Number](fieldValue *modelv1.FieldValue) (N, error) {
	switch v := fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return N(v.Int.Value), nil
	case *modelv1.FieldValue_Float:
		return N(v.Float.Value), nil
	default:
		var zero N
		return zero, fmt.Errorf("unsupported field value type: %T", v)
	}
}

func ToFieldValue[N Number](value N) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Float{
			Float: &modelv1.Float{Value: float64(value)},
		},
	}
}

// Aggregator interface
type Aggregator interface {
	Combine(values []float64)
	Result() float64
	Reset()
}

type MinAggregator struct {
	minimum     float64
	isInitiated bool
}

func (m *MinAggregator) Combine(values []float64) {
	for _, value := range values {
		if !m.isInitiated {
			m.minimum = value
			m.isInitiated = true
		} else if value < m.minimum {
			m.minimum = value
		}
	}
}

func (m *MinAggregator) Result() float64 {
	if !m.isInitiated {
		return 0
	}
	return m.minimum
}

func (m *MinAggregator) Reset() {
	m.minimum = 0
	m.isInitiated = false
}

func NewMinAggregator() *MinAggregator {
	return &MinAggregator{
		isInitiated: false,
	}
}

// MaxAggregator calculates the maximum value.
type MaxAggregator struct {
	maximum     float64
	isInitiated bool
}

func (m *MaxAggregator) Combine(values []float64) {
	for _, value := range values {
		if !m.isInitiated {
			m.maximum = value
			m.isInitiated = true
		} else if value > m.maximum {
			m.maximum = value
		}
	}
}

func (m *MaxAggregator) Result() float64 {
	if !m.isInitiated {
		return 0
	}
	return m.maximum
}

func (m *MaxAggregator) Reset() {
	m.maximum = 0
	m.isInitiated = false
}

func NewMaxAggregator() *MaxAggregator {
	return &MaxAggregator{
		isInitiated: false,
	}
}

// NewAggregator creates a new aggregator based on the specified function
func NewAggregator(aggFunc modelv1.MeasureAggregate) Aggregator {
	switch aggFunc {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		return NewMinAggregator()
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MAX:
		return NewMaxAggregator()
	default:
		return nil
	}
}

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
	projTags := make([]model.TagProjection, len(uis.projectionTags))
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
	query, entities, _, err := inverted.BuildLocalQuery(uis.criteria, s, entityMap, entity)
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
		query:                query,
		entities:             entities,
		groupByEntity:        uis.groupByEntity,
		uis:                  uis,
		l:                    logger.GetLogger("query", "measure", uis.metadata.Group, uis.metadata.Name, "local-index"),
	}, nil
}

var (
	_ logical.Plan   = (*localIndexScan)(nil)
	_ logical.Sorter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	query                index.Query
	schema               logical.Schema
	uis                  *unresolvedIndexScan
	order                *logical.OrderBy
	metadata             *commonv1.Metadata
	l                    *logger.Logger
	timeRange            timestamp.TimeRange
	projectionTags       []model.TagProjection
	projectionTagsRefs   [][]*logical.TagRef
	projectionFieldsRefs []*logical.FieldRef
	entities             [][]*modelv1.TagValue
	projectionFields     []string
	groupByEntity        bool
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	var orderBy *model.OrderBy
	orderByType := model.OrderByTypeTime
	if i.order != nil {
		if i.order.Index != nil {
			orderByType = model.OrderByTypeIndex
		}
		orderBy = &model.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	if i.groupByEntity {
		orderByType = model.OrderByTypeSeries
	}
	ec := executor.FromMeasureExecutionContext(ctx)
	ctx, stop := i.startSpan(ctx, query.GetTracer(ctx), orderByType, orderBy)
	defer stop(err)
	result, err := ec.Query(ctx, model.MeasureQueryOptions{
		Name:            i.metadata.GetName(),
		TimeRange:       &i.timeRange,
		Entities:        i.entities,
		Query:           i.query,
		OrderByType:     orderByType,
		Order:           orderBy,
		TagProjection:   i.projectionTags,
		FieldProjection: i.projectionFields,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query measure: %w", err)
	}
	isDelta := false
	aggregators := make(map[string]Aggregator)
	for _, fieldSpec := range i.projectionFieldsRefs {
		if fieldSpec.Spec.Spec.AggregateFunction != modelv1.MeasureAggregate_MEASURE_AGGREGATE_UNSPECIFIED {
			isDelta = true
			aggregators["_"+fieldSpec.Field.Name] = NewAggregator(fieldSpec.Spec.Spec.AggregateFunction)
		}
	}

	if isDelta {
		return &deltaResultMIterator{
			result:      result,
			aggregators: aggregators,
		}, nil
	}

	return &resultMIterator{
		result: result,
	}, nil
	//todo 这里对于MIiter进行切换，通过什么进行区分呢，是否可以通过是否传递聚合函数类型呢，也就是
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; order=%s;",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.query, logical.FormatTagRefs(", ", i.projectionTagsRefs...), i.order)
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
	result  model.MeasureQueryResult
	current []*measurev1.DataPoint
	i       int
}

func (ei *resultMIterator) Next() bool {
	if ei.result == nil {
		return false
	}
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
			Sid:       uint64(r.SID),
			Version:   r.Versions[i],
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
	if ei.result != nil {
		ei.result.Release()
	}
	return nil
}

func (i *localIndexScan) startSpan(ctx context.Context, tracer *query.Tracer, orderType model.OrderByType, orderBy *model.OrderBy) (context.Context, func(error)) {
	if tracer == nil {
		return ctx, func(error) {}
	}

	span, ctx := tracer.StartSpan(ctx, "indexScan-%s", i.metadata)
	sortName := modelv1.Sort_name[int32(orderBy.Sort)]
	switch orderType {
	case model.OrderByTypeTime:
		span.Tag("orderBy", "time "+sortName)
	case model.OrderByTypeIndex:
		span.Tag("orderBy", fmt.Sprintf("indexRule:%s", orderBy.Index.Metadata.Name))
	case model.OrderByTypeSeries:
		span.Tag("orderBy", "series")
	}
	span.Tag("details", i.String())

	return ctx, func(err error) {
		if err != nil {
			span.Error(err)
		}
		span.Stop()
	}
}

// deltaResultMIterator is a MIterator that calculates the delta of the data points
type deltaResultMIterator struct {
	result      model.MeasureQueryResult
	aggregators map[string]Aggregator
	current     []*measurev1.DataPoint
	i           int
}

func (di *deltaResultMIterator) Next() bool {
	// If we have exhausted the current batch of data points, fetch the next batch
	if di.i >= len(di.current) {
		if di.result == nil {
			return false
		}

		mr := di.result.Pull()
		if mr == nil {
			return false
		}

		di.current = di.current[:0]
		di.i = 0

		// Group fields by timestamp
		groupedFields := make(map[int64]map[string][]*modelv1.FieldValue)
		for i, ts := range mr.Timestamps {
			if groupedFields[ts] == nil {
				groupedFields[ts] = make(map[string][]*modelv1.FieldValue)
			}
			for _, f := range mr.Fields {
				if strings.HasPrefix(f.Name, "_") {
					groupedFields[ts][f.Name] = append(groupedFields[ts][f.Name], f.Values[i])
				}
			}
		}

		// Sort timestamps
		timestamps := make([]int64, 0, len(groupedFields))
		for ts := range groupedFields {
			timestamps = append(timestamps, ts)
		}
		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

		// Process each timestamp
		for _, ts := range timestamps {
			dataPoint := &measurev1.DataPoint{
				Timestamp: timestamppb.New(time.Unix(0, ts)),
				Fields:    make([]*measurev1.DataPoint_Field, 0, len(groupedFields[ts])),
			}

			for fieldName, values := range groupedFields[ts] {
				aggregator := di.aggregators[fieldName]
				aggregator.Reset() // Reset aggregator for each field

				// Aggregate values for this field at this timestamp
				for _, value := range values {
					floatValue, _ := FromFieldValue[float64](value)
					aggregator.Combine([]float64{floatValue})
				}

				aggregatedValue := aggregator.Result()
				dataPoint.Fields = append(dataPoint.Fields, &measurev1.DataPoint_Field{
					Name:  fieldName[1:], // Remove leading underscore
					Value: ToFieldValue(aggregatedValue),
				})
			}

			// Sort fields by name to ensure consistent order
			sort.Slice(dataPoint.Fields, func(i, j int) bool {
				return dataPoint.Fields[i].Name < dataPoint.Fields[j].Name
			})

			di.current = append(di.current, dataPoint)
		}
	}

	// If we still have no data points after fetching, return false
	if len(di.current) == 0 {
		return false
	}

	// Move to the next data point
	di.i++

	return true
}

func (di *deltaResultMIterator) Current() []*measurev1.DataPoint {
	if di.i > 0 && di.i <= len(di.current) {
		return []*measurev1.DataPoint{di.current[di.i-1]}
	}
	return nil
}
func (di *deltaResultMIterator) Close() error {
	if di.result != nil {
		di.result.Release()
	}
	return nil
}
