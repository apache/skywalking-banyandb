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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure/aggregate"
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

	var aggregateFields []*databasev1.AggregateField
	var interval string
	var isDelta bool
	if ms, ok := i.schema.(*schema); ok {
		interval = ms.measure.GetInterval()
		aggregateFields = ms.measure.GetAggregateField()
		fmt.Println("Interval:", interval)
	} else {
		fmt.Println("Error: Schema does not support getting the interval directly")
	}

	aggregatorConfigs, err := buildAggregatorConfigs(aggregateFields, i.projectionFieldsRefs)
	if err != nil {
		return nil, fmt.Errorf("failed to query measure: %w", err)
	}
	if aggregatorConfigs != nil {
		isDelta = true
	}

	//

	if isDelta {
		return &deltaResultMIterator{
			result:           result,
			interval:         interval,
			aggregatorConfig: aggregatorConfigs,
		}, nil
	}

	return &resultMIterator{
		result: result,
	}, nil
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

// create aggregate function.
func createSingleAggregateFunction(config AggregatorConfig) (interface{}, error) {
	switch config.InputField.Type {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return createInt64AggregateFunction(config)
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return createDoubleAggregateFunction(config)
	//
	default:
		return nil, fmt.Errorf("unsupported field types: %v", config.InputField.Type)
	}
}

// create int64 aggregate function.
func createInt64AggregateFunction(config AggregatorConfig) (interface{}, error) {
	switch config.Function {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		return aggregate.NewFunction[int64, aggregate.Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
		return aggregate.NewFunction[int64, int64, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG)
	//
	default:
		return nil, fmt.Errorf("unsupported aggregate functions: %v", config.Function)
	}
}

// create float 64 aggregate function.
func createDoubleAggregateFunction(config AggregatorConfig) (interface{}, error) {
	switch config.Function {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		return aggregate.NewFunction[float64, aggregate.Void, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
		return aggregate.NewFunction[float64, float64, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG)
	//
	default:
		return nil, fmt.Errorf("unsupported aggregate functions: %v", config.Function)
	}
}

// convert field value to can be aggregated value.
func convertToAggregateValue(fieldValue *modelv1.FieldValue) (interface{}, error) {
	switch v := fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return v.Int.GetValue(), nil
	case *modelv1.FieldValue_Float:
		return v.Float.GetValue(), nil
	default:
		return nil, fmt.Errorf("unsupported field type for aggregation")
	}
}

func findFieldRef(fieldName string, fieldRefs []*logical.FieldRef) (*logical.FieldRef, error) {
	for _, ref := range fieldRefs {
		if ref.Field.Name == fieldName {
			return ref, nil
		}
	}
	return nil, fmt.Errorf("field not found: %s", fieldName)
}

func buildAggregatorConfigs(fieldAggregation []*databasev1.AggregateField, fieldRefs []*logical.FieldRef) ([]AggregatorConfig, error) {
	configs := make([]AggregatorConfig, 0, len(fieldAggregation))

	for _, agg := range fieldAggregation {
		inputFieldRef, err := findFieldRef(agg.InputField1, fieldRefs)
		if err != nil {
			return nil, fmt.Errorf("error constructing aggregate configuration: %w", err)
		}

		inputField := FieldInfo{
			Name: agg.InputField1,
			Type: inputFieldRef.Spec.Spec.GetFieldType(),
		}

		outputFieldRef, err := findFieldRef(agg.OutputField, fieldRefs)
		if err != nil {
			return nil, fmt.Errorf("error constructing aggregate configuration: %w", err)
		}
		outputField := FieldInfo{
			Name: agg.OutputField,
			Type: outputFieldRef.Spec.Spec.GetFieldType(),
		}

		config := AggregatorConfig{
			InputField:  inputField,
			OutputField: outputField,
			Function:    agg.AggregateFunction,
		}

		if agg.AggregateFunction == modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG {
			inputFieldRef2, err := findFieldRef(agg.InputField2, fieldRefs)
			if err != nil {
				return nil, fmt.Errorf("error constructing aggregate configuration: %w", err)
			}
			inputField2 := FieldInfo{
				Name: agg.InputField2,
				Type: inputFieldRef2.Spec.Spec.GetFieldType(),
			}
			config.ExtraFields = inputField2
		}

		configs = append(configs, config)
	}

	return configs, nil
}

// AggregatorConfig  use to config the aggregator.
type AggregatorConfig struct {
	InputField  FieldInfo
	ExtraFields FieldInfo // Used to store additional required fields, such as the count field for AVG.
	OutputField FieldInfo

	Function modelv1.MeasureAggregate // This Should Store TheSpecificFunctionName
}

// FieldInfo  use to store the field information.
type FieldInfo struct {
	Name string
	Type databasev1.FieldType
}
type deltaResultMIterator struct {
	interval         string
	current          []*measurev1.DataPoint
	result           model.MeasureQueryResult
	aggregatorConfig []AggregatorConfig
	i                int
}

func (di *deltaResultMIterator) Next() bool {
	// If we have exhausted the current batch of data points, fetch the next batch
	if di.result == nil {
		return false
	}
	di.i++
	if di.i < len(di.current) {
		return true
	}

	r := di.result.Pull()
	if r == nil {
		return false
	}
	di.current = di.current[:0]
	di.i = 0
	duration, err := time.ParseDuration(di.interval)
	if err != nil {
		fmt.Printf("Error parsing '%s': %v\n", di.interval, err)
	}
	groupAndAggregate(di.aggregatorConfig, duration, r)
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
		di.current = append(di.current, dp)
	}

	return true
}

func groupAndAggregate(configs []AggregatorConfig, interval time.Duration, r *model.MeasureResult) []*measurev1.DataPoint {
	// sort timestamps
	sort.Slice(r.Timestamps, func(i, j int) bool {
		return r.Timestamps[i] < r.Timestamps[j]
	})

	groups := make(map[int64][]*measurev1.DataPoint)
	for i, ts := range r.Timestamps {
		groupKey := ts / int64(interval)
		dp := createDataPoint(r, i)
		groups[groupKey] = append(groups[groupKey], dp)
	}

	result := make([]*measurev1.DataPoint, 0, len(groups))
	for _, dps := range groups {
		aggregatedDP, _ := aggregateDataPoints(dps, configs)
		result = append(result, aggregatedDP)
	}

	return result
}

func createDataPoint(r *model.MeasureResult, index int) *measurev1.DataPoint {
	dp := &measurev1.DataPoint{
		Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[index])),
		Sid:       uint64(r.SID),
		Version:   r.Versions[index],
	}

	for _, tf := range r.TagFamilies {
		tagFamily := &modelv1.TagFamily{Name: tf.Name}
		for _, t := range tf.Tags {
			tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
				Key:   t.Name,
				Value: t.Values[index],
			})
		}
		dp.TagFamilies = append(dp.TagFamilies, tagFamily)
	}
	//
	for _, f := range r.Fields {
		dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
			Name:  f.Name,
			Value: f.Values[index],
		})
	}

	return dp
}

func aggregateDataPoints(dps []*measurev1.DataPoint, configs []AggregatorConfig) (*measurev1.DataPoint, error) {
	if len(dps) == 0 {
		return nil, fmt.Errorf("no point")
	}

	// Use the latest datapoint.
	result := dps[len(dps)-1]

	for _, config := range configs {
		aggregateFunc, err := createSingleAggregateFunction(config)
		if err != nil {
			return nil, fmt.Errorf("fail create: %w", err)
		}
		if err != nil {
			return nil, fmt.Errorf("fail get inputvalue: %w", err)
		}

		values := make([]interface{}, 0, len(dps))
		for _, dp := range dps {
			value, valueErr := getFieldValue(dp, config.InputField.Name)
			if valueErr != nil {
				continue
			}

			values = append(values, value)
		}

		var aggregatedValue interface{}

		switch f := aggregateFunc.(type) {
		case aggregate.Function[int64, int64, int64]:
			intValues := make([]int64, len(values))
			for i, v := range values {
				intValues[i] = v.(int64)
			}
			var args aggregate.Arguments[int64, int64]
			if config.Function == modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG {
				counts := make([]int64, len(values))
				for i := range counts {
					counts[i] = 1
				}
				args, err = CreateArguments[int64, int64](config.InputField.Type, config.Function, intValues, counts)
			} else {
				args, err = CreateArguments[int64, int64](config.InputField.Type, config.Function, intValues, nil)
			}
			if err != nil {
				return nil, fmt.Errorf("fail to create int64 aggregate parameter: %w", err)
			}
			err = f.Combine(args)
			if err != nil {
				return nil, fmt.Errorf("fail to combine data : %w", err)
			}
			aggregatedValue = f.Result()
		case aggregate.Function[float64, float64, float64]:
			floatValues := make([]float64, len(values))
			for i, v := range values {
				floatValues[i] = v.(float64)
			}
			var args aggregate.Arguments[float64, float64]
			if config.Function == modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG {
				counts := make([]int64, len(values))
				for i := range counts {
					counts[i] = 1
				}
				args, err = CreateArguments[float64, float64](config.InputField.Type, config.Function, floatValues, counts)
			} else {
				args, err = CreateArguments[float64, float64](config.InputField.Type, config.Function, floatValues, nil)
			}
			if err != nil {
				return nil, fmt.Errorf("fail to create float64 aggregate parameter: %w", err)
			}
			err = f.Combine(args)
			if err != nil {
				return nil, fmt.Errorf("failed to combine float64: %w", err)
			}
			aggregatedValue = f.Result()
		default:
			return nil, fmt.Errorf("no support field type")
		}

		if aggregatedValue != nil {
			err = addFieldToDataPoint(result, config.OutputField.Name, aggregatedValue)
			if err != nil {
				return nil, fmt.Errorf("failedToAddAggregateResultField: %w", err)
			}
			if config.Function == modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG {
				count := int64(len(values))
				err = addFieldToDataPoint(result, config.OutputField.Name+"_count", count)
				if err != nil {
					return nil, fmt.Errorf("failed To AddThe AverageCountField: %w", err)
				}
			}
		}
	}

	return result, nil
}

func addFieldToDataPoint(dp *measurev1.DataPoint, name string, value interface{}) error {
	var fieldValue *modelv1.FieldValue
	switch v := value.(type) {
	case int64:
		fieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
	case float64:
		fieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
	default:
		return fmt.Errorf("unsupportedFieldTypes")
	}

	dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
		Name:  name,
		Value: fieldValue,
	})
	return nil
}

func getFieldValue(dp *measurev1.DataPoint, name string) (interface{}, error) {
	for _, field := range dp.Fields {
		if field.Name == name {
			return convertToAggregateValue(field.Value)
		}
	}
	return nil, fmt.Errorf("fieldNotFound: %s", name)
}

// CreateArguments creates the arguments for the aggregation function.
func CreateArguments[A, B aggregate.Input](
	fieldType databasev1.FieldType,
	aggregateType modelv1.MeasureAggregate,
	arg0 interface{},
	arg1 interface{},
) (aggregate.Arguments[A, B], error) {
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		switch aggregateType {
		case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
			args := aggregate.NewMinArguments(arg0.([]int64))
			return any(args).(aggregate.Arguments[A, B]), nil
		case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
			args := aggregate.NewAvgArguments(arg0.([]int64), arg1.([]int64))
			return any(args).(aggregate.Arguments[A, B]), nil
		default:
			return aggregate.Arguments[A, B]{}, fmt.Errorf("unsupported aggregate type for int: %v", aggregateType)
		}
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		switch aggregateType {
		case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
			args := aggregate.NewMinArguments(arg0.([]float64))
			return any(args).(aggregate.Arguments[A, B]), nil
		case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
			args := aggregate.NewAvgArguments(arg0.([]float64), arg1.([]int64))
			return any(args).(aggregate.Arguments[A, B]), nil
		default:
			return aggregate.Arguments[A, B]{}, fmt.Errorf("unsupported aggregate type for float: %v", aggregateType)
		}
	default:
		return aggregate.Arguments[A, B]{}, fmt.Errorf("unsupported field type for aggregation: %v", fieldType)
	}
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
