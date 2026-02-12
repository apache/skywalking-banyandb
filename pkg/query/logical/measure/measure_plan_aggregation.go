// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const aggCountFieldName = "__agg_count"

var (
	_ logical.UnresolvedPlan = (*unresolvedAggregation)(nil)

	errUnsupportedAggregationField = errors.New("unsupported aggregation operation on this field")
)

// aggAccumulator abstracts the aggregation logic for both map and reduce modes.
// It is injected into the existing iterators to avoid creating separate iterator types.
type aggAccumulator[N aggregation.Number] interface {
	Feed(dp *measurev1.DataPoint, fieldIdx int) error
	Result(fieldName string) ([]*measurev1.DataPoint_Field, error)
	Reset()
}

// mapAccumulator implements aggAccumulator for the map phase (data node side).
type mapAccumulator[N aggregation.Number] struct {
	mapFunc     aggregation.Map[N]
	aggrType    modelv1.AggregationFunction
	emitPartial bool
}

func (a *mapAccumulator[N]) Feed(dp *measurev1.DataPoint, fieldIdx int) error {
	v, parseErr := aggregation.FromFieldValue[N](dp.GetFields()[fieldIdx].GetValue())
	if parseErr != nil {
		return parseErr
	}
	a.mapFunc.In(v)
	return nil
}

func (a *mapAccumulator[N]) Result(fieldName string) ([]*measurev1.DataPoint_Field, error) {
	if a.emitPartial {
		part := a.mapFunc.Partial()
		fvs, partErr := aggregation.PartialToFieldValues(a.aggrType, part)
		if partErr != nil {
			return nil, partErr
		}
		fields := make([]*measurev1.DataPoint_Field, len(fvs))
		for idx, fv := range fvs {
			name := fieldName
			if idx > 0 {
				name = aggCountFieldName
			}
			fields[idx] = &measurev1.DataPoint_Field{Name: name, Value: fv}
		}
		return fields, nil
	}
	val, valErr := aggregation.ToFieldValue(a.mapFunc.Val())
	if valErr != nil {
		return nil, valErr
	}
	return []*measurev1.DataPoint_Field{{Name: fieldName, Value: val}}, nil
}

func (a *mapAccumulator[N]) Reset() {
	a.mapFunc.Reset()
}

// reduceAccumulator implements aggAccumulator for the reduce phase (liaison side).
type reduceAccumulator[N aggregation.Number] struct {
	reduceFunc aggregation.Reduce[N]
	aggrType   modelv1.AggregationFunction
}

func (a *reduceAccumulator[N]) Feed(dp *measurev1.DataPoint, _ int) error {
	fvs := make([]*modelv1.FieldValue, len(dp.GetFields()))
	for idx, f := range dp.GetFields() {
		fvs[idx] = f.GetValue()
	}
	part, partErr := aggregation.FieldValuesToPartial[N](a.aggrType, fvs)
	if partErr != nil {
		return partErr
	}
	a.reduceFunc.Combine(part)
	return nil
}

func (a *reduceAccumulator[N]) Result(fieldName string) ([]*measurev1.DataPoint_Field, error) {
	val, valErr := aggregation.ToFieldValue(a.reduceFunc.Val())
	if valErr != nil {
		return nil, valErr
	}
	return []*measurev1.DataPoint_Field{{Name: fieldName, Value: val}}, nil
}

func (a *reduceAccumulator[N]) Reset() {
	a.reduceFunc.Reset()
}

type unresolvedAggregation struct {
	unresolvedInput  logical.UnresolvedPlan
	aggregationField *logical.Field
	aggrFunc         modelv1.AggregationFunction
	isGroup          bool
	emitPartial      bool
	reduceMode       bool
}

func newUnresolvedAggregation(input logical.UnresolvedPlan, aggrField *logical.Field, aggrFunc modelv1.AggregationFunction,
	isGroup bool, emitPartial bool, reduceMode bool,
) logical.UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrFunc:         aggrFunc,
		aggregationField: aggrField,
		isGroup:          isGroup,
		emitPartial:      emitPartial,
		reduceMode:       reduceMode,
	}
}

func (gba *unresolvedAggregation) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	// check validity of aggregation fields
	schema := prevPlan.Schema()
	aggregationFieldRefs, err := schema.CreateFieldRef(gba.aggregationField)
	if err != nil {
		return nil, err
	}
	if len(aggregationFieldRefs) == 0 {
		return nil, errors.Wrap(errFieldNotDefined, "aggregation schema")
	}
	fieldRef := aggregationFieldRefs[0]
	switch fieldRef.Spec.Spec.FieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return newAggregationPlan[int64](gba, prevPlan, schema, fieldRef)
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return newAggregationPlan[float64](gba, prevPlan, schema, fieldRef)
	default:
		return nil, errors.WithMessagef(errUnsupportedAggregationField, "field: %s", fieldRef.Spec.Spec)
	}
}

type aggregationPlan[N aggregation.Number] struct {
	*logical.Parent
	schema              logical.Schema
	aggregationFieldRef *logical.FieldRef
	accumulator         aggAccumulator[N]
	aggrType            modelv1.AggregationFunction
	isGroup             bool
}

func newAggregationPlan[N aggregation.Number](gba *unresolvedAggregation, prevPlan logical.Plan,
	measureSchema logical.Schema, fieldRef *logical.FieldRef,
) (*aggregationPlan[N], error) {
	var acc aggAccumulator[N]
	if gba.reduceMode {
		reduceFunc, reduceErr := aggregation.NewReduce[N](gba.aggrFunc)
		if reduceErr != nil {
			return nil, reduceErr
		}
		acc = &reduceAccumulator[N]{reduceFunc: reduceFunc, aggrType: gba.aggrFunc}
	} else {
		mapFunc, mapErr := aggregation.NewMap[N](gba.aggrFunc)
		if mapErr != nil {
			return nil, mapErr
		}
		acc = &mapAccumulator[N]{mapFunc: mapFunc, aggrType: gba.aggrFunc, emitPartial: gba.emitPartial}
	}
	return &aggregationPlan[N]{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:              measureSchema,
		accumulator:         acc,
		aggregationFieldRef: fieldRef,
		aggrType:            gba.aggrFunc,
		isGroup:             gba.isGroup,
	}, nil
}

func (g *aggregationPlan[N]) String() string {
	return fmt.Sprintf("%s aggregation: aggregation{type=%d,field=%s}",
		g.Input,
		g.aggrType,
		g.aggregationFieldRef.Field.Name)
}

func (g *aggregationPlan[N]) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *aggregationPlan[N]) Schema() logical.Schema {
	return g.schema.ProjFields(g.aggregationFieldRef)
}

func (g *aggregationPlan[N]) Execute(ec context.Context) (executor.MIterator, error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	if g.isGroup {
		return newAggGroupMIterator[N](iter, g.aggregationFieldRef, g.accumulator), nil
	}
	return newAggAllIterator[N](iter, g.aggregationFieldRef, g.accumulator), nil
}

type aggGroupIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	accumulator         aggAccumulator[N]
	err                 error
}

func newAggGroupMIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	accumulator aggAccumulator[N],
) executor.MIterator {
	return &aggGroupIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		accumulator:         accumulator,
	}
}

func (ami *aggGroupIterator[N]) Next() bool {
	if ami.err != nil {
		return false
	}
	return ami.prev.Next()
}

func (ami *aggGroupIterator[N]) Current() []*measurev1.InternalDataPoint {
	if ami.err != nil {
		return nil
	}
	ami.accumulator.Reset()
	group := ami.prev.Current()
	var resultDp *measurev1.DataPoint
	var shardID uint32
	for _, idp := range group {
		dp := idp.GetDataPoint()
		if feedErr := ami.accumulator.Feed(dp, ami.aggregationFieldRef.Spec.FieldIdx); feedErr != nil {
			ami.err = feedErr
			return nil
		}
		if resultDp != nil {
			continue
		}
		shardID = idp.GetShardId()
		resultDp = &measurev1.DataPoint{
			TagFamilies: dp.TagFamilies,
		}
	}
	if resultDp == nil {
		return nil
	}
	fields, resultErr := ami.accumulator.Result(ami.aggregationFieldRef.Field.Name)
	if resultErr != nil {
		ami.err = resultErr
		return nil
	}
	resultDp.Fields = fields
	return []*measurev1.InternalDataPoint{{DataPoint: resultDp, ShardId: shardID}}
}

func (ami *aggGroupIterator[N]) Close() error {
	return multierr.Combine(ami.err, ami.prev.Close())
}

type aggAllIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	accumulator         aggAccumulator[N]
	result              *measurev1.DataPoint
	err                 error
}

func newAggAllIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	accumulator aggAccumulator[N],
) executor.MIterator {
	return &aggAllIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		accumulator:         accumulator,
	}
}

func (ami *aggAllIterator[N]) Next() bool {
	if ami.result != nil || ami.err != nil {
		return false
	}
	var resultDp *measurev1.DataPoint
	for ami.prev.Next() {
		group := ami.prev.Current()
		for _, idp := range group {
			dp := idp.GetDataPoint()
			if feedErr := ami.accumulator.Feed(dp, ami.aggregationFieldRef.Spec.FieldIdx); feedErr != nil {
				ami.err = feedErr
				return false
			}
			if resultDp != nil {
				continue
			}
			resultDp = &measurev1.DataPoint{
				TagFamilies: dp.TagFamilies,
			}
		}
	}
	if resultDp == nil {
		return false
	}
	fields, resultErr := ami.accumulator.Result(ami.aggregationFieldRef.Field.Name)
	if resultErr != nil {
		ami.err = resultErr
		return false
	}
	resultDp.Fields = fields
	ami.result = resultDp
	return true
}

func (ami *aggAllIterator[N]) Current() []*measurev1.InternalDataPoint {
	if ami.result == nil {
		return nil
	}
	return []*measurev1.InternalDataPoint{{DataPoint: ami.result, ShardId: 0}}
}

func (ami *aggAllIterator[N]) Close() error {
	return ami.prev.Close()
}
