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

var (
	_ logical.UnresolvedPlan = (*unresolvedAggregation)(nil)

	errUnsupportedAggregationField = errors.New("unsupported aggregation operation on this field")
)

type unresolvedAggregation struct {
	unresolvedInput  logical.UnresolvedPlan
	aggregationField *logical.Field
	aggrFunc         modelv1.AggregationFunction
	isGroup          bool
	emitPartial      bool
	useReduceMode    bool
}

func newUnresolvedAggregation(input logical.UnresolvedPlan, aggrField *logical.Field, aggrFunc modelv1.AggregationFunction,
	isGroup bool, emitPartial bool, useReduceMode bool,
) logical.UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrFunc:         aggrFunc,
		aggregationField: aggrField,
		isGroup:          isGroup,
		emitPartial:      emitPartial,
		useReduceMode:    useReduceMode,
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
	mapFunc             aggregation.Map[N]
	reduceFunc          aggregation.Reduce[N]
	groupByTagsRefs     [][]*logical.TagRef
	aggrType            modelv1.AggregationFunction
	isGroup             bool
	emitPartial         bool
	useReduceMode       bool
}

func newAggregationPlan[N aggregation.Number](gba *unresolvedAggregation, prevPlan logical.Plan,
	measureSchema logical.Schema, fieldRef *logical.FieldRef,
) (*aggregationPlan[N], error) {
	mapFunc, err := aggregation.NewMap[N](gba.aggrFunc)
	if err != nil {
		return nil, err
	}
	var reduceFunc aggregation.Reduce[N]
	var groupByTagsRefs [][]*logical.TagRef
	if gba.useReduceMode {
		reduceFunc, err = aggregation.NewReduce[N](gba.aggrFunc)
		if err != nil {
			return nil, err
		}
		if gp, ok := prevPlan.(*groupBy); ok {
			groupByTagsRefs = gp.groupByTagsRefs
		}
	}
	return &aggregationPlan[N]{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:              measureSchema,
		mapFunc:             mapFunc,
		reduceFunc:          reduceFunc,
		aggregationFieldRef: fieldRef,
		aggrType:            gba.aggrFunc,
		isGroup:             gba.isGroup,
		emitPartial:         gba.emitPartial,
		useReduceMode:       gba.useReduceMode,
		groupByTagsRefs:     groupByTagsRefs,
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
	if g.useReduceMode {
		return newReduceGroupIterator(iter, g.aggregationFieldRef, g.reduceFunc, g.aggrType, g.groupByTagsRefs), nil
	}
	if g.isGroup {
		return newAggGroupMIterator(iter, g.aggregationFieldRef, g.mapFunc, g.aggrType, g.emitPartial), nil
	}
	return newAggAllIterator(iter, g.aggregationFieldRef, g.mapFunc, g.aggrType, g.emitPartial), nil
}

type aggGroupIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	mapFunc             aggregation.Map[N]
	err                 error
	aggrType            modelv1.AggregationFunction
	emitPartial         bool
}

func newAggGroupMIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	mapFunc aggregation.Map[N],
	aggrType modelv1.AggregationFunction,
	emitPartial bool,
) executor.MIterator {
	return &aggGroupIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		mapFunc:             mapFunc,
		aggrType:            aggrType,
		emitPartial:         emitPartial,
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
	ami.mapFunc.Reset()
	group := ami.prev.Current()
	var resultDp *measurev1.DataPoint
	var shardID uint32
	for _, idp := range group {
		dp := idp.GetDataPoint()
		value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
			GetValue()
		v, err := aggregation.FromFieldValue[N](value)
		if err != nil {
			ami.err = err
			return nil
		}
		ami.mapFunc.In(v)
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
	if ami.emitPartial {
		part := ami.mapFunc.Partial()
		fvs, err := aggregation.PartialToFieldValues(ami.aggrType, part)
		if err != nil {
			ami.err = err
			return nil
		}
		resultDp.Fields = make([]*measurev1.DataPoint_Field, len(fvs))
		for i, fv := range fvs {
			name := ami.aggregationFieldRef.Field.Name
			if i > 0 {
				name = "__agg_count"
			}
			resultDp.Fields[i] = &measurev1.DataPoint_Field{Name: name, Value: fv}
		}
	} else {
		val, err := aggregation.ToFieldValue(ami.mapFunc.Val())
		if err != nil {
			ami.err = err
			return nil
		}
		resultDp.Fields = []*measurev1.DataPoint_Field{
			{Name: ami.aggregationFieldRef.Field.Name, Value: val},
		}
	}
	return []*measurev1.InternalDataPoint{{DataPoint: resultDp, ShardId: shardID}}
}

func (ami *aggGroupIterator[N]) Close() error {
	return multierr.Combine(ami.err, ami.prev.Close())
}

type aggAllIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	mapFunc             aggregation.Map[N]
	result              *measurev1.DataPoint
	err                 error
	aggrType            modelv1.AggregationFunction
	emitPartial         bool
}

func newAggAllIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	mapFunc aggregation.Map[N],
	aggrType modelv1.AggregationFunction,
	emitPartial bool,
) executor.MIterator {
	return &aggAllIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		mapFunc:             mapFunc,
		aggrType:            aggrType,
		emitPartial:         emitPartial,
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
			value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
				GetValue()
			v, err := aggregation.FromFieldValue[N](value)
			if err != nil {
				ami.err = err
				return false
			}
			ami.mapFunc.In(v)
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
	if ami.emitPartial {
		part := ami.mapFunc.Partial()
		fvs, err := aggregation.PartialToFieldValues(ami.aggrType, part)
		if err != nil {
			ami.err = err
			return false
		}
		resultDp.Fields = make([]*measurev1.DataPoint_Field, len(fvs))
		for i, fv := range fvs {
			name := ami.aggregationFieldRef.Field.Name
			if i > 0 {
				name = "__agg_count"
			}
			resultDp.Fields[i] = &measurev1.DataPoint_Field{Name: name, Value: fv}
		}
	} else {
		val, err := aggregation.ToFieldValue(ami.mapFunc.Val())
		if err != nil {
			ami.err = err
			return false
		}
		resultDp.Fields = []*measurev1.DataPoint_Field{
			{Name: ami.aggregationFieldRef.Field.Name, Value: val},
		}
	}
	ami.result = resultDp
	return true
}

func (ami *aggAllIterator[N]) Current() []*measurev1.InternalDataPoint {
	if ami.result == nil {
		return nil
	}
	// For aggregation across all data, shard ID is not applicable
	return []*measurev1.InternalDataPoint{{DataPoint: ami.result, ShardId: 0}}
}

func (ami *aggAllIterator[N]) Close() error {
	return ami.prev.Close()
}

type reduceGroupIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	reduceFunc          aggregation.Reduce[N]
	err                 error
	groupMap            map[uint64][]*measurev1.InternalDataPoint
	groupByTagsRefs     [][]*logical.TagRef
	groupKeys           []uint64
	aggrType            modelv1.AggregationFunction
	index               int
}

func newReduceGroupIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	reduceFunc aggregation.Reduce[N],
	aggrType modelv1.AggregationFunction,
	groupByTagsRefs [][]*logical.TagRef,
) executor.MIterator {
	return &reduceGroupIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		reduceFunc:          reduceFunc,
		aggrType:            aggrType,
		groupByTagsRefs:     groupByTagsRefs,
		groupMap:            make(map[uint64][]*measurev1.InternalDataPoint),
	}
}

func (rgi *reduceGroupIterator[N]) loadGroups() bool {
	if rgi.groupKeys != nil {
		return true
	}
	for rgi.prev.Next() {
		group := rgi.prev.Current()
		for _, idp := range group {
			key, keyErr := formatGroupByKey(idp.GetDataPoint(), rgi.groupByTagsRefs)
			if keyErr != nil {
				rgi.err = keyErr
				return false
			}
			rgi.groupMap[key] = append(rgi.groupMap[key], idp)
		}
	}
	if closeErr := rgi.prev.Close(); closeErr != nil {
		rgi.err = closeErr
		return false
	}
	rgi.groupKeys = make([]uint64, 0, len(rgi.groupMap))
	for k := range rgi.groupMap {
		rgi.groupKeys = append(rgi.groupKeys, k)
	}
	return true
}

func (rgi *reduceGroupIterator[N]) Next() bool {
	if rgi.err != nil {
		return false
	}
	if !rgi.loadGroups() {
		return false
	}
	return rgi.index < len(rgi.groupKeys)
}

func (rgi *reduceGroupIterator[N]) Current() []*measurev1.InternalDataPoint {
	if rgi.err != nil || rgi.groupKeys == nil || rgi.index >= len(rgi.groupKeys) {
		return nil
	}
	key := rgi.groupKeys[rgi.index]
	rgi.index++
	idps := rgi.groupMap[key]
	if len(idps) == 0 {
		return nil
	}
	rgi.reduceFunc.Reset()
	var resultDp *measurev1.DataPoint
	for _, idp := range idps {
		dp := idp.GetDataPoint()
		fvs := make([]*modelv1.FieldValue, len(dp.GetFields()))
		for i, f := range dp.GetFields() {
			fvs[i] = f.GetValue()
		}
		part, partErr := aggregation.FieldValuesToPartial[N](rgi.aggrType, fvs)
		if partErr != nil {
			rgi.err = partErr
			return nil
		}
		rgi.reduceFunc.Combine(part)
		if resultDp == nil {
			resultDp = &measurev1.DataPoint{TagFamilies: dp.TagFamilies}
		}
	}
	if resultDp == nil {
		return nil
	}
	val, err := aggregation.ToFieldValue(rgi.reduceFunc.Val())
	if err != nil {
		rgi.err = err
		return nil
	}
	resultDp.Fields = []*measurev1.DataPoint_Field{
		{Name: rgi.aggregationFieldRef.Field.Name, Value: val},
	}
	return []*measurev1.InternalDataPoint{{DataPoint: resultDp, ShardId: 0}}
}

func (rgi *reduceGroupIterator[N]) Close() error {
	return rgi.prev.Close()
}
