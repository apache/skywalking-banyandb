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
	"fmt"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"

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
	aggrType         modelv1.AggregationFunction
	isGroup          bool
	isTop            bool
}

func newUnresolvedAggregation(input logical.UnresolvedPlan, aggrField *logical.Field, aggrFunc modelv1.AggregationFunction, isGroup bool, isTop bool) logical.UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrFunc:         aggrFunc,
		aggregationField: aggrField,
		isGroup:          isGroup,
		isTop:            isTop,
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
	aggrFunc            aggregation.Func[N]
	aggrType            modelv1.AggregationFunction
	isGroup             bool
	isTop               bool
}

func newAggregationPlan[N aggregation.Number](gba *unresolvedAggregation, prevPlan logical.Plan,
	measureSchema logical.Schema, fieldRef *logical.FieldRef,
) (*aggregationPlan[N], error) {
	aggrFunc, err := aggregation.NewFunc[N](gba.aggrFunc)
	if err != nil {
		return nil, err
	}
	return &aggregationPlan[N]{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:              measureSchema,
		aggrFunc:            aggrFunc,
		aggrType:            gba.aggrFunc,
		aggregationFieldRef: fieldRef,
		isGroup:             gba.isGroup,
		isTop:               gba.isTop,
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

func (g *aggregationPlan[N]) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	if g.isGroup {
		return newAggGroupMIterator[N](iter, g.aggregationFieldRef, g.aggrFunc), nil
	}
	if g.isTop {
		return newAggTopIterator[N](iter, g.aggregationFieldRef, g.aggrType), nil
	}
	return newAggAllIterator[N](iter, g.aggregationFieldRef, g.aggrFunc), nil
}

type aggGroupIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	aggrFunc            aggregation.Func[N]

	err error
}

func newAggGroupMIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrFunc aggregation.Func[N],
) executor.MIterator {
	return &aggGroupIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggGroupIterator[N]) Next() bool {
	if ami.err != nil {
		return false
	}
	return ami.prev.Next()
}

func (ami *aggGroupIterator[N]) Current() []*measurev1.DataPoint {
	if ami.err != nil {
		return nil
	}
	ami.aggrFunc.Reset()
	group := ami.prev.Current()
	var resultDp *measurev1.DataPoint
	for _, dp := range group {
		value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
			GetValue()
		v, err := aggregation.FromFieldValue[N](value)
		if err != nil {
			ami.err = err
			return nil
		}
		ami.aggrFunc.In(v)
		if resultDp != nil {
			continue
		}
		resultDp = &measurev1.DataPoint{
			TagFamilies: dp.TagFamilies,
		}
	}
	if resultDp == nil {
		return nil
	}
	val, err := aggregation.ToFieldValue(ami.aggrFunc.Val())
	if err != nil {
		ami.err = err
		return nil
	}
	resultDp.Fields = []*measurev1.DataPoint_Field{
		{
			Name:  ami.aggregationFieldRef.Field.Name,
			Value: val,
		},
	}
	return []*measurev1.DataPoint{resultDp}
}

func (ami *aggGroupIterator[N]) Close() error {
	return multierr.Combine(ami.err, ami.prev.Close())
}

type aggAllIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	aggrFunc            aggregation.Func[N]

	result *measurev1.DataPoint
	err    error
}

func newAggAllIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrFunc aggregation.Func[N],
) executor.MIterator {
	return &aggAllIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggAllIterator[N]) Next() bool {
	if ami.result != nil || ami.err != nil {
		return false
	}
	var resultDp *measurev1.DataPoint
	for ami.prev.Next() {
		group := ami.prev.Current()
		for _, dp := range group {
			value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
				GetValue()
			v, err := aggregation.FromFieldValue[N](value)
			if err != nil {
				ami.err = err
				return false
			}
			ami.aggrFunc.In(v)
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
	val, err := aggregation.ToFieldValue(ami.aggrFunc.Val())
	if err != nil {
		ami.err = err
		return false
	}
	resultDp.Fields = []*measurev1.DataPoint_Field{
		{
			Name:  ami.aggregationFieldRef.Field.Name,
			Value: val,
		},
	}
	ami.result = resultDp
	return true
}

func (ami *aggAllIterator[N]) Current() []*measurev1.DataPoint {
	if ami.result == nil {
		return nil
	}
	return []*measurev1.DataPoint{ami.result}
}

func (ami *aggAllIterator[N]) Close() error {
	return ami.prev.Close()
}

type aggTopIterator[N aggregation.Number] struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	aggrType            modelv1.AggregationFunction
	cache               map[string]*aggregatorItem[N]
	result              []*measurev1.DataPoint
	err                 error
}

func newAggTopIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrType modelv1.AggregationFunction,
) executor.MIterator {
	return &aggTopIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrType:            aggrType,
		cache:               make(map[string]*aggregatorItem[N]),
	}
}

func (ati *aggTopIterator[N]) Next() bool {
	var latestTimestamp int64
	if ati.result != nil || ati.err != nil {
		return false
	}
	var resultDps []*measurev1.DataPoint
	for ati.prev.Next() {
		group := ati.prev.Current()
		for _, dp := range group {
			timestampMillis := dp.GetTimestamp().AsTime().Unix()
			if latestTimestamp < timestampMillis {
				latestTimestamp = timestampMillis
			}
			value := dp.GetFields()[ati.aggregationFieldRef.Spec.FieldIdx].
				GetValue()
			v, err := aggregation.FromFieldValue[N](value)
			if err != nil {
				ati.err = err
				return false
			}

			tagFamily := dp.GetTagFamilies()[0]
			if tagFamily.GetName() == measure.TopNTagFamily {
				key := tagFamily.String()
				if item, found := ati.cache[key]; found {
					item.aggrFunc.In(v)
					continue
				}

				aggrFunc, err := aggregation.NewFunc[N](ati.aggrType)
				if err != nil {
					return false
				}
				item := &aggregatorItem[N]{
					key:      key,
					aggrFunc: aggrFunc,
					values:   tagFamily,
				}
				item.aggrFunc.In(v)
				ati.cache[key] = item
			}
		}
	}

	for _, item := range ati.cache {
		val, err := aggregation.ToFieldValue(item.aggrFunc.Val())
		if err != nil {
			ati.err = err
			return false
		}
		resultDp := &measurev1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(0, latestTimestamp)),
			TagFamilies: []*modelv1.TagFamily{
				item.values,
			},
			Fields: []*measurev1.DataPoint_Field{
				{
					Name:  ati.aggregationFieldRef.Field.Name,
					Value: val,
				},
			},
		}
		resultDps = append(resultDps, resultDp)
	}
	ati.result = resultDps
	return true
}

func (ati *aggTopIterator[N]) Current() []*measurev1.DataPoint {
	if ati.result == nil {
		return nil
	}
	return ati.result
}

func (ati *aggTopIterator[N]) Close() error {
	return ati.prev.Close()
}

type aggregatorItem[N aggregation.Number] struct {
	aggrFunc aggregation.Func[N]
	key      string
	values   *modelv1.TagFamily
}
