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
//
package logical

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ UnresolvedPlan = (*unresolvedAggregation)(nil)
	_ Plan           = (*measureAggregation)(nil)
)

type unresolvedAggregation struct {
	unresolvedInput UnresolvedPlan
	// aggrFunc is the type of aggregation
	aggrFunc modelv1.AggregationFunction
	// groupBy should be a subset of tag projection
	aggregationField *Field
	isGroup          bool
}

func Aggregation(input UnresolvedPlan, aggrField *Field, aggrFunc modelv1.AggregationFunction, isGroup bool) UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrFunc:         aggrFunc,
		aggregationField: aggrField,
		isGroup:          isGroup,
	}
}

func (gba *unresolvedAggregation) Analyze(measureSchema Schema) (Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	// check validity of aggregation fields
	aggregationFieldRefs, err := prevPlan.Schema().CreateFieldRef(gba.aggregationField)
	if err != nil {
		return nil, err
	}
	if len(aggregationFieldRefs) == 0 {
		return nil, errors.Wrap(ErrFieldNotDefined, "aggregation schema")
	}
	aggrFunc, err := aggregation.NewInt64Func(gba.aggrFunc)
	if err != nil {
		return nil, err
	}
	return &measureAggregation{
		parent: &parent{
			unresolvedInput: gba.unresolvedInput,
			input:           prevPlan,
		},
		schema:              measureSchema,
		aggrFunc:            aggrFunc,
		aggregationFieldRef: aggregationFieldRefs[0],
		isGroup:             gba.isGroup,
	}, nil
}

type measureAggregation struct {
	*parent
	schema              Schema
	aggregationFieldRef *FieldRef
	aggrFunc            aggregation.Int64Func
	aggrType            modelv1.AggregationFunction
	isGroup             bool
}

func (g *measureAggregation) String() string {
	return fmt.Sprintf("aggregation: aggregation{type=%d,field=%s}",
		g.aggrType,
		g.aggregationFieldRef.field.name)
}

func (g *measureAggregation) Type() PlanType {
	return PlanGroupByAggregation
}

func (g *measureAggregation) Equal(plan Plan) bool {
	if plan.Type() != PlanGroupByAggregation {
		return false
	}
	other := plan.(*measureAggregation)
	if g.aggrType == other.aggrType &&
		cmp.Equal(g.aggregationFieldRef, other.aggregationFieldRef) {
		return g.parent.input.Equal(other.parent.input)
	}

	return false
}

func (g *measureAggregation) Children() []Plan {
	return []Plan{g.input}
}

func (g *measureAggregation) Schema() Schema {
	return g.schema.ProjFields(g.aggregationFieldRef)
}

func (g *measureAggregation) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	if g.isGroup {
		return newAggGroupMIterator(iter, g.aggregationFieldRef, g.aggrFunc), nil
	}
	return newAggAllMIterator(iter, g.aggregationFieldRef, g.aggrFunc), nil
}

type aggGroupMIterator struct {
	prev                executor.MIterator
	aggregationFieldRef *FieldRef
	aggrFunc            aggregation.Int64Func
}

func newAggGroupMIterator(
	prev executor.MIterator,
	aggregationFieldRef *FieldRef,
	aggrFunc aggregation.Int64Func) executor.MIterator {
	return &aggGroupMIterator{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggGroupMIterator) Next() bool {
	return ami.prev.Next()
}

func (ami *aggGroupMIterator) Current() []*measurev1.DataPoint {
	ami.aggrFunc.Reset()
	group := ami.prev.Current()
	var resultDp *measurev1.DataPoint
	for _, dp := range group {
		value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
			GetValue().
			GetInt().
			GetValue()
		ami.aggrFunc.In(value)
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
	resultDp.Fields = []*measurev1.DataPoint_Field{
		&measurev1.DataPoint_Field{
			Name: ami.aggregationFieldRef.field.name,
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{
						Value: ami.aggrFunc.Val(),
					},
				},
			},
		},
	}
	return []*measurev1.DataPoint{resultDp}
}

func (ami *aggGroupMIterator) Close() error {
	return ami.prev.Close()
}

type aggAllMIterator struct {
	prev                executor.MIterator
	aggregationFieldRef *FieldRef
	aggrFunc            aggregation.Int64Func

	result *measurev1.DataPoint
}

func newAggAllMIterator(
	prev executor.MIterator,
	aggregationFieldRef *FieldRef,
	aggrFunc aggregation.Int64Func) executor.MIterator {
	return &aggAllMIterator{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggAllMIterator) Next() bool {
	if ami.result != nil {
		return false
	}
	defer ami.prev.Close()
	var resultDp *measurev1.DataPoint
	for ami.prev.Next() {
		group := ami.prev.Current()
		for _, dp := range group {
			value := dp.GetFields()[ami.aggregationFieldRef.Spec.FieldIdx].
				GetValue().
				GetInt().
				GetValue()
			ami.aggrFunc.In(value)
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
	resultDp.Fields = []*measurev1.DataPoint_Field{
		&measurev1.DataPoint_Field{
			Name: ami.aggregationFieldRef.field.name,
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{
						Value: ami.aggrFunc.Val(),
					},
				},
			},
		},
	}
	ami.result = resultDp
	return true
}

func (ami *aggAllMIterator) Current() []*measurev1.DataPoint {
	if ami.result == nil {
		return nil
	}
	return []*measurev1.DataPoint{ami.result}
}

func (ami *aggAllMIterator) Close() error {
	return nil
}
