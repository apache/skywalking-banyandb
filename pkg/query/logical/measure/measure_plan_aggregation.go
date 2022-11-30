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

	"github.com/pkg/errors"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	_ logical.UnresolvedPlan = (*unresolvedAggregation)(nil)
	_ logical.Plan           = (*aggregationPlan)(nil)
)

type unresolvedAggregation struct {
	unresolvedInput logical.UnresolvedPlan
	// aggrFunc is the type of aggregation
	aggrFunc modelv1.AggregationFunction
	// groupBy should be a subset of tag projection
	aggregationField *logical.Field
	isGroup          bool
}

func Aggregation(input logical.UnresolvedPlan, aggrField *logical.Field, aggrFunc modelv1.AggregationFunction, isGroup bool) logical.UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrFunc:         aggrFunc,
		aggregationField: aggrField,
		isGroup:          isGroup,
	}
}

func (gba *unresolvedAggregation) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
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
		return nil, errors.Wrap(logical.ErrFieldNotDefined, "aggregation schema")
	}
	aggrFunc, err := aggregation.NewInt64Func(gba.aggrFunc)
	if err != nil {
		return nil, err
	}
	return &aggregationPlan{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:              measureSchema,
		aggrFunc:            aggrFunc,
		aggregationFieldRef: aggregationFieldRefs[0],
		isGroup:             gba.isGroup,
	}, nil
}

type aggregationPlan struct {
	*logical.Parent
	schema              logical.Schema
	aggregationFieldRef *logical.FieldRef
	aggrFunc            aggregation.Int64Func
	aggrType            modelv1.AggregationFunction
	isGroup             bool
}

func (g *aggregationPlan) String() string {
	return fmt.Sprintf("%s aggregation: aggregation{type=%d,field=%s}",
		g.Input,
		g.aggrType,
		g.aggregationFieldRef.Field.Name)
}

func (g *aggregationPlan) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *aggregationPlan) Schema() logical.Schema {
	return g.schema.ProjFields(g.aggregationFieldRef)
}

func (g *aggregationPlan) Execute(ec executor.MeasureExecutionContext) (executor.MIterator, error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	if g.isGroup {
		return newAggGroupMIterator(iter, g.aggregationFieldRef, g.aggrFunc), nil
	}
	return newAggAllIterator(iter, g.aggregationFieldRef, g.aggrFunc), nil
}

type aggGroupIterator struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	aggrFunc            aggregation.Int64Func
}

func newAggGroupMIterator(
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrFunc aggregation.Int64Func,
) executor.MIterator {
	return &aggGroupIterator{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggGroupIterator) Next() bool {
	return ami.prev.Next()
}

func (ami *aggGroupIterator) Current() []*measurev1.DataPoint {
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
		{
			Name: ami.aggregationFieldRef.Field.Name,
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

func (ami *aggGroupIterator) Close() error {
	return ami.prev.Close()
}

type aggAllIterator struct {
	prev                executor.MIterator
	aggregationFieldRef *logical.FieldRef
	aggrFunc            aggregation.Int64Func

	result *measurev1.DataPoint
}

func newAggAllIterator(
	prev executor.MIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrFunc aggregation.Int64Func,
) executor.MIterator {
	return &aggAllIterator{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrFunc:            aggrFunc,
	}
}

func (ami *aggAllIterator) Next() bool {
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
		{
			Name: ami.aggregationFieldRef.Field.Name,
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

func (ami *aggAllIterator) Current() []*measurev1.DataPoint {
	if ami.result == nil {
		return nil
	}
	return []*measurev1.DataPoint{ami.result}
}

func (ami *aggAllIterator) Close() error {
	return nil
}
