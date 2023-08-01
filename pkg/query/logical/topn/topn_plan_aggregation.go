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

package topn

import (
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/pkg/errors"
)

var errUnsupportedAggregationField = errors.New("unsupported aggregation operation on this field")

type unresolvedAggregation struct {
	unresolvedInput  logical.UnresolvedPlan
	aggrType         modelv1.AggregationFunction
	aggregationField *logical.Field
}

func newUnresolvedAggregation(input logical.UnresolvedPlan, aggrType modelv1.AggregationFunction, aggrField *logical.Field) logical.UnresolvedPlan {
	return &unresolvedAggregation{
		unresolvedInput:  input,
		aggrType:         aggrType,
		aggregationField: aggrField,
	}
}

func (gba *unresolvedAggregation) Analyze(s logical.Schema) (logical.Plan, error) {
	prevPlan, err := gba.unresolvedInput.Analyze(s)
	if err != nil {
		return nil, err
	}
	schema := prevPlan.Schema()
	aggregationFieldRefs, err := schema.CreateFieldRef(gba.aggregationField)
	if err != nil {
		return nil, err
	}
	fieldRef := aggregationFieldRefs[0]
	switch fieldRef.Spec.Spec.FieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return newAggregationPlan[int64](gba, prevPlan, schema, fieldRef)
	default:
		return nil, errors.WithMessagef(errUnsupportedAggregationField, "field: %s", fieldRef.Spec.Spec)
	}
}

type aggregationPlan[N aggregation.Number] struct {
	*logical.Parent
	schema              logical.Schema
	aggregationFieldRef *logical.FieldRef
	aggrType            modelv1.AggregationFunction
}

func newAggregationPlan[N aggregation.Number](gba *unresolvedAggregation, prevPlan logical.Plan,
	topNSchema logical.Schema, fieldRef *logical.FieldRef,
) (*aggregationPlan[N], error) {
	return &aggregationPlan[N]{
		Parent: &logical.Parent{
			UnresolvedInput: gba.unresolvedInput,
			Input:           prevPlan,
		},
		schema:              topNSchema,
		aggrType:            gba.aggrType,
		aggregationFieldRef: fieldRef,
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

func (g *aggregationPlan[N]) Execute(ec executor.TopNExecutionContext) (executor.TIterator, error) {
	iter, err := g.Parent.Input.(executor.TopNExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	return newAggIterator(iter, g.aggregationFieldRef, g.aggrType), nil
}

type aggIterator[N aggregation.Number] struct {
	prev                executor.TIterator
	aggregationFieldRef *logical.FieldRef
	aggrType            modelv1.AggregationFunction
	cache               map[string]*aggregatorItem[N]
	result              []*streaming.Tuple2
	err                 error
}

func (ai *aggIterator[N]) Next() bool {
	if ai.result != nil || ai.err != nil {
		return false
	}
	for ai.prev.Next() {
		group := ai.prev.Current()
		for _, tuple := range group {
			v, err := aggregation.FromFieldValue[N](tuple.V2.(*modelv1.FieldValue))
			if err != nil {
				ai.err = err
				return false
			}

			entityValues := tuple.V1.(tsdb.EntityValues)
			key := entityValues.String()
			if item, found := ai.cache[key]; found {
				item.aggrFunc.In(v)
				continue
			}

			aggrFunc, err := aggregation.NewFunc[N](ai.aggrType)
			if err != nil {
				return false
			}
			item := &aggregatorItem[N]{
				key:      key,
				aggrFunc: aggrFunc,
				values:   entityValues,
			}
			item.aggrFunc.In(v)
			ai.cache[key] = item
		}
	}

	return true
}

func (ai *aggIterator[N]) Current() []*streaming.Tuple2 {
	var result []*streaming.Tuple2
	for _, item := range ai.cache {
		v, err := aggregation.ToFieldValue(item.aggrFunc.Val())
		if err != nil {
			ai.err = err
			return nil
		}
		dp := &streaming.Tuple2{
			V1: item.values,
			V2: v,
		}
		result = append(result, dp)
	}
	return result
}

func (ai *aggIterator[N]) Close() error {
	return ai.prev.Close()
}

func newAggIterator[N aggregation.Number](
	prev executor.TIterator,
	aggregationFieldRef *logical.FieldRef,
	aggrType modelv1.AggregationFunction,
) executor.TIterator {
	return &aggIterator[N]{
		prev:                prev,
		aggregationFieldRef: aggregationFieldRef,
		aggrType:            aggrType,
	}
}

type aggregatorItem[N aggregation.Number] struct {
	aggrFunc aggregation.Func[N]
	key      string
	values   tsdb.EntityValues
}

func (n *aggregatorItem[N]) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}
