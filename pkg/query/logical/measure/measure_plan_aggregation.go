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

// distributedMeanFieldNames returns the field names for sum and count in distributed mean aggregation.
func distributedMeanFieldNames(fieldName string) (sumName, countName string) {
	return fieldName + "_sum", fieldName + "_count"
}

// buildAggregationOutputFields builds output fields from aggregation result.
// For distributed mean at liaison (1 ref), outputs Val() (mean); at data node (2 refs), outputs sum and count.
func buildAggregationOutputFields[N aggregation.Number](
	aggrFunc aggregation.Func[N],
	outputRefs []*logical.FieldRef,
	aggrType modelv1.AggregationFunction,
) ([]*measurev1.DataPoint_Field, error) {
	if aggrType == modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN {
		if len(outputRefs) == 1 {
			val, valErr := aggregation.ToFieldValue(aggrFunc.Val())
			if valErr != nil {
				return nil, valErr
			}
			return []*measurev1.DataPoint_Field{
				{Name: outputRefs[0].Field.Name, Value: val},
			}, nil
		}
		type sumCountGetter interface {
			GetSumCount() (N, N)
		}
		if sc, ok := any(aggrFunc).(sumCountGetter); ok {
			sumVal, countVal := sc.GetSumCount()
			sumFieldVal, sumErr := aggregation.ToFieldValue(sumVal)
			if sumErr != nil {
				return nil, sumErr
			}
			countFieldVal, countErr := aggregation.ToFieldValue(countVal)
			if countErr != nil {
				return nil, countErr
			}
			return []*measurev1.DataPoint_Field{
				{Name: outputRefs[0].Field.Name, Value: sumFieldVal},
				{Name: outputRefs[1].Field.Name, Value: countFieldVal},
			}, nil
		}
	}
	val, err := aggregation.ToFieldValue(aggrFunc.Val())
	if err != nil {
		return nil, err
	}
	return []*measurev1.DataPoint_Field{
		{Name: outputRefs[0].Field.Name, Value: val},
	}, nil
}

type unresolvedAggregation struct {
	unresolvedInput  logical.UnresolvedPlan
	aggregationField *logical.Field
	aggrFunc         modelv1.AggregationFunction
	isGroup          bool
}

func newUnresolvedAggregation(
	input logical.UnresolvedPlan,
	aggrField *logical.Field,
	aggrFunc modelv1.AggregationFunction,
	isGroup bool,
) logical.UnresolvedPlan {
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
	schema := prevPlan.Schema()
	inputRefs, err := schema.CreateFieldRef(gba.aggregationField)
	if err != nil {
		return nil, err
	}
	if len(inputRefs) == 0 {
		return nil, errors.Wrap(errFieldNotDefined, "aggregation schema")
	}
	inputRef := inputRefs[0]
	isLiaisonMerge := isAggregationOverDistributedPlan(prevPlan)
	outputRefs := resolveAggregationOutputFieldRefs(inputRef, gba.aggrFunc, isLiaisonMerge)
	switch inputRef.Spec.Spec.FieldType {
	case databasev1.FieldType_FIELD_TYPE_INT:
		return newAggregationPlan[int64](gba, prevPlan, schema, inputRef, outputRefs)
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return newAggregationPlan[float64](gba, prevPlan, schema, inputRef, outputRefs)
	default:
		return nil, errors.WithMessagef(errUnsupportedAggregationField, "field: %s", inputRef.Spec.Spec)
	}
}

// isAggregationOverDistributedPlan returns true when aggregation is over GroupBy over DistributedPlan (liaison merge).
func isAggregationOverDistributedPlan(prevPlan logical.Plan) bool {
	gb, ok := prevPlan.(*groupBy)
	if !ok {
		return false
	}
	_, ok = gb.Input.(*distributedPlan)
	return ok
}

// resolveAggregationOutputFieldRefs returns the output field refs for aggregation.
// For distributed mean at liaison, returns single "value" ref (mean); at data node returns field_sum and field_count.
func resolveAggregationOutputFieldRefs(inputRef *logical.FieldRef, aggrFunc modelv1.AggregationFunction,
	isLiaisonMerge bool,
) []*logical.FieldRef {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN {
		if isLiaisonMerge {
			return []*logical.FieldRef{inputRef}
		}
		sumName, countName := distributedMeanFieldNames(inputRef.Field.Name)
		fieldType := inputRef.Spec.Spec.FieldType
		return []*logical.FieldRef{
			{Field: logical.NewField(sumName), Spec: &logical.FieldSpec{FieldIdx: 0, Spec: &databasev1.FieldSpec{Name: sumName, FieldType: fieldType}}},
			{Field: logical.NewField(countName), Spec: &logical.FieldSpec{FieldIdx: 1, Spec: &databasev1.FieldSpec{Name: countName, FieldType: fieldType}}},
		}
	}
	return []*logical.FieldRef{inputRef}
}

type aggregationPlan[N aggregation.Number] struct {
	schema   logical.Schema
	aggrFunc aggregation.Func[N]
	*logical.Parent
	aggregationInputFieldRef *logical.FieldRef
	aggregationOutputRefs    []*logical.FieldRef
	aggrType                 modelv1.AggregationFunction
	isGroup                  bool
}

func newAggregationPlan[N aggregation.Number](gba *unresolvedAggregation, prevPlan logical.Plan,
	measureSchema logical.Schema, inputRef *logical.FieldRef, outputRefs []*logical.FieldRef,
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
		schema:                   measureSchema,
		aggrFunc:                 aggrFunc,
		aggregationInputFieldRef: inputRef,
		aggregationOutputRefs:    outputRefs,
		aggrType:                 gba.aggrFunc,
		isGroup:                  gba.isGroup,
	}, nil
}

func (g *aggregationPlan[N]) String() string {
	return fmt.Sprintf("%s aggregation: aggregation{type=%d,field=%s}",
		g.Input,
		g.aggrType,
		g.aggregationInputFieldRef.Field.Name)
}

func (g *aggregationPlan[N]) Children() []logical.Plan {
	return []logical.Plan{g.Input}
}

func (g *aggregationPlan[N]) Schema() logical.Schema {
	mSchema, ok := g.schema.(*schema)
	if !ok {
		return g.schema.ProjFields(g.aggregationOutputRefs...)
	}
	extended := mSchema.extendWithFieldRefs(g.aggregationOutputRefs)
	return extended.ProjFields(g.aggregationOutputRefs...)
}

func (g *aggregationPlan[N]) Execute(ec context.Context) (executor.MIterator, error) {
	iter, err := g.Parent.Input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	if g.isGroup {
		return newAggGroupMIterator(iter, g.aggregationInputFieldRef, g.aggregationOutputRefs, g.aggrFunc, g.aggrType), nil
	}
	return newAggAllIterator(iter, g.aggregationInputFieldRef, g.aggregationOutputRefs, g.aggrFunc, g.aggrType), nil
}

type aggGroupIterator[N aggregation.Number] struct {
	prev                  executor.MIterator
	aggrFunc              aggregation.Func[N]
	err                   error
	aggregationInputRef   *logical.FieldRef
	aggregationOutputRefs []*logical.FieldRef
	aggrType              modelv1.AggregationFunction
}

func newAggGroupMIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationInputRef *logical.FieldRef,
	aggregationOutputRefs []*logical.FieldRef,
	aggrFunc aggregation.Func[N],
	aggrType modelv1.AggregationFunction,
) executor.MIterator {
	return &aggGroupIterator[N]{
		prev:                  prev,
		aggregationInputRef:   aggregationInputRef,
		aggregationOutputRefs: aggregationOutputRefs,
		aggrFunc:              aggrFunc,
		aggrType:              aggrType,
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
	ami.aggrFunc.Reset()
	group := ami.prev.Current()
	var resultDp *measurev1.DataPoint
	var shardID uint32
	for _, idp := range group {
		dp := idp.GetDataPoint()
		if ami.aggrType == modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN {
			sumName, countName := distributedMeanFieldNames(ami.aggregationInputRef.Field.Name)
			var sumVal, countVal *modelv1.FieldValue
			for _, f := range dp.GetFields() {
				switch f.Name {
				case sumName:
					sumVal = f.GetValue()
				case countName:
					countVal = f.GetValue()
				}
			}
			if sumVal != nil && countVal != nil {
				sum, sumErr := aggregation.FromFieldValue[N](sumVal)
				if sumErr != nil {
					ami.err = sumErr
					return nil
				}
				count, countErr := aggregation.FromFieldValue[N](countVal)
				if countErr != nil {
					ami.err = countErr
					return nil
				}
				ami.aggrFunc.In(sum, count)
			} else {
				value := dp.GetFields()[ami.aggregationInputRef.Spec.FieldIdx].GetValue()
				v, parseErr := aggregation.FromFieldValue[N](value)
				if parseErr != nil {
					ami.err = parseErr
					return nil
				}
				ami.aggrFunc.In(v)
			}
		} else {
			value := dp.GetFields()[ami.aggregationInputRef.Spec.FieldIdx].GetValue()
			v, parseErr := aggregation.FromFieldValue[N](value)
			if parseErr != nil {
				ami.err = parseErr
				return nil
			}
			ami.aggrFunc.In(v)
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
	fields, buildErr := buildAggregationOutputFields(ami.aggrFunc, ami.aggregationOutputRefs, ami.aggrType)
	if buildErr != nil {
		ami.err = buildErr
		return nil
	}
	resultDp.Fields = fields
	return []*measurev1.InternalDataPoint{{DataPoint: resultDp, ShardId: shardID}}
}

func (ami *aggGroupIterator[N]) Close() error {
	return multierr.Combine(ami.err, ami.prev.Close())
}

type aggAllIterator[N aggregation.Number] struct {
	prev                  executor.MIterator
	aggrFunc              aggregation.Func[N]
	err                   error
	aggregationInputRef   *logical.FieldRef
	result                *measurev1.DataPoint
	aggregationOutputRefs []*logical.FieldRef
	aggrType              modelv1.AggregationFunction
}

func newAggAllIterator[N aggregation.Number](
	prev executor.MIterator,
	aggregationInputRef *logical.FieldRef,
	aggregationOutputRefs []*logical.FieldRef,
	aggrFunc aggregation.Func[N],
	aggrType modelv1.AggregationFunction,
) executor.MIterator {
	return &aggAllIterator[N]{
		prev:                  prev,
		aggregationInputRef:   aggregationInputRef,
		aggregationOutputRefs: aggregationOutputRefs,
		aggrFunc:              aggrFunc,
		aggrType:              aggrType,
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
			if ami.aggrType == modelv1.AggregationFunction_AGGREGATION_FUNCTION_DISTRIBUTED_MEAN {
				sumName, countName := distributedMeanFieldNames(ami.aggregationInputRef.Field.Name)
				var sumVal, countVal *modelv1.FieldValue
				for _, f := range dp.GetFields() {
					if f.Name == sumName {
						sumVal = f.GetValue()
					} else if f.Name == countName {
						countVal = f.GetValue()
					}
				}
				if sumVal != nil && countVal != nil {
					sum, sumErr := aggregation.FromFieldValue[N](sumVal)
					if sumErr != nil {
						ami.err = sumErr
						return false
					}
					count, countErr := aggregation.FromFieldValue[N](countVal)
					if countErr != nil {
						ami.err = countErr
						return false
					}
					ami.aggrFunc.In(sum, count)
				} else {
					value := dp.GetFields()[ami.aggregationInputRef.Spec.FieldIdx].GetValue()
					v, parseErr := aggregation.FromFieldValue[N](value)
					if parseErr != nil {
						ami.err = parseErr
						return false
					}
					ami.aggrFunc.In(v)
				}
			} else {
				value := dp.GetFields()[ami.aggregationInputRef.Spec.FieldIdx].GetValue()
				v, parseErr := aggregation.FromFieldValue[N](value)
				if parseErr != nil {
					ami.err = parseErr
					return false
				}
				ami.aggrFunc.In(v)
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
	fields, buildErr := buildAggregationOutputFields(ami.aggrFunc, ami.aggregationOutputRefs, ami.aggrType)
	if buildErr != nil {
		ami.err = buildErr
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
	// For aggregation across all data, shard ID is not applicable
	return []*measurev1.InternalDataPoint{{DataPoint: ami.result, ShardId: 0}}
}

func (ami *aggAllIterator[N]) Close() error {
	return ami.prev.Close()
}
