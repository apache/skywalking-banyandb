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

package aggregation

import (
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// ResultCombiner handles combining aggregation results from multiple data nodes.
type ResultCombiner interface {
	Combine(futures []bus.Future, span *query.Span) (*measurev1.DataPoint, error)
}

// CombineFunc defines how to combine extracted values.
type CombineFunc func(values []float64) (value float64, isInt bool)

// universalCombiner handles all aggregation types with a unified approach.
type universalCombiner struct {
	combineFunc CombineFunc
	fieldName   string
	isMean      bool
}

// NewResultCombiner creates a result combiner for the specified aggregation function.
func NewResultCombiner(aggrFunc modelv1.AggregationFunction, fieldName string) (ResultCombiner, error) {
	switch aggrFunc {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return &universalCombiner{fieldName: fieldName, combineFunc: meanCombine, isMean: true}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return &universalCombiner{fieldName: fieldName, combineFunc: countCombine}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return &universalCombiner{fieldName: fieldName, combineFunc: maxCombine}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return &universalCombiner{fieldName: fieldName, combineFunc: minCombine}, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return &universalCombiner{fieldName: fieldName, combineFunc: sumCombine}, nil
	default:
		return nil, errors.WithMessagef(errUnknownFunc, "unknown function:%s", modelv1.AggregationFunction_name[int32(aggrFunc)])
	}
}

// Combine implements the universal combining logic.
func (u *universalCombiner) Combine(futures []bus.Future, span *query.Span) (*measurev1.DataPoint, error) {
	if u.isMean {
		return u.combineMean(futures, span)
	}
	return u.combineGeneric(futures, span)
}

// combineGeneric handles all non-mean aggregations.
func (u *universalCombiner) combineGeneric(futures []bus.Future, span *query.Span) (*measurev1.DataPoint, error) {
	values, err := extractValues(futures, u.fieldName, span)
	if err != nil {
		return nil, err
	}

	result, isInt := u.combineFunc(values)
	return createDataPoint(u.fieldName, result, isInt), nil
}

// combineMean handles mean aggregation with special sum/count logic.
func (u *universalCombiner) combineMean(futures []bus.Future, span *query.Span) (*measurev1.DataPoint, error) {
	var totalSum, totalCount float64
	var err error
	for _, f := range futures {
		if msg, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else if d := msg.Data(); d != nil {
			resp := d.(*measurev1.QueryResponse)
			if span != nil {
				span.AddSubTrace(resp.Trace)
			}

			for _, dp := range resp.DataPoints {
				mean, count := extractMeanAndCount(dp, u.fieldName)
				totalSum += mean * count
				totalCount += count
			}
		}
	}

	if err != nil {
		return nil, err
	}

	finalMean := float64(0)
	if totalCount > 0 {
		finalMean = totalSum / totalCount
	}

	return createDataPoint(u.fieldName, finalMean, false), nil
}

// extractValues extracts and converts field values to float64.
func extractValues(futures []bus.Future, fieldName string, span *query.Span) ([]float64, error) {
	var values []float64
	var err error

	for _, f := range futures {
		if msg, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else if d := msg.Data(); d != nil {
			resp := d.(*measurev1.QueryResponse)
			if span != nil {
				span.AddSubTrace(resp.Trace)
			}

			for _, dp := range resp.DataPoints {
				for _, field := range dp.Fields {
					if field.Name == fieldName {
						values = append(values, convertToFloat64(field.Value))
					}
				}
			}
		}
	}

	return values, err
}

// extractMeanAndCount extracts mean and count values from a DataPoint.
func extractMeanAndCount(dp *measurev1.DataPoint, fieldName string) (mean, count float64) {
	for _, field := range dp.Fields {
		switch field.Name {
		case fieldName:
			mean = convertToFloat64(field.Value)
		case "count":
			count = convertToFloat64(field.Value)
		}
	}
	return
}

// convertToFloat64 converts a FieldValue to float64.
func convertToFloat64(fieldValue *modelv1.FieldValue) float64 {
	switch v := fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return float64(v.Int.Value)
	case *modelv1.FieldValue_Float:
		return v.Float.Value
	default:
		return 0
	}
}

// createDataPoint creates a DataPoint with the specified value and type.
func createDataPoint(fieldName string, value float64, isInt bool) *measurev1.DataPoint {
	var fieldValue *modelv1.FieldValue
	if isInt {
		fieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(value)}}}
	} else {
		fieldValue = &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: value}}}
	}

	return &measurev1.DataPoint{
		Fields: []*measurev1.DataPoint_Field{{Name: fieldName, Value: fieldValue}},
	}
}

// Combine functions - each returns (result, isInt).
func countCombine(values []float64) (float64, bool) {
	sum := float64(0)
	for _, v := range values {
		sum += v
	}
	return sum, true // COUNT returns Int
}

func sumCombine(values []float64) (float64, bool) {
	sum := float64(0)
	for _, v := range values {
		sum += v
	}
	return sum, false // SUM returns Float
}

func maxCombine(values []float64) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	maxVal := values[0]
	for _, v := range values[1:] {
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal, false
}

func minCombine(values []float64) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	minVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
	}
	return minVal, false
}

func meanCombine(_ []float64) (float64, bool) {
	// This function won't be called as mean uses special logic
	return 0, false
}
