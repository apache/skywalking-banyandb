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

// Package aggregation implements aggregation functions to statistic a range of values.
package aggregation

import (
	"math"

	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var (
	errUnknownFunc          = errors.New("unknown aggregation function")
	errUnSupportedFieldType = errors.New("unsupported field type")
)

// Partial represents the intermediate result of a Map phase.
// For most functions only Value is meaningful; for MEAN both Value (sum) and Count are used.
type Partial[N Number] struct {
	Value N
	Count N
}

// Map accumulates raw values and produces aggregation results.
// It serves as the local accumulator for raw data points.
type Map[N Number] interface {
	In(N)
	Val() N
	Partial() Partial[N]
	Reset()
}

// Reduce combines intermediate results from Map phases into a final value.
type Reduce[N Number] interface {
	Combine(Partial[N])
	Val() N
	Reset()
}

// Number denotes the supported number types.
type Number interface {
	~int64 | ~float64
}

// NewMap returns a Map aggregation function for the given type.
func NewMap[N Number](af modelv1.AggregationFunction) (Map[N], error) {
	var result Map[N]
	switch af {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		result = &meanFunc[N]{zero: zero[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		result = &countFunc[N]{zero: zero[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		result = &maxFunc[N]{min: minOf[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		result = &minFunc[N]{max: maxOf[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		result = &sumFunc[N]{zero: zero[N]()}
	default:
		return nil, errors.WithMessagef(errUnknownFunc, "unknown function:%s", modelv1.AggregationFunction_name[int32(af)])
	}
	result.Reset()
	return result, nil
}

// NewReduce returns a Reduce aggregation function for the given type.
func NewReduce[N Number](af modelv1.AggregationFunction) (Reduce[N], error) {
	var result Reduce[N]
	switch af {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		result = &meanReduceFunc[N]{zero: zero[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		result = &countReduceFunc[N]{zero: zero[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		result = &maxReduceFunc[N]{min: minOf[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		result = &minReduceFunc[N]{max: maxOf[N]()}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		result = &sumReduceFunc[N]{zero: zero[N]()}
	default:
		return nil, errors.WithMessagef(errUnknownFunc, "unknown function:%s", modelv1.AggregationFunction_name[int32(af)])
	}
	result.Reset()
	return result, nil
}

// FromFieldValue transforms modelv1.FieldValue to Number.
func FromFieldValue[N Number](fieldValue *modelv1.FieldValue) (N, error) {
	switch fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return N(fieldValue.GetInt().Value), nil
	case *modelv1.FieldValue_Float:
		return N(fieldValue.GetFloat().Value), nil
	}
	return zero[N](), errUnSupportedFieldType
}

// ToFieldValue transforms Number to modelv1.FieldValue.
func ToFieldValue[N Number](value N) (*modelv1.FieldValue, error) {
	switch any(value).(type) {
	case int64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(value)}}}, nil
	case float64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(value)}}}, nil
	}
	return nil, errUnSupportedFieldType
}

// PartialToFieldValues converts a Partial to field values for wire transport.
// For MEAN it returns two values (Value/sum first, Count second); for others one value.
func PartialToFieldValues[N Number](af modelv1.AggregationFunction, p Partial[N]) ([]*modelv1.FieldValue, error) {
	if af == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN {
		vFv, err := ToFieldValue(p.Value)
		if err != nil {
			return nil, err
		}
		cFv, err := ToFieldValue(p.Count)
		if err != nil {
			return nil, err
		}
		return []*modelv1.FieldValue{vFv, cFv}, nil
	}
	vFv, err := ToFieldValue(p.Value)
	if err != nil {
		return nil, err
	}
	return []*modelv1.FieldValue{vFv}, nil
}

// FieldValuesToPartial converts field values from wire transport to a Partial.
// For MEAN expects two values (sum, count); for others one value (Count will be zero).
func FieldValuesToPartial[N Number](af modelv1.AggregationFunction, fvs []*modelv1.FieldValue) (Partial[N], error) {
	var p Partial[N]
	if len(fvs) == 0 {
		return p, nil
	}
	v, err := FromFieldValue[N](fvs[0])
	if err != nil {
		return p, err
	}
	p.Value = v
	if af == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN && len(fvs) >= 2 {
		c, err := FromFieldValue[N](fvs[1])
		if err != nil {
			return p, err
		}
		p.Count = c
	}
	return p, nil
}

func minOf[N Number]() (r N) {
	switch x := any(&r).(type) {
	case *int64:
		*x = math.MinInt64
	case *float64:
		*x = -math.MaxFloat64
	default:
		panic("unreachable")
	}
	return
}

func maxOf[N Number]() (r N) {
	switch x := any(&r).(type) {
	case *int64:
		*x = math.MaxInt64
	case *float64:
		*x = math.MaxFloat64
	default:
		panic("unreachable")
	}
	return
}

func zero[N Number]() N {
	var z N
	return z
}
