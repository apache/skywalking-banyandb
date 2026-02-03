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

// Func supports aggregation operations.
type Func[N Number] interface {
	In(...N)
	Val() N
	Reset()
}

// Number denotes the supported number types.
type Number interface {
	~int64 | ~float64
}

// NewFunc returns a aggregation function based on function type.
// If forDistributedMean is true and af is MEAN, it returns a distributedMeanFunc that aggregates sum and count.
func NewFunc[N Number](af modelv1.AggregationFunction, forDistributedMean bool) (Func[N], error) {
	var result Func[N]
	switch af {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		if forDistributedMean {
			result = &distributedMeanFunc[N]{zero: zero[N]()}
		} else {
			result = &meanFunc[N]{zero: zero[N]()}
		}
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

// IsDistributedMean checks if the function is a distributed mean function.
func IsDistributedMean[N Number](f Func[N]) bool {
	_, ok := f.(*distributedMeanFunc[N])
	return ok
}

// GetSumCount returns sum and count if the function is a distributed mean function.
func GetSumCount[N Number](f Func[N]) (sum N, count N, ok bool) {
	if dmf, ok := f.(*distributedMeanFunc[N]); ok {
		return dmf.sum, dmf.count, true
	}
	return zero[N](), zero[N](), false
}
