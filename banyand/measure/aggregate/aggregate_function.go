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

// Package aggregate for measure aggregate function.
package aggregate

import (
	"fmt"
	"math"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// Void type contains nothing. It works as a placeholder for type parameters of `Arguments`.
type Void struct{}

// Input covers possible types of Function's arguments. It synchronizes with
// `pbv1.ValueType`, excluding `ValueTypeUnknown` and `ValueTypeBinaryData`.
type Input interface {
	Void | ~int64 | ~[]int64 | ~float64 | ~string | ~[]string
}

// Output covers possible types of Function's return value.
// todo It doesn't cover string type.
type Output interface {
	~int64 | ~float64
}

var errFieldValueType = fmt.Errorf("unsupported input value type on this field")

// Arguments represents the argument array, with at most three arguments.
type Arguments[A, B, C Input] struct {
	arg0 []A
	arg1 []B
	arg2 []C
}

// Function describes two stages of aggregation.
type Function[A, B, C Input, K Output] interface {
	// Combine takes elements to do the aggregation.
	// It uses a two-dimensional array to represent the argument array.
	Combine(arguments Arguments[A, B, C]) error

	// Result gives the result for the aggregation.
	// It uses "keep" value type to represent output value type.
	Result() K
}

// NewMeasureAggregateFunction is the factory for Function.
func NewMeasureAggregateFunction[A, B, C Input, K Output](aggregate modelv1.MeasureAggregate) (Function[A, B, C, K], error) {
	var function Function[A, B, C, K]
	switch aggregate {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		function = &Min[A, B, C, K]{minimum: maxValue[K]()}
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
		function = &Avg[A, B, C, K]{summation: zeroValue[K](), count: 0}
	default:
		return nil, fmt.Errorf("MeasureAggregate unknown")
	}

	return function, nil
}

func zeroValue[K Output]() K {
	var z K
	return z
}

func maxValue[K Output]() (r K) {
	switch x := any(&r).(type) {
	case *int64:
		*x = math.MaxInt64
	case *float64:
		*x = math.MaxFloat64
	case *string:
		*x = ""
	default:
		panic("unreachable")
	}
	return
}
