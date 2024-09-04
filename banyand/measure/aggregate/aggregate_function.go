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

// Input covers possible types of Function's arguments. It synchronizes with `FieldType` in schema.
type Input interface {
	Void | ~int64 | ~float64
}

// Output covers possible types of Function's return value.
type Output interface {
	~int64 | ~float64
}

var errFieldValueType = fmt.Errorf("unsupported input value type on this field")

// Arguments represents the argument array, with one argument or two arguments.
type Arguments[A, B Input] struct {
	arg0 []A
	arg1 []B
}

// Function describes two stages of aggregation.
type Function[A, B Input, R Output] interface {
	// Combine takes elements to do the aggregation.
	// It uses a two-dimensional array to represent the argument array.
	Combine(arguments Arguments[A, B]) error

	// Result gives the result for the aggregation.
	Result() R
}

// NewFunction constructs the aggregate function with given kind and parameter types.
func NewFunction[A, B Input, R Output](kind modelv1.MeasureAggregate) (Function[A, B, R], error) {
	var function Function[A, B, R]
	switch kind {
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN:
		function = &Min[A, B, R]{minimum: maxValue[R]()}
	case modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG:
		function = &Avg[A, B, R]{summation: zeroValue[R](), count: 0}
	default:
		return nil, fmt.Errorf("MeasureAggregate unknown")
	}

	return function, nil
}

func zeroValue[R Output]() R {
	var r R
	return r
}

func maxValue[R Output]() (r R) {
	switch a := any(&r).(type) {
	case *int64:
		*a = math.MaxInt64
	case *float64:
		*a = math.MaxFloat64
	case *string:
		*a = ""
	default:
		panic("unreachable")
	}
	return
}
