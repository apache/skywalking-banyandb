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

// Void type contains nothing. It works as a placeholder for type parameter.
type Void struct{}

// MAFInput synchronizes with `pbv1.ValueType`, excluding `ValueTypeUnknown`
// and `ValueTypeBinaryData`.
type MAFInput interface {
	Void | ~string | ~int64 | ~float64 | ~[]string | ~[]int64
}

// MAFKeep represents the only two types of value hold by MAF.
type MAFKeep interface {
	~int64 | ~float64
}

var errFieldValueType = fmt.Errorf("unsupported input value type on this field")

// MAFArguments represents the argument array, with at most three arguments.
type MAFArguments[A, B, C MAFInput] struct {
	arg0 []A
	arg1 []B
	arg2 []C
}

// MAF describes two stages of aggregation.
type MAF[A, B, C MAFInput, K MAFKeep] interface {
	// Combine takes elements to do the aggregation.
	// It uses a two-dimensional array to represent the argument array.
	Combine(arguments MAFArguments[A, B, C]) error

	// Result gives the result for the aggregation.
	// It uses "keep" value type to represent output value type.
	Result() K
}

// NewMeasureAggregateFunction is the factory for MAF.
func NewMeasureAggregateFunction[A, B, C MAFInput, K MAFKeep](aggregate modelv1.MeasureAggregate) (MAF[A, B, C, K], error) {
	var function MAF[A, B, C, K]
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

func zeroValue[K MAFKeep]() K {
	var z K
	return z
}

func maxValue[K MAFKeep]() (r K) {
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
