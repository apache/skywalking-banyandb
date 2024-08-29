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

package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestMin(t *testing.T) {
	var err error

	// case1: input int64 elements
	minInt64, _ := NewMeasureAggregateFunction[int64, Void, Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minInt64.Combine(Arguments[int64, Void, Void]{
		arg0: []int64{1, 2, 3},
		arg1: nil,
		arg2: nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), minInt64.Result())

	// case2: input float64 elements
	minFloat64, _ := NewMeasureAggregateFunction[float64, Void, Void, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minFloat64.Combine(Arguments[float64, Void, Void]{
		arg0: []float64{1.0, 2.0, 3.0},
		arg1: nil,
		arg2: nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1.0, minFloat64.Result())

	// case3: input []int64 elements
	minInt64Arr, _ := NewMeasureAggregateFunction[[]int64, Void, Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minInt64Arr.Combine(Arguments[[]int64, Void, Void]{
		arg0: [][]int64{{1, 2}, {10, 20}},
		arg1: nil,
		arg2: nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), minInt64Arr.Result())

	// case4: unexpected input type
	minStr, _ := NewMeasureAggregateFunction[string, Void, Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minStr.Combine(Arguments[string, Void, Void]{
		// fixme If there is no element, can't recognize the wrong input type. It needs at least one variable.
		arg0: []string{"a"},
		arg1: nil,
		arg2: nil,
	})
	assert.Errorf(t, err, errFieldValueType.Error())

	// case5: input nothing, always OK
	minStrArr, _ := NewMeasureAggregateFunction[[]string, Void, Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_MIN)
	err = minStrArr.Combine(Arguments[[]string, Void, Void]{
		arg0: [][]string{},
		arg1: nil,
		arg2: nil,
	})
	assert.NoError(t, err)
}
