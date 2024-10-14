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

package aggregate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure/aggregate"
)

func TestSum(t *testing.T) {
	var err error

	// case1: input int64 values
	sumInt64, _ := aggregate.NewFunction[int64, aggregate.Void, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_SUM)
	err = sumInt64.Combine(aggregate.NewSumArguments[int64](
		[]int64{1, 2, 3}, // mock the "summation" column
	))
	assert.NoError(t, err)
	_, _, r1 := sumInt64.Result()
	assert.Equal(t, int64(6), r1)

	// case2: input float64 values
	sumFloat64, _ := aggregate.NewFunction[float64, aggregate.Void, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_SUM)
	err = sumFloat64.Combine(aggregate.NewSumArguments[float64](
		[]float64{1.0, 2.0, 3.0}, // mock the "summation" column
	))
	assert.NoError(t, err)
	_, _, r2 := sumFloat64.Result()
	assert.Equal(t, 6.0, r2)

	// case3: 7 values inputted
	sumFloat64, _ = aggregate.NewFunction[float64, aggregate.Void, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_SUM)
	err = sumFloat64.Combine(aggregate.NewSumArguments[float64](
		[]float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0}, // mock the "summation" column
	))
	assert.NoError(t, err)
	_, _, r3 := sumFloat64.Result()
	assert.Equal(t, 28.0, r3)

	// case4: 4 values inputted
	sumFloat64, _ = aggregate.NewFunction[float64, aggregate.Void, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_SUM)
	err = sumFloat64.Combine(aggregate.NewSumArguments[float64](
		[]float64{1.0, 2.0, 3.0, 4.0}, // mock the "summation" column
	))
	assert.NoError(t, err)
	_, _, r4 := sumFloat64.Result()
	assert.Equal(t, 10.0, r4)
}
