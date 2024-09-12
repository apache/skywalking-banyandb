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

func TestAvg(t *testing.T) {
	var err error

	// case1: input int64 values
	avgInt64, _ := aggregate.NewFunction[int64, int64, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG)
	err = avgInt64.Combine(aggregate.NewAvgArguments[int64](
		[]int64{1, 2, 3}, // mock the "summation" column
		[]int64{1, 1, 1}, // mock the "count" column
	))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), avgInt64.Result()) // note that 7/3 becomes 2 as int

	// case2: input float64 elements
	avgFloat64, _ := aggregate.NewFunction[float64, int64, float64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_AVG)
	err = avgFloat64.Combine(aggregate.NewAvgArguments[float64](
		[]float64{1.0, 3.0, 3.0}, // mock the "summation" column
		[]int64{1, 1, 1},         // mock the "count" column
	))
	assert.NoError(t, err)
	assert.Equal(t, 7.0/3, avgFloat64.Result())
}
