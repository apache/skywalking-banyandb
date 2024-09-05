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

func TestRate(t *testing.T) {
	var err error

	// case1: input int64 values
	rateInt64, _ := aggregate.NewFunction[int64, int64, int64](modelv1.MeasureAggregate_MEASURE_AGGREGATE_RATE)
	err = rateInt64.Combine(aggregate.NewRateArguments(
		[]int64{10, 100, 1000}, // mock the "denominator" column
		[]int64{1, 10, 100},    // mock the "numerator" column
	))
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), rateInt64.Result())
}
