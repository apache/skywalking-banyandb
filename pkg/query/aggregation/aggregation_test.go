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

package aggregation

import (
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TestNewMap_RejectsCountDistinct and TestNewReduce_RejectsCountDistinct pin
// that NewMap/NewReduce keep returning an unknown-function error for
// COUNT_DISTINCT (design doc §7.3) — this is not an oversight, it is the
// mechanism by which a COUNT_DISTINCT TopNAggregation rule fails loudly
// (banyand/measure/topn_post_processor.go calls NewMap and propagates the
// error) instead of mis-aggregating. COUNT_DISTINCT has its own accumulator
// (Distinct, see distinct.go) precisely because it does not fit the
// Number-parameterized Map/Reduce shape.
func TestNewMap_RejectsCountDistinct(t *testing.T) {
	if _, err := NewMap[int64](modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT); err == nil {
		t.Fatal("NewMap must reject AGGREGATION_FUNCTION_COUNT_DISTINCT")
	}
}

func TestNewReduce_RejectsCountDistinct(t *testing.T) {
	if _, err := NewReduce[int64](modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT); err == nil {
		t.Fatal("NewReduce must reject AGGREGATION_FUNCTION_COUNT_DISTINCT")
	}
}
