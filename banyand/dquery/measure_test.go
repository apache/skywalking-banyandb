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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dquery

import (
	"testing"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestUseVecDistributedMeasurePlan(t *testing.T) {
	cases := []struct {
		name string
		raw  bool
		req  *measurev1.QueryRequest
		want bool
	}{
		{
			name: "raw agg",
			raw:  true,
			req: &measurev1.QueryRequest{
				Agg: &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: "value",
				},
			},
			want: true,
		},
		{
			name: "raw non agg",
			raw:  true,
			req:  &measurev1.QueryRequest{Groups: []string{"default"}},
			want: true,
		},
		{
			name: "raw non agg multi group",
			raw:  true,
			req:  &measurev1.QueryRequest{Groups: []string{"a", "b"}},
			want: false,
		},
		{
			name: "raw non agg top",
			raw:  true,
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				Top: &measurev1.QueryRequest_Top{
					Number:         5,
					FieldName:      "value",
					FieldValueSort: modelv1.Sort_SORT_DESC,
				},
			},
			want: false,
		},
		{
			name: "proto agg",
			raw:  false,
			req: &measurev1.QueryRequest{
				Agg: &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: "value",
				},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := useVecDistributedMeasurePlan(tc.raw, tc.req); got != tc.want {
				t.Fatalf("useVecDistributedMeasurePlan(%v, agg=%v) = %v, want %v", tc.raw, tc.req.GetAgg() != nil, got, tc.want)
			}
		})
	}
}
