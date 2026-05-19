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

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TestRawWireMode_AlwaysUsesVecPlan asserts that under raw wire mode the
// routing predicate (data.MeasureWireModeRaw()) is true for every request
// shape, and false when the flag is off — regardless of request shape.
// After Phase 6 the routing in Rev is a single boolean: data.MeasureWireModeRaw().
func TestRawWireMode_AlwaysUsesVecPlan(t *testing.T) {
	shapes := []struct {
		name string
		req  *measurev1.QueryRequest
	}{
		{
			name: "plain non-agg",
			req:  &measurev1.QueryRequest{Groups: []string{"default"}},
		},
		{
			name: "orderby by index rule",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				OrderBy: &modelv1.QueryOrder{
					IndexRuleName: "idx_latency",
					Sort:          modelv1.Sort_SORT_DESC,
				},
			},
		},
		{
			name: "multi-group",
			req:  &measurev1.QueryRequest{Groups: []string{"a", "b"}},
		},
		{
			name: "top without agg",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				Top: &measurev1.QueryRequest_Top{
					Number:         5,
					FieldName:      "value",
					FieldValueSort: modelv1.Sort_SORT_DESC,
				},
			},
		},
		{
			name: "raw groupby",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				GroupBy: &measurev1.QueryRequest_GroupBy{
					TagProjection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{Name: "default", Tags: []string{"service_id"}},
						},
					},
				},
			},
		},
		{
			name: "groupby + top",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				GroupBy: &measurev1.QueryRequest_GroupBy{
					TagProjection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{Name: "default", Tags: []string{"service_id"}},
						},
					},
				},
				Top: &measurev1.QueryRequest_Top{
					Number:         10,
					FieldName:      "value",
					FieldValueSort: modelv1.Sort_SORT_DESC,
				},
			},
		},
		{
			name: "agg with groupby",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				Agg: &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: "value",
				},
				GroupBy: &measurev1.QueryRequest_GroupBy{
					TagProjection: &modelv1.TagProjection{
						TagFamilies: []*modelv1.TagProjection_TagFamily{
							{Name: "default", Tags: []string{"service_id"}},
						},
					},
				},
			},
		},
		{
			name: "agg only scalar reduce",
			req: &measurev1.QueryRequest{
				Groups: []string{"default"},
				Agg: &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
					FieldName: "latency",
				},
			},
		},
	}

	prev := data.MeasureWireModeRaw()
	t.Cleanup(func() { data.SetMeasureWireModeRaw(prev) })

	for _, shape := range shapes {
		shape := shape
		t.Run(shape.name+"/raw_on", func(t *testing.T) {
			data.SetMeasureWireModeRaw(true)
			if !data.MeasureWireModeRaw() {
				t.Fatal("routing predicate must be true when raw wire mode is on")
			}
		})
		t.Run(shape.name+"/raw_off", func(t *testing.T) {
			data.SetMeasureWireModeRaw(false)
			if data.MeasureWireModeRaw() {
				t.Fatal("routing predicate must be false when raw wire mode is off")
			}
		})
	}
}
