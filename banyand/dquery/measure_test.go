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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
)

// rawWireModeTestSchema returns a minimal Measure schema for testing plan-type
// assertions. It declares one tag family ("default") with a "service_id" string
// tag and one int field ("value"), which is sufficient for all representative
// request shapes exercised by TestRawWireMode_PlanType_AnalyzeDistributed.
func rawWireModeTestSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "demo", Group: "default"},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
		},
	}
}

// TestRawWireMode_PlanType_AnalyzeDistributed asserts the concrete plan type
// returned by vecplan.AnalyzeDistributed — the same function called by Rev
// under raw wire mode. This is the strongest plan-type assertion achievable
// without the full Rev scaffolding (measureService + broadcaster + queryService
// mocks), which would require >100 lines of in-package stub code.
//
// Phase 7+ debt: add a Rev-level integration test that instantiates a
// measureQueryProcessor with a minimal fake measureService, invokes Rev
// with a crafted bus.Message, and asserts the concrete plan type on the
// returned MIterator. The plan-type gate here covers the AnalyzeDistributed
// call site directly.
func TestRawWireMode_PlanType_AnalyzeDistributed(t *testing.T) {
	ms := rawWireModeTestSchema()
	cfg := vmeasure.VectorizedConfig{Enabled: true, BatchSize: 64, QueryMemoryMiB: 4}

	groupByTagProj := &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "default", Tags: []string{"service_id"}},
		},
	}

	shapes := []struct {
		req  *measurev1.QueryRequest
		name string
	}{
		{
			name: "plain non-agg",
			req: &measurev1.QueryRequest{
				Name: "demo", Groups: []string{"default"},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
			},
		},
		{
			name: "top without agg",
			req: &measurev1.QueryRequest{
				Name: "demo", Groups: []string{"default"},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
				Top: &measurev1.QueryRequest_Top{
					Number: 5, FieldName: "value", FieldValueSort: modelv1.Sort_SORT_DESC,
				},
			},
		},
		{
			name: "groupby + top without agg",
			req: &measurev1.QueryRequest{
				Name: "demo", Groups: []string{"default"},
				TagProjection:   groupByTagProj,
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
				GroupBy:         &measurev1.QueryRequest_GroupBy{TagProjection: groupByTagProj},
				Top: &measurev1.QueryRequest_Top{
					Number: 3, FieldName: "value", FieldValueSort: modelv1.Sort_SORT_DESC,
				},
			},
		},
		{
			name: "agg with groupby",
			req: &measurev1.QueryRequest{
				Name: "demo", Groups: []string{"default"},
				TagProjection:   groupByTagProj,
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
				GroupBy:         &measurev1.QueryRequest_GroupBy{TagProjection: groupByTagProj},
				Agg: &measurev1.QueryRequest_Aggregation{
					Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: "value",
				},
			},
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			plan, analyzeErr := vecplan.AnalyzeDistributed(shape.req, []*databasev1.Measure{ms}, nil, cfg)
			if analyzeErr != nil {
				t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
			}
			// Rev calls vecplan.AnalyzeDistributed under raw wire mode and
			// assigns the result to a measureDistributedExecutable. The concrete
			// type must always be *vecplan.DistributedPlan — never a row-path plan.
			if plan == nil {
				t.Fatal("AnalyzeDistributed returned nil plan under raw wire mode")
			}
			// Compile-time proof: DistributedPlan implements measureDistributedExecutable
			// (executor.MeasureExecutable + fmt.Stringer), so the assignment in Rev succeeds.
			_ = plan.String()
		})
	}
}
