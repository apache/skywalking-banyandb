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

package measure_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

// RegisterQLBatchTable verifies both public callers of the shared batch boundary.
func RegisterQLBatchTable(description string, observeNodes ...func(*measurev1.QueryRequest, *measurev1.QueryResponse)) bool {
	return g.Describe(description, g.Ordered, g.Label("measure-ql", "QLB-T02"), func() {
		var base time.Time
		var request *measurev1.QueryRequest
		var want *measurev1.QueryResponse
		var query string
		g.BeforeAll(func() {
			gm.Expect(vectorized.QueryMetricsEnabled).To(gm.BeTrue(), "QLB-T02 requires -tags=querymetrics")
			base = SharedContext.BaseTime.Add(-time.Minute)
			measuredata.Write(SharedContext.Connection, "qlb_fixture", "replicated_group", "qlb_fixture_old.json", base, time.Second)
			measuredata.Write(SharedContext.Connection, "qlb_fixture", "replicated_group", "qlb_fixture.json", base.Add(5*time.Second), time.Second)
			measuredata.Write(SharedContext.Connection, "qlb_fixture", "sw_metric", "qlb_fixture_other.json", base, time.Second)
			request = &measurev1.QueryRequest{}
			helpers.UnmarshalYAML([]byte(`name: qlb_fixture
groups: [replicated_group]
tagProjection:
  tagFamilies: [{name: default, tags: [rid, host, zone]}]
fieldProjection: {names: [v, f]}
`), request)
			request.TimeRange = helpers.TimeRange(helpers.Args{Offset: -time.Minute - time.Second, Duration: 7 * time.Second}, SharedContext)
			query = renderQLFixture(base, "legacy-select.ql", "")
			want = &measurev1.QueryResponse{}
			helpers.UnmarshalYAML([]byte(renderQLFixture(base, "six-rows.yaml", "")), want)
			for _, observe := range observeNodes {
				observe(request, want)
			}
			gm.Eventually(func(inner gm.Gomega) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := measuredata.VerifyQL(ctx, SharedContext.Connection, query, []byte(renderQLFixture(base, "six-rows.yaml", "")), false)
				inner.Expect(err).NotTo(gm.HaveOccurred())
			}, flags.EventuallyTimeout).Should(gm.Succeed())
		})
		g.It("raw-and-aggregate-equivalence", func() {
			verifyQLBatchCallers(query, request, want)
			aggregate := qlBatchAggregateRequest(request)
			verifyQLBatchCallers(qlBatchAggregateText(base, "replicated_group"), aggregate, qlBatchAggregateExpected(2, 4, 6))
		})
		g.It("hidden-columns-and-empty", func() {
			hidden := qlBatchAggregateRequest(request)
			hidden.Agg, hidden.GroupBy = nil, nil
			hidden.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"rid"}}}}
			hidden.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
				Name: "host", Op: modelv1.Condition_BINARY_OP_EQ, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}},
			}}}
			expected := &measurev1.QueryResponse{}
			helpers.UnmarshalYAML([]byte(renderQLFixture(base, "host-a.yaml", "")), expected)
			for _, point := range expected.DataPoints {
				point.TagFamilies[0].Tags = point.TagFamilies[0].Tags[:1]
				point.Fields = point.Fields[:1]
			}
			hiddenQL := fmt.Sprintf("SELECT rid, v::field FROM MEASURE qlb_fixture IN replicated_group TIME BETWEEN '%s' AND '%s' WHERE host = 'a'",
				base.Add(-time.Second).Format(time.RFC3339Nano), base.Add(6*time.Second).Format(time.RFC3339Nano))
			verifyQLBatchCallers(hiddenQL, hidden, expected)
			hidden.Criteria.GetCondition().Value.GetStr().Value = "absent"
			verifyQLBatchCallers(strings.ReplaceAll(hiddenQL, "'a'", "'absent'"), hidden, &measurev1.QueryResponse{})
		})
		g.It("multi-group-and-replica-dedup", func() {
			multiple := proto.Clone(request).(*measurev1.QueryRequest)
			multiple.Groups = []string{"replicated_group", "sw_metric"}
			expected := proto.Clone(want).(*measurev1.QueryResponse)
			other := &measurev1.DataPoint{}
			helpers.UnmarshalYAML([]byte(fmt.Sprintf(`timestamp: %s
tagFamilies:
- name: default
  tags:
  - {key: rid, value: {int: {value: 7}}}
  - {key: host, value: {str: {value: d}}}
  - {key: zone, value: {str: {value: z}}}
fields:
- {name: v, value: {int: {value: 10}}}
- {name: f, value: {float: {value: 8}}}
`, base.Format(time.RFC3339Nano))), other)
			expected.DataPoints = append(expected.DataPoints, other)
			gm.Eventually(func(inner gm.Gomega) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				response, err := measurev1.NewMeasureServiceClient(SharedContext.Connection).Query(ctx, multiple)
				inner.Expect(err).NotTo(gm.HaveOccurred())
				inner.Expect(measuredata.CompareQLResult(response, expected, false)).To(gm.Succeed())
			}, flags.EventuallyTimeout).Should(gm.Succeed())
			verifyQLBatchCallers(strings.Replace(query, "IN replicated_group", "IN replicated_group, sw_metric", 1), multiple, expected)
			verifyQLBatchCallers(query, request, want)
		})
	})
}

func verifyQLBatchCallers(text string, request *measurev1.QueryRequest, want *measurev1.QueryResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	qlText, params, err := helpers.ExtractQL(text)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	before := vmeasure.BatchExecutionStats()
	owners, reservations := vectorized.OutstandingOwnerPins(), vectorized.QueryMemoryStats().LiveReservedBytes
	native, err := measurev1.NewMeasureServiceClient(SharedContext.Connection).Query(ctx, request)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	gm.Expect(measuredata.CompareQLResult(native, want, false)).To(gm.Succeed())
	response, err := bydbqlv1.NewBydbQLServiceClient(SharedContext.Connection).Query(ctx, &bydbqlv1.QueryRequest{Query: qlText, Params: params})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	gm.Expect(measuredata.CompareQLResult(response.GetMeasureResult(), want, false)).To(gm.Succeed())
	after := vmeasure.BatchExecutionStats()
	gm.Expect(after.NativeBatchExecutions - before.NativeBatchExecutions).To(gm.BeNumerically(">=", 1))
	gm.Expect(after.QLBatchExecutions - before.QLBatchExecutions).To(gm.Equal(uint64(1)))
	gm.Expect(after.IntermediateDataPoints - before.IntermediateDataPoints).To(gm.BeZero())
	gm.Eventually(func(inner gm.Gomega) {
		inner.Expect(vmeasure.BatchExecutionStats().LiveResults).To(gm.Equal(before.LiveResults))
		inner.Expect(vectorized.OutstandingOwnerPins()).To(gm.Equal(owners))
		inner.Expect(vectorized.QueryMemoryStats().LiveReservedBytes).To(gm.Equal(reservations))
	}, flags.EventuallyTimeout).Should(gm.Succeed())
}

func qlBatchAggregateRequest(request *measurev1.QueryRequest) *measurev1.QueryRequest {
	result := proto.Clone(request).(*measurev1.QueryRequest)
	result.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"host"}}}}
	result.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{"v"}}
	result.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: result.TagProjection, FieldName: "v"}
	result.Agg = &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: "v"}
	return result
}

func qlBatchAggregateText(base time.Time, groups string) string {
	return fmt.Sprintf("SELECT host, v::field, SUM(v) FROM MEASURE qlb_fixture IN %s TIME BETWEEN '%s' AND '%s' GROUP BY host, v::field",
		groups, base.Add(-time.Second).Format(time.RFC3339Nano), base.Add(6*time.Second).Format(time.RFC3339Nano))
}

func qlBatchAggregateExpected(values ...int64) *measurev1.QueryResponse {
	result := &measurev1.QueryResponse{}
	for index, host := range []string{"a", "b", "c"} {
		result.DataPoints = append(result.DataPoints, &measurev1.DataPoint{
			TagFamilies: []*modelv1.TagFamily{{Name: "default", Tags: []*modelv1.Tag{
				{Key: "host", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: host}}}},
			}}},
			Fields: []*measurev1.DataPoint_Field{{Name: "v", Value: &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: values[index]}}}}},
		})
	}
	return result
}
