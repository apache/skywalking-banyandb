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

package query_test

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	vtrace "github.com/apache/skywalking-banyandb/pkg/query/vectorized/trace"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

var _ = ginkgo.Describe("trace span-ID filter before limit", func() {
	ginkgo.It("finds an older matching trace beyond the newest candidates (issue 14172)", func() {
		client := tracev1.NewTraceServiceClient(casestrace.SharedContext.Connection)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// The shared fixture has eight traces. trace_001 is older than the
		// newest two, but its span must still match when the result limit is two.
		request := &tracev1.QueryRequest{
			Name:   "sw",
			Groups: []string{"test-trace-group"},
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(casestrace.SharedContext.BaseTime),
				End:   timestamppb.New(casestrace.SharedContext.BaseTime.Add(time.Hour)),
			},
			OrderBy: &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC},
			Limit:   2,
		}
		newest, queryErr := client.Query(ctx, request)
		gomega.Expect(queryErr).NotTo(gomega.HaveOccurred())
		gomega.Expect(newest.Traces).To(gomega.HaveLen(2))
		gomega.Expect(newest.Traces[0].TraceId).To(gomega.Equal("trace_008"))
		gomega.Expect(newest.Traces[1].TraceId).To(gomega.Equal("trace_007"))

		request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: "span_id",
				Op:   modelv1.Condition_BINARY_OP_IN,
				Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{
					StrArray: &modelv1.StrArray{Value: []string{"span_001_1"}},
				}},
			},
		}}
		// Only the limit changes: a wider candidate window proves that the
		// data exists and the same time range and span-ID filter can find it.
		wideRequest := proto.Clone(request).(*tracev1.QueryRequest)
		wideRequest.Limit = 8
		before := vtrace.QueryCount()
		wide, wideErr := client.Query(ctx, wideRequest)
		gomega.Expect(wideErr).NotTo(gomega.HaveOccurred())
		gomega.Expect(wide.Traces).To(gomega.HaveLen(1))
		gomega.Expect(wide.Traces[0].TraceId).To(gomega.Equal("trace_001"))
		gomega.Expect(vtrace.QueryCount()).To(gomega.BeNumerically(">", before), "must exercise the vectorized path")

		before = vtrace.QueryCount()
		limited, limitedErr := client.Query(ctx, request)
		gomega.Expect(limitedErr).NotTo(gomega.HaveOccurred())
		gomega.Expect(vtrace.QueryCount()).To(gomega.BeNumerically(">", before), "limited query must exercise the vectorized path")
		ginkgo.GinkgoWriter.Printf("span-ID query: limit=8 returned %d trace; limit=2 returned %d traces\n", len(wide.Traces), len(limited.Traces))
		gomega.Expect(limited.Traces).To(gomega.HaveLen(1),
			"LIMIT 2 must retain the sole matching trace, even when newer nonmatching traces precede it")
		gomega.Expect(proto.Equal(limited, wide)).To(gomega.BeTrue(), "changing the limit must preserve the sole matching trace and its spans")
	})
})
