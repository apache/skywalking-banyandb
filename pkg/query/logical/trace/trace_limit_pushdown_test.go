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

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type limitPushdownExecutionContext struct {
	executor.TraceExecutionContext
}

func (limitPushdownExecutionContext) GetTagValueDecoder() model.TagValueDecoder {
	return nil
}

func TestTraceLimitPushdownWithResidualFilters(t *testing.T) {
	metadata := &commonv1.Metadata{Name: "trace", Group: "default"}
	traceSchema, schemaErr := BuildSchema(&databasev1.Trace{
		Metadata:         metadata,
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}, []*databasev1.IndexRule{{Metadata: &commonv1.Metadata{Name: "timestamp"}, Tags: []string{"timestamp"}}})
	require.NoError(t, schemaErr)

	equals := func(name, value string) *modelv1.Criteria {
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  name,
			Op:    modelv1.Condition_BINARY_OP_EQ,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
		}}}
	}
	and := func(left, right *modelv1.Criteria) *modelv1.Criteria {
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
			Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
			Left:  left,
			Right: right,
		}}}
	}
	for _, testCase := range []struct {
		criteria     *modelv1.Criteria
		name         string
		maxTraceSize int
	}{
		{name: "unfiltered", maxTraceSize: 5},
		{name: "trace ID only", criteria: equals("trace_id", "trace_001"), maxTraceSize: 5},
		{name: "span ID", criteria: equals("span_id", "span_001")},
		{name: "trace ID and span ID", criteria: and(equals("trace_id", "trace_001"), equals("span_id", "span_001"))},
		{name: "trace ID and tag", criteria: and(equals("trace_id", "trace_001"), equals("service", "api"))},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			plan, analyzeErr := Analyze(&tracev1.QueryRequest{
				Name:   metadata.Name,
				Groups: []string{metadata.Group},
				TimeRange: &modelv1.TimeRange{
					Begin: timestamppb.New(time.Unix(0, 0)),
					End:   timestamppb.New(time.Unix(3600, 0)),
				},
				Criteria: testCase.criteria,
				OrderBy:  &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC},
				Limit:    2,
				Offset:   3,
			}, []*commonv1.Metadata{metadata}, []logical.Schema{traceSchema},
				[]executor.TraceExecutionContext{limitPushdownExecutionContext{}},
				[]string{"trace_id"}, []string{"span_id"}, []string{"timestamp"})
			require.NoError(t, analyzeErr)
			limit, isLimit := plan.(*traceLimit)
			require.True(t, isLimit)
			require.Equal(t, uint32(2), limit.limitNum)
			require.Equal(t, uint32(3), limit.offsetNum)

			// Walk the analyzed plan so this covers filter construction and the
			// optimizer's traversal, rather than calling localScan.Limit directly.
			scanPlan := plan
			for len(scanPlan.Children()) > 0 {
				children := scanPlan.Children()
				require.Len(t, children, 1)
				scanPlan = children[0]
			}
			scan, isScan := scanPlan.(*localScan)
			require.True(t, isScan)
			require.Equal(t, testCase.maxTraceSize, scan.maxTraceSize)
			require.NotNil(t, scan.order)
			require.Equal(t, modelv1.Sort_SORT_DESC, scan.order.Sort)
		})
	}
}
