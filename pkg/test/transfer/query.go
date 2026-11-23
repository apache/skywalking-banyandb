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

package transfer

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

const (
	// queryLimit bounds every suite query explicitly, far above the rows the suites write.
	queryLimit = 1000
	// queryWindow is how far before and after now the suite queries look.
	queryWindow = 24 * time.Hour
)

// suiteQuery is one query-layer check: run returns the normalized result of the query on
// the instance behind conn.
type suiteQuery struct {
	run  func(conn *grpc.ClientConn) ([]string, error)
	name string
}

// ExpectSameQueries runs the suite's query-layer checks against src and dst over the same
// time window and asserts that each query returns a non-empty result on src and the same
// normalized result on dst: a measure query by an entity tag, a stream query by a
// non-entity indexed tag, a trace query ordered by an index rule (the order must match),
// and an index-mode measure query. Rowdiff compares stored rows only; these queries also
// read the target's series and inverted indexes.
func ExpectSameQueries(g gomega.Gomega, src, dst *grpc.ClientConn) {
	now := time.Now().Truncate(time.Millisecond)
	tr := &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-queryWindow)), End: timestamppb.New(now.Add(queryWindow))}
	for _, q := range suiteQueries(tr) {
		want, err := q.run(src)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "%s on the source", q.name)
		g.Expect(want).NotTo(gomega.BeEmpty(), "%s on the source", q.name)
		got, err := q.run(dst)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "%s on the target", q.name)
		g.Expect(got).To(gomega.Equal(want), "%s returns the same result on the source and the target", q.name)
	}
}

func suiteQueries(tr *modelv1.TimeRange) []suiteQuery {
	return []suiteQuery{
		{name: "the measure query by the entity tag", run: measureQuery(&measurev1.QueryRequest{
			Groups:          []string{MeasureGroup},
			Name:            MeasureName,
			TimeRange:       tr,
			Criteria:        eqCriteria("entity_id", "entity_1"),
			TagProjection:   &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"id", "entity_id"}}}},
			FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"total", "value"}},
			Limit:           queryLimit,
		})},
		{name: "the stream query by a non-entity indexed tag", run: streamQuery(&streamv1.QueryRequest{
			Groups:    []string{StreamGroup},
			Name:      StreamName,
			TimeRange: tr,
			Criteria:  eqCriteria("endpoint_id", "/home_id"),
			Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "searchable", Tags: []string{"trace_id", "service_id", "service_instance_id", "state", "endpoint_id", "duration"}},
			}},
			Limit: queryLimit,
		})},
		{name: "the trace query ordered by timestamp", run: traceQuery(&tracev1.QueryRequest{
			Groups:        []string{TraceGroup},
			Name:          TraceName,
			TimeRange:     tr,
			OrderBy:       &modelv1.QueryOrder{IndexRuleName: "timestamp", Sort: modelv1.Sort_SORT_DESC},
			TagProjection: []string{"trace_id", "span_id", "service_id", "duration"},
			Limit:         queryLimit,
		})},
		{name: "the index-mode measure query", run: measureQuery(&measurev1.QueryRequest{
			Groups:    []string{IndexModeGroup},
			Name:      IndexModeName,
			TimeRange: tr,
			TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"id", "service_id", "name", "short_name", "service_group", "layer"}},
			}},
			Limit: queryLimit,
		})},
	}
}

func eqCriteria(tag, value string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  tag,
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
	}}}
}

// measureQuery renders each data point as its timestamp, version, tags and fields; the
// series id is left out, and the points are sorted and deduplicated.
func measureQuery(req *measurev1.QueryRequest) func(conn *grpc.ClientConn) ([]string, error) {
	return func(conn *grpc.ClientConn) ([]string, error) {
		ctx, cancel := callCtx()
		defer cancel()
		resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, req)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetDataPoints()))
		for _, dp := range resp.GetDataPoints() {
			var sb strings.Builder
			fmt.Fprintf(&sb, "%d v%d", dp.GetTimestamp().AsTime().UnixNano(), dp.GetVersion())
			writeTagFamilies(&sb, dp.GetTagFamilies())
			for _, f := range dp.GetFields() {
				fmt.Fprintf(&sb, " %s=%s", f.GetName(), f.GetValue().String())
			}
			out = append(out, sb.String())
		}
		return sortedSet(out), nil
	}
}

// streamQuery renders each element as its id, timestamp and tags, sorted and deduplicated.
func streamQuery(req *streamv1.QueryRequest) func(conn *grpc.ClientConn) ([]string, error) {
	return func(conn *grpc.ClientConn) ([]string, error) {
		ctx, cancel := callCtx()
		defer cancel()
		resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, req)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetElements()))
		for _, e := range resp.GetElements() {
			var sb strings.Builder
			fmt.Fprintf(&sb, "%s %d", e.GetElementId(), e.GetTimestamp().AsTime().UnixNano())
			writeTagFamilies(&sb, e.GetTagFamilies())
			out = append(out, sb.String())
		}
		return sortedSet(out), nil
	}
}

// traceQuery renders each trace as its id and its spans, keeping the order of the traces
// and sorting the spans of each trace.
func traceQuery(req *tracev1.QueryRequest) func(conn *grpc.ClientConn) ([]string, error) {
	return func(conn *grpc.ClientConn) ([]string, error) {
		ctx, cancel := callCtx()
		defer cancel()
		resp, err := tracev1.NewTraceServiceClient(conn).Query(ctx, req)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetTraces()))
		for _, t := range resp.GetTraces() {
			spans := make([]string, 0, len(t.GetSpans()))
			for _, s := range t.GetSpans() {
				var sb strings.Builder
				fmt.Fprintf(&sb, "%s %s", s.GetSpanId(), base64.StdEncoding.EncodeToString(s.GetSpan()))
				writeTags(&sb, "", s.GetTags())
				spans = append(spans, sb.String())
			}
			out = append(out, t.GetTraceId()+": "+strings.Join(sortedSet(spans), " | "))
		}
		return out, nil
	}
}

func writeTagFamilies(sb *strings.Builder, families []*modelv1.TagFamily) {
	for _, f := range families {
		writeTags(sb, f.GetName()+".", f.GetTags())
	}
}

func writeTags(sb *strings.Builder, prefix string, tags []*modelv1.Tag) {
	for _, t := range tags {
		fmt.Fprintf(sb, " %s%s=%s", prefix, t.GetKey(), t.GetValue().String())
	}
}

func sortedSet(s []string) []string {
	slices.Sort(s)
	return slices.Compact(s)
}
