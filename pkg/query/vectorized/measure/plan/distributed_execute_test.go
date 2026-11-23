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

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	baseframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/frame"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

type fakeFuture struct{ msg bus.Message }

func (f fakeFuture) Get() (bus.Message, error)      { return f.msg, nil }
func (f fakeFuture) GetAll() ([]bus.Message, error) { return []bus.Message{f.msg}, nil }

// fakeCluster plays the data nodes: it answers a broadcast with one columnar frame per node, or
// with rawBody verbatim when set.
type fakeCluster struct {
	build   func(node int) *vectorized.RecordBatch
	rawBody []byte
	nodes   int
	traced  bool
}

func (c *fakeCluster) Broadcast(_ time.Duration, _ bus.Topic, _ bus.Message) ([]bus.Future, error) {
	futures := make([]bus.Future, 0, c.nodes)
	for node := range c.nodes {
		body := c.rawBody
		if body == nil {
			var err error
			body, err = frame.Encode(c.build(node))
			if err != nil {
				return nil, err
			}
		}
		var data any = body
		if c.traced {
			data = &measurev1.InternalQueryResponse{RawFrameBody: body}
		}
		futures = append(futures, fakeFuture{msg: bus.NewMessage(bus.MessageID(node), data)})
	}
	return futures, nil
}

func (c *fakeCluster) TimeRange() *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(1000, 0)), End: timestamppb.New(time.Unix(2000, 0))}
}

func (c *fakeCluster) NodeSelectors() map[string][]string { return nil }

func (c *fakeCluster) Scatter(_ context.Context, topic bus.Topic, message bus.Message, _ bus.ScatterOptions) ([]bus.NodeStream, error) {
	return scatterOfBroadcast(c.Broadcast, topic, message)
}

var _ executor.DistributedExecutionContext = (*fakeCluster)(nil)

func rowsBatch(node int) *vectorized.RecordBatch {
	schema := distributedRowsOrderBySchema()
	b := vectorized.NewRecordBatch(schema, 0)
	for i := range 60 {
		ts := int64(1000 + i*3 + node)
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(ts)
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(1)
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(i % 7))
		b.Columns[3].(*vectorized.TypedColumn[int64]).Append(int64(node))
		field := b.Columns[4].(*vectorized.TypedColumn[int64])
		if i%11 == 5 {
			field.AppendNull()
		} else {
			field.Append(int64(i * (node + 1)))
		}
		svc := b.Columns[5].(*vectorized.TypedColumn[string])
		if i%9 == 4 {
			svc.AppendNull()
		} else {
			svc.Append(fmt.Sprintf("service-%d-of-node-%d", i%5, node))
		}
		b.Columns[6].(*vectorized.TypedColumn[int64]).Append(int64(i % 3))
		b.Len++
	}
	return b
}

func executeRows(t *testing.T, cluster *fakeCluster) []*measurev1.InternalDataPoint {
	t.Helper()
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{defaultName},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           100,
	}
	plan, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 64})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	ctx := executor.WithDistributedExecutionContext(t.Context(), cluster)
	iter, execErr := plan.Execute(ctx)
	if execErr != nil {
		t.Fatalf("Execute: %v", execErr)
	}
	var out []*measurev1.InternalDataPoint
	for iter.Next() {
		for _, idp := range iter.Current() {
			out = append(out, proto.Clone(idp).(*measurev1.InternalDataPoint))
		}
	}
	if iterErr := iter.Close(); iterErr != nil {
		t.Fatalf("Close: %v", iterErr)
	}
	return out
}

// TestExecute_Rows runs the liaison plan against data nodes that send columnar frames, with and
// without tracing: the client sees the same rows either way, and no pin on a received frame
// outlives the query.
func TestExecute_Rows(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	pinsBefore := vectorized.OutstandingOwnerPins()
	var baseline []*measurev1.InternalDataPoint
	for _, traced := range []bool{false, true} {
		t.Run(fmt.Sprintf("traced=%t", traced), func(t *testing.T) {
			got := executeRows(t, &fakeCluster{nodes: 3, build: rowsBatch, traced: traced})
			if len(got) == 0 {
				t.Fatal("no rows")
			}
			if baseline == nil {
				baseline = got
				return
			}
			requireSameRows(t, baseline, got)
		})
	}
	if got := vectorized.OutstandingOwnerPins() - pinsBefore; got != 0 {
		t.Fatalf("%d pins on received frames outlived the queries", got)
	}
}

// TestExecute_RejectsOtherFrameVersions feeds the liaison a frame of the removed row-oriented
// version 3, as a data node of an older release would send: the query fails with a clear error
// instead of reading the frame as columnar.
func TestExecute_RejectsOtherFrameVersions(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{defaultName},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           100,
	}
	plan, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 64})
	if analyzeErr != nil {
		t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
	}
	cluster := &fakeCluster{nodes: 2, rawBody: []byte{0x00, 'V', 'F', 'R', 3, 0, 0}}
	iter, execErr := plan.Execute(executor.WithDistributedExecutionContext(t.Context(), cluster))
	if execErr == nil {
		for iter.Next() {
			t.Fatal("a version 3 frame produced rows")
		}
		execErr = iter.Close()
	}
	if execErr == nil || !strings.Contains(execErr.Error(), "wire version") {
		t.Fatalf("got %v, want a wire version error", execErr)
	}
}

func requireSameRows(t *testing.T, want, got []*measurev1.InternalDataPoint) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("rows: want %d, got %d", len(want), len(got))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Fatalf("row %d differs:\n want %v\n got  %v", i, want[i], got[i])
		}
	}
}

// TestExecute_EarlyCloseReleasesFrames closes the result before reading it: the limit leaves most
// merged batches unread, and the frames they pin must still be released.
func TestExecute_EarlyCloseReleasesFrames(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	pinsBefore := vectorized.OutstandingOwnerPins()
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{defaultName},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           3,
	}
	plan, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 4, QueryMemoryMiB: 64})
	if analyzeErr != nil {
		t.Fatal(analyzeErr)
	}
	cluster := &fakeCluster{nodes: 3, build: rowsBatch}
	iter, execErr := plan.Execute(executor.WithDistributedExecutionContext(t.Context(), cluster))
	if execErr != nil {
		t.Fatal(execErr)
	}
	if got := vectorized.OutstandingOwnerPins() - pinsBefore; got == 0 {
		t.Fatal("the merged batches pin no frame; the test would prove nothing")
	}
	if !iter.Next() {
		t.Fatal("no rows")
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if got := vectorized.OutstandingOwnerPins() - pinsBefore; got != 0 {
		t.Fatalf("%d pins survived Close", got)
	}
}

func aggBatch(node int) *vectorized.RecordBatch {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: defaultName, Name: tagSvc, Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
	})
	b := vectorized.NewRecordBatch(schema, 0)
	for i := range 40 {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(int64(node))
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("group-with-a-longer-name-%d", (i+node)%13))
		b.Columns[2].(*vectorized.TypedColumn[int64]).Append(int64(i*10 + node))
		b.Len++
	}
	return b
}

// TestExecute_Agg is TestExecute_Rows for an aggregation: the liaison reduces the columnar partials of the data nodes.
func TestExecute_Agg(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	pinsBefore := vectorized.OutstandingOwnerPins()
	run := func(t *testing.T) []*measurev1.InternalDataPoint {
		req := &measurev1.QueryRequest{
			Name:          "demo",
			Groups:        []string{defaultName},
			TagProjection: projTagProj(),
			GroupBy:       &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj()},
			Agg:           &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
			Top:           &measurev1.QueryRequest_Top{Number: 5, FieldName: fieldValue, FieldValueSort: modelv1.Sort_SORT_DESC},
		}
		plan, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 64})
		if analyzeErr != nil {
			t.Fatalf("AnalyzeDistributed: %v", analyzeErr)
		}
		cluster := &fakeCluster{nodes: 3, build: aggBatch}
		iter, execErr := plan.Execute(executor.WithDistributedExecutionContext(t.Context(), cluster))
		if execErr != nil {
			t.Fatalf("Execute: %v", execErr)
		}
		var out []*measurev1.InternalDataPoint
		for iter.Next() {
			for _, idp := range iter.Current() {
				out = append(out, proto.Clone(idp).(*measurev1.InternalDataPoint))
			}
		}
		if closeErr := iter.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		return out
	}
	if got := run(t); len(got) != 5 {
		t.Fatalf("top-5 returned %d rows", len(got))
	}
	if got := vectorized.OutstandingOwnerPins() - pinsBefore; got != 0 {
		t.Fatalf("%d pins survived", got)
	}
}

// TestExecute_PromotedStringsOutliveTheFrames runs a query whose data nodes disagree on a tag's column type
// (a tag move that reached them unevenly): one sends the tag as a native string column, the other as a TagValue,
// so the liaison wraps the string cells into the promoted column. The rows the client receives are built after
// the plan's frames are released, and must not read them.
func TestExecute_PromotedStringsOutliveTheFrames(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	defer baseframe.SetPoisonReleasedMessages(baseframe.SetPoisonReleasedMessages(true))
	cluster := &fakeCluster{nodes: 2, build: func(node int) *vectorized.RecordBatch {
		native := rowsBatch(node)
		if node == 0 {
			return native
		}
		defs := append([]vectorized.ColumnDef(nil), native.Schema.Columns...)
		defs[5].Type = vectorized.ColumnTypeTagValue
		promoted := vectorized.NewRecordBatch(vectorized.NewBatchSchema(defs), 0)
		for ci := range native.Columns {
			if ci != 5 {
				promoted.Columns[ci] = native.Columns[ci]
				continue
			}
			strs := native.Columns[5].(*vectorized.TypedColumn[string])
			col := promoted.Columns[5].(*vectorized.TypedColumn[*modelv1.TagValue])
			for i, v := range strs.Data() {
				if strs.IsNull(i) {
					col.AppendNull()
					continue
				}
				col.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}})
			}
		}
		promoted.Len = native.Len
		return promoted
	}}
	req := &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{defaultName},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		Limit:           200,
	}
	plan, analyzeErr := AnalyzeDistributed(req, []*databasev1.Measure{testMeasureSchema()}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 64})
	if analyzeErr != nil {
		t.Fatal(analyzeErr)
	}
	iter, execErr := plan.Execute(executor.WithDistributedExecutionContext(t.Context(), cluster))
	if execErr != nil {
		t.Fatal(execErr)
	}
	var rows []*measurev1.InternalDataPoint
	for iter.Next() {
		rows = append(rows, iter.Current()...)
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	// Every frame has been released by now; a string that still pointed into one reads as poison.
	seen := 0
	for _, row := range rows {
		for _, family := range row.GetDataPoint().GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if tag.GetKey() != "svc" || tag.GetValue().GetStr() == nil {
					continue
				}
				seen++
				if value := tag.GetValue().GetStr().GetValue(); !strings.HasPrefix(value, "service-") {
					t.Fatalf("svc = %q, want a service-... name", value)
				}
			}
		}
	}
	if seen == 0 {
		t.Fatal("no svc string cell reached the client")
	}
}
