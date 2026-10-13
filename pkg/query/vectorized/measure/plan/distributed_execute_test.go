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
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
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

// fakeCluster plays the data nodes: it answers a broadcast with one frame per node, encoded with
// the version the node would negotiate for the request it received.
type fakeCluster struct {
	build     func(node int) *vectorized.RecordBatch
	requested []uint32
	sent      []uint8
	nodes     int
	traced    bool
}

func (c *fakeCluster) Broadcast(_ time.Duration, _ bus.Topic, message bus.Message) ([]bus.Future, error) {
	request := message.Data().(*measurev1.InternalQueryRequest)
	c.requested = append(c.requested, request.GetMaxFrameVersion())
	version := frame.NegotiateVersion(request.GetMaxFrameVersion())
	futures := make([]bus.Future, 0, c.nodes)
	for node := range c.nodes {
		body, err := frame.EncodeVersion(c.build(node), version)
		if err != nil {
			return nil, err
		}
		c.sent = append(c.sent, body[frame.MagicLen])
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

// TestExecute_FrameVersionMatrix runs the liaison plan against data nodes of every combination of
// old and new release. A frame is columnar only when both sides are new, and the client sees the
// same rows whichever frames traveled; no pin on a received frame outlives the query.
func TestExecute_FrameVersionMatrix(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	defer frame.SetAdvertisedVersion(frame.SetAdvertisedVersion(frame.WireVersionColumnar))
	defer frame.SetMaxEmitVersion(frame.SetMaxEmitVersion(frame.WireVersionColumnar))
	pinsBefore := vectorized.OutstandingOwnerPins()

	type combo struct {
		name      string
		advertise uint8
		emitCap   uint8
		want      uint8
	}
	combos := []combo{
		{"old liaison, old data node", 0, frame.WireVersion, frame.WireVersion},
		{"old liaison, new data node", 0, frame.WireVersionColumnar, frame.WireVersion},
		{"new liaison, old data node", frame.WireVersionColumnar, frame.WireVersion, frame.WireVersion},
		{"new liaison, new data node", frame.WireVersionColumnar, frame.WireVersionColumnar, frame.WireVersionColumnar},
	}
	var baseline []*measurev1.InternalDataPoint
	for _, c := range combos {
		for _, traced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s traced=%t", c.name, traced), func(t *testing.T) {
				frame.SetAdvertisedVersion(c.advertise)
				frame.SetMaxEmitVersion(c.emitCap)
				cluster := &fakeCluster{nodes: 3, build: rowsBatch, traced: traced}
				got := executeRows(t, cluster)
				if len(got) == 0 {
					t.Fatal("no rows")
				}
				if len(cluster.requested) != 1 || cluster.requested[0] != uint32(c.advertise) {
					t.Fatalf("the liaison advertised %v, want [%d]", cluster.requested, c.advertise)
				}
				for _, v := range cluster.sent {
					if v != c.want {
						t.Fatalf("a data node sent frame version %d, want %d", v, c.want)
					}
				}
				if baseline == nil {
					baseline = got
					return
				}
				requireSameRows(t, baseline, got)
			})
		}
	}
	if got := vectorized.OutstandingOwnerPins() - pinsBefore; got != 0 {
		t.Fatalf("%d pins on received frames outlived the queries", got)
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

// TestExecute_AggFrameVersionMatrix is TestExecute_FrameVersionMatrix for an aggregation: the liaison
// reduces the partials of data nodes that may each have sent either frame version.
func TestExecute_AggFrameVersionMatrix(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)
	defer frame.SetAdvertisedVersion(frame.SetAdvertisedVersion(frame.WireVersionColumnar))
	defer frame.SetMaxEmitVersion(frame.SetMaxEmitVersion(frame.WireVersionColumnar))
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
	frame.SetAdvertisedVersion(0)
	frame.SetMaxEmitVersion(frame.WireVersion)
	baseline := run(t)
	if len(baseline) != 5 {
		t.Fatalf("top-5 returned %d rows", len(baseline))
	}
	frame.SetAdvertisedVersion(frame.WireVersionColumnar)
	frame.SetMaxEmitVersion(frame.WireVersionColumnar)
	requireSameRows(t, baseline, run(t))
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

// TestInternalQueryRequest_WireCompatibleWithTheOlderSchema pins the negotiation field's backward
// compatibility on the wire: a data node of the release before it (the schema without field 3) must read the
// request a new liaison sends, and a new data node must read the request an old liaison sends as "no
// advertisement".
func TestInternalQueryRequest_WireCompatibleWithTheOlderSchema(t *testing.T) {
	fileProto := protodesc.ToFileDescriptorProto(measurev1.File_banyandb_measure_v1_query_proto)
	for _, message := range fileProto.MessageType {
		if message.GetName() != "InternalQueryRequest" {
			continue
		}
		kept := message.Field[:0]
		for _, field := range message.Field {
			if field.GetNumber() != 3 {
				kept = append(kept, field)
			}
		}
		message.Field = kept
	}
	oldFile, fileErr := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	if fileErr != nil {
		t.Fatal(fileErr)
	}
	oldDesc := oldFile.Messages().ByName("InternalQueryRequest")
	if oldDesc.Fields().ByNumber(3) != nil {
		t.Fatal("the older schema still has field 3")
	}

	newReq := &measurev1.InternalQueryRequest{
		Request:          &measurev1.QueryRequest{Name: "demo", Groups: []string{"g"}},
		AggReturnPartial: true,
		MaxFrameVersion:  uint32(frame.WireVersionColumnar),
	}
	wire, marshalErr := proto.Marshal(newReq)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	old := dynamicpb.NewMessage(oldDesc)
	if unmarshalErr := proto.Unmarshal(wire, old); unmarshalErr != nil {
		t.Fatalf("an older data node cannot read the new request: %v", unmarshalErr)
	}
	if got := old.Get(oldDesc.Fields().ByName("agg_return_partial")).Bool(); !got {
		t.Fatal("the older schema lost agg_return_partial")
	}
	oldRequest := old.Get(oldDesc.Fields().ByName("request")).Message()
	if got := oldRequest.Get(oldRequest.Descriptor().Fields().ByName("name")).String(); got != "demo" {
		t.Fatalf("the older schema read request.name = %q", got)
	}

	// An old liaison's request carries no field 3.
	withoutField, stripErr := proto.Marshal(&measurev1.InternalQueryRequest{Request: newReq.Request, AggReturnPartial: true})
	if stripErr != nil {
		t.Fatal(stripErr)
	}
	var parsed measurev1.InternalQueryRequest
	if unmarshalErr := proto.Unmarshal(withoutField, &parsed); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	if parsed.GetMaxFrameVersion() != 0 || frame.NegotiateVersion(parsed.GetMaxFrameVersion()) != frame.WireVersion {
		t.Fatalf("a request without the field must negotiate the original version, got %d", frame.NegotiateVersion(parsed.GetMaxFrameVersion()))
	}
}
