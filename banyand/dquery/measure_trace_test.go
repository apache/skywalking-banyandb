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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package dquery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
	"github.com/apache/skywalking-banyandb/pkg/schema"
)

func TestDistributedBatchTraceIncludesFinalConsumption(t *testing.T) {
	tracer, ctx := query.NewTracer(context.Background(), t.Name())
	processor, request := newTraceMeasureProcessor(t, nil)
	response := processor.Rev(ctx, bus.NewMessage(1, &vmeasure.BatchQueryRequest{Request: request}))
	result, ok := response.Data().(*vecplan.BatchResult)
	if !ok {
		t.Fatalf("Rev returned %T: %v", response.Data(), response.Data())
	}
	t.Cleanup(func() { _ = result.Close() })
	assertTraceSpanOpen(t, tracer.ToProto(), "distributed-test")
	if result.Trace != nil {
		t.Fatal("Rev snapshotted the trace before final consumption")
	}
	if result.QueryTrace() != nil {
		t.Fatal("QueryTrace completed a result before Close")
	}
	assertTraceSpanOpen(t, tracer.ToProto(), "distributed-test")
	scan := ql.NewNativeScan(result)
	iter, err := result.NativeIteratorFrom(ctx, scan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = iter.Close() })
	assertTraceSpanOpen(t, tracer.ToProto(), "iterator")
	var rows int
	for iter.Next() {
		rows++
		if len(iter.Current()) != 1 {
			t.Fatal("final serializer did not return a row")
		}
		assertTraceSpanOpen(t, tracer.ToProto(), "distributed-test")
	}
	if rows != 2 {
		t.Fatalf("got %d rows, want limit 2 after offset", rows)
	}
	if err = iter.Close(); err != nil {
		t.Fatal(err)
	}
	completed := result.QueryTrace()
	if completed == nil {
		t.Fatal("Close did not attach the completed trace")
	}
	root := findMeasureTraceSpan(completed.Spans, "distributed-test")
	iterator := findMeasureTraceSpan(completed.Spans, "iterator")
	if root == nil || root.EndTime == nil || iterator == nil || iterator.EndTime == nil {
		t.Fatalf("trace not completed after final consumption: %v", completed)
	}
	for _, key := range []string{tracelabels.TagRounds, tracelabels.TagSize} {
		if traceMeasureTag(iterator, key) != "2" {
			t.Fatalf("iterator %s = %q, want 2", key, traceMeasureTag(iterator, key))
		}
	}
	if root.EndTime.AsTime().Before(iterator.EndTime.AsTime()) {
		t.Fatal("distributed span ended before the final iterator")
	}
	assertMeasureTraceCompleted(t, completed.Spans)
	if err = result.Close(); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(completed, result.QueryTrace()) {
		t.Fatal("repeated Close/QueryTrace changed the completed trace")
	}
}

func TestDistributedBatchTraceClosesOnEarlyClose(t *testing.T) {
	for _, consume := range []bool{false, true} {
		t.Run(fmt.Sprintf("consume=%t", consume), func(t *testing.T) {
			tracer, ctx := query.NewTracer(context.Background(), t.Name())
			processor, request := newTraceMeasureProcessor(t, nil)
			response := processor.Rev(ctx, bus.NewMessage(1, &vmeasure.BatchQueryRequest{Request: request}))
			result, ok := response.Data().(*vecplan.BatchResult)
			if !ok {
				t.Fatalf("Rev returned %T: %v", response.Data(), response.Data())
			}
			if consume {
				iter, err := result.NativeIteratorFrom(ctx, ql.NewNativeScan(result))
				if err != nil {
					t.Fatal(err)
				}
				if !iter.Next() {
					t.Fatal("expected a row before early Close")
				}
			}
			if err := result.Close(); err != nil {
				t.Fatal(err)
			}
			root := findMeasureTraceSpan(tracer.ToProto().Spans, "distributed-test")
			if root == nil || root.EndTime == nil || result.QueryTrace() == nil {
				t.Fatal("closing an unconsumed result did not complete its trace")
			}
			assertMeasureTraceCompleted(t, result.QueryTrace().Spans)
		})
	}
}

func TestDistributedBatchTraceConsumerFailure(t *testing.T) {
	for _, failure := range []string{"canceled", "serialization budget"} {
		t.Run(failure, func(t *testing.T) {
			_, traceCtx := query.NewTracer(context.Background(), t.Name())
			ctx, cancel := context.WithCancel(traceCtx)
			defer cancel()
			const limit = 4 << 20
			ctx = vectorized.WithQueryMemoryTracker(ctx, limit)
			processor, request := newTraceMeasureProcessor(t, nil)
			response := processor.Rev(ctx, bus.NewMessage(1, &vmeasure.BatchQueryRequest{Request: request}))
			result, ok := response.Data().(*vecplan.BatchResult)
			if !ok {
				t.Fatalf("Rev returned %T: %v", response.Data(), response.Data())
			}
			t.Cleanup(func() { _ = result.Close() })
			iter, err := result.NativeIteratorFrom(ctx, ql.NewNativeScan(result))
			if err != nil {
				t.Fatal(err)
			}
			wantErr := context.Canceled
			if failure == "canceled" {
				cancel()
			} else {
				wantErr = vectorized.ErrMemoryBudgetExceeded
				tracker := vectorized.QueryMemoryTracker(ctx, limit)
				remaining := limit - tracker.Used()
				if err = tracker.Reserve(remaining); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { tracker.Release(remaining) })
			}
			if iter.Next() {
				t.Fatal("failed serialization unexpectedly returned a row")
			}
			withErr, ok := iter.(interface{ Err() error })
			if !ok || !errors.Is(withErr.Err(), wantErr) {
				t.Fatalf("iterator error = %v, want %v", iter, wantErr)
			}
			if err = iter.Close(); !errors.Is(err, wantErr) {
				t.Fatalf("Close error = %v, want %v", err, wantErr)
			}
			completed := result.QueryTrace()
			if completed == nil || !completed.Error {
				t.Fatalf("failed consumer trace not marked as error: %v", completed)
			}
			iterator := findMeasureTraceSpan(completed.Spans, "iterator")
			root := findMeasureTraceSpan(completed.Spans, "distributed-test")
			if iterator == nil || iterator.EndTime == nil || !iterator.Error || root == nil || root.EndTime == nil {
				t.Fatalf("failed consumer trace was not completed: %v", completed)
			}
		})
	}
}

func TestDistributedTracedExecutionErrorPreservesStatus(t *testing.T) {
	for _, failure := range []error{status.Error(codes.ResourceExhausted, "test budget"), context.Canceled, context.DeadlineExceeded, errors.New("invalid topic")} {
		t.Run(failure.Error(), func(t *testing.T) {
			tracer, ctx := query.NewTracer(context.Background(), t.Name())
			processor, request := newTraceMeasureProcessor(t, failure)
			response := processor.Rev(ctx, bus.NewMessage(1, &vmeasure.BatchQueryRequest{Request: request}))
			resultErr, ok := response.Data().(error)
			if !ok || !errors.Is(resultErr, failure) {
				t.Fatalf("Rev error = %v (%T), want original %v", response.Data(), response.Data(), failure)
			}
			if gotCode := status.Code(resultErr); gotCode != status.Code(failure) {
				t.Fatalf("Rev status = %v, want %v", gotCode, status.Code(failure))
			}
			root := findMeasureTraceSpan(tracer.ToProto().Spans, "distributed-test")
			if root == nil || root.EndTime == nil || !root.Error {
				t.Fatalf("failed Rev did not finish its error span: %v", root)
			}
		})
	}
}

type traceMeasureService struct{ measure.Service }

func (traceMeasureService) Measure(*commonv1.Metadata) (measure.Measure, error) {
	return traceMeasureContext{}, nil
}

func (traceMeasureService) LoadGroup(string) (schema.Group, bool) { return traceMeasureGroup{}, true }

type traceMeasureContext struct{ measure.Measure }

func (traceMeasureContext) GetSchema() *databasev1.Measure { return rawWireModeTestSchema() }

func (traceMeasureContext) GetIndexRules() []*databasev1.IndexRule { return nil }

func (traceMeasureContext) VectorizedConfig() vmeasure.VectorizedConfig {
	return vmeasure.VectorizedConfig{BatchSize: 2, QueryMemoryMiB: 4}
}

type traceMeasureGroup struct{ schema.Group }

func (traceMeasureGroup) GetSchema() *commonv1.Group {
	return &commonv1.Group{ResourceOpts: &commonv1.ResourceOpts{}}
}

type traceMeasureBroadcaster struct {
	err  error
	body []byte
}

func (b traceMeasureBroadcaster) Broadcast(time.Duration, bus.Topic, bus.Message) ([]bus.Future, error) {
	if b.err != nil {
		return nil, b.err
	}
	return []bus.Future{traceMeasureFuture{body: b.body}}, nil
}

type traceMeasureFuture struct {
	bus.Future
	body []byte
}

func (f traceMeasureFuture) Get() (bus.Message, error) { return bus.NewMessage(1, f.body), nil }

func newTraceMeasureProcessor(t *testing.T, broadcastErr error) (*measureQueryProcessor, *measurev1.QueryRequest) {
	t.Helper()
	previousMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previousMode) })
	batchSchema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleVersion, Name: "_version", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleSeriesID, Name: "_sid", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "value", Type: vectorized.ColumnTypeInt64},
	})
	batch := vectorized.NewRecordBatch(batchSchema, 3)
	for row := range 3 {
		for col := range batch.Columns {
			batch.Columns[col].(*vectorized.TypedColumn[int64]).Append(int64(row + 1))
		}
		batch.Len++
	}
	body, err := frame.Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	processor := &measureQueryProcessor{
		measureService: traceMeasureService{},
		broadcaster:    traceMeasureBroadcaster{body: body, err: broadcastErr},
		queryService:   &queryService{nodeID: "test", log: logger.GetLogger("trace-test")},
	}
	request := &measurev1.QueryRequest{
		Name: "demo", Groups: []string{"default"}, Trace: true, Limit: 2, Offset: 1,
		TimeRange:       &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(1, 0))},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
	}
	return processor, request
}

func assertTraceSpanOpen(t *testing.T, trace *commonv1.Trace, name string) {
	t.Helper()
	span := findMeasureTraceSpan(trace.Spans, name)
	if span == nil || span.EndTime != nil {
		t.Fatalf("span %q should remain open: %v", name, span)
	}
}

func findMeasureTraceSpan(spans []*commonv1.Span, name string) *commonv1.Span {
	for _, span := range spans {
		if span.Message == name {
			return span
		}
		if child := findMeasureTraceSpan(span.Children, name); child != nil {
			return child
		}
	}
	return nil
}

func traceMeasureTag(span *commonv1.Span, key string) string {
	for _, tag := range span.Tags {
		if tag.Key == key {
			return tag.Value
		}
	}
	return ""
}

func assertMeasureTraceCompleted(t *testing.T, spans []*commonv1.Span) {
	t.Helper()
	for _, span := range spans {
		if span.EndTime == nil {
			t.Fatalf("final trace contains an unfinished span: %q", span.Message)
		}
		assertMeasureTraceCompleted(t, span.Children)
	}
}
