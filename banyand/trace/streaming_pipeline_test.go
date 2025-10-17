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
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

type fakeSIDX struct {
	responses []*sidx.QueryResponse
}

func (f *fakeSIDX) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse, len(f.responses))
	errCh := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errCh)

		for _, resp := range f.responses {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case results <- resp:
			}
		}
	}()

	return results, errCh
}

func (f *fakeSIDX) IntroduceMemPart(uint64, *sidx.MemPart)          { panic("not implemented") }
func (f *fakeSIDX) IntroduceFlushed(*sidx.FlusherIntroduction)      {}
func (f *fakeSIDX) IntroduceMerged(*sidx.MergerIntroduction) func() { return func() {} }
func (f *fakeSIDX) ConvertToMemPart([]sidx.WriteRequest, int64) (*sidx.MemPart, error) {
	panic("not implemented")
}

func (f *fakeSIDX) Query(context.Context, sidx.QueryRequest) (*sidx.QueryResponse, error) {
	panic("not implemented")
}
func (f *fakeSIDX) Stats(context.Context) (*sidx.Stats, error) { return &sidx.Stats{}, nil }
func (f *fakeSIDX) Close() error                               { return nil }
func (f *fakeSIDX) Flush(map[uint64]struct{}) (*sidx.FlusherIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDX) Merge(<-chan struct{}, map[uint64]struct{}, uint64) (*sidx.MergerIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDX) StreamingParts(map[uint64]struct{}, string, uint32, string) ([]queue.StreamingPartData, []func()) {
	panic("not implemented")
}
func (f *fakeSIDX) IntroduceSynced(map[uint64]struct{}) func() { return func() {} }

type fakeSIDXWithErr struct {
	*fakeSIDX
	err error
}

func (f *fakeSIDXWithErr) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse, len(f.responses))
	errCh := make(chan error, 1)

	go func() {
		defer close(results)

		for _, resp := range f.responses {
			select {
			case <-ctx.Done():
				return
			case results <- resp:
			}
		}
	}()

	go func() {
		defer close(errCh)

		<-ctx.Done()
		if f.err != nil {
			errCh <- f.err
		}
	}()

	return results, errCh
}

func encodeTraceIDForTest(id string) []byte {
	buf := make([]byte, len(id)+1)
	buf[0] = byte(idFormatV1)
	copy(buf[1:], id)
	return buf
}

func findSpan(spans []*commonv1.Span, message string) *commonv1.Span {
	for _, span := range spans {
		if span.GetMessage() == message {
			return span
		}
		for _, child := range span.GetChildren() {
			if match := findSpan([]*commonv1.Span{child}, message); match != nil {
				return match
			}
		}
	}
	return nil
}

func spanTagsToMap(span *commonv1.Span) map[string]string {
	tags := make(map[string]string, len(span.GetTags()))
	for _, tag := range span.GetTags() {
		tags[tag.GetKey()] = tag.GetValue()
	}
	return tags
}

func expectTag(t *testing.T, tags map[string]string, key, want string) {
	t.Helper()
	got, ok := tags[key]
	if !ok {
		t.Fatalf("expected tag %q to be present, all tags=%v", key, tags)
	}
	if got != want {
		t.Fatalf("unexpected value for tag %q: got %q, want %q", key, got, want)
	}
}

func TestStreamSIDXTraceBatches_ProducesOrderedBatches(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 2,
	}

	responses := []*sidx.QueryResponse{
		{
			Keys: []int64{1, 2},
			Data: [][]byte{
				encodeTraceIDForTest("a"),
				encodeTraceIDForTest("b"),
			},
		},
		{
			Keys: []int64{2, 3},
			Data: [][]byte{
				encodeTraceIDForTest("b"),
				encodeTraceIDForTest("c"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{&fakeSIDX{responses: responses}}, req, 3)

	var batches []traceBatch
	for batch := range batchCh {
		if batch.err != nil {
			t.Fatalf("unexpected error batch: %v", batch.err)
		}
		batches = append(batches, batch)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	if diff := cmp.Diff([]string{"a", "b"}, batches[0].traceIDs); diff != "" {
		t.Fatalf("first batch mismatch (-got +want):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"c"}, batches[1].traceIDs); diff != "" {
		t.Fatalf("second batch mismatch (-got +want):\n%s", diff)
	}

	wantKeys := map[string]int64{"a": 1, "b": 2, "c": 3}
	for _, batch := range batches {
		for _, tid := range batch.traceIDs {
			if got := batch.keys[tid]; got != wantKeys[tid] {
				t.Fatalf("unexpected key for %s: got %d, want %d", tid, got, wantKeys[tid])
			}
		}
	}
}

func TestStreamSIDXTraceBatches_OrdersDescending(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: 2,
	}

	responses := []*sidx.QueryResponse{
		{
			Keys: []int64{5, 4},
			Data: [][]byte{
				encodeTraceIDForTest("e"),
				encodeTraceIDForTest("d"),
			},
		},
		{
			Keys: []int64{4, 3},
			Data: [][]byte{
				encodeTraceIDForTest("d"),
				encodeTraceIDForTest("c"),
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{&fakeSIDX{responses: responses}}, req, 3)

	var batches []traceBatch
	for batch := range batchCh {
		if batch.err != nil {
			t.Fatalf("unexpected error batch: %v", batch.err)
		}
		batches = append(batches, batch)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	if diff := cmp.Diff([]string{"e", "d"}, batches[0].traceIDs); diff != "" {
		t.Fatalf("first batch mismatch (-got +want):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"c"}, batches[1].traceIDs); diff != "" {
		t.Fatalf("second batch mismatch (-got +want):\n%s", diff)
	}

	wantKeys := map[string]int64{"e": 5, "d": 4, "c": 3}
	for _, batch := range batches {
		for _, tid := range batch.traceIDs {
			if got := batch.keys[tid]; got != wantKeys[tid] {
				t.Fatalf("unexpected key for %s: got %d, want %d", tid, got, wantKeys[tid])
			}
		}
	}
}

func TestStreamSIDXTraceBatches_Tracing(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: 2,
	}

	responses := []*sidx.QueryResponse{
		{
			Keys: []int64{5, 4},
			Data: [][]byte{
				encodeTraceIDForTest("e"),
				encodeTraceIDForTest("d"),
			},
		},
		{
			Keys: []int64{4, 3},
			Data: [][]byte{
				encodeTraceIDForTest("d"),
				encodeTraceIDForTest("c"),
			},
		},
	}

	tracer, tracerCtx := query.NewTracer(context.Background(), "trace-sidx-stream")

	var tr trace
	const maxTraceSize = 3
	batchCh := tr.streamSIDXTraceBatches(tracerCtx, []sidx.SIDX{&fakeSIDX{responses: responses}}, req, maxTraceSize)

	var (
		batches   []traceBatch
		uniqueIDs = make(map[string]struct{})
	)
	for batch := range batchCh {
		if batch.err != nil {
			t.Fatalf("unexpected error: %v", batch.err)
		}
		batches = append(batches, batch)
		for _, tid := range batch.traceIDs {
			uniqueIDs[tid] = struct{}{}
		}
	}

	if len(batches) == 0 {
		t.Fatalf("expected batches to be produced")
	}

	traceProto := tracer.ToProto()
	if traceProto == nil {
		t.Fatalf("expected tracing data")
	}

	span := findSpan(traceProto.GetSpans(), "sidx-stream")
	if span == nil {
		t.Fatalf("expected sidx-stream span, got %v", traceProto.GetSpans())
	}
	if span.GetError() {
		t.Fatalf("span unexpectedly marked as error: %v", span.GetTags())
	}

	tags := spanTagsToMap(span)

	expectTag(t, tags, "filter_present", "false")
	expectTag(t, tags, "order_sort", req.Order.Sort.String())
	expectTag(t, tags, "order_type", "0")
	expectTag(t, tags, "series_id_candidates", "0")
	expectTag(t, tags, "max_element_size", strconv.Itoa(req.MaxElementSize))
	expectTag(t, tags, "max_trace_size", strconv.Itoa(maxTraceSize))
	expectTag(t, tags, "sidx_instance_count", "1")
	expectTag(t, tags, "batches_emitted", strconv.Itoa(len(batches)))
	expectTag(t, tags, "trace_ids_emitted", strconv.Itoa(len(uniqueIDs)))
	expectTag(t, tags, "max_trace_limit_hit", "true")

	if dup, ok := tags["duplicate_trace_ids"]; !ok || dup != "1" {
		t.Fatalf("expected duplicate_trace_ids=1, tags=%v", tags)
	}
}

func TestStreamSIDXTraceBatches_PropagatesErrorAfterCancellation(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 1,
	}

	streamErr := errors.New("stream failure")

	sidxInstance := &fakeSIDXWithErr{
		fakeSIDX: &fakeSIDX{
			responses: []*sidx.QueryResponse{
				{
					Keys: []int64{1},
					Data: [][]byte{
						encodeTraceIDForTest("trace-1"),
					},
				},
			},
		},
		err: streamErr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0)

	var (
		dataSeen bool
		errBatch traceBatch
		errSeen  bool
	)

	for batch := range batchCh {
		if batch.err != nil {
			errBatch = batch
			errSeen = true
			continue
		}
		if len(batch.traceIDs) > 0 {
			dataSeen = true
		}
	}

	if !dataSeen {
		t.Fatalf("expected data batch before error")
	}
	if !errSeen {
		t.Fatalf("expected error batch but none received")
	}
	if !errors.Is(errBatch.err, streamErr) {
		t.Fatalf("unexpected error: %v", errBatch.err)
	}
}
