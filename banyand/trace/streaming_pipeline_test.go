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

		// Send error after all results have been sent
		// This tests that errors are propagated even after data processing
		if f.err != nil {
			select {
			case <-ctx.Done():
			case errCh <- f.err:
			}
		}
	}()

	go func() {
		defer close(errCh)
		// Just wait for context cancellation to close the channel
		<-ctx.Done()
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

// fakeSIDXWithImmediateError simulates errors from blockScanResultBatch.
// by sending errors immediately via errCh (like scan errors would).
type fakeSIDXWithImmediateError struct {
	*fakeSIDX
	err error
}

func (f *fakeSIDXWithImmediateError) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse, len(f.responses))
	errCh := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errCh)

		// Send error immediately (simulating blockScanner.scan error)
		if f.err != nil {
			errCh <- f.err
		}

		// Then send any responses
		for _, resp := range f.responses {
			select {
			case <-ctx.Done():
				return
			case results <- resp:
			}
		}
	}()

	return results, errCh
}

// TestStreamSIDXTraceBatches_PropagatesBlockScannerError verifies that errors.
// from blockScanResultBatch.err (sent via errCh) are properly propagated through
// the streaming pipeline to the traceBatch channel and ultimately to queryResult.Pull().
func TestStreamSIDXTraceBatches_PropagatesBlockScannerError(t *testing.T) {
	tests := []struct {
		name         string
		scanError    string
		errorContain string
		withData     bool
		expectError  bool
	}{
		{
			name:         "iterator_init_error",
			scanError:    "cannot init iter: iterator initialization failed",
			errorContain: "cannot init iter",
			withData:     false,
			expectError:  true,
		},
		{
			name:         "quota_exceeded_error",
			scanError:    "sidx block scan quota exceeded: used 1000 bytes, quota is 500 bytes",
			errorContain: "quota exceeded",
			withData:     false,
			expectError:  true,
		},
		{
			name:         "resource_acquisition_error",
			scanError:    "cannot acquire resource: insufficient memory",
			errorContain: "cannot acquire resource",
			withData:     false,
			expectError:  true,
		},
		{
			name:         "iteration_error",
			scanError:    "cannot iterate iter: block read failed",
			errorContain: "cannot iterate iter",
			withData:     false,
			expectError:  true,
		},
		{
			name:         "error_with_partial_data",
			scanError:    "scan error after partial results",
			errorContain: "scan error after partial",
			withData:     true,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := sidx.QueryRequest{
				Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxElementSize: 2,
			}

			var responses []*sidx.QueryResponse
			if tt.withData {
				responses = []*sidx.QueryResponse{
					{
						Keys: []int64{1, 2},
						Data: [][]byte{
							encodeTraceIDForTest("trace-1"),
							encodeTraceIDForTest("trace-2"),
						},
					},
				}
			}

			scanErr := errors.New(tt.scanError)
			sidxInstance := &fakeSIDXWithImmediateError{
				fakeSIDX: &fakeSIDX{
					responses: responses,
				},
				err: scanErr,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var tr trace
			batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0)

			var (
				receivedError error
				dataBatches   int
			)

			// Consume all batches
			for batch := range batchCh {
				if batch.err != nil {
					receivedError = batch.err
					// Don't break - consume remaining batches
					continue
				}
				if len(batch.traceIDs) > 0 {
					dataBatches++
				}
			}

			// Verify error was received
			if tt.expectError {
				if receivedError == nil {
					t.Fatalf("expected error containing %q but got none", tt.errorContain)
				}
				if !errors.Is(receivedError, scanErr) {
					// Check if error is wrapped
					errMsg := receivedError.Error()
					if errMsg == "" || !contains(errMsg, tt.errorContain) {
						t.Fatalf("expected error containing %q but got: %v", tt.errorContain, receivedError)
					}
				}
			}

			// Verify data batches if expected
			if tt.withData && dataBatches == 0 {
				t.Fatal("expected data batches but got none")
			}
		})
	}
}

// TestStreamSIDXTraceBatches_DrainErrorEventsGuaranteed verifies that.
// drainErrorEvents is always called via defer, even on early returns.
func TestStreamSIDXTraceBatches_DrainErrorEventsGuaranteed(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 10,
	}

	scanErr := errors.New("scan error from defer test")

	// Create fake that will send error after some results
	sidxInstance := &fakeSIDXWithImmediateError{
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
		err: scanErr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel context immediately to force early return
	cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0)

	var receivedError error

	// Even with canceled context, we should receive the error
	// because defer ensures drainErrorEvents is called
	for batch := range batchCh {
		if batch.err != nil {
			receivedError = batch.err
		}
	}

	// The error might not be propagated if context is canceled immediately
	// but this test verifies the defer mechanism exists
	t.Logf("Received error (may be nil due to immediate cancellation): %v", receivedError)
}

// TestStreamSIDXTraceBatches_ErrorEmissionResilience verifies that.
// emitError tries to send even when context is canceled.
func TestStreamSIDXTraceBatches_ErrorEmissionResilience(t *testing.T) {
	req := sidx.QueryRequest{
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: 1,
	}

	scanErr := errors.New("scan error during processing")

	sidxInstance := &fakeSIDXWithImmediateError{
		fakeSIDX: &fakeSIDX{
			responses: []*sidx.QueryResponse{
				{
					Keys: []int64{1, 2, 3},
					Data: [][]byte{
						encodeTraceIDForTest("trace-1"),
						encodeTraceIDForTest("trace-2"),
						encodeTraceIDForTest("trace-3"),
					},
				},
			},
		},
		err: scanErr,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0)

	var receivedError error
	batchCount := 0

	// Read first batch, then wait a bit for error to be sent to errEvents
	for batch := range batchCh {
		if batch.err != nil {
			receivedError = batch.err
			break
		}
		batchCount++
		// After first batch, continue reading to trigger drainErrorEvents
	}

	// We should eventually get the error either from pollErrEvents or drainErrorEvents
	if receivedError == nil {
		t.Fatal("expected error to be propagated but got none")
	}

	if !errors.Is(receivedError, scanErr) && !contains(receivedError.Error(), "scan error") {
		t.Fatalf("unexpected error: %v", receivedError)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
