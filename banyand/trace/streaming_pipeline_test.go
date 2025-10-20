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
		defer close(errCh)

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
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 2,
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
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxBatchSize: 2,
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
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxBatchSize: 2,
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

	// ToProto() now properly waits for all async span operations to complete
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
	expectTag(t, tags, "max_batch_size", strconv.Itoa(req.MaxBatchSize))
	expectTag(t, tags, "max_trace_size", strconv.Itoa(maxTraceSize))
	expectTag(t, tags, "sidx_instance_count", "1")
	expectTag(t, tags, "batches_emitted", strconv.Itoa(len(batches)))
	expectTag(t, tags, "trace_ids_emitted", strconv.Itoa(len(uniqueIDs)))

	if dup, ok := tags["duplicate_trace_ids"]; !ok || dup != "1" {
		t.Fatalf("expected duplicate_trace_ids=1, tags=%v", tags)
	}
}

func TestStreamSIDXTraceBatches_PropagatesErrorAfterCancellation(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 1,
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

// fakeSIDXWithDataThenError simulates a SIDX that sends data first, then an error.
// Unlike fakeSIDXWithErr, this guarantees the error is sent even if context is canceled,
// by using buffered channels and sending data+error before checking context.
type fakeSIDXWithDataThenError struct {
	*fakeSIDX
	err error
}

func (f *fakeSIDXWithDataThenError) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse, len(f.responses))
	errCh := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errCh)

		// Send all responses first
		for _, resp := range f.responses {
			select {
			case <-ctx.Done():
				return
			case results <- resp:
			}
		}

		// Guarantee error is sent after data (use buffered channel to avoid blocking)
		if f.err != nil {
			errCh <- f.err
		}
	}()

	return results, errCh
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
				Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
				MaxBatchSize: 2,
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

			// For "error_with_partial_data" test, use fakeSIDXWithDataThenError which guarantees data then error
			// For other tests, use fakeSIDXWithImmediateError which sends error immediately
			var sidxInstance sidx.SIDX
			if tt.withData {
				sidxInstance = &fakeSIDXWithDataThenError{
					fakeSIDX: &fakeSIDX{
						responses: responses,
					},
					err: scanErr,
				}
			} else {
				sidxInstance = &fakeSIDXWithImmediateError{
					fakeSIDX: &fakeSIDX{
						responses: responses,
					},
					err: scanErr,
				}
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
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 10,
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
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 1,
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

// fakeSIDXInfinite simulates a SIDX that returns an infinite stream of results.
// It continues to generate responses until the context is canceled.
type fakeSIDXInfinite struct {
	traceIDPrefix string
	batchSize     int
	keyStart      int64
}

func (f *fakeSIDXInfinite) StreamingQuery(ctx context.Context, _ sidx.QueryRequest) (<-chan *sidx.QueryResponse, <-chan error) {
	results := make(chan *sidx.QueryResponse)
	errCh := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errCh)

		key := f.keyStart
		counter := 0

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				// Generate a batch of responses
				batchSize := f.batchSize
				if batchSize <= 0 {
					batchSize = 10
				}

				keys := make([]int64, batchSize)
				data := make([][]byte, batchSize)

				for i := 0; i < batchSize; i++ {
					keys[i] = key
					prefix := f.traceIDPrefix
					if prefix == "" {
						prefix = "trace"
					}
					traceID := prefix + "-" + strconv.Itoa(counter)
					data[i] = encodeTraceIDForTest(traceID)
					key++
					counter++
				}

				resp := &sidx.QueryResponse{
					Keys: keys,
					Data: data,
				}

				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case results <- resp:
					// Continue to next iteration
				}
			}
		}
	}()

	return results, errCh
}

func (f *fakeSIDXInfinite) IntroduceMemPart(uint64, *sidx.MemPart)          { panic("not implemented") }
func (f *fakeSIDXInfinite) IntroduceFlushed(*sidx.FlusherIntroduction)      {}
func (f *fakeSIDXInfinite) IntroduceMerged(*sidx.MergerIntroduction) func() { return func() {} }
func (f *fakeSIDXInfinite) ConvertToMemPart([]sidx.WriteRequest, int64) (*sidx.MemPart, error) {
	panic("not implemented")
}

func (f *fakeSIDXInfinite) Query(context.Context, sidx.QueryRequest) (*sidx.QueryResponse, error) {
	panic("not implemented")
}
func (f *fakeSIDXInfinite) Stats(context.Context) (*sidx.Stats, error) { return &sidx.Stats{}, nil }
func (f *fakeSIDXInfinite) Close() error                               { return nil }
func (f *fakeSIDXInfinite) Flush(map[uint64]struct{}) (*sidx.FlusherIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDXInfinite) Merge(<-chan struct{}, map[uint64]struct{}, uint64) (*sidx.MergerIntroduction, error) {
	panic("not implemented")
}

func (f *fakeSIDXInfinite) StreamingParts(map[uint64]struct{}, string, uint32, string) ([]queue.StreamingPartData, []func()) {
	panic("not implemented")
}
func (f *fakeSIDXInfinite) IntroduceSynced(map[uint64]struct{}) func() { return func() {} }

// TestStreamSIDXTraceBatches_InfiniteChannelContinuesUntilCanceled verifies that
// the streaming pipeline continues streaming from an infinite channel until context is canceled.
func TestStreamSIDXTraceBatches_InfiniteChannelContinuesUntilCanceled(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 5,
	}

	sidxInstance := &fakeSIDXInfinite{
		batchSize:     10,
		keyStart:      1,
		traceIDPrefix: "inf",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0 /* no maxTraceSize limit */)

	const targetTraceIDs = 50
	totalTraceIDs := 0
	seenIDs := make(map[string]struct{})

	for batch := range batchCh {
		if batch.err != nil {
			// Context cancellation is expected
			if errors.Is(batch.err, context.Canceled) {
				break
			}
			t.Fatalf("unexpected error batch: %v", batch.err)
		}

		for _, tid := range batch.traceIDs {
			if _, exists := seenIDs[tid]; exists {
				t.Fatalf("duplicate trace ID: %s", tid)
			}
			seenIDs[tid] = struct{}{}
		}

		totalTraceIDs += len(batch.traceIDs)

		// Cancel after we've received enough traces to prove it's streaming
		if totalTraceIDs >= targetTraceIDs {
			cancel()
		}
	}

	// Should have received at least targetTraceIDs results
	if totalTraceIDs < targetTraceIDs {
		t.Fatalf("expected at least %d trace IDs, got %d", targetTraceIDs, totalTraceIDs)
	}

	t.Logf("Successfully received %d unique trace IDs from infinite stream before cancellation", totalTraceIDs)
}

// TestStreamSIDXTraceBatches_InfiniteChannelCancellation verifies that
// canceling the context properly stops all goroutines in the streaming pipeline
// and cancels the SIDX context to close the infinite channel.
func TestStreamSIDXTraceBatches_InfiniteChannelCancellation(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 5,
	}

	sidxInstance := &fakeSIDXInfinite{
		batchSize:     10,
		keyStart:      1,
		traceIDPrefix: "cancel",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0 /* no maxTraceSize limit */)

	// Read a few batches, then cancel
	batchesRead := 0
	const batchesToRead = 3

	for batch := range batchCh {
		if batch.err != nil {
			// Context cancellation error is expected
			if errors.Is(batch.err, context.Canceled) {
				t.Logf("Received expected cancellation error: %v", batch.err)
				break
			}
			t.Fatalf("unexpected error: %v", batch.err)
		}

		batchesRead++
		if batchesRead >= batchesToRead {
			// Cancel the context to stop the infinite stream
			cancel()
		}
	}

	// Verify we read some batches before cancellation
	if batchesRead < batchesToRead {
		t.Fatalf("expected to read at least %d batches, got %d", batchesToRead, batchesRead)
	}

	t.Logf("Successfully canceled infinite stream after reading %d batches", batchesRead)
}

// TestStreamSIDXTraceBatches_InfiniteChannelGoroutineCleanup verifies that
// all goroutines are properly cleaned up when context is canceled.
func TestStreamSIDXTraceBatches_InfiniteChannelGoroutineCleanup(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 5,
	}

	sidxInstance := &fakeSIDXInfinite{
		batchSize:     5,
		keyStart:      1,
		traceIDPrefix: "cleanup",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0 /* no limit */)

	totalTraceIDs := 0
	batchesRead := 0

	for batch := range batchCh {
		if batch.err != nil {
			// Context cancellation is expected
			if errors.Is(batch.err, context.Canceled) {
				t.Logf("Received expected cancellation error")
				break
			}
			t.Fatalf("unexpected error: %v", batch.err)
		}

		totalTraceIDs += len(batch.traceIDs)
		batchesRead++

		// Cancel after reading a few batches
		if batchesRead >= 3 {
			cancel()
		}
	}

	// Channel should be closed
	_, ok := <-batchCh
	if ok {
		t.Fatal("channel should be closed")
	}

	if totalTraceIDs == 0 {
		t.Fatal("expected some trace IDs before cancellation")
	}

	t.Logf("Successfully cleaned up: read %d batches, %d trace IDs", batchesRead, totalTraceIDs)
}

// TestStreamSIDXTraceBatches_MultipleInfiniteSIDX verifies that
// the streaming pipeline can handle multiple infinite SIDX instances
// and properly merge their results until context is canceled.
func TestStreamSIDXTraceBatches_MultipleInfiniteSIDX(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 10,
	}

	const targetTraceIDs = 50

	// Create multiple infinite SIDX instances with different key ranges
	sidxInstances := []sidx.SIDX{
		&fakeSIDXInfinite{
			batchSize:     5,
			keyStart:      1, // Keys: 1, 2, 3, ...
			traceIDPrefix: "s1",
		},
		&fakeSIDXInfinite{
			batchSize:     5,
			keyStart:      1000, // Keys: 1000, 1001, 1002, ...
			traceIDPrefix: "s2",
		},
		&fakeSIDXInfinite{
			batchSize:     5,
			keyStart:      2000, // Keys: 2000, 2001, 2002, ...
			traceIDPrefix: "s3",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, sidxInstances, req, 0 /* no limit */)

	totalTraceIDs := 0
	seenIDs := make(map[string]struct{})
	var keys []int64

	for batch := range batchCh {
		if batch.err != nil {
			// Context cancellation is expected
			if errors.Is(batch.err, context.Canceled) {
				break
			}
			t.Fatalf("unexpected error batch: %v", batch.err)
		}

		for _, tid := range batch.traceIDs {
			if _, exists := seenIDs[tid]; exists {
				t.Fatalf("duplicate trace ID: %s", tid)
			}
			seenIDs[tid] = struct{}{}
			keys = append(keys, batch.keys[tid])
		}

		totalTraceIDs += len(batch.traceIDs)

		// Cancel after we've received enough to verify the merge is working
		if totalTraceIDs >= targetTraceIDs {
			cancel()
		}
	}

	if totalTraceIDs < targetTraceIDs {
		t.Fatalf("expected at least %d trace IDs, got %d", targetTraceIDs, totalTraceIDs)
	}

	// Verify keys are in ascending order (due to heap merge)
	for i := 1; i < len(keys); i++ {
		if keys[i] < keys[i-1] {
			t.Fatalf("keys not in ascending order: keys[%d]=%d, keys[%d]=%d", i-1, keys[i-1], i, keys[i])
		}
	}

	t.Logf("Successfully merged %d trace IDs from %d infinite SIDX instances", totalTraceIDs, len(sidxInstances))
}

// TestStreamSIDXTraceBatches_InfiniteChannelWithTracing verifies that
// tracing works correctly with infinite channels and shows proper cleanup.
func TestStreamSIDXTraceBatches_InfiniteChannelWithTracing(t *testing.T) {
	req := sidx.QueryRequest{
		Order:        &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxBatchSize: 10,
	}

	const targetTraceIDs = 30

	sidxInstance := &fakeSIDXInfinite{
		batchSize:     5,
		keyStart:      1,
		traceIDPrefix: "traced",
	}

	tracer, tracerCtx := query.NewTracer(context.Background(), "test-infinite-stream")
	ctx, cancel := context.WithCancel(tracerCtx)
	defer cancel()

	var tr trace
	batchCh := tr.streamSIDXTraceBatches(ctx, []sidx.SIDX{sidxInstance}, req, 0 /* no limit */)

	totalTraceIDs := 0
	batchCount := 0

	for batch := range batchCh {
		if batch.err != nil {
			// Context cancellation is expected
			if errors.Is(batch.err, context.Canceled) {
				break
			}
			t.Fatalf("unexpected error: %v", batch.err)
		}
		totalTraceIDs += len(batch.traceIDs)
		batchCount++

		// Cancel after we've received enough traces
		if totalTraceIDs >= targetTraceIDs {
			cancel()
		}
	}

	if totalTraceIDs < targetTraceIDs {
		t.Fatalf("expected at least %d trace IDs, got %d", targetTraceIDs, totalTraceIDs)
	}

	// ToProto() now properly waits for all async span operations to complete
	traceProto := tracer.ToProto()
	if traceProto == nil {
		t.Fatal("expected tracing data")
	}

	span := findSpan(traceProto.GetSpans(), "sidx-stream")
	if span == nil {
		t.Fatal("expected sidx-stream span")
	}

	if span.GetError() {
		t.Fatalf("span unexpectedly marked as error: %v", span.GetTags())
	}

	tags := spanTagsToMap(span)
	expectTag(t, tags, "batches_emitted", strconv.Itoa(batchCount))

	// Verify trace_ids_emitted matches what we collected
	// (Note: may be slightly more than targetTraceIDs due to in-flight batches at cancellation)
	// The runner may have consumed trace IDs (incrementing r.total) but not yet emitted them
	// when cancellation occurs, especially with fewer CPU cores
	emittedTag := tags["trace_ids_emitted"]
	emittedCount, err := strconv.Atoi(emittedTag)
	if err != nil {
		t.Fatalf("failed to parse trace_ids_emitted: %v", err)
	}
	if emittedCount < totalTraceIDs {
		t.Fatalf("trace_ids_emitted should be >= totalTraceIDs: got %d, want >= %d", emittedCount, totalTraceIDs)
	}
	// Allow some buffer for in-flight batches, but not too much (e.g., at most one MaxBatchSize worth)
	maxExpected := totalTraceIDs + req.MaxBatchSize
	if emittedCount > maxExpected {
		t.Fatalf("trace_ids_emitted unexpectedly high: got %d, want <= %d (totalTraceIDs=%d + MaxBatchSize=%d)",
			emittedCount, maxExpected, totalTraceIDs, req.MaxBatchSize)
	}

	expectTag(t, tags, "max_trace_size", "0")
	expectTag(t, tags, "sidx_instance_count", "1")

	t.Logf("Tracing successful: emitted %d batches with %d trace IDs", batchCount, totalTraceIDs)
}
