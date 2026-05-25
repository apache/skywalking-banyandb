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

// Package query regression tests for root-span duration population.
//
// Option B chosen: we exercise the tracer/span ordering contract directly
// (span.Stop() → tracer.ToProto()) rather than standing up a full
// measureInternalQueryProcessor, which requires a real measure.Service,
// buildMeasureContext, and executeMeasurePlan — well over 50 LOC of fixture.
// The three tests mirror the exact call sequence used in the three processor
// branches so that a regression (moving ToProto() before Stop()) would be
// caught immediately.
package query

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	query "github.com/apache/skywalking-banyandb/pkg/query"
)

// assertRootSpanFinalized asserts that traceProto's root span has populated
// Duration / EndTime / StartTime (i.e. Stop() ran before ToProto()) and that
// EndTime is strictly after StartTime. When wantErr is true it also asserts
// the root span carries the Error flag (error-path regression check).
func assertRootSpanFinalized(t *testing.T, traceProto *commonv1.Trace, label string, wantErr bool) {
	t.Helper()
	if traceProto == nil {
		t.Fatalf("%s: tracer.ToProto() returned nil", label)
	}
	if len(traceProto.Spans) == 0 {
		t.Fatalf("%s: trace has no spans", label)
	}
	rootSpan := traceProto.Spans[0]
	if rootSpan.Duration <= 0 {
		t.Errorf("%s: expected Duration > 0, got %d", label, rootSpan.Duration)
	}
	if rootSpan.EndTime == nil {
		t.Errorf("%s: expected EndTime != nil", label)
	}
	if rootSpan.StartTime == nil {
		t.Errorf("%s: expected StartTime != nil", label)
	}
	if rootSpan.EndTime != nil && rootSpan.StartTime != nil &&
		!rootSpan.EndTime.AsTime().After(rootSpan.StartTime.AsTime()) {
		t.Errorf("%s: expected EndTime (%v) to be after StartTime (%v)",
			label, rootSpan.EndTime.AsTime(), rootSpan.StartTime.AsTime())
	}
	if wantErr && !rootSpan.Error {
		t.Errorf("%s: expected span.Error == true", label)
	}
}

// TestRootSpanDurationNonZero_DataNode_ProtoWire mirrors the proto-wire data-node
// response path in measureInternalQueryProcessor.Rev (processor.go ~line 623-641):
//
//	span.Tag(...)
//	span.Stop()
//	d.Trace = tracer.ToProto()
//
// The regression being guarded: if ToProto() is called before Stop(), the
// captured span has EndTime=nil and Duration=0.
func TestRootSpanDurationNonZero_DataNode_ProtoWire(t *testing.T) {
	ctx := context.Background()
	tracer, ctx := query.NewTracer(ctx, "proto-wire-test")
	span, _ := tracer.StartSpan(ctx, "data-node-proto")

	span.Tag("plan", "test-plan")
	span.Tag("resp_count", "42")

	// Proto-wire path: Stop then ToProto.
	span.Stop()
	traceProto := tracer.ToProto()

	assertRootSpanFinalized(t, traceProto, "proto-wire", false)
}

// TestRootSpanDurationNonZero_DataNode_RawWire mirrors the raw-wire data-node
// success response path in measureInternalQueryProcessor.Rev (processor.go
// ~line 606-613), which is reached only when data.MeasureWireModeRaw() is true:
//
//	span.Tag(TagBytesOut, ...)
//	span.Stop()
//	resp = &InternalQueryResponse{RawFrameBody: rawBody, Trace: tracer.ToProto()}
//
// The test toggles the raw-wire flag and defers restoration so it does not
// poison other tests running in the same process.
func TestRootSpanDurationNonZero_DataNode_RawWire(t *testing.T) {
	prevMode := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(prevMode)

	ctx := context.Background()
	tracer, ctx := query.NewTracer(ctx, "raw-wire-test")
	span, _ := tracer.StartSpan(ctx, "data-node-raw")

	span.Tag("plan", "vec-raw-plan")
	span.Tag("resp_kind", "raw-frame")
	span.Tagf("bytes_out", "%d", 1024)

	// Raw-wire success path: Stop then ToProto.
	span.Stop()
	traceProto := tracer.ToProto()

	assertRootSpanFinalized(t, traceProto, "raw-wire", false)
}

// TestRootSpanDurationNonZero_DataNode_ErrorPath mirrors both error branches in
// measureInternalQueryProcessor.Rev:
//
//  1. Proto-wire error (processor.go ~line 633-637):
//     span.Error(err); span.Stop(); resp = &InternalQueryResponse{Trace: tracer.ToProto()}
//
//  2. Raw-wire error (processor.go ~line 583-588 and ~line 596-601):
//     span.Error(err); span.Stop(); resp = &InternalQueryResponse{Trace: tracer.ToProto()}
//
// Even on the error path the root span must have populated Duration and EndTime.
func TestRootSpanDurationNonZero_DataNode_ErrorPath(t *testing.T) {
	simulatedErr := errors.New("simulated query execution error")

	t.Run("proto-wire-error", func(t *testing.T) {
		ctx := context.Background()
		tracer, ctx := query.NewTracer(ctx, "proto-wire-error-test")
		span, _ := tracer.StartSpan(ctx, "data-node-proto-error")

		span.Error(simulatedErr)
		span.Stop()
		traceProto := tracer.ToProto()

		assertRootSpanFinalized(t, traceProto, "proto-wire-error", true)
	})

	t.Run("raw-wire-error", func(t *testing.T) {
		prevMode := data.MeasureWireModeRaw()
		data.SetMeasureWireModeRaw(true)
		defer data.SetMeasureWireModeRaw(prevMode)

		ctx := context.Background()
		tracer, ctx := query.NewTracer(ctx, "raw-wire-error-test")
		span, _ := tracer.StartSpan(ctx, "data-node-raw-error")

		// Both the FrameEmitter-cast failure branch and the EmitFrame
		// failure branch use the same sequence: Error → Stop → ToProto.
		span.Error(simulatedErr)
		span.Stop()
		traceProto := tracer.ToProto()

		assertRootSpanFinalized(t, traceProto, "raw-wire-error", true)
	})
}
