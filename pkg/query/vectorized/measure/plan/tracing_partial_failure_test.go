// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Partial-failure test: when some data nodes answer a traced raw-frame query with an error, the
// liaison must still surface the other nodes' raw frames and traces alongside those partial
// errors — no panic, no silent drop.

package plan

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// failingNodeError is the error text a failing data node returns for a traced raw-frame query.
const failingNodeError = "simulated data node failure on TopicInternalMeasureQuery"

// TestRawFrameTraceCollectsPartialNodeFailures verifies partial-failure
// semantics: a broadcast that mixes healthy nodes (returning RawFrameBody +
// Trace) with failing nodes (returning *common.Error) must not crash or
// silently drop the healthy results.
//
// Expected invariants:
//   - The returned raw-frame slice contains exactly the 3 frames from the
//     healthy nodes.
//   - The returned trace slice contains exactly the 3 traces from the
//     healthy nodes.
//   - A non-nil error is returned that encodes both failing failures; the
//     error text mentions "data node error" for each failing.
func TestRawFrameTraceCollectsPartialNodeFailures(t *testing.T) {
	// validRawFrameWithMagic: minimal frame starting with the raw-frame magic
	// leading byte (0x00) so the collector treats it as a real raw frame body.
	validRawFrame := []byte{0x00, 0x01, 0x02}
	healthyTrace := &commonv1.Trace{TraceId: "healthy"}

	// 3 healthy nodes: RawFrameBody + Trace.
	healthyFutures := []bus.Future{
		rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        healthyTrace,
		})},
		rawFrameFuture{message: bus.NewMessage(2, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        healthyTrace,
		})},
		rawFrameFuture{message: bus.NewMessage(3, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        healthyTrace,
		})},
	}

	// 2 failing nodes: *common.Error.
	failingFutures := []bus.Future{
		rawFrameFuture{message: bus.NewMessage(4, common.NewError(failingNodeError))},
		rawFrameFuture{message: bus.NewMessage(5, common.NewError(failingNodeError))},
	}

	allFutures := make([]bus.Future, 0, len(healthyFutures)+len(failingFutures))
	allFutures = append(allFutures, healthyFutures...)
	allFutures = append(allFutures, failingFutures...)

	frames, traces, collectErr := collectRawFrameResponses(allFutures)

	// The call must not panic (enforced by reaching this line).

	// Healthy frames must all be present.
	if len(frames) != 3 {
		t.Fatalf("frames len = %d, want 3 (one per healthy node)", len(frames))
	}
	for idx, frame := range frames {
		if !bytes.Equal(frame, validRawFrame) {
			t.Errorf("frames[%d] = %v, want %v", idx, frame, validRawFrame)
		}
	}

	// Healthy traces must all be present.
	if len(traces) != 3 {
		t.Fatalf("traces len = %d, want 3 (one per healthy node)", len(traces))
	}
	for idx, trace := range traces {
		if trace.GetTraceId() != "healthy" {
			t.Errorf("traces[%d].TraceId = %q, want %q", idx, trace.GetTraceId(), "healthy")
		}
	}

	// Laggard errors must be surfaced in the returned error.
	if collectErr == nil {
		t.Fatal("collectRawFrameResponses returned nil error; expected partial-failure error for 2 failing nodes")
	}
	errMsg := collectErr.Error()
	const wantSubstring = "data node error"
	count := strings.Count(errMsg, wantSubstring)
	if count != 2 {
		t.Errorf("error string contains %q %d time(s), want 2; full error: %s", wantSubstring, count, errMsg)
	}
}
