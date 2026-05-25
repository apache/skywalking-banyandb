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

// §11.8 Rolling-upgrade negative test: during a rolling upgrade some data
// nodes are on the old build that hard-errors on Trace=true under raw wire
// mode ("vec wire mode (flag-on) does not support trace=true …"). The
// liaison must surface the upgraded nodes' raw frames and traces alongside
// those partial errors — no panic, no silent drop.

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

// preStory2TraceError is the exact hard-error message emitted by old data
// nodes (pre-Story-2 build) when a Trace=true request arrives under raw wire
// mode. The rolling-upgrade test uses this string to simulate laggard nodes.
const preStory2TraceError = "vec wire mode (flag-on) does not support trace=true on TopicInternalMeasureQuery;" +
	" disable --measure-vectorized-enabled or omit trace from the query"

// TestRawFrameTraceRequiresAllNodesUpgraded verifies §11.8 partial-failure
// semantics: a broadcast that mixes upgraded nodes (returning RawFrameBody +
// Trace) with laggard nodes (returning *common.Error with the pre-Story-2
// hard-error text) must not crash or silently drop the upgraded results.
//
// Expected invariants:
//   - The returned raw-frame slice contains exactly the 3 frames from the
//     upgraded nodes.
//   - The returned trace slice contains exactly the 3 traces from the
//     upgraded nodes.
//   - A non-nil error is returned that encodes both laggard failures; the
//     error text mentions "data node error" for each laggard.
func TestRawFrameTraceRequiresAllNodesUpgraded(t *testing.T) {
	// validRawFrameWithMagic: minimal frame starting with the raw-frame magic
	// leading byte (0x00) so the collector treats it as a real raw frame body.
	validRawFrame := []byte{0x00, 0x01, 0x02}
	upgradeTrace := &commonv1.Trace{TraceId: "upgraded"}

	// 3 upgraded nodes: RawFrameBody + Trace.
	upgradedFutures := []bus.Future{
		rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        upgradeTrace,
		})},
		rawFrameFuture{message: bus.NewMessage(2, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        upgradeTrace,
		})},
		rawFrameFuture{message: bus.NewMessage(3, &measurev1.InternalQueryResponse{
			RawFrameBody: validRawFrame,
			Trace:        upgradeTrace,
		})},
	}

	// 2 laggard nodes: *common.Error with the pre-Story-2 hard-error text.
	laggardFutures := []bus.Future{
		rawFrameFuture{message: bus.NewMessage(4, common.NewError(preStory2TraceError))},
		rawFrameFuture{message: bus.NewMessage(5, common.NewError(preStory2TraceError))},
	}

	allFutures := make([]bus.Future, 0, len(upgradedFutures)+len(laggardFutures))
	allFutures = append(allFutures, upgradedFutures...)
	allFutures = append(allFutures, laggardFutures...)

	frames, traces, collectErr := collectRawFrameResponses(allFutures)

	// The call must not panic (enforced by reaching this line).

	// Upgraded frames must all be present.
	if len(frames) != 3 {
		t.Fatalf("frames len = %d, want 3 (one per upgraded node)", len(frames))
	}
	for idx, frame := range frames {
		if !bytes.Equal(frame, validRawFrame) {
			t.Errorf("frames[%d] = %v, want %v", idx, frame, validRawFrame)
		}
	}

	// Upgraded traces must all be present.
	if len(traces) != 3 {
		t.Fatalf("traces len = %d, want 3 (one per upgraded node)", len(traces))
	}
	for idx, trace := range traces {
		if trace.GetTraceId() != "upgraded" {
			t.Errorf("traces[%d].TraceId = %q, want %q", idx, trace.GetTraceId(), "upgraded")
		}
	}

	// Laggard errors must be surfaced in the returned error.
	if collectErr == nil {
		t.Fatal("collectRawFrameResponses returned nil error; expected partial-failure error for 2 laggard nodes")
	}
	errMsg := collectErr.Error()
	const wantSubstring = "data node error"
	count := strings.Count(errMsg, wantSubstring)
	if count != 2 {
		t.Errorf("error string contains %q %d time(s), want 2; full error: %s", wantSubstring, count, errMsg)
	}
}
