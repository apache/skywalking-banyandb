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

package query

import "testing"

// TestStreamVecEmitAsFrame_TraceStaysProto exhaustively pins the data-node emit
// gate. The load-bearing case: traced=true MUST NOT emit a frame in any
// combination, because a *streamv1.QueryResponse is the only body that carries
// common.v1.Trace. Standalone (!distributed) and wire-mode-off also stay proto.
func TestStreamVecEmitAsFrame_TraceStaysProto(t *testing.T) {
	cases := []struct {
		name        string
		distributed bool
		wireRaw     bool
		traced      bool
		wantFrame   bool
	}{
		{name: "distributed+raw+untraced=frame", distributed: true, wireRaw: true, traced: false, wantFrame: true},
		{name: "distributed+raw+traced=proto", distributed: true, wireRaw: true, traced: true, wantFrame: false},
		{name: "distributed+off+untraced=proto", distributed: true, wireRaw: false, traced: false, wantFrame: false},
		{name: "standalone+raw+untraced=proto", distributed: false, wireRaw: true, traced: false, wantFrame: false},
		{name: "standalone+raw+traced=proto", distributed: false, wireRaw: true, traced: true, wantFrame: false},
		{name: "standalone+off+untraced=proto", distributed: false, wireRaw: false, traced: false, wantFrame: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := streamVecEmitAsFrame(tc.distributed, tc.wireRaw, tc.traced)
			if got != tc.wantFrame {
				t.Fatalf("streamVecEmitAsFrame(%v,%v,%v)=%v want %v",
					tc.distributed, tc.wireRaw, tc.traced, got, tc.wantFrame)
			}
			// Invariant: a traced query never emits a frame.
			if tc.traced && got {
				t.Fatalf("traced query emitted a frame — trace channel would be lost")
			}
		})
	}
}
