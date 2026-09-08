// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package query

import "testing"

func TestAddWindowRejectsUint32Overflow(t *testing.T) {
	if window, valid := AddWindow(^uint32(0), 1); valid || window != uint64(^uint32(0))+1 {
		t.Fatalf("unexpected overflow result %d %v", window, valid)
	}
}

// Reporter pattern: Trace/Stream combined limit+offset must be computed in uint64
// so MaxUint32+1 is detected instead of wrapping to 0.
func TestAddWindowReporterPatterns(t *testing.T) {
	cases := []struct {
		name          string
		limit, offset uint32
		wantValid     bool
		wantWindow    uint64
	}{
		{name: "max_limit_zero_offset", limit: ^uint32(0), offset: 0, wantValid: true, wantWindow: uint64(^uint32(0))},
		{name: "one_plus_max_offset_overflow", limit: 1, offset: ^uint32(0), wantValid: false, wantWindow: uint64(^uint32(0)) + 1},
		{name: "max_limit_plus_one_overflow", limit: ^uint32(0), offset: 1, wantValid: false, wantWindow: uint64(^uint32(0)) + 1},
		{name: "ordinary_window", limit: 20, offset: 5, wantValid: true, wantWindow: 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			window, valid := AddWindow(tc.limit, tc.offset)
			if valid != tc.wantValid || window != tc.wantWindow {
				t.Fatalf("AddWindow(%d,%d)=(%d,%v), want (%d,%v)",
					tc.limit, tc.offset, window, valid, tc.wantWindow, tc.wantValid)
			}
		})
	}
}
