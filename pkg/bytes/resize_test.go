// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bytes

import (
	"testing"
)

func TestResizeOver(t *testing.T) {
	testCases := []struct {
		name    string
		input   []byte
		n       int
		wantLen int
		wantCap int
	}{
		{"Resize to Smaller Size", []byte{1, 2, 3}, 2, 2, 3},
		{"Resize to Larger Size", []byte{1, 2, 3}, 5, 5, 8},
		{"Resize to Same Size", []byte{1, 2, 3}, 3, 3, 3},
		{"Empty Slice Resize", []byte{}, 5, 5, 8},
		{"Resize to Zero Size", []byte{1, 2, 3}, 0, 0, 3},
		{"Resize to Large Power of 2", []byte{1, 2, 3}, 1024, 1024, 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resized := ResizeOver(tc.input, tc.n)
			if got, want := len(resized), tc.wantLen; got != want {
				t.Errorf("got length %d, want %d", got, want)
			}
			if got, want := cap(resized), tc.wantCap; got != want {
				t.Errorf("got capacity %d, want %d", got, want)
			}
		})
	}
}

func TestResizeExact(t *testing.T) {
	testCases := []struct {
		name    string
		input   []byte
		n       int
		wantLen int
		wantCap int
	}{
		{"Resize to Smaller Size", []byte{1, 2, 3}, 2, 2, 3},
		{"Resize to Larger Size", []byte{1, 2, 3}, 5, 5, 5},
		{"Resize to Same Size", []byte{1, 2, 3}, 3, 3, 3},
		{"Empty Slice Resize", []byte{}, 5, 5, 5},
		{"Resize to Zero Size", []byte{1, 2, 3}, 0, 0, 3},
		{"Resize to Large Size", []byte{1, 2, 3}, 1000, 1000, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resized := ResizeExact(tc.input, tc.n)
			if got, want := len(resized), tc.wantLen; got != want {
				t.Errorf("got length %d, want %d", got, want)
			}
			if got, want := cap(resized), tc.wantCap; got != want {
				t.Errorf("got capacity %d, want %d", got, want)
			}
		})
	}
}

func TestRoundToNearestPow2(t *testing.T) {
	testCases := []struct {
		name  string
		input int
		want  int
	}{
		{"Round to Nearest Pow2 (16)", 15, 16},
		{"Round to Nearest Pow2 (64)", 50, 64},
		{"Round to Nearest Pow2 (128)", 100, 128},
		{"Round to Nearest Pow2 (512)", 400, 512},
		{"Round to Nearest Pow2 (1024)", 900, 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := roundToNearestPow2(tc.input)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}
