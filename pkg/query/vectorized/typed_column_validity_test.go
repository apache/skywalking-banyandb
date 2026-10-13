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

package vectorized

import (
	"math/rand"
	"testing"
)

func TestSetNullsFromValidity(t *testing.T) {
	rng := rand.New(rand.NewSource(3)) //nolint:gosec // deterministic fixture
	for _, n := range []int{0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 130, 1000} {
		valid := make([]byte, (n+7)/8)
		// Garbage past the last row must not turn into nulls.
		for i := range valid {
			valid[i] = byte(rng.Intn(256))
		}
		col := NewColumnFromSlice(ColumnTypeInt64, make([]int64, n))
		col.SetNullsFromValidity(valid)
		anyNull := false
		for i := range n {
			wantNull := valid[i/8]&(1<<uint(i%8)) == 0
			if col.IsNull(i) != wantNull {
				t.Fatalf("n=%d row %d: IsNull=%t, want %t", n, i, col.IsNull(i), wantNull)
			}
			anyNull = anyNull || wantNull
		}
		if col.IsNull(n) || col.IsNull(n+64) {
			t.Fatalf("n=%d: a row past the end reads as null", n)
		}
		if col.MayHaveNulls() != anyNull {
			t.Fatalf("n=%d: MayHaveNulls=%t, want %t", n, col.MayHaveNulls(), anyNull)
		}
	}
}

func TestSetNullsFromValidityAllValidReplacesEarlierNulls(t *testing.T) {
	col := NewColumnFromSlice(ColumnTypeString, []string{"a", "b", "c"})
	col.MarkNullAt(1)
	col.SetNullsFromValidity([]byte{0xff})
	if col.IsNull(0) || col.IsNull(1) || col.IsNull(2) || col.MayHaveNulls() {
		t.Fatal("an all-valid bitmap left nulls behind")
	}
	// A short bitmap (the caller passed fewer bytes than rows need) reads the missing rows as null.
	col.SetNullsFromValidity(nil)
	if !col.IsNull(0) || !col.IsNull(2) {
		t.Fatal("rows without validity bits must be null")
	}
}
