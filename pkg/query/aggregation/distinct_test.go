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

package aggregation

import "testing"

func TestDistinct_CountsUniqueKeysOnly(t *testing.T) {
	d := NewDistinct()
	cases := []struct {
		key       string
		wantAdded bool
	}{
		{"a", true},
		{"b", true},
		{"a", false},
		{"c", true},
		{"b", false},
	}
	for _, c := range cases {
		if got := d.In([]byte(c.key)); got != c.wantAdded {
			t.Errorf("In(%q) = %v, want %v", c.key, got, c.wantAdded)
		}
	}
	if got := d.Val(); got != 3 {
		t.Fatalf("Val() = %d, want 3 (a, b, c)", got)
	}
}

func TestDistinct_EmptySet(t *testing.T) {
	d := NewDistinct()
	if got := d.Val(); got != 0 {
		t.Fatalf("Val() on a fresh Distinct = %d, want 0", got)
	}
}

// TestDistinct_Reset pins that Reset discards all previously seen keys —
// a repeat key after Reset must be reported as newly-added, not as a
// duplicate of the pre-reset state.
func TestDistinct_Reset(t *testing.T) {
	d := NewDistinct()
	d.In([]byte("a"))
	d.In([]byte("b"))
	if got := d.Val(); got != 2 {
		t.Fatalf("Val() before Reset = %d, want 2", got)
	}
	d.Reset()
	if got := d.Val(); got != 0 {
		t.Fatalf("Val() after Reset = %d, want 0", got)
	}
	if added := d.In([]byte("a")); !added {
		t.Fatal("In(\"a\") after Reset must report added=true — Reset must discard prior state")
	}
	if got := d.Val(); got != 1 {
		t.Fatalf("Val() after re-adding \"a\" post-Reset = %d, want 1", got)
	}
}

// TestDistinct_DistinguishesByteContentNotIdentity pins that keys are
// compared by content — two separately-allocated []byte slices with the
// same bytes must be treated as the same key, not as distinct ones by
// slice identity.
func TestDistinct_DistinguishesByteContentNotIdentity(t *testing.T) {
	d := NewDistinct()
	first := []byte("same-value")
	second := make([]byte, len(first))
	copy(second, first)

	if added := d.In(first); !added {
		t.Fatal("first In() call must report added=true")
	}
	if added := d.In(second); added {
		t.Fatal("second In() call with identical byte content must report added=false")
	}
	if got := d.Val(); got != 1 {
		t.Fatalf("Val() = %d, want 1", got)
	}
}
