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

package transfer

import "testing"

func TestUniqueSorted_NilForEmptyInput(t *testing.T) {
	if got := UniqueSorted(nil); got != nil {
		t.Fatalf("UniqueSorted(nil) = %v, want nil", got)
	}
	if got := UniqueSorted([]string{}); got != nil {
		t.Fatalf("UniqueSorted([]) = %v, want nil", got)
	}
}

func TestUniqueSorted_DedupsAndSorts(t *testing.T) {
	got := UniqueSorted([]string{"c", "a", "b", "a", "c"})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("UniqueSorted = %v, want %v", got, want)
	}
	for i, v := range want {
		if got[i] != v {
			t.Fatalf("UniqueSorted[%d] = %q, want %q", i, got[i], v)
		}
	}
}

func TestNormalizeStage(t *testing.T) {
	if NormalizeStage("") != "hot" || NormalizeStage("warm") != "warm" {
		t.Fatal("empty stage must normalize to hot")
	}
}
