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

package plan

import (
	"bytes"
	"math"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestEncodeSortKey_Int64_NegativeSortsBelowPositive(t *testing.T) {
	col := vectorized.NewInt64Column(4)
	col.Append(-1)
	col.Append(0)
	col.Append(1)
	col.Append(math.MinInt64)

	keyNeg, err := encodeSortKey(col, 0)
	if err != nil {
		t.Fatalf("encodeSortKey(-1): %v", err)
	}
	keyZero, err := encodeSortKey(col, 1)
	if err != nil {
		t.Fatalf("encodeSortKey(0): %v", err)
	}
	keyPos, err := encodeSortKey(col, 2)
	if err != nil {
		t.Fatalf("encodeSortKey(1): %v", err)
	}
	keyMin, err := encodeSortKey(col, 3)
	if err != nil {
		t.Fatalf("encodeSortKey(MinInt64): %v", err)
	}

	if bytes.Compare(keyMin, keyNeg) >= 0 {
		t.Fatalf("MinInt64 key must sort below -1: %x vs %x", keyMin, keyNeg)
	}
	if bytes.Compare(keyNeg, keyZero) >= 0 {
		t.Fatalf("-1 key must sort below 0: %x vs %x", keyNeg, keyZero)
	}
	if bytes.Compare(keyZero, keyPos) >= 0 {
		t.Fatalf("0 key must sort below 1: %x vs %x", keyZero, keyPos)
	}
}

func TestEncodeSortKey_Float64_OrderingAcrossSpecials(t *testing.T) {
	col := vectorized.NewFloat64Column(7)
	col.Append(math.Inf(-1))
	col.Append(-1.5)
	col.Append(math.Copysign(0, -1))
	col.Append(0)
	col.Append(1.5)
	col.Append(math.Inf(1))
	col.Append(math.NaN())

	keys := make([][]byte, col.Len())
	for i := 0; i < col.Len(); i++ {
		k, err := encodeSortKey(col, i)
		if err != nil {
			t.Fatalf("encodeSortKey row %d: %v", i, err)
		}
		keys[i] = k
	}

	// -Inf < -1.5 < -0 == +0 < +1.5 < +Inf < NaN (NaN sorts last by the
	// IEEE 754 monotonic bit trick used in encodeSortKey).
	for i := 0; i < len(keys)-1; i++ {
		cmp := bytes.Compare(keys[i], keys[i+1])
		if i == 2 { // -0 vs +0
			if cmp != 0 {
				t.Fatalf("-0 and +0 must encode equal, got %d (%x vs %x)", cmp, keys[i], keys[i+1])
			}
			continue
		}
		if cmp >= 0 {
			t.Fatalf("expected keys[%d] < keys[%d]; got cmp=%d (%x vs %x)", i, i+1, cmp, keys[i], keys[i+1])
		}
	}
}

func TestEncodeSortKey_String(t *testing.T) {
	col := vectorized.NewStringColumn(3)
	col.Append("apple")
	col.Append("banana")
	col.Append("apple")

	k0, _ := encodeSortKey(col, 0)
	k1, _ := encodeSortKey(col, 1)
	k2, _ := encodeSortKey(col, 2)
	if !bytes.Equal(k0, []byte("apple")) {
		t.Fatalf("apple key got %x", k0)
	}
	if bytes.Compare(k0, k1) >= 0 {
		t.Fatalf("apple must sort below banana")
	}
	if !bytes.Equal(k0, k2) {
		t.Fatalf("equal strings must encode equal: %x vs %x", k0, k2)
	}
}

func TestEncodeSortKey_TagValue_PassthroughMatchesMarshalTagValue(t *testing.T) {
	col := vectorized.NewTagValueColumn(2)
	tv := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-7"}}}
	col.Append(tv)
	col.Append(nil)

	got, err := encodeSortKey(col, 0)
	if err != nil {
		t.Fatalf("encodeSortKey TagValue str: %v", err)
	}
	want, err := pbv1.MarshalTagValue(tv)
	if err != nil {
		t.Fatalf("pbv1.MarshalTagValue: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encodeSortKey must produce pbv1.MarshalTagValue bytes; got %x want %x", got, want)
	}

	// Nil TagValue cell encodes as nil so the comparator treats it as the
	// smallest sort key (consistent with how the row path handles missing
	// values on the sort column).
	gotNil, err := encodeSortKey(col, 1)
	if err != nil {
		t.Fatalf("encodeSortKey TagValue nil: %v", err)
	}
	if gotNil != nil {
		t.Fatalf("nil TagValue must encode to nil; got %x", gotNil)
	}
}

func TestEncodeSortKey_UnsupportedType(t *testing.T) {
	col := vectorized.NewInt64ArrayColumn(1)
	col.Append([]int64{1, 2, 3})
	if _, err := encodeSortKey(col, 0); err == nil {
		t.Fatal("encodeSortKey on []int64 column must error")
	}
}
