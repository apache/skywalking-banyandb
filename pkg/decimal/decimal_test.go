// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decimal_test

import (
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/decimal"
)

func TestInt128AddAndFormat(t *testing.T) {
	var n decimal.Int128
	n, err := n.AddInt64(-123)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.String(2); got != "-1.23" {
		t.Fatalf("got %q", got)
	}
}

func TestInt128AdditionAndComparison(t *testing.T) {
	n := decimal.FromInt64(1)
	for i := 0; i < 126; i++ {
		var err error
		n, err = n.Add(n)
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := n.String(0); got != "85070591730234615865843651857942052864" {
		t.Fatalf("got %s", got)
	}
	if _, err := n.Add(n); !errors.Is(err, decimal.ErrOverflow) {
		t.Fatalf("overflow: %v", err)
	}
	neg := decimal.FromInt64(-9223372036854775808)
	sum, err := neg.Add(decimal.FromInt64(9223372036854775807))
	if err != nil || sum.String(0) != "-1" {
		t.Fatalf("sum %s, error %v", sum.String(0), err)
	}
	if sum.Cmp(decimal.FromInt64(0)) >= 0 {
		t.Fatal("negative must compare less than zero")
	}
}

func TestCompareScaled(t *testing.T) {
	cases := []struct {
		a, b   int64
		want   int
		as, bs uint8
	}{
		{120, 12, 0, 2, 1},
		{-1, 0, -1, 18, 0},
		{1, 999999999999999999, 1, 0, 18},
		{-200, -19, -1, 2, 1},
		{0, 0, 0, 18, 0},
	}
	for _, c := range cases {
		if got := decimal.FromInt64(c.a).CmpScaled(c.as, decimal.FromInt64(c.b), c.bs); got != c.want {
			t.Fatalf("%+v: got %d", c, got)
		}
	}
}

func TestPrecisionLimit(t *testing.T) {
	var maximum decimal.Int128
	for i := 0; i < 38; i++ {
		next := maximum
		for j := 0; j < 9; j++ {
			var err error
			next, err = next.Add(maximum)
			if err != nil {
				t.Fatal(err)
			}
		}
		var err error
		maximum, err = next.AddInt64(9)
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := maximum.String(18); got != "99999999999999999999.999999999999999999" {
		t.Fatalf("got %s", got)
	}
	if _, err := maximum.AddInt64(1); !errors.Is(err, decimal.ErrOverflow) {
		t.Fatalf("overflow: %v", err)
	}
	if got := maximum.CmpScaled(18, decimal.FromInt64(1), 0); got != 1 {
		t.Fatalf("scaled comparison: %d", got)
	}
}
