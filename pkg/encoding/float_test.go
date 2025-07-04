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

package encoding

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloat64ListToDecimalIntList1(t *testing.T) {
	cases := []struct {
		name     string
		input    []float64
		wantInts []int64
		wantExp  int16
		wantErr  bool
	}{
		{
			name:     "Int values",
			input:    []float64{1.0, 2.0, 3.0},
			wantInts: []int64{1, 2, 3},
			wantExp:  0,
		},
		{
			name:     "Normal float values 1",
			input:    []float64{1.23, 4.56, 7.89},
			wantInts: []int64{123, 456, 789},
			wantExp:  -2,
		},
		{
			name:     "Normal float values 2",
			input:    []float64{1.4999, 1.5001},
			wantInts: []int64{14999, 15001},
			wantExp:  -4,
		},
		{
			name:     "Mixed decimal precision",
			input:    []float64{0.1, 0.12, 0.123},
			wantInts: []int64{100, 120, 123},
			wantExp:  -3,
		},
		{
			name: "Function countDecimalPlaces() logic test",
			// 0.00_000_000_000_002 < 1-e9, it will be discarded.
			input:    []float64{1.000_000_000_000_001, 2.100_000_000_000_002, 3.1},
			wantInts: []int64{10, 21, 31},
			wantExp:  -1,
		},
		{
			name:    "Contains NaN",
			input:   []float64{math.NaN(), 1.23},
			wantErr: true,
		},
		{
			name:    "Contains Inf",
			input:   []float64{math.Inf(1), 1.23},
			wantErr: true,
		},
		{
			name:    "Multiply by scale overflow",
			input:   []float64{math.MaxFloat64},
			wantErr: true,
			wantExp: -1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotInts, gotExp, err := Float64ListToDecimalIntList(nil, c.input)

			if c.wantErr {
				if err == nil {
					t.Errorf("Expected error, but no error returned.")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error in DecimalIntListToFloat64List: %v", err)
				return
			}

			if !reflect.DeepEqual(gotInts, c.wantInts) {
				t.Errorf("Conversion result error, got: %v, want: %v", gotInts, c.wantInts)
			}

			if gotExp != c.wantExp {
				t.Errorf("Exp conversion error, got: %v, want: %v", gotExp, c.wantExp)
			}
		})
	}
}

func TestEmptyInputPanics(t *testing.T) {
	assert.Panics(t, func() {
		_, _, _ = Float64ListToDecimalIntList([]int64{}, []float64{})
	})
}
