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

package aggregation

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var rangeFoldFuncs = []modelv1.AggregationFunction{
	modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
	modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT,
	modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN,
	modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
	modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN,
}

func sameBits[N Number](t *testing.T, want, got Map[N]) {
	t.Helper()
	wp, gp := want.Partial(), got.Partial()
	require.Equal(t, math.Float64bits(float64(wp.Value)), math.Float64bits(float64(gp.Value)))
	require.Equal(t, math.Float64bits(float64(wp.Count)), math.Float64bits(float64(gp.Count)))
	require.Equal(t, math.Float64bits(float64(want.Val())), math.Float64bits(float64(got.Val())))
}

// TestRangeFoldMatchesIn proves the range folds equal one In call per value: the same
// partial state and result, a float sum bit for bit.
func TestRangeFoldMatchesIn(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // a fixed seed makes the generated rows reproducible
	for _, fn := range rangeFoldFuncs {
		for range 200 {
			n := rng.Intn(300)
			floats := make([]float64, n)
			ints := make([]int64, n)
			for i := range floats {
				// Magnitudes that make float addition order matter.
				floats[i] = (rng.Float64() - 0.3) * math.Pow(10, float64(rng.Intn(12)-4))
				ints[i] = rng.Int63n(1<<40) - 1<<39
			}
			var sel []uint16
			for i := range n {
				if rng.Intn(2) == 0 {
					sel = append(sel, uint16(i))
				}
			}
			rng.Shuffle(len(sel), func(i, j int) { sel[i], sel[j] = sel[j], sel[i] })

			wantF, _ := NewMap[float64](fn)
			sliceF, _ := NewMap[float64](fn)
			selF, _ := NewMap[float64](fn)
			for _, v := range floats {
				wantF.In(v)
			}
			sliceF.(RangeFolder[float64]).InSlice(floats)
			sameBits(t, wantF, sliceF)
			wantSelF, _ := NewMap[float64](fn)
			for _, r := range sel {
				wantSelF.In(floats[r])
			}
			selF.(RangeFolder[float64]).InSelected(floats, sel)
			sameBits(t, wantSelF, selF)

			wantI, _ := NewMap[int64](fn)
			sliceI, _ := NewMap[int64](fn)
			selI, _ := NewMap[int64](fn)
			for _, v := range ints {
				wantI.In(v)
			}
			sliceI.(RangeFolder[int64]).InSlice(ints)
			sameBits(t, wantI, sliceI)
			wantSelI, _ := NewMap[int64](fn)
			for _, r := range sel {
				wantSelI.In(ints[r])
			}
			selI.(RangeFolder[int64]).InSelected(ints, sel)
			sameBits(t, wantSelI, selI)
		}
	}
}

// TestRangeFoldContinuesState checks that a range fold continues where earlier calls
// stopped, mixing In and range calls.
func TestRangeFoldContinuesState(t *testing.T) {
	vals := []float64{1e16, 1, 1, -1e16, 3.5, 7e-3}
	for _, fn := range rangeFoldFuncs {
		want, _ := NewMap[float64](fn)
		got, _ := NewMap[float64](fn)
		for _, v := range vals {
			want.In(v)
		}
		got.In(vals[0])
		got.(RangeFolder[float64]).InSlice(vals[1:4])
		got.In(vals[4])
		got.(RangeFolder[float64]).InSelected(vals, []uint16{5})
		sameBits(t, want, got)
	}
}
