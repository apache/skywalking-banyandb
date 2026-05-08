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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloat64ListToDecimalIntList(t *testing.T) {
	cases := []struct {
		name     string
		input    []float64
		wantInts []int64
		wantExp  int16
	}{
		{"int values", []float64{1.0, 2.0, 3.0}, []int64{1, 2, 3}, 0},
		{"normal floats", []float64{1.23, 4.56, 7.89}, []int64{123, 456, 789}, -2},
		{"near-equal floats", []float64{1.4999, 1.5001}, []int64{14999, 15001}, -4},
		{"mixed decimal precision", []float64{0.1, 0.12, 0.123}, []int64{100, 120, 123}, -3},
		{
			"high precision",
			[]float64{1.000_000_000_000_001, 2.100_000_000_000_002, 3.1},
			[]int64{1000000000000001, 2100000000000002, 3100000000000000},
			-15,
		},
		{"negative zero", []float64{math.Copysign(0, -1)}, []int64{0}, 0},
		{"MaxFloat64", []float64{math.MaxFloat64}, []int64{17976931348623157}, 292},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotInts, gotExp, err := Float64ListToDecimalIntList(nil, c.input)
			require.NoError(t, err)
			assert.Equal(t, c.wantInts, gotInts)
			assert.Equal(t, c.wantExp, gotExp)
		})
	}

	// Empty input returns nil (dst[:0] with nil dst).
	t.Run("empty", func(t *testing.T) {
		gotInts, gotExp, err := Float64ListToDecimalIntList(nil, []float64{})
		require.NoError(t, err)
		assert.Empty(t, gotInts)
		assert.Equal(t, int16(0), gotExp)
	})
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		inputs []float64
	}{
		{"single positive", []float64{3.14}},
		{"single negative", []float64{-2.718}},
		{"single zero", []float64{0}},
		{"single large int", []float64{1e15}},
		{"common decimals", []float64{1.23, 4.56, 7.89, 0.1, 0.123456789}},
		{"integers", []float64{0, 1, 100, -42, 999999}},
		{"mixed precision", []float64{0.1, 0.12, 0.123, 1.0, 100.0}},
		{"negative values", []float64{-1.5, -0.007, -99.99}},
		{"high precision", []float64{1.000_000_000_000_001, 2.100_000_000_000_002, 3.1}},
		{"small positive", []float64{5e-10, 3.14e-5, 1e-15}},
		{"large values", []float64{1e15, 1.5e20}},
		{"MaxFloat64", []float64{math.MaxFloat64}},
		{"SmallestPositiveFloat", []float64{math.SmallestNonzeroFloat64}},
		{"all zeros", []float64{0, 0, 0, 0, 0}},
		{"pos neg mix", []float64{1.5, -1.5, 0.003, -0.003, 100, -100}},
		{"all same value", []float64{3.14, 3.14, 3.14, 3.14}},
		{"near powers of 10", []float64{0.99, 1.0, 1.01, 9.99, 10.0, 10.01, 99.99, 100.0, 100.01}},
		{"mixed int and frac", []float64{0, 0.1, 1, 1.0, 10, 10.5, 100, 100.001}},
		{"trailing zeros", []float64{100, 1000, 10000, 100000}},
		{"negative zero mixed", []float64{math.Copysign(0, -1), 0, 1.5, -1.5}},
		{"tiny fractions", []float64{0.0000000001, 0.00000000001, 0.000000000001}},
		{"large batch", generateDiverseFloats(1000)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ints, exp, encodeErr := Float64ListToDecimalIntList(nil, c.inputs)
			require.NoError(t, encodeErr)

			floats, decodeErr := DecimalIntListToFloat64List(nil, ints, exp, len(c.inputs))
			require.NoError(t, decodeErr)
			require.Equal(t, len(c.inputs), len(floats))

			for idx, original := range c.inputs {
				assert.True(t, original == floats[idx],
					"round-trip mismatch at index %d: got %v (%b), want %v (%b)",
					idx, floats[idx], math.Float64bits(floats[idx]), original, math.Float64bits(original))
			}
		})
	}
}

// generateDiverseFloats creates a slice of float64 values spanning a limited range of magnitudes.
// Values are constructed to be exactly representable in float64.
func generateDiverseFloats(n int) []float64 {
	result := make([]float64, n)
	for i := range n {
		v := float64(i%100 + 1)
		switch i % 8 {
		case 0:
			result[i] = v / 100
		case 1:
			result[i] = v
		case 2:
			result[i] = v * 100
		case 3:
			result[i] = -v / 100
		case 4:
			result[i] = -v
		case 5:
			result[i] = -v * 100
		case 6:
			result[i] = v / 10000
		case 7:
			result[i] = 0
		}
	}
	return result
}

func TestMulPow10Fast(t *testing.T) {
	cases := []struct {
		name   string
		wantOK bool
		n      int16
		v      int64
		want   int64
	}{
		{"zero n", true, 0, 5, 5},
		{"n=1", true, 1, 3, 30},
		{"n=5", true, 5, 7, 700000},
		{"n=18 boundary", true, 18, 1, 1_000_000_000_000_000_000},
		{"n=18 near overflow", true, 18, math.MaxInt64 / pow10tab[18], (math.MaxInt64 / pow10tab[18]) * pow10tab[18]},
		{"n=18 overflow", false, 18, math.MaxInt64/pow10tab[18] + 1, 0},
		{"n=19 overflows int64", false, 19, 1, 0},
		{"n=37 overflows", false, 37, 1, 0},
		{"MaxInt64 n=37", false, 37, math.MaxInt64, 0},
		{"negative n", false, -1, 5, 0},
		{"zero value", true, 10, 0, 0},
		{"negative value", true, 3, -3, -3000},
		{"negative near overflow", true, 18, math.MinInt64 / pow10tab[18], (math.MinInt64 / pow10tab[18]) * pow10tab[18]},
		{"negative overflow", false, 18, math.MinInt64/pow10tab[18] - 1, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := mulPow10Fast(c.v, c.n)
			assert.Equal(t, c.wantOK, ok, "ok mismatch")
			if c.wantOK {
				assert.Equal(t, c.want, got, "result mismatch")
			}
		})
	}
}

func TestFloat64ListToDecimalIntListErrorCases(t *testing.T) {
	cases := []struct {
		name  string
		input []float64
	}{
		{"NaN at start", []float64{math.NaN(), 1.0, 2.0}},
		{"NaN in middle", []float64{1.0, math.NaN(), 2.0}},
		{"NaN at end", []float64{1.0, 2.0, math.NaN()}},
		{"Inf at start", []float64{math.Inf(1), 1.0}},
		{"-Inf at end", []float64{1.0, math.Inf(-1)}},
		{"all NaN", []float64{math.NaN(), math.NaN()}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Float64ListToDecimalIntList(nil, c.input)
			assert.Error(t, err)
			assert.ErrorIs(t, err, errCannotEncodeLossless)
		})
	}
}

func TestFloat64ListToDecimalIntListDstReuse(t *testing.T) {
	dst := make([]int64, 0, 10)
	result, exp, err := Float64ListToDecimalIntList(dst, []float64{1.5, 2.5})
	require.NoError(t, err)
	assert.Equal(t, []int64{15, 25}, result)
	assert.Equal(t, int16(-1), exp)
	assert.True(t, cap(result) <= cap(dst))
}

func TestDecimalIntListToFloat64ListEdgeCases(t *testing.T) {
	t.Run("empty values non-zero itemsCount", func(t *testing.T) {
		dst, err := DecimalIntListToFloat64List(nil, []int64{}, 0, 5)
		assert.NoError(t, err)
		assert.Empty(t, dst)
	})

	t.Run("dst reuse", func(t *testing.T) {
		dst := make([]float64, 0, 10)
		result, err := DecimalIntListToFloat64List(dst, []int64{1, 2, 3}, -2, 3)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(result))
		assert.InDelta(t, 0.01, result[0], 1e-18)
		assert.InDelta(t, 0.02, result[1], 1e-18)
		assert.InDelta(t, 0.03, result[2], 1e-18)
	})

	t.Run("positive exponent decode", func(t *testing.T) {
		result, err := DecimalIntListToFloat64List(nil, []int64{1, 2, 3}, 3, 3)
		assert.NoError(t, err)
		assert.Equal(t, []float64{1000, 2000, 3000}, result)
	})

	t.Run("zero exponent decode", func(t *testing.T) {
		result, err := DecimalIntListToFloat64List(nil, []int64{1, 2, 3}, 0, 3)
		assert.NoError(t, err)
		assert.Equal(t, []float64{1, 2, 3}, result)
	})

	t.Run("large negative exponent", func(t *testing.T) {
		result, err := DecimalIntListToFloat64List(nil, []int64{1}, -310, 1)
		assert.NoError(t, err)
		assert.InDelta(t, 1e-310, result[0], 1e-320)
	})
}

func TestComputeDivisors(t *testing.T) {
	cases := []struct {
		name    string
		negExp  int
		wantLen int
	}{
		{"zero", 0, 0},
		{"small exponent", 5, 1},
		{"at boundary", 308, 1},
		{"just over boundary", 309, 2},
		{"double boundary", 616, 2},
		{"over double boundary", 617, 3},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			divs := computeDivisors(c.negExp, make([]float64, 0, 2))
			assert.Equal(t, c.wantLen, len(divs))
			product := 1.0
			for _, d := range divs {
				product *= d
			}
			assert.InDelta(t, math.Pow10(c.negExp), product, 0)
		})
	}
}

// Each value is tested in its own encode-decode cycle to avoid scale alignment overflow
// when multiple values share a common exponent.
func TestEncodeDecodeSymmetry(t *testing.T) {
	inputs := []float64{
		0,
		1,
		-1,
		0.5,
		-0.5,
		1e-15,
		-1e-15,
		1e15,
		-1e15,
		float64(math.MaxInt64),
		float64(math.MinInt64),
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		1.7976931348623157e+308, // near MaxFloat64
		2.2250738585072014e-308, // near smallest normal
		1.23456789012345,
		-9.87654321098765,
		1e-300,
		-1e-300,
		1e200,
		-1e200,
	}

	for _, original := range inputs {
		ints, exp, encodeErr := Float64ListToDecimalIntList(nil, []float64{original})
		if encodeErr != nil {
			t.Errorf("encode failed for %v: %v", original, encodeErr)
			continue
		}

		decoded, decodeErr := DecimalIntListToFloat64List(nil, ints, exp, 1)
		if decodeErr != nil {
			t.Errorf("decode failed for %v: %v", original, decodeErr)
			continue
		}

		if original != decoded[0] {
			t.Errorf("symmetry mismatch: got %v (%b), want %v (%b)",
				decoded[0], math.Float64bits(decoded[0]), original, math.Float64bits(original))
		}
	}
}

func TestEncodeZeroVariants(t *testing.T) {
	posZero := 0.0
	negZero := math.Copysign(0, -1)

	ints1, exp1, err1 := Float64ListToDecimalIntList(nil, []float64{posZero})
	require.NoError(t, err1)

	ints2, exp2, err2 := Float64ListToDecimalIntList(nil, []float64{negZero})
	require.NoError(t, err2)

	assert.Equal(t, ints1, ints2)
	assert.Equal(t, exp1, exp2)
}

func TestEncodeSingleValue(t *testing.T) {
	cases := []struct {
		name    string
		input   float64
		wantInt int64
		wantExp int16
	}{
		{"zero", 0, 0, 0},
		{"one", 1, 1, 0},
		{"ten", 10, 1, 1},
		{"hundred", 100, 1, 2},
		{"0.1", 0.1, 1, -1},
		{"0.01", 0.01, 1, -2},
		{"3.14", 3.14, 314, -2},
		{"-3.14", -3.14, -314, -2},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ints, exp, err := Float64ListToDecimalIntList(nil, []float64{c.input})
			require.NoError(t, err)
			assert.Equal(t, []int64{c.wantInt}, ints)
			assert.Equal(t, c.wantExp, exp)

			decoded, decodeErr := DecimalIntListToFloat64List(nil, ints, exp, 1)
			require.NoError(t, decodeErr)
			assert.True(t, c.input == decoded[0],
				"round-trip failed: got %v, want %v", decoded[0], c.input)
		})
	}
}

func TestEncodeScaleAlignmentOverflow(t *testing.T) {
	cases := []struct {
		name  string
		input []float64
	}{
		{"large int with tiny decimal", []float64{1e18, 0.0000000000000001}},
		{"wide range", []float64{1e-15, 1e15}},
		{"negative wide range", []float64{-1e-15, -1e15}},
		{"subnormal with normal", []float64{math.SmallestNonzeroFloat64, 1.0}},
		{"large negative exponent pair", []float64{1e-300, 2e-200}},
		{"large positive exponent pair", []float64{1e200, 2e250}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Float64ListToDecimalIntList(nil, c.input)
			assert.ErrorIs(t, err, errCannotEncodeLossless)
		})
	}
}
