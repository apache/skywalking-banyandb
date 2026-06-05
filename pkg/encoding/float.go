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
	"errors"
	"math"
	"strconv"
	"unsafe"
)

var errCannotEncodeLossless = errors.New("cannot encode float64 losslessly as decimal int")

// Float64ListToDecimalIntList converts float64 values to int64s with a common decimal scale factor.
func Float64ListToDecimalIntList(dst []int64, src []float64) ([]int64, int16, error) {
	if len(src) == 0 {
		return dst[:0], 0, nil
	}

	decimals := dst[:0]
	exps := getInt16Scratch(len(src))
	defer putInt16Scratch(exps)

	buf := getScratchBuf(64)
	defer putScratchBuf(buf)

	minExp := int16(math.MaxInt16)
	for i, f := range src {
		d, e, ok := floatToDecimal(f, buf)
		if !ok {
			return nil, 0, errCannotEncodeLossless
		}
		decimals = append(decimals, d)
		exps[i] = e
		if e < minExp {
			minExp = e
		}
	}
	for i := range decimals {
		diff := exps[i] - minExp
		if diff == 0 {
			continue
		}
		scaled, ok := mulPow10Fast(decimals[i], diff)
		if !ok {
			return nil, 0, errCannotEncodeLossless
		}
		decimals[i] = scaled
	}
	return decimals, minExp, nil
}

// DecimalIntListToFloat64List restores float64 values from scaled int64s using a decimal exponent.
func DecimalIntListToFloat64List(dst []float64, values []int64, exponent int16, itemsCount int) ([]float64, error) {
	dst = ExtendListCapacity(dst, itemsCount)

	if len(values) == 0 {
		return dst[:0], nil
	}
	if exponent >= 0 {
		scale := math.Pow10(int(exponent))
		for _, v := range values {
			dst = append(dst, float64(v)*scale)
		}
	} else {
		var divisorsBuf [4]float64
		divisors := computeDivisors(int(-exponent), divisorsBuf[:0])
		for _, v := range values {
			result := float64(v)
			for _, d := range divisors {
				result /= d
			}
			dst = append(dst, result)
		}
	}
	return dst, nil
}

// computeDivisors splits 10^negExp into chunked float64 divisors to avoid Pow10 overflow.
func computeDivisors(negExp int, dst []float64) []float64 {
	for negExp > 0 {
		step := min(negExp, 308)
		dst = append(dst, math.Pow10(step))
		negExp -= step
	}
	return dst
}

// floatToDecimal returns (mantissa, exponent, ok) such that
// mantissa * 10^exponent == f exactly (round-trip safe).
func floatToDecimal(f float64, buf []byte) (int64, int16, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, 0, false
	}
	if f == 0 {
		return 0, 0, true
	}
	if u := int64(f); float64(u) == f {
		exp := int16(0)
		for u != 0 && u%10 == 0 {
			u /= 10
			exp++
		}
		return u, exp, true
	}
	return floatToDecimalSlow(f, buf)
}

// floatToDecimalSlow uses strconv to get the shortest round-trip representation of f,
// then parses it into (mantissa, exponent).
func floatToDecimalSlow(f float64, buf []byte) (int64, int16, bool) {
	s := strconv.AppendFloat(buf[:0], f, 'e', -1, 64)

	dotIdx := -1
	expIdx := -1
	for i, ch := range s {
		if ch == '.' {
			dotIdx = i
		} else if ch == 'e' || ch == 'E' {
			expIdx = i
			break
		}
	}
	if expIdx == -1 {
		return 0, 0, false
	}

	sciExp, err := strconv.ParseInt(bytesToString(s[expIdx+1:]), 10, 32)
	if err != nil {
		return 0, 0, false
	}

	fracDigits := int16(0)
	mantissaEnd := expIdx
	if dotIdx >= 0 {
		copy(s[dotIdx:], s[dotIdx+1:expIdx])
		mantissaEnd--
		fracDigits = int16(expIdx - dotIdx - 1)
	}

	negative := mantissaEnd > 0 && s[0] == '-'
	start := 0
	if negative {
		start = 1
	}

	for mantissaEnd > start+1 && s[mantissaEnd-1] == '0' {
		mantissaEnd--
		fracDigits--
	}

	mantissa, err := strconv.ParseInt(bytesToString(s[start:mantissaEnd]), 10, 64)
	if err != nil {
		return 0, 0, false
	}

	if sciExp > math.MaxInt16 || sciExp < math.MinInt16 {
		return 0, 0, false
	}

	exp := int16(sciExp) - fracDigits
	if negative {
		mantissa = -mantissa
	}
	return mantissa, exp, true
}

// pow10tab holds precomputed powers of 10 for fast int64 scaling.
var pow10tab = [19]int64{
	1e00, 1e01, 1e02, 1e03, 1e04, 1e05, 1e06, 1e07, 1e08, 1e09,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
}

// mulPow10Fast multiplies v by 10^n with overflow checking.
// n must be >= 0. Uses lookup table for n <= 18 (covers all practical diff values).
func mulPow10Fast(v int64, n int16) (int64, bool) {
	if n < 0 {
		return 0, false
	}
	if n <= 18 {
		if v > math.MaxInt64/pow10tab[n] || v < math.MinInt64/pow10tab[n] {
			return 0, false
		}
		return v * pow10tab[n], true
	}
	return mulPow10Large(v, n)
}

// mulPow10Large handles the rare case where n > 18.
func mulPow10Large(v int64, n int16) (int64, bool) {
	for n >= 19 {
		if v > math.MaxInt64/pow10tab[18] || v < math.MinInt64/pow10tab[18] {
			return 0, false
		}
		v *= pow10tab[18]
		n -= 18
	}
	if n > 0 {
		if v > math.MaxInt64/pow10tab[n] || v < math.MinInt64/pow10tab[n] {
			return 0, false
		}
		v *= pow10tab[n]
	}
	return v, true
}

// bytesToString converts a byte slice to a string without allocation.
// The caller must ensure the byte slice remains valid for the duration of use.
func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

var int16ScratchPool = make(chan []int16, 32)

func getInt16Scratch(n int) []int16 {
	select {
	case s := <-int16ScratchPool:
		if cap(s) >= n {
			return s[:n]
		}
	default:
	}
	return make([]int16, n)
}

func putInt16Scratch(s []int16) {
	select {
	case int16ScratchPool <- s:
	default:
	}
}

var scratchBufPool = make(chan []byte, 32)

func getScratchBuf(n int) []byte {
	select {
	case buf := <-scratchBufPool:
		if cap(buf) >= n {
			return buf
		}
	default:
	}
	return make([]byte, n)
}

func putScratchBuf(buf []byte) {
	select {
	case scratchBufPool <- buf:
	default:
	}
}
