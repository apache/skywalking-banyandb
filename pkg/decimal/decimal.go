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

// Package decimal provides bounded decimal coefficients.
package decimal

import (
	"errors"
	"math/bits"
	"strings"
)

// ErrOverflow indicates a coefficient exceeds 38 decimal digits.
var ErrOverflow = errors.New("decimal coefficient overflow")

// Int128 is a signed decimal coefficient with at most 38 digits.
// Its zero value represents zero.
type Int128 struct {
	hi, lo   uint64
	negative bool
}

// FromInt64 constructs a coefficient from an integer.
func FromInt64(v int64) Int128 {
	if v < 0 {
		return Int128{lo: uint64(-(v + 1)) + 1, negative: true}
	}
	return Int128{lo: uint64(v)}
}

// AddInt64 returns the sum of the coefficient and an integer.
func (n Int128) AddInt64(v int64) (Int128, error) { return n.Add(FromInt64(v)) }

// Add returns the sum of two coefficients, or ErrOverflow if it exceeds 38 digits.
func (n Int128) Add(other Int128) (Int128, error) {
	if n.negative == other.negative {
		var carry uint64
		n.lo, carry = bits.Add64(n.lo, other.lo, 0)
		n.hi, carry = bits.Add64(n.hi, other.hi, carry)
		if carry != 0 || n.hi > 0x4b3b4ca85a86c47a || (n.hi == 0x4b3b4ca85a86c47a && n.lo > 0x098a223fffffffff) {
			return Int128{}, ErrOverflow
		}
	} else {
		if n.hi < other.hi || (n.hi == other.hi && n.lo < other.lo) {
			n, other = other, n
		}
		var borrow uint64
		n.lo, borrow = bits.Sub64(n.lo, other.lo, 0)
		n.hi, _ = bits.Sub64(n.hi, other.hi, borrow)
	}
	if n.hi == 0 && n.lo == 0 {
		n.negative = false
	}
	return n, nil
}

// Cmp compares two coefficients and returns -1, 0, or 1.
func (n Int128) Cmp(other Int128) int {
	if n.negative != other.negative {
		if n.negative {
			return -1
		}
		return 1
	}
	cmp := 0
	if n.hi < other.hi || (n.hi == other.hi && n.lo < other.lo) {
		cmp = -1
	} else if n.hi > other.hi || (n.hi == other.hi && n.lo > other.lo) {
		cmp = 1
	}
	if n.negative {
		return -cmp
	}
	return cmp
}

// String formats the coefficient with the given decimal scale.
func (n Int128) String(scale uint8) string {
	var buf [39]byte
	pos := len(buf)
	hi, lo := n.hi, n.lo
	for hi != 0 || lo != 0 {
		qhi, rem := bits.Div64(0, hi, 10)
		qlo, rem := bits.Div64(rem, lo, 10)
		pos--
		buf[pos] = byte('0' + rem)
		hi, lo = qhi, qlo
	}
	digits := string(buf[pos:])
	if digits == "" {
		digits = "0"
	}
	if scale > 0 {
		if len(digits) <= int(scale) {
			digits = strings.Repeat("0", int(scale)+1-len(digits)) + digits
		}
		point := len(digits) - int(scale)
		digits = digits[:point] + "." + digits[point:]
	}
	if n.negative {
		return "-" + digits
	}
	return digits
}

// CmpScaled compares coefficients interpreted with their respective scales.
// Scales must be between 0 and 18.
func (n Int128) CmpScaled(scale uint8, other Int128, otherScale uint8) int {
	if n.negative != other.negative {
		if n.negative {
			return -1
		}
		return 1
	}
	left := [3]uint64{n.lo, n.hi, 0}
	right := [3]uint64{other.lo, other.hi, 0}
	if scale < otherScale {
		left = scaleMagnitude(left, otherScale-scale)
	} else {
		right = scaleMagnitude(right, scale-otherScale)
	}
	cmp := 0
	for i := 2; i >= 0; i-- {
		if left[i] < right[i] {
			cmp = -1
			break
		}
		if left[i] > right[i] {
			cmp = 1
			break
		}
	}
	if n.negative {
		return -cmp
	}
	return cmp
}

func scaleMagnitude(v [3]uint64, scale uint8) [3]uint64 {
	for i := uint8(0); i < scale; i++ {
		var carry uint64
		for j := range v {
			hi, lo := bits.Mul64(v[j], 10)
			lo, overflow := bits.Add64(lo, carry, 0)
			v[j] = lo
			carry = hi + overflow
		}
	}
	return v
}
