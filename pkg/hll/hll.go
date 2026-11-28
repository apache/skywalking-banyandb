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

// Package hll implements a HyperLogLog sketch over uniformly distributed 64-bit hashes.
package hll

import (
	"math"
	"math/bits"
	"sync/atomic"
)

// Precision is the number of leading hash bits that select a register.
const Precision = 14

const registerCount = 1 << Precision

// maxEstimate is the smallest float64 that does not fit in a uint64.
const maxEstimate = float64(1 << 64)

// maxRank is the largest register value: a 64-bit hash leaves 64-Precision bits for the rank.
const maxRank = 64 - Precision + 1

// Sketch is a HyperLogLog sketch. Add calls may run concurrently with each other and with Estimate.
// Reset may run concurrently with Add, but then it does not guarantee that the Add is kept: callers
// that need a value added during a Reset to be counted must serialize the two.
type Sketch struct{ registers [registerCount]atomic.Uint32 }

// New returns an empty sketch.
func New() *Sketch { return &Sketch{} }

// Add records x. The registers are chosen by the bits of x, so x must be a uniformly distributed 64-bit
// hash, such as a series ID; raw sequential values skew the estimate.
func (s *Sketch) Add(x uint64) {
	r := &s.registers[x>>(64-Precision)]
	rank := uint32(bits.LeadingZeros64(x<<Precision|1<<(Precision-1))) + 1
	for cur := r.Load(); rank > cur; cur = r.Load() {
		if r.CompareAndSwap(cur, rank) {
			return
		}
	}
}

// Estimate returns the estimated number of distinct values added since creation or the last Reset.
// An estimate that does not fit in a uint64 is capped at math.MaxUint64.
// It uses the improved raw estimator of Ertl (https://arxiv.org/abs/1702.01284), which has no
// bias over the whole cardinality range and needs no linear counting switch or bias table.
func (s *Sketch) Estimate() uint64 {
	var counts [maxRank + 1]uint32
	for i := range s.registers {
		counts[s.registers[i].Load()]++
	}
	if counts[0] == registerCount {
		return 0
	}
	m := float64(registerCount)
	z := m * tau(1-float64(counts[maxRank])/m)
	for k := maxRank - 1; k >= 1; k-- {
		z = 0.5 * (z + float64(counts[k]))
	}
	z += m * sigma(float64(counts[0])/m)
	estimate := math.Round(m * m / (2 * math.Ln2 * z))
	if !(estimate < maxEstimate) {
		// Every register at the largest rank makes z zero and the estimate infinite.
		return math.MaxUint64
	}
	return uint64(estimate)
}

// sigma computes x + sum_{k>=1} x^(2^k) * 2^(k-1) for x in [0, 1).
func sigma(x float64) float64 {
	z := x
	for y := 1.0; ; y += y {
		x *= x
		prev := z
		z += x * y
		if z == prev {
			return z
		}
	}
}

// tau computes (1 - x - sum_{k>=1} (1 - x^(2^-k))^2 * 2^-k) / 3 for x in [0, 1].
func tau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	z := 1 - x
	for y := 1.0; ; {
		x = math.Sqrt(x)
		prev := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if z == prev {
			return z / 3
		}
	}
}

// Reset clears every register.
func (s *Sketch) Reset() {
	for i := range s.registers {
		s.registers[i].Store(0)
	}
}
