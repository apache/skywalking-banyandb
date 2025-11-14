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

// Package filter defines and implements data structures and interfaces for skipping index.
package filter

import (
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const (
	k = 10
	// B specifies the number of bits allocated for each item.
	// Using B=16 (power of 2) maintains memory alignment and enables shift-based math.
	// With 8k items per block: memory = 8192 * 16 / 8 = 16KB per block.
	// FPR with k=10, B=16: ~0.042%.
	B = 16
	// _bMustBePowerOf2 ensures B is a power of 2 at compile time.
	// A number is a power of 2 if and only if B > 0 and B & (B - 1) == 0.
	// This check uses: 1 / (1 - (B & (B - 1))).
	// - For powers of 2: B & (B - 1) == 0, so 1 / (1 - 0) = 1 (valid).
	// - For non-powers of 2 where B & (B - 1) == 1: 1 / (1 - 1) = 1 / 0 (compile error).
	// - For other non-powers of 2: may compile but provides basic validation.
	// Note: This is a best-effort compile-time check. For complete validation,
	// ensure B is explicitly set to a power of 2 (1, 2, 4, 8, 16, 32, 64, ...).
	_bMustBePowerOf2 = 1 / (1 - (B & (B - 1)))
)

var _ = _bMustBePowerOf2 // Ensure compile-time check is evaluated

// BloomFilter is a probabilistic data structure designed to test whether an element is a member of a set.
type BloomFilter struct {
	bits []uint64
	n    int
}

// NewBloomFilter creates a new Bloom filter with the number of items n and false positive rate p.
func NewBloomFilter(n int) *BloomFilter {
	// With B=16, we can optimize: m = n * 16 = n << 4
	// Number of uint64s needed: (n * 16) / 64 = n / 4 = n >> 2
	// Ensure at least 1 uint64 to avoid empty slice
	numBits := n >> 2
	if numBits == 0 {
		numBits = 1
	}
	bits := make([]uint64, numBits)
	return &BloomFilter{
		bits,
		n,
	}
}

// Reset resets the Bloom filter.
func (bf *BloomFilter) Reset() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.bits = bf.bits[:0]
	bf.n = 0
}

// Add adds an item to the Bloom filter.
func (bf *BloomFilter) Add(item []byte) bool {
	h := xxhash.Sum64(item)
	bits := bf.bits
	maxBits := uint64(len(bits)) * 64
	bp := (*[8]byte)(unsafe.Pointer(&h))
	b := bp[:]
	isNew := false
	for i := 0; i < k; i++ {
		hi := xxhash.Sum64(b)
		h++
		idx := hi % maxBits
		i, j := idx/64, idx%64
		mask := uint64(1) << j
		w := atomic.LoadUint64(&bits[i])
		for (w & mask) == 0 {
			wNew := w | mask
			if atomic.CompareAndSwapUint64(&bits[i], w, wNew) {
				isNew = true
				break
			}
			w = atomic.LoadUint64(&bits[i])
		}
	}
	return isNew
}

// MightContain checks if an item might be in the Bloom filter.
func (bf *BloomFilter) MightContain(item []byte) bool {
	h := xxhash.Sum64(item)
	bits := bf.bits
	maxBits := uint64(len(bits)) * 64
	bp := (*[8]byte)(unsafe.Pointer(&h))
	b := bp[:]
	for i := 0; i < k; i++ {
		hi := xxhash.Sum64(b)
		h++
		idx := hi % maxBits
		i, j := idx/64, idx%64
		mask := uint64(1) << j
		w := atomic.LoadUint64(&bits[i])
		if (w & mask) == 0 {
			return false
		}
	}
	return true
}

// Bits returns the underlying bitset.
func (bf *BloomFilter) Bits() []uint64 {
	return bf.bits
}

// N returns the number of items.
func (bf *BloomFilter) N() int {
	return bf.n
}

// SetBits sets the underlying bitset.
func (bf *BloomFilter) SetBits(bits []uint64) {
	bf.bits = bits
}

// SetN sets the number of items.
func (bf *BloomFilter) SetN(n int) {
	bf.n = n
}

// ResizeBits resizes the underlying bitset.
func (bf *BloomFilter) ResizeBits(n int) {
	bits := bf.bits
	if m := n - cap(bits); m > 0 {
		bits = append(bits[:cap(bits)], make([]uint64, m)...)
	}
	bits = bits[:n]
	bf.bits = bits
}

// OptimalBitsSize returns the optimal number of uint64s needed for n items.
// With B=16, this is simply n/4 (n >> 2), with a minimum of 1.
func OptimalBitsSize(n int) int {
	size := n >> 2
	if size == 0 {
		return 1
	}
	return size
}
