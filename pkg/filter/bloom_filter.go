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

	"github.com/cespare/xxhash"
)

const (
	k = 7
	// B specifies the number of bits allocated for each item.
	B = 10
)

// BloomFilter is a probabilistic data structure designed to test whether an element is a member of a set.
type BloomFilter struct {
	bits []uint64
	n    int
}

// NewBloomFilter creates a new Bloom filter with the number of items n and false positive rate p.
func NewBloomFilter(n int) *BloomFilter {
	m := n * B
	bits := make([]uint64, (m+63)/64)
	return &BloomFilter{
		bits,
		n,
	}
}

// Reset resets the Bloom filter.
func (bf *BloomFilter) Reset() {
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
