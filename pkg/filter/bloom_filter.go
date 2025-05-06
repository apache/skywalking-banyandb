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

package filter

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"

	"github.com/bits-and-blooms/bitset"
)

const FalsePositiveRate = 0.01

type BloomFilter struct {
	m        uint64
	k        uint
	bits     *bitset.BitSet
	hashFunc hash.Hash64
}

// NewBloomFilter creates a new Bloom filter with the number of items n and false positive rate p.
func NewBloomFilter(n uint64, p float64) (*BloomFilter, error) {
	if p <= 0 || p >= 1 {
		return nil, errors.New("invalid false positive rate")
	}
	if n <= 0 {
		return nil, errors.New("invalid number of items")
	}
	m := calculateM(n, p)
	k := calculateK(n, m)
	return &BloomFilter{
		m:        m,
		k:        k,
		bits:     bitset.New(uint(m)),
		hashFunc: fnv.New64a(),
	}, nil
}

func calculateM(n uint64, p float64) uint64 {
	val := math.Ceil(-(float64(n) * math.Log(p)) / (math.Ln2 * math.Ln2))
	return uint64(math.Max(float64(val), 1))
}

func calculateK(n uint64, m uint64) uint {
	val := math.Ceil((float64(m) / float64(n)) * math.Ln2)
	return uint(math.Max(float64(val), 1))
}

// Add adds an item to the Bloom filter.
func (bf *BloomFilter) Add(item []byte) error {
	hashes, err := bf.getHashes(item)
	if err != nil {
		return err
	}
	for _, h := range hashes {
		bf.bits.Set(uint(h % bf.m))
	}
	return nil
}

// MightContain checks if an item might be in the Bloom filter.
func (bf *BloomFilter) MightContain(item []byte) (bool, error) {
	hashes, err := bf.getHashes(item)
	if err != nil {
		return false, err
	}
	for _, h := range hashes {
		if !bf.bits.Test(uint(h % bf.m)) {
			return false, nil
		}
	}
	return true, nil
}

func (bf *BloomFilter) getHashes(item []byte) ([]uint64, error) {
	bf.hashFunc.Reset()
	_, err := bf.hashFunc.Write(item)
	if err != nil {
		return nil, err
	}
	h1 := bf.hashFunc.Sum64()

	h2 := h1 >> 32

	hashes := make([]uint64, bf.k)
	for i := uint(0); i < bf.k; i++ {
		hashes[i] = h1 + uint64(i)*h2
	}
	return hashes, nil
}

// M returns the size of the Bloom filter.
func (bf *BloomFilter) M() uint64 {
	return bf.m
}

// K returns the number of hash functions.
func (bf *BloomFilter) K() uint {
	return bf.k
}

// BitSet returns the underlying bitset.
func (bf *BloomFilter) Bits() *bitset.BitSet {
	return bf.bits
}

// SetM sets the size of the Bloom filter.
func (bf *BloomFilter) SetM(m uint64) {
	bf.m = m
}

// SetK sets the number of hash functions.
func (bf *BloomFilter) SetK(k uint) {
	bf.k = k
}

// SetBits sets the underlying bitset.
func (bf *BloomFilter) SetBits(bits *bitset.BitSet) {
	bf.bits = bits
}

// SetHashFunc sets the hash function used by the Bloom filter.
func (bf *BloomFilter) SetHashFunc(hashFunc hash.Hash64) {
	bf.hashFunc = hashFunc
}
