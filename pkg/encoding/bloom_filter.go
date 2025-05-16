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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/bits-and-blooms/bitset"

	"github.com/apache/skywalking-banyandb/pkg/filter"
)

// BloomFilterToBytes encodes a Bloom filter to bytes.
func BloomFilterToBytes(bf *filter.BloomFilter) ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, bf.M())
	if err != nil {
		return nil, fmt.Errorf("failed to write m: %w", err)
	}

	err = binary.Write(&buf, binary.BigEndian, uint64(bf.K()))
	if err != nil {
		return nil, fmt.Errorf("failed to write k: %w", err)
	}

	words := bf.Bits().Words()
	err = binary.Write(&buf, binary.BigEndian, uint64(len(words)))
	if err != nil {
		return nil, fmt.Errorf("failed to write bitset words length: %w", err)
	}

	for _, word := range words {
		err = binary.Write(&buf, binary.BigEndian, word)
		if err != nil {
			return nil, fmt.Errorf("failed to write bitset word: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// BytesToBloomFilter decodes a Bloom filter from bytes.
func BytesToBloomFilter(data []byte) (*filter.BloomFilter, error) {
	bf := &filter.BloomFilter{}
	bf.SetHashFunc(fnv.New64a())
	buf := bytes.NewReader(data)

	var m uint64
	err := binary.Read(buf, binary.BigEndian, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to read m: %w", err)
	}
	bf.SetM(m)

	var k uint64
	err = binary.Read(buf, binary.BigEndian, &k)
	if err != nil {
		return nil, fmt.Errorf("failed to read k: %w", err)
	}
	bf.SetK(uint(k))

	var length uint64
	err = binary.Read(buf, binary.BigEndian, &length)
	if err != nil {
		return nil, fmt.Errorf("failed to read bitset data length: %w", err)
	}

	words := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		err = binary.Read(buf, binary.BigEndian, &words[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read bitset data word %d: %w", i, err)
		}
	}
	bf.SetBits(bitset.From(words))

	if buf.Len() > 0 {
		return nil, fmt.Errorf("unexpected trailing data (%d bytes)", buf.Len())
	}
	return bf, nil
}
