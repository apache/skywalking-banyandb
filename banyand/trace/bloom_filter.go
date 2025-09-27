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

package trace

import (
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func encodeBloomFilter(dst []byte, bf *filter.BloomFilter) []byte {
	dst = encoding.Int64ToBytes(dst, int64(bf.N()))
	dst = encoding.EncodeUint64Block(dst, bf.Bits())
	return dst
}

func decodeBloomFilter(src []byte, bf *filter.BloomFilter) *filter.BloomFilter {
	n := encoding.BytesToInt64(src)
	bf.SetN(int(n))

	m := n * filter.B
	bits := make([]uint64, 0)
	bits, _, err := encoding.DecodeUint64Block(bits[:0], src[8:], uint64((m+63)/64))
	if err != nil {
		logger.Panicf("failed to decode Bloom filter: %v", err)
	}
	bf.SetBits(bits)

	return bf
}
