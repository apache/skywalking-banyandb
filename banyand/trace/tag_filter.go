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
	"bytes"
	"fmt"

	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
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

func generateBloomFilter() *filter.BloomFilter {
	v := bloomFilterPool.Get()
	if v == nil {
		return filter.NewBloomFilter(0)
	}
	return v
}

func releaseBloomFilter(bf *filter.BloomFilter) {
	bf.Reset()
	bloomFilterPool.Put(bf)
}

var bloomFilterPool = pool.Register[*filter.BloomFilter]("trace-bloomFilter")

type tagFilter struct {
	filter *filter.BloomFilter
	min    []byte
	max    []byte
}

func (tf *tagFilter) reset() {
	tf.filter = nil
	tf.min = tf.min[:0]
	tf.max = tf.max[:0]
}

func (tf *tagFilter) unmarshal(tagMetadataBlock *dataBlock, metaReader, filterReader fs.Reader) string {
	bb := bigValuePool.Generate()
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagMetadataBlock.offset), bb.Buf)
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)
	err := tm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tm.filterBlock.size))
	fs.MustReadData(filterReader, int64(tm.filterBlock.offset), bb.Buf)
	bf := generateBloomFilter()
	bf = decodeBloomFilter(bb.Buf, bf)
	tf.filter = bf
	if tm.valueType == pbv1.ValueTypeInt64 {
		tf.min = tm.min
		tf.max = tm.max
	}
	return tm.name
}

func generateTagFilter() *tagFilter {
	v := tagFilterPool.Get()
	if v == nil {
		return &tagFilter{}
	}
	return v
}

func releaseTagFilter(tf *tagFilter) {
	releaseBloomFilter(tf.filter)
	tf.reset()
	tagFilterPool.Put(tf)
}

var tagFilterPool = pool.Register[*tagFilter]("trace-tagFilter")

type tagFilters struct {
	tagFilters map[string]*tagFilter
}

func (tfs *tagFilters) reset() {
	clear(tfs.tagFilters)
}

func (tfs *tagFilters) unmarshal(tags map[string]*dataBlock, metaReader, filterReader map[string]fs.Reader) {
	for t := range tags {
		tf := generateTagFilter()
		name := tf.unmarshal(tags[t], metaReader[t], filterReader[t])
		tfs.tagFilters[name] = tf
	}
}

func (tfs *tagFilters) Eq(tagName string, tagValue string) bool {
	if tf, ok := tfs.tagFilters[tagName]; ok {
		return tf.filter.MightContain([]byte(tagValue))
	}
	return true
}

func (tfs *tagFilters) Range(tagName string, rangeOpts index.RangeOpts) (bool, error) {
	if tf, ok := tfs.tagFilters[tagName]; ok {
		if rangeOpts.Lower != nil {
			lower, ok := rangeOpts.Lower.(*index.FloatTermValue)
			if !ok {
				return false, fmt.Errorf("lower is not a float value: %v", rangeOpts.Lower)
			}
			value := make([]byte, 0)
			value = encoding.Int64ToBytes(value, int64(lower.Value))
			if bytes.Compare(tf.max, value) == -1 || !rangeOpts.IncludesLower && bytes.Equal(tf.max, value) {
				return false, nil
			}
		}
		if rangeOpts.Upper != nil {
			upper, ok := rangeOpts.Upper.(*index.FloatTermValue)
			if !ok {
				return false, fmt.Errorf("upper is not a float value: %v", rangeOpts.Upper)
			}
			value := make([]byte, 0)
			value = encoding.Int64ToBytes(value, int64(upper.Value))
			if bytes.Compare(tf.min, value) == 1 || !rangeOpts.IncludesUpper && bytes.Equal(tf.min, value) {
				return false, nil
			}
		}
	}
	return true, nil
}

func generateTagFilters() *tagFilters {
	v := tagFiltersPool.Get()
	if v == nil {
		return &tagFilters{}
	}
	return v
}

func releaseTagFilters(tfs *tagFilters) {
	for _, tf := range tfs.tagFilters {
		releaseTagFilter(tf)
	}
	tfs.reset()
	tagFiltersPool.Put(tfs)
}

var tagFiltersPool = pool.Register[*tagFilters]("trace-tagFilters")
