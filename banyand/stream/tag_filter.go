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

package stream

import (
	"github.com/apache/skywalking-banyandb/pkg/bytes"
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

var bloomFilterPool = pool.Register[*filter.BloomFilter]("stream-bloomFilter")

type tagFilter struct {
	filter *filter.BloomFilter
	min    int64
	max    int64
}

type tagFamilyFilter map[string]*tagFilter

func (tff *tagFamilyFilter) reset() {
	clear(*tff)
}

func (tff tagFamilyFilter) unmarshal(tagFamilyMetadataBlock *dataBlock, metaReader, filterReader fs.Reader) {
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tagFamilyMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagFamilyMetadataBlock.offset), bb.Buf)
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	for _, tm := range tfm.tagMetadata {
		if tm.filterBlock.size == 0 {
			continue
		}
		bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.filterBlock.size))
		fs.MustReadData(filterReader, int64(tm.filterBlock.offset), bb.Buf)
		bf := generateBloomFilter()
		bf = decodeBloomFilter(bb.Buf, bf)
		tf := &tagFilter{
			filter: bf,
		}
		if tm.valueType == pbv1.ValueTypeInt64 {
			tf.min = tm.min
			tf.max = tm.max
		}
		tff[tm.name] = tf
	}
}

func generateTagFamilyFilter() *tagFamilyFilter {
	v := tagFamilyFilterPool.Get()
	if v == nil {
		return &tagFamilyFilter{}
	}
	return v
}

func releaseTagFamilyFilter(tff *tagFamilyFilter) {
	for _, tf := range *tff {
		releaseBloomFilter(tf.filter)
	}
	tff.reset()
	tagFamilyFilterPool.Put(tff)
}

var tagFamilyFilterPool = pool.Register[*tagFamilyFilter]("stream-tagFamilyFilter")

type tagFamilyFilters struct {
	tagFamilyFilters []*tagFamilyFilter
}

func (tfs *tagFamilyFilters) reset() {
	tfs.tagFamilyFilters = tfs.tagFamilyFilters[:0]
}

func (tfs *tagFamilyFilters) unmarshal(tagFamilies map[string]*dataBlock, metaReader, filterReader map[string]fs.Reader) {
	for tf := range tagFamilies {
		tff := generateTagFamilyFilter()
		tff.unmarshal(tagFamilies[tf], metaReader[tf], filterReader[tf])
		tfs.tagFamilyFilters = append(tfs.tagFamilyFilters, tff)
	}
}

func (tfs *tagFamilyFilters) Eq(tagName string, tagValue string) bool {
	for _, tff := range tfs.tagFamilyFilters {
		if tf, ok := (*tff)[tagName]; ok {
			return tf.filter.MightContain([]byte(tagValue))
		}
	}
	return true
}

func (tfs *tagFamilyFilters) Range(tagName string, rangeOpts index.RangeOpts) bool {
	for _, tff := range tfs.tagFamilyFilters {
		if tf, ok := (*tff)[tagName]; ok {
			if rangeOpts.Lower != nil {
				lower, ok := rangeOpts.Lower.(*index.FloatTermValue)
				if !ok {
					logger.Panicf("lower is not a float value: %v", rangeOpts.Lower)
				}
				if tf.max < int64(lower.Value) || !rangeOpts.IncludesLower && tf.max == int64(lower.Value) {
					return false
				}
			}
			if rangeOpts.Upper != nil {
				upper, ok := rangeOpts.Upper.(*index.FloatTermValue)
				if !ok {
					logger.Panicf("upper is not a float value: %v", rangeOpts.Upper)
				}
				if tf.min > int64(upper.Value) || !rangeOpts.IncludesUpper && tf.min == int64(upper.Value) {
					return false
				}
			}
		}
	}
	return true
}

func generateTagFamilyFilters() *tagFamilyFilters {
	v := tagFamilyFiltersPool.Get()
	if v == nil {
		return &tagFamilyFilters{}
	}
	return v
}

func releaseTagFamilyFilters(tfs *tagFamilyFilters) {
	for _, tff := range tfs.tagFamilyFilters {
		releaseTagFamilyFilter(tff)
	}
	tfs.reset()
	tagFamilyFiltersPool.Put(tfs)
}

var tagFamilyFiltersPool = pool.Register[*tagFamilyFilters]("stream-tagFamilyFilters")
