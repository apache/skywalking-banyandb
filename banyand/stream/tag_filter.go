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

var bloomFilterPool = pool.Register[*filter.BloomFilter]("stream-bloomFilter")

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

var tagFilterPool = pool.Register[*tagFilter]("stream-tagFilter")

type tagFamilyFilter map[string]*tagFilter

func (tff *tagFamilyFilter) reset() {
	clear(*tff)
}

func (tff tagFamilyFilter) unmarshal(tagFamilyMetadataBlock *dataBlock, metaReader, filterReader fs.Reader) {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = pkgbytes.ResizeExact(bb.Buf[:0], int(tagFamilyMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagFamilyMetadataBlock.offset), bb.Buf)
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	for _, tm := range tfm.tagMetadata {
		if tm.filterBlock.size == 0 {
			continue
		}
		bb.Buf = pkgbytes.ResizeExact(bb.Buf[:0], int(tm.filterBlock.size))
		fs.MustReadData(filterReader, int64(tm.filterBlock.offset), bb.Buf)
		bf := generateBloomFilter()
		bf = decodeBloomFilter(bb.Buf, bf)
		tf := generateTagFilter()
		tf.filter = bf
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
		releaseTagFilter(tf)
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

func (tfs *tagFamilyFilters) Range(tagName string, rangeOpts index.RangeOpts) (bool, error) {
	for _, tff := range tfs.tagFamilyFilters {
		if tf, ok := (*tff)[tagName]; ok {
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
	}
	return true, nil
}

// Having checks if any of the provided tag values might exist in the bloom filter.
// It returns true if at least one value might be contained in any tag family filter.
func (tfs *tagFamilyFilters) Having(tagName string, tagValues []string) bool {
	foundTag := false
	for _, tff := range tfs.tagFamilyFilters {
		if tf, ok := (*tff)[tagName]; ok {
			foundTag = true
			if tf.filter != nil {
				for _, tagValue := range tagValues {
					if tf.filter.MightContain([]byte(tagValue)) {
						return true // Return true as soon as we find a potential match
					}
				}
				// None of the values might exist in this tag family filter, continue checking others
				continue
			}
			// If no bloom filter, conservatively return true
			return true
		}
	}
	// If tag was found in at least one tag family filter, but no values matched, return false
	if foundTag {
		return false
	}
	// If tag is not found in any tag family filter, return true (conservative)
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
