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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

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
		bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
		fs.MustReadData(filterReader, int64(tm.offset), bb.Buf)
		bf, err := encoding.BytesToBloomFilter(bb.Buf)
		if err != nil {
			logger.Panicf("%s: cannot unmarshal tagFamilyFilter: %v", filterReader.Path(), err)
		}
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
	tff.reset()
	tagFamilyFilterPool.Put(tff)
}

var tagFamilyFilterPool = pool.Register[*tagFamilyFilter]("stream-tagFamilyFilter")

type tagFamilyFilters struct {
	tagFamilyFilters []*tagFamilyFilter
}

func (tfs *tagFamilyFilters) reset() {
	for _, tff := range tfs.tagFamilyFilters {
		tff.reset()
	}
	tfs.tagFamilyFilters = tfs.tagFamilyFilters[:0]
}

func (tfs *tagFamilyFilters) unmarshal(tagFamilies map[string]*dataBlock, metaReader, filterReader map[string]fs.Reader) {
	tff := generateTagFamilyFilter()
	defer releaseTagFamilyFilter(tff)
	for tf := range tagFamilies {
		tff.reset()
		tff.unmarshal(tagFamilies[tf], metaReader[tf], filterReader[tf])
		tfs.tagFamilyFilters = append(tfs.tagFamilyFilters, tff)
	}
}

func (tfs *tagFamilyFilters) ShouldNotSkip(tagName string, tagValue string) bool {
	for _, tff := range tfs.tagFamilyFilters {
		if tf, ok := (*tff)[tagName]; ok {
			shouldNotSkip, _ := tf.filter.MightContain([]byte(tagValue))
			return shouldNotSkip
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
	tfs.reset()
	tagFamilyFiltersPool.Put(tfs)
}

var tagFamilyFiltersPool = pool.Register[*tagFamilyFilters]("stream-tagFamilyFilters")
