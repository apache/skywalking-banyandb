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
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type part struct {
	meta                 fs.Reader
	primary              fs.Reader
	timestamps           fs.Reader
	elementIDs           fs.Reader
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

func (p *part) containTimestamp(timestamp common.ItemID) bool {
	return timestamp >= common.ItemID(p.partMetadata.MinTimestamp) && timestamp <= common.ItemID(p.partMetadata.MaxTimestamp)
}

func (p *part) getElement(seriesID common.SeriesID, timestamp common.ItemID) (*streamv1.Element, error) {
	// TODO: refactor to column-based query
	// TODO: cache blocks
	for _, primary := range p.primaryBlockMetadata {
		if !primary.mightContainElement(seriesID, timestamp) {
			continue
		}
		var metaBuf []byte
		p.meta.Read(int64(primary.offset), metaBuf)
		var meta blockMetadata
		meta.unmarshal(metaBuf)
		// TODO: column prunning
		var block block
		var tagValuesDecoder *encoding.BytesBlockDecoder
		block.mustReadFrom(tagValuesDecoder, p, meta)
		for i, ts := range block.timestamps {
			if common.ItemID(ts) == timestamp {
				var tfs []*modelv1.TagFamily
				// for _, cf := range block.tagFamilies {
				// 	tf := tagFamily{
				// 		name: cf.name,
				// 	}
				// 	for i := range cf.tags {
				// 		tag := tag{
				// 			name:      cf.tags[i].name,
				// 			valueType: cf.tags[i].valueType,
				// 		}
				// 		if len(cf.tags[i].values) == 0 {
				// 			continue
				// 		}
				// 		if len(cf.tags[i].values) != len(block.timestamps) {
				// 			logger.Panicf("unexpected number of values for tags %q: got %d; want %d", cf.tags[i].name, len(cf.tags[i].values), len(block.timestamps))
				// 		}
				// 		tag.values = append(tag.values, cf.tags[i].values[start:end]...)
				// 		tf.tags = append(tf.tags, tag)
				// 	}
				// }
				// for _, tagFamily := range block.tagFamilies {
				// 	tfs = append(tfs, tagFamily)
				// }
				return &streamv1.Element{
					// Timestamp:   common.ItemID(ts),
					ElementId:   block.elementIDs[i],
					TagFamilies: tfs,
				}, nil
			}
		}
	}
	return nil, errors.New("element not found")
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], newReader(&mp.meta))

	// Open data files
	p.meta = &mp.meta
	p.primary = &mp.primary
	p.timestamps = &mp.timestamps
	p.elementIDs = &mp.elementIDs
	if mp.tagFamilies != nil {
		p.tagFamilies = make(map[string]fs.Reader)
		p.tagFamilyMetadata = make(map[string]fs.Reader)
		for name, tf := range mp.tagFamilies {
			p.tagFamilies[name] = tf
			p.tagFamilyMetadata[name] = mp.tagFamilyMetadata[name]
		}
	}
	return &p
}

type memPart struct {
	tagFamilyMetadata map[string]*bytes.Buffer
	tagFamilies       map[string]*bytes.Buffer
	meta              bytes.Buffer
	primary           bytes.Buffer
	timestamps        bytes.Buffer
	elementIDs        bytes.Buffer
	partMetadata      partMetadata
}

func (mp *memPart) mustCreateMemTagFamilyWriters(name string) (fs.Writer, fs.Writer) {
	if mp.tagFamilies == nil {
		mp.tagFamilies = make(map[string]*bytes.Buffer)
		mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
	}
	tf, ok := mp.tagFamilies[name]
	tfh := mp.tagFamilyMetadata[name]
	if ok {
		tf.Reset()
		tfh.Reset()
		return tfh, tf
	}
	mp.tagFamilies[name] = &bytes.Buffer{}
	mp.tagFamilyMetadata[name] = &bytes.Buffer{}
	return mp.tagFamilyMetadata[name], mp.tagFamilies[name]
}

func (mp *memPart) reset() {
	mp.partMetadata.reset()
	mp.meta.Reset()
	mp.primary.Reset()
	mp.timestamps.Reset()
	if mp.tagFamilies != nil {
		for _, tf := range mp.tagFamilies {
			tf.Reset()
		}
	}
	if mp.tagFamilyMetadata != nil {
		for _, tfh := range mp.tagFamilyMetadata {
			tfh.Reset()
		}
	}
}

func (mp *memPart) mustInitFromElements(es *elements) {
	mp.reset()

	if len(es.timestamps) == 0 {
		return
	}

	sort.Sort(es)

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	var sidPrev common.SeriesID
	uncompressedBlockSizeBytes := uint64(0)
	var indexPrev int
	for i := range es.timestamps {
		sid := es.seriesIDs[i]
		if sidPrev == 0 {
			sidPrev = sid
		}

		if uncompressedBlockSizeBytes >= maxUncompressedBlockSize ||
			(i-indexPrev) > maxBlockLength || sid != sidPrev {
			bsw.MustWriteElements(sidPrev, es.timestamps[indexPrev:i], es.elementIDs[indexPrev:i], es.tagFamilies[indexPrev:i])
			sidPrev = sid
			indexPrev = i
			uncompressedBlockSizeBytes = 0
		}
		uncompressedBlockSizeBytes += uncompressedElementSizeBytes(i, es)
	}
	bsw.MustWriteElements(sidPrev, es.timestamps[indexPrev:], es.elementIDs[indexPrev:], es.tagFamilies[indexPrev:])
	bsw.Flush(&mp.partMetadata)
	releaseBlockWriter(bsw)
}

func uncompressedElementSizeBytes(index int, es *elements) uint64 {
	n := uint64(len(time.RFC3339Nano))
	for i := range es.tagFamilies[index] {
		n += uint64(len(es.tagFamilies[index][i].tag))
		for j := range es.tagFamilies[index][i].values {
			n += uint64(es.tagFamilies[index][i].values[j].size())
		}
	}
	return n
}

func generateMemPart() *memPart {
	v := memPartPool.Get()
	if v == nil {
		return &memPart{}
	}
	return v.(*memPart)
}

func releaseMemPart(mp *memPart) {
	mp.reset()
	memPartPool.Put(mp)
}

var memPartPool sync.Pool

type partWrapper struct {
	mp  *memPart
	p   *part
	ref int32
}

func newMemPartWrapper(mp *memPart, p *part) *partWrapper {
	return &partWrapper{mp: mp, p: p, ref: 1}
}

func (pw *partWrapper) incRef() {
	atomic.AddInt32(&pw.ref, 1)
}

func (pw *partWrapper) decRef() {
	n := atomic.AddInt32(&pw.ref, -1)
	if n > 0 {
		return
	}
	if pw.mp != nil {
		releaseMemPart(pw.mp)
		pw.mp = nil
	}
}
