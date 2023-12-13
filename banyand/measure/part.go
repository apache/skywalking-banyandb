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

package measure

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type part struct {
	partMetadata partMetadata

	primaryBlockMetadata []primaryBlockMetadata

	meta              fs.Reader
	primary           fs.Reader
	tagFamilyMetadata map[string]fs.Reader
	tagFamilies       map[string]fs.Reader
	timestamps        fs.Reader
	fieldValues       fs.Reader
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], newReader(&mp.meta))

	// Open data files
	p.meta = &mp.meta
	p.primary = &mp.primary
	p.timestamps = &mp.timestamps
	p.fieldValues = &mp.fieldValues
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
	partMetadata partMetadata

	meta              bytes.Buffer
	primary           bytes.Buffer
	tagFamilyMetadata map[string]*bytes.Buffer
	tagFamilies       map[string]*bytes.Buffer
	timestamps        bytes.Buffer
	fieldValues       bytes.Buffer
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
	mp.fieldValues.Reset()
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

func (mp *memPart) mustInitFromDataPoints(dps *dataPoints) {
	mp.reset()

	if len(dps.timestamps) == 0 {
		return
	}

	sort.Sort(dps)

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	var sidPrev common.SeriesID
	uncompressedBlockSizeBytes := uint64(0)
	var indexPrev int
	for i := range dps.timestamps {
		sid := dps.seriesIDs[i]
		if sidPrev == 0 {
			sidPrev = sid
		}

		if uncompressedBlockSizeBytes >= maxUncompressedBlockSize ||
			(i-indexPrev) > maxBlockLength || sid != sidPrev {
			bsw.MustWriteDataPoints(sidPrev, dps.timestamps[indexPrev:i], dps.tagFamilies[indexPrev:i], dps.fields[indexPrev:i])
			sidPrev = sid
			indexPrev = i
			uncompressedBlockSizeBytes = 0
		}
		uncompressedBlockSizeBytes += uncompressedDataPointSizeBytes(i, dps)
	}
	bsw.MustWriteDataPoints(sidPrev, dps.timestamps[indexPrev:], dps.tagFamilies[indexPrev:], dps.fields[indexPrev:])
	bsw.Flush(&mp.partMetadata)
	releaseBlockWriter(bsw)
}

func uncompressedDataPointSizeBytes(index int, dps *dataPoints) uint64 {
	n := uint64(len(time.RFC3339Nano))
	n += uint64(len(dps.fields[index].name))
	for i := range dps.fields[index].values {
		n += uint64(dps.fields[index].values[i].size())
	}
	for i := range dps.tagFamilies[index] {
		n += uint64(len(dps.tagFamilies[index][i].name))
		for j := range dps.tagFamilies[index][i].values {
			n += uint64(dps.tagFamilies[index][i].values[j].size())
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
	ref int32

	mp *memPart
	p  *part
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
