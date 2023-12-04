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
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type block struct {
	timestamps []int64

	tagFamilies []columnFamily

	field columnFamily
}

func (b *block) reset() {
	b.timestamps = b.timestamps[:0]

	tff := b.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	b.tagFamilies = tff[:0]
	b.field.reset()
}

func (b *block) assertValid() {
	timestamps := b.timestamps
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i-1] > timestamps[i] {
			logger.Panicf("log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}

	itemsCount := len(timestamps)
	tff := b.tagFamilies
	for _, tf := range tff {
		for _, c := range tf.columns {
			if len(c.values) != itemsCount {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", c.name, len(c.values), itemsCount)
			}
		}
	}
	ff := b.field
	for _, f := range ff.columns {
		if len(f.values) != itemsCount {
			logger.Panicf("unexpected number of values for fields %q: got %d; want %d", f.name, len(f.values), itemsCount)
		}
	}
}

func (b *block) MustInitFromDataPoints(timestamps []int64, tagFamilies [][]nameValues, fields []nameValues) {
	b.reset()

	assertTimestampsSorted(timestamps)
	b.timestamps = append(b.timestamps, timestamps...)
	b.mustInitFromDataPoints(tagFamilies, fields)
}

func assertTimestampsSorted(timestamps []int64) {
	for i := range timestamps {
		if i > 0 && timestamps[i-1] > timestamps[i] {
			logger.Panicf("log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}
}

func (b *block) mustInitFromDataPoints(tagFamilies [][]nameValues, fields []nameValues) {
	dataPointsLen := len(tagFamilies)
	if dataPointsLen == 0 {
		return
	}
	for i, tff := range tagFamilies {
		b.processTagFamilies(tff, i, dataPointsLen)
	}
	for i, f := range fields {
		b.processTags(f, b.field, i, dataPointsLen)
	}
}

func (b *block) processTagFamilies(tff []nameValues, i int, dataPointsLen int) {
	tagFamilies := b.resizeTagFamilies(len(tff))
	for j, tf := range tff {
		b.processTags(tf, tagFamilies[j], i, dataPointsLen)
	}
}

func (b *block) processTags(tf nameValues, cf columnFamily, i int, dataPointsLen int) {
	cf.resizeColumns(len(tf.values))
	for k, t := range tf.values {
		b.processTag(t, cf.columns[k], i, dataPointsLen)
	}
}

func (b *block) processTag(t *nameValue, c column, i int, dataPointsLen int) {
	c.resizeValues(dataPointsLen)
	c.valueType = t.valueType
	c.values[i] = t.marshal()
}

func (b *block) resizeTagFamilies(tagFamiliesLen int) []columnFamily {
	tff := b.tagFamilies[:0]
	if n := tagFamiliesLen - cap(tff); n > 0 {
		tff = append(tff[:cap(tff)], make([]columnFamily, n)...)
	}
	tff = tff[:tagFamiliesLen]
	b.tagFamilies = tff
	return tff
}

func (b *block) Len() int {
	return len(b.timestamps)
}

func (b *block) mustWriteTo(sid common.SeriesID, bh *blockMetadata, sw *writers) {
	b.assertValid()
	bh.reset()

	bh.seriesID = sid
	bh.uncompressedSizeBytes = b.uncompressedSizeBytes()
	bh.count = uint64(b.Len())

	// Marshal timestamps
	mustWriteTimestampsTo(&bh.timestamps, b.timestamps, sw)

	// Marshal tagFamilies
	tff := b.tagFamilies
	for ti := range tff {
		b.marshalTagFamily(tff[ti], bh, sw)
	}

	// Marshal field
	f := b.field
	cc := f.columns
	chh := bh.field.resizeColumnMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&chh[i], &sw.fieldValuesWriter)
	}
}

func (b *block) marshalTagFamily(tf columnFamily, bh *blockMetadata, sw *writers) {
	hw, w := sw.getColumnMetadataWriterAndColumnWriter(tf.name)
	cc := tf.columns
	cfh := getColumnFamilyMetadata()
	chh := cfh.resizeColumnMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&chh[i], w)
	}
	bb := longTermBufPool.Get()
	bb.Buf = cfh.marshal(bb.Buf)
	putColumnFamilyMetadata(cfh)
	tfh := bh.getTagFamilyMetadata(tf.name)
	tfh.offset = w.bytesWritten
	tfh.size = uint64(len(bb.Buf))
	if tfh.size > maxTagFamiliesMetadataSize {
		logger.Panicf("too big columnFamilyMetadataSize: %d bytes; mustn't exceed %d bytes", tfh.size, maxTagFamiliesMetadataSize)
	}
	hw.MustWrite(bb.Buf)
	longTermBufPool.Put(bb)
}

func (b *block) uncompressedSizeBytes() uint64 {
	dataPointsCount := uint64(b.Len())

	n := dataPointsCount * 8

	tff := b.tagFamilies
	for i := range tff {
		tf := &tff[i]
		nameLen := uint64(len(tf.name))
		for _, c := range tf.columns {
			nameLen += uint64(len(c.name))
			for _, v := range c.values {
				if len(v) > 0 {
					n += nameLen + uint64(len(v))
				}
			}
		}
	}

	ff := b.field
	for i := range ff.columns {
		c := &ff.columns[i]
		nameLen := uint64(len(c.name))
		for _, v := range c.values {
			if len(v) > 0 {
				n += nameLen + uint64(len(v))
			}
		}
	}
	return n
}

func mustWriteTimestampsTo(th *timestampsMetadata, timestamps []int64, sw *writers) {
	th.reset()

	bb := longTermBufPool.Get()
	bb.Buf, th.marshalType, th.min = encoding.Int64ListToBytes(bb.Buf[:0], timestamps)
	if len(bb.Buf) > maxTimestampsBlockSize {
		logger.Panicf("BUG: too big block with timestamps: %d bytes; the maximum supported size is %d bytes", len(bb.Buf), maxTimestampsBlockSize)
	}
	th.max = timestamps[len(timestamps)-1]
	th.offset = sw.timestampsWriter.bytesWritten
	th.size = uint64(len(bb.Buf))
	sw.timestampsWriter.MustWrite(bb.Buf)
	longTermBufPool.Put(bb)
}

func getBlock() *block {
	v := blockPool.Get()
	if v == nil {
		return &block{}
	}
	return v.(*block)
}

func putBlock(b *block) {
	b.reset()
	blockPool.Put(b)
}

var blockPool sync.Pool
