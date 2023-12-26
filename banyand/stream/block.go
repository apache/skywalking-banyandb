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
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type block struct {
	timestamps  []int64
	elementIDs  []string
	tagFamilies []tagFamily
}

func (b *block) reset() {
	b.timestamps = b.timestamps[:0]
	b.elementIDs = b.elementIDs[:0]

	tff := b.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	b.tagFamilies = tff[:0]
}

func (b *block) mustInitFromElements(timestamps []int64, elementIDs []string, tagFamilies [][]tagValues) {
	b.reset()
	size := len(timestamps)
	if size == 0 {
		return
	}
	if size != len(tagFamilies) {
		logger.Panicf("the number of timestamps %d must match the number of tagFamilies %d", size, len(tagFamilies))
	}

	assertTimestampsSorted(timestamps)
	b.timestamps = append(b.timestamps, timestamps...)
	b.elementIDs = append(b.elementIDs, elementIDs...)
	b.mustInitFromTags(tagFamilies)
}

func assertTimestampsSorted(timestamps []int64) {
	for i := range timestamps {
		if i > 0 && timestamps[i-1] > timestamps[i] {
			logger.Panicf("log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}
}

func (b *block) mustInitFromTags(tagFamilies [][]tagValues) {
	elementsLen := len(tagFamilies)
	if elementsLen == 0 {
		return
	}
	for i, tff := range tagFamilies {
		b.processTagFamilies(tff, i, elementsLen)
	}
}

func (b *block) processTagFamilies(tff []tagValues, i int, elementsLen int) {
	tagFamilies := b.resizeTagFamilies(len(tff))
	for j, tf := range tff {
		tagFamilies[j].name = tf.tag
		b.processTags(tf, j, i, elementsLen)
	}
}

func (b *block) processTags(tf tagValues, tagFamilyIdx, i int, elementsLen int) {
	tags := b.tagFamilies[tagFamilyIdx].resizeTags(len(tf.values))
	for j, t := range tf.values {
		tags[j].name = t.tag
		tags[j].resizeValues(elementsLen)
		tags[j].valueType = t.valueType
		tags[j].values[i] = t.marshal()
	}
}

func (b *block) resizeTagFamilies(tagFamiliesLen int) []tagFamily {
	tff := b.tagFamilies[:0]
	if n := tagFamiliesLen - cap(tff); n > 0 {
		tff = append(tff[:cap(tff)], make([]tagFamily, n)...)
	}
	tff = tff[:tagFamiliesLen]
	b.tagFamilies = tff
	return tff
}

func (b *block) Len() int {
	return len(b.timestamps)
}

func (b *block) mustWriteTo(sid common.SeriesID, bm *blockMetadata, ww *writers) {
	b.validate()
	bm.reset()

	bm.seriesID = sid
	bm.uncompressedSizeBytes = b.uncompressedSizeBytes()
	bm.count = uint64(b.Len())

	mustWriteTimestampsTo(&bm.timestamps, b.timestamps, ww.timestampsWriter)
	mustWriteElementIDsTo(&bm.elementIDs, b.elementIDs, ww.elementIDsWriter)

	for ti := range b.tagFamilies {
		b.marshalTagFamily(b.tagFamilies[ti], bm, ww)
	}
}

func (b *block) validate() {
	timestamps := b.timestamps
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i-1] > timestamps[i] {
			logger.Panicf("log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}

	itemsCount := len(timestamps)
	if itemsCount != len(b.elementIDs) {
		logger.Panicf("unexpected number of values for elementIDs: got %d; want %d", len(b.elementIDs), itemsCount)
	}
	tff := b.tagFamilies
	for _, tf := range tff {
		for _, c := range tf.tags {
			if len(c.values) != itemsCount {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", c.name, len(c.values), itemsCount)
			}
		}
	}
}

func (b *block) marshalTagFamily(tf tagFamily, bm *blockMetadata, ww *writers) {
	hw, w := ww.getTagMetadataWriterAndTagWriter(tf.name)
	cc := tf.tags
	cfm := generateTagFamilyMetadata()
	cmm := cfm.resizeTagMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&cmm[i], w)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = cfm.marshal(bb.Buf)
	releaseTagFamilyMetadata(cfm)
	tfm := bm.getTagFamilyMetadata(tf.name)
	tfm.offset = hw.bytesWritten
	tfm.size = uint64(len(bb.Buf))
	if tfm.size > maxTagFamiliesMetadataSize {
		logger.Panicf("too big tagFamilyMetadataSize: %d bytes; mustn't exceed %d bytes", tfm.size, maxTagFamiliesMetadataSize)
	}
	hw.MustWrite(bb.Buf)
}

func (b *block) unmarshalTagFamily(decoder *encoding.BytesBlockDecoder, tfIndex int, name string,
	tagFamilyMetadataBlock *dataBlock, tagProjection []string, metaReader, valueReader fs.Reader,
) {
	if len(tagProjection) < 1 {
		return
	}
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tagFamilyMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagFamilyMetadataBlock.offset), bb.Buf)
	cfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(cfm)
	_, err := cfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	b.tagFamilies[tfIndex].name = name
	cc := b.tagFamilies[tfIndex].resizeTags(len(tagProjection))

	for j := range tagProjection {
		for i := range cfm.tagMetadata {
			if tagProjection[j] == cfm.tagMetadata[i].name {
				cc[j].mustReadValues(decoder, valueReader, cfm.tagMetadata[i], uint64(b.Len()))
				break
			}
		}
	}
}

func (b *block) uncompressedSizeBytes() uint64 {
	elementsCount := uint64(b.Len())

	n := elementsCount * 8

	tff := b.tagFamilies
	for i := range tff {
		tf := tff[i]
		nameLen := uint64(len(tf.name))
		for _, c := range tf.tags {
			nameLen += uint64(len(c.name))
			for _, v := range c.values {
				if len(v) > 0 {
					n += nameLen + uint64(len(v))
				}
			}
		}
	}
	return n
}

func (b *block) mustReadFrom(decoder *encoding.BytesBlockDecoder, p *part, bm blockMetadata) {
	b.reset()

	b.timestamps = mustReadTimestampsFrom(b.timestamps, &bm.timestamps, int(bm.count), p.timestamps)
	b.elementIDs = mustReadElementIDsFrom(b.elementIDs, &bm.elementIDs, int(bm.count), p.elementIDs)

	_ = b.resizeTagFamilies(len(bm.tagProjection))
	for i := range bm.tagProjection {
		name := bm.tagProjection[i].Family
		block, ok := bm.tagFamilies[name]
		if !ok {
			continue
		}
		b.unmarshalTagFamily(decoder, i, name, block, bm.tagProjection[i].Names, p.tagFamilyMetadata[name], p.tagFamilies[name])
	}
}

// For testing purpose only.
func (b *block) sortTagFamilies() {
	sort.Slice(b.tagFamilies, func(i, j int) bool {
		return b.tagFamilies[i].name < b.tagFamilies[j].name
	})
}

func mustWriteTimestampsTo(tm *timestampsMetadata, timestamps []int64, timestampsWriter writer) {
	tm.reset()

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf, tm.encodeType, tm.min = encoding.Int64ListToBytes(bb.Buf[:0], timestamps)
	if len(bb.Buf) > maxTimestampsBlockSize {
		logger.Panicf("too big block with timestamps: %d bytes; the maximum supported size is %d bytes", len(bb.Buf), maxTimestampsBlockSize)
	}
	tm.max = timestamps[len(timestamps)-1]
	tm.offset = timestampsWriter.bytesWritten
	tm.size = uint64(len(bb.Buf))
	timestampsWriter.MustWrite(bb.Buf)
}

func mustReadTimestampsFrom(dst []int64, tm *timestampsMetadata, count int, reader fs.Reader) []int64 {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
	fs.MustReadData(reader, int64(tm.offset), bb.Buf)
	var err error
	dst, err = encoding.BytesToInt64List(dst, bb.Buf, tm.encodeType, tm.min, count)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal timestamps: %v", reader.Path(), err)
	}
	return dst
}

func mustWriteElementIDsTo(em *elementIDsMetadata, elementIDs []string, elementIDsWriter writer) {
	em.reset()

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	elementIDsByteSlice := make([][]byte, len(elementIDs))
	for i, elementID := range elementIDs {
		elementIDsByteSlice[i] = []byte(elementID)
	}
	bb.Buf = encoding.EncodeBytesBlock(bb.Buf, elementIDsByteSlice)
	if len(bb.Buf) > maxElementIDsBlockSize {
		logger.Panicf("too big block with elementIDs: %d bytes; the maximum supported size is %d bytes", len(bb.Buf), maxElementIDsBlockSize)
	}
	em.encodeType = encoding.EncodeTypeUnknown
	em.offset = elementIDsWriter.bytesWritten
	em.size = uint64(len(bb.Buf))
	elementIDsWriter.MustWrite(bb.Buf)
}

func mustReadElementIDsFrom(dst []string, em *elementIDsMetadata, count int, reader fs.Reader) []string {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(em.size))
	fs.MustReadData(reader, int64(em.offset), bb.Buf)
	decoder := encoding.BytesBlockDecoder{}
	var elementIDsByteSlice [][]byte
	elementIDsByteSlice, err := decoder.Decode(elementIDsByteSlice, bb.Buf, uint64(count))
	if err != nil {
		logger.Panicf("%s: cannot unmarshal elementIDs: %v", reader.Path(), err)
	}
	for _, elementID := range elementIDsByteSlice {
		dst = append(dst, string(elementID))
	}
	return dst
}

func generateBlock() *block {
	v := blockPool.Get()
	if v == nil {
		return &block{}
	}
	return v.(*block)
}

func releaseBlock(b *block) {
	b.reset()
	blockPool.Put(b)
}

var blockPool sync.Pool

type blockCursor struct {
	p                *part
	timestamps       []int64
	elementIDs       []string
	tagFamilies      []tagFamily
	tagValuesDecoder encoding.BytesBlockDecoder
	tagProjection    []pbv1.TagProjection
	bm               blockMetadata
	idx              int
	minTimestamp     int64
	maxTimestamp     int64
}

func (bc *blockCursor) reset() {
	bc.idx = 0
	bc.p = nil
	bc.bm = blockMetadata{}
	bc.minTimestamp = 0
	bc.maxTimestamp = 0
	bc.tagProjection = bc.tagProjection[:0]

	bc.timestamps = bc.timestamps[:0]
	bc.elementIDs = bc.elementIDs[:0]

	tff := bc.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	bc.tagFamilies = tff[:0]
}

func (bc *blockCursor) init(p *part, bm blockMetadata, queryOpts queryOptions) {
	bc.reset()
	bc.p = p
	bc.bm = bm
	bc.minTimestamp = queryOpts.minTimestamp
	bc.maxTimestamp = queryOpts.maxTimestamp
	bc.tagProjection = queryOpts.TagProjection
}

func (bc *blockCursor) copyAllTo(r *pbv1.Result) {
	r.SID = bc.bm.seriesID
	r.Timestamps = append(r.Timestamps, bc.timestamps...)
	r.ElementIDs = append(r.ElementIDs, bc.elementIDs...)
	for _, cf := range bc.tagFamilies {
		tf := pbv1.TagFamily{
			Name: cf.name,
		}
		for _, c := range cf.tags {
			t := pbv1.Tag{
				Name: c.name,
			}
			for _, v := range c.values {
				t.Values = append(t.Values, mustDecodeTagValue(c.valueType, v))
			}
			tf.Tags = append(tf.Tags, t)
		}
		r.TagFamilies = append(r.TagFamilies, tf)
	}
}

func (bc *blockCursor) copyTo(r *pbv1.Result) {
	r.SID = bc.bm.seriesID
	r.Timestamps = append(r.Timestamps, bc.timestamps[bc.idx])
	r.ElementIDs = append(r.ElementIDs, bc.elementIDs[bc.idx])
	if len(r.TagFamilies) != len(bc.tagProjection) {
		for _, tp := range bc.tagProjection {
			tf := pbv1.TagFamily{
				Name: tp.Family,
			}
			for _, n := range tp.Names {
				t := pbv1.Tag{
					Name: n,
				}
				tf.Tags = append(tf.Tags, t)
			}
			r.TagFamilies = append(r.TagFamilies, tf)
		}
	}
	if len(bc.tagFamilies) != len(r.TagFamilies) {
		logger.Panicf("unexpected number of tag families: got %d; want %d", len(bc.tagFamilies), len(r.TagFamilies))
	}
	for i, cf := range bc.tagFamilies {
		if len(r.TagFamilies[i].Tags) != len(cf.tags) {
			logger.Panicf("unexpected number of tags: got %d; want %d", len(r.TagFamilies[i].Tags), len(bc.tagProjection[i].Names))
		}
		for i2, c := range cf.tags {
			r.TagFamilies[i].Tags[i2].Values = append(r.TagFamilies[i].Tags[i2].Values, mustDecodeTagValue(c.valueType, c.values[bc.idx]))
		}
	}
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	bc.bm.tagProjection = bc.tagProjection
	tf := make(map[string]*dataBlock, len(bc.tagProjection))
	for i := range bc.tagProjection {
		for tfName, block := range bc.bm.tagFamilies {
			if bc.tagProjection[i].Family == tfName {
				tf[tfName] = block
			}
		}
	}
	bc.bm.tagFamilies = tf
	tmpBlock.mustReadFrom(&bc.tagValuesDecoder, bc.p, bc.bm)

	start, end, ok := findRange(tmpBlock.timestamps, bc.minTimestamp, bc.maxTimestamp)
	if !ok {
		return false
	}
	bc.timestamps = append(bc.timestamps, tmpBlock.timestamps[start:end]...)
	bc.elementIDs = append(bc.elementIDs, tmpBlock.elementIDs[start:end]...)

	for _, cf := range tmpBlock.tagFamilies {
		tf := tagFamily{
			name: cf.name,
		}
		for i := range cf.tags {
			tag := tag{
				name:      cf.tags[i].name,
				valueType: cf.tags[i].valueType,
			}
			if len(cf.tags[i].values) == 0 {
				continue
			}
			if len(cf.tags[i].values) != len(tmpBlock.timestamps) {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", cf.tags[i].name, len(cf.tags[i].values), len(tmpBlock.timestamps))
			}
			tag.values = append(tag.values, cf.tags[i].values[start:end]...)
			tf.tags = append(tf.tags, tag)
		}
		bc.tagFamilies = append(bc.tagFamilies, tf)
	}
	return true
}

func findRange(timestamps []int64, min int64, max int64) (int, int, bool) {
	l := len(timestamps)
	start, end := -1, -1

	for i := 0; i < l; i++ {
		if timestamps[i] > max || timestamps[l-i-1] < min {
			break
		}
		if timestamps[i] >= min && start == -1 {
			start = i
		}
		if timestamps[l-i-1] <= max && end == -1 {
			end = l - i
		}
		if start != -1 && end != -1 {
			break
		}
	}

	if start == -1 || end == -1 || start >= end {
		return 0, 0, false
	}

	return start, end, true
}

var blockCursorPool sync.Pool

func generateBlockCursor() *blockCursor {
	v := blockCursorPool.Get()
	if v == nil {
		return &blockCursor{}
	}
	return v.(*blockCursor)
}

func releaseBlockCursor(bc *blockCursor) {
	bc.reset()
	blockCursorPool.Put(bc)
}
