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
	"fmt"
	"sort"

	"golang.org/x/exp/slices"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type block struct {
	timestamps  []int64
	elementIDs  []uint64
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

func (b *block) mustInitFromElements(timestamps []int64, elementIDs []uint64, tagFamilies [][]tagValues) {
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
			logger.Panicf("elements must be sorted by timestamp; got the previous element with bigger timestamp %d than the current element with timestamp %d",
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

	mustWriteTimestampsTo(&bm.timestamps, b.timestamps, b.elementIDs, &ww.timestampsWriter)

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
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	b.tagFamilies[tfIndex].name = name
	cc := b.tagFamilies[tfIndex].resizeTags(len(tagProjection))
	for j := range tagProjection {
		for i := range tfm.tagMetadata {
			if tagProjection[j] == tfm.tagMetadata[i].name {
				cc[j].mustReadValues(decoder, valueReader, tfm.tagMetadata[i], uint64(b.Len()))
				break
			}
		}
	}
}

func (b *block) unmarshalTagFamilyFromSeqReaders(decoder *encoding.BytesBlockDecoder, tfIndex int, name string,
	columnFamilyMetadataBlock *dataBlock, metaReader, valueReader *seqReader,
) {
	if columnFamilyMetadataBlock.offset != metaReader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", columnFamilyMetadataBlock.offset, metaReader.bytesRead)
	}
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(columnFamilyMetadataBlock.size))
	metaReader.mustReadFull(bb.Buf)
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal columnFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	b.tagFamilies[tfIndex].name = name

	cc := b.tagFamilies[tfIndex].resizeTags(len(tfm.tagMetadata))
	for i := range tfm.tagMetadata {
		cc[i].mustSeqReadValues(decoder, valueReader, tfm.tagMetadata[i], uint64(b.Len()))
	}
}

func (b *block) uncompressedSizeBytes() uint64 {
	elementsCount := uint64(b.Len())

	n := elementsCount * (8 + 8) // 8 bytes for timestamp and 8 bytes for elementID

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

	b.timestamps, b.elementIDs = mustReadTimestampsFrom(b.timestamps, b.elementIDs, &bm.timestamps, int(bm.count), p.timestamps)

	_ = b.resizeTagFamilies(len(bm.tagProjection))
	for i := range bm.tagProjection {
		name := bm.tagProjection[i].Family
		block, ok := bm.tagFamilies[name]
		if !ok {
			continue
		}
		b.unmarshalTagFamily(decoder, i, name, block,
			bm.tagProjection[i].Names, p.tagFamilyMetadata[name],
			p.tagFamilies[name])
	}
}

func (b *block) mustSeqReadFrom(decoder *encoding.BytesBlockDecoder, seqReaders *seqReaders, bm blockMetadata) {
	b.reset()

	b.timestamps, b.elementIDs = mustSeqReadTimestampsFrom(b.timestamps, b.elementIDs, &bm.timestamps, int(bm.count), &seqReaders.timestamps)

	_ = b.resizeTagFamilies(len(bm.tagFamilies))
	keys := make([]string, 0, len(bm.tagFamilies))
	for k := range bm.tagFamilies {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, name := range keys {
		block := bm.tagFamilies[name]
		b.unmarshalTagFamilyFromSeqReaders(decoder, i, name, block,
			seqReaders.tagFamilyMetadata[name], seqReaders.tagFamilies[name])
	}
}

// For testing purpose only.
func (b *block) sortTagFamilies() {
	sort.Slice(b.tagFamilies, func(i, j int) bool {
		return b.tagFamilies[i].name < b.tagFamilies[j].name
	})
}

func mustWriteTimestampsTo(tm *timestampsMetadata, timestamps []int64, elementIDs []uint64, timestampsWriter *writer) {
	tm.reset()

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf, tm.encodeType, tm.min = encoding.Int64ListToBytes(bb.Buf[:0], timestamps)
	tm.max = timestamps[len(timestamps)-1]
	tm.offset = timestampsWriter.bytesWritten
	tm.elementIDsOffset = uint64(len(bb.Buf))
	timestampsWriter.MustWrite(bb.Buf)
	bb.Buf = encoding.VarUint64sToBytes(bb.Buf[:0], elementIDs)
	tm.size = tm.elementIDsOffset + uint64(len(bb.Buf))
	timestampsWriter.MustWrite(bb.Buf)
}

func mustReadTimestampsFrom(timestamps []int64, elementIDs []uint64, tm *timestampsMetadata, count int, reader fs.Reader) ([]int64, []uint64) {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
	fs.MustReadData(reader, int64(tm.offset), bb.Buf)
	return mustDecodeTimestampsWithVersions(timestamps, elementIDs, tm, count, reader.Path(), bb.Buf)
}

func mustDecodeTimestampsWithVersions(timestamps []int64, elementIDs []uint64, tm *timestampsMetadata, count int, path string, src []byte) ([]int64, []uint64) {
	if tm.size < tm.elementIDsOffset {
		logger.Panicf("size %d must be greater than elementIDsOffset %d", tm.size, tm.elementIDsOffset)
	}
	var err error
	timestamps, err = encoding.BytesToInt64List(timestamps, src[:tm.elementIDsOffset], tm.encodeType, tm.min, count)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal timestamps: %v", path, err)
	}
	elementIDs = encoding.ExtendListCapacity(elementIDs, count)
	elementIDs = elementIDs[:count]
	_, err = encoding.BytesToVarUint64s(elementIDs, src[tm.elementIDsOffset:])
	if err != nil {
		logger.Panicf("%s: cannot unmarshal element ids: %v", path, err)
	}
	return timestamps, elementIDs
}

func mustSeqReadTimestampsFrom(timestamps []int64, elementIDs []uint64, tm *timestampsMetadata, count int, reader *seqReader) ([]int64, []uint64) {
	if tm.offset != reader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", tm.offset, reader.bytesRead)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
	reader.mustReadFull(bb.Buf)
	return mustDecodeTimestampsWithVersions(timestamps, elementIDs, tm, count, reader.Path(), bb.Buf)
}

func generateBlock() *block {
	v := blockPool.Get()
	if v == nil {
		return &block{}
	}
	return v
}

func releaseBlock(b *block) {
	b.reset()
	blockPool.Put(b)
}

var blockPool = pool.Register[*block]("stream-block")

type blockCursor struct {
	p                *part
	timestamps       []int64
	elementFilter    posting.List
	elementIDs       []uint64
	tagFamilies      []tagFamily
	tagValuesDecoder encoding.BytesBlockDecoder
	tagProjection    []model.TagProjection
	bm               blockMetadata
	idx              int
	minTimestamp     int64
	maxTimestamp     int64
}

func (bc *blockCursor) reset() {
	bc.idx = 0
	bc.p = nil
	bc.bm.reset()
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

func (bc *blockCursor) init(p *part, bm *blockMetadata, opts queryOptions) {
	bc.reset()
	bc.p = p
	bc.bm.copyFrom(bm)
	bc.minTimestamp = opts.minTimestamp
	bc.maxTimestamp = opts.maxTimestamp
	bc.tagProjection = opts.TagProjection
	bc.elementFilter = opts.elementFilter
}

func (bc *blockCursor) copyAllTo(r *model.StreamResult, desc bool) {
	start, end := 0, bc.idx+1
	if !desc {
		start, end = bc.idx, len(bc.timestamps)
	}
	if end <= start {
		return
	}

	r.Timestamps = append(r.Timestamps, bc.timestamps[start:end]...)
	r.ElementIDs = append(r.ElementIDs, bc.elementIDs[start:end]...)
	requiredCapacity := end - start
	r.SIDs = append(r.SIDs, make([]common.SeriesID, requiredCapacity)...)
	for i := range r.SIDs[len(r.SIDs)-requiredCapacity:] {
		r.SIDs[len(r.SIDs)-requiredCapacity+i] = bc.bm.seriesID
	}

	if desc {
		slices.Reverse(r.Timestamps)
		slices.Reverse(r.ElementIDs)
	}

	if len(r.TagFamilies) != len(bc.tagProjection) {
		r.TagFamilies = make([]model.TagFamily, len(bc.tagProjection))
		for i, tp := range bc.tagProjection {
			r.TagFamilies[i] = model.TagFamily{Name: tp.Family, Tags: make([]model.Tag, len(tp.Names))}
			for j, n := range tp.Names {
				r.TagFamilies[i].Tags[j] = model.Tag{Name: n}
			}
		}
	}

	for i, cf := range bc.tagFamilies {
		for j, c := range cf.tags {
			values := make([]*modelv1.TagValue, end-start)
			for k := start; k < end; k++ {
				if c.values != nil {
					values[k-start] = mustDecodeTagValue(c.valueType, c.values[k])
				} else {
					values[k-start] = pbv1.NullTagValue
				}
			}
			if desc {
				slices.Reverse(values)
			}
			r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, values...)
		}
	}
}

func (bc *blockCursor) copyTo(r *model.StreamResult) {
	r.Timestamps = append(r.Timestamps, bc.timestamps[bc.idx])
	r.ElementIDs = append(r.ElementIDs, bc.elementIDs[bc.idx])
	r.SIDs = append(r.SIDs, bc.bm.seriesID)
	if len(r.TagFamilies) != len(bc.tagProjection) {
		for _, tp := range bc.tagProjection {
			tf := model.TagFamily{
				Name: tp.Family,
			}
			for _, n := range tp.Names {
				t := model.Tag{
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
			if c.values != nil {
				r.TagFamilies[i].Tags[i2].Values = append(r.TagFamilies[i].Tags[i2].Values, mustDecodeTagValue(c.valueType, c.values[bc.idx]))
			} else {
				r.TagFamilies[i].Tags[i2].Values = append(r.TagFamilies[i].Tags[i2].Values, pbv1.NullTagValue)
			}
		}
	}
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	bc.bm.tagProjection = bc.tagProjection
	var tf map[string]*dataBlock
	for i := range bc.tagProjection {
		for tfName, block := range bc.bm.tagFamilies {
			if bc.tagProjection[i].Family == tfName {
				if tf == nil {
					tf = make(map[string]*dataBlock, len(bc.tagProjection))
				}
				tf[tfName] = block
			}
		}
	}
	bc.bm.tagFamilies = tf
	tmpBlock.mustReadFrom(&bc.tagValuesDecoder, bc.p, bc.bm)

	idxList := make([]int, 0)
	var start, end int
	if bc.elementFilter != nil {
		for i := range tmpBlock.elementIDs {
			if bc.elementFilter.Contains(tmpBlock.elementIDs[i]) {
				idxList = append(idxList, i)
				bc.timestamps = append(bc.timestamps, tmpBlock.timestamps[i])
				bc.elementIDs = append(bc.elementIDs, tmpBlock.elementIDs[i])
			}
		}
		if len(bc.timestamps) == 0 {
			return false
		}
	} else {
		s, e, ok := timestamp.FindRange(tmpBlock.timestamps, bc.minTimestamp, bc.maxTimestamp)
		start, end = s, e
		if !ok {
			return false
		}
		bc.timestamps = append(bc.timestamps, tmpBlock.timestamps[s:e+1]...)
		bc.elementIDs = append(bc.elementIDs, tmpBlock.elementIDs[s:e+1]...)
	}

	for i, projection := range bc.bm.tagProjection {
		tf := tagFamily{
			name: projection.Family,
		}
		blockIndex := 0
		for _, name := range projection.Names {
			t := tag{
				name: name,
			}
			if len(tmpBlock.tagFamilies[i].tags) != 0 && tmpBlock.tagFamilies[i].tags[blockIndex].name == name {
				t.valueType = tmpBlock.tagFamilies[i].tags[blockIndex].valueType
				if len(tmpBlock.tagFamilies[i].tags[blockIndex].values) != len(tmpBlock.timestamps) {
					logger.Panicf("unexpected number of values for tags %q: got %d; want %d",
						tmpBlock.tagFamilies[i].tags[blockIndex].name, len(tmpBlock.tagFamilies[i].tags[blockIndex].values), len(tmpBlock.timestamps))
				}
				if len(idxList) > 0 {
					for _, idx := range idxList {
						t.values = append(t.values, tmpBlock.tagFamilies[i].tags[blockIndex].values[idx])
					}
				} else {
					t.values = append(t.values, tmpBlock.tagFamilies[i].tags[blockIndex].values[start:end+1]...)
				}
			}
			blockIndex++
			tf.tags = append(tf.tags, t)
		}
		bc.tagFamilies = append(bc.tagFamilies, tf)
	}
	return true
}

var blockCursorPool = pool.Register[*blockCursor]("stream-blockCursor")

func generateBlockCursor() *blockCursor {
	v := blockCursorPool.Get()
	if v == nil {
		return &blockCursor{}
	}
	return v
}

func releaseBlockCursor(bc *blockCursor) {
	bc.reset()
	blockCursorPool.Put(bc)
}

type blockPointer struct {
	block
	bm  blockMetadata
	idx int
}

func (bi *blockPointer) updateMetadata() {
	if len(bi.block.timestamps) == 0 {
		return
	}
	// only update timestamps since they are used for merging
	// blockWriter will recompute all fields
	bi.bm.timestamps.min = bi.block.timestamps[0]
	bi.bm.timestamps.max = bi.block.timestamps[len(bi.timestamps)-1]
}

func (bi *blockPointer) copyFrom(src *blockPointer) {
	bi.idx = 0
	bi.bm.copyFrom(&src.bm)
	bi.appendAll(src)
}

func (bi *blockPointer) appendAll(b *blockPointer) {
	if len(b.timestamps) == 0 {
		return
	}
	bi.append(b, len(b.timestamps))
}

var log = logger.GetLogger("stream").Named("block")

func (bi *blockPointer) append(b *blockPointer, offset int) {
	if offset <= b.idx {
		return
	}
	if len(bi.tagFamilies) == 0 && len(b.tagFamilies) > 0 {
		fullTagAppend(bi, b, offset)
	} else {
		if err := fastTagAppend(bi, b, offset); err != nil {
			if log.Debug().Enabled() {
				log.Debug().Msgf("fastTagMerge failed: %v; falling back to fullTagMerge", err)
			}
			fullTagAppend(bi, b, offset)
		}
	}

	assertIdxAndOffset("timestamps", len(b.timestamps), bi.idx, offset)
	bi.timestamps = append(bi.timestamps, b.timestamps[b.idx:offset]...)
	bi.elementIDs = append(bi.elementIDs, b.elementIDs[b.idx:offset]...)
}

func fastTagAppend(bi, b *blockPointer, offset int) error {
	if len(bi.tagFamilies) != len(b.tagFamilies) {
		return fmt.Errorf("unexpected number of tag families: got %d; want %d", len(b.tagFamilies), len(bi.tagFamilies))
	}
	for i := range bi.tagFamilies {
		if bi.tagFamilies[i].name != b.tagFamilies[i].name {
			return fmt.Errorf("unexpected tag family name: got %q; want %q", b.tagFamilies[i].name, bi.tagFamilies[i].name)
		}
		if len(bi.tagFamilies[i].tags) != len(b.tagFamilies[i].tags) {
			return fmt.Errorf("unexpected number of tags for tag family %q: got %d; want %d",
				bi.tagFamilies[i].name, len(b.tagFamilies[i].tags), len(bi.tagFamilies[i].tags))
		}
		for j := range bi.tagFamilies[i].tags {
			if bi.tagFamilies[i].tags[j].name != b.tagFamilies[i].tags[j].name {
				return fmt.Errorf("unexpected tag name for tag family %q: got %q; want %q",
					bi.tagFamilies[i].name, b.tagFamilies[i].tags[j].name, bi.tagFamilies[i].tags[j].name)
			}
			assertIdxAndOffset(b.tagFamilies[i].tags[j].name, len(b.tagFamilies[i].tags[j].values), b.idx, offset)
			bi.tagFamilies[i].tags[j].values = append(bi.tagFamilies[i].tags[j].values, b.tagFamilies[i].tags[j].values[b.idx:offset]...)
		}
	}
	return nil
}

func fullTagAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.timestamps)
	appendTagFamilies := func(tf tagFamily) {
		tfv := tagFamily{name: tf.name}
		for i := range tf.tags {
			assertIdxAndOffset(tf.tags[i].name, len(tf.tags[i].values), b.idx, offset)
			col := tag{name: tf.tags[i].name, valueType: tf.tags[i].valueType}
			for j := 0; j < existDataSize; j++ {
				col.values = append(col.values, nil)
			}
			col.values = append(col.values, tf.tags[i].values[b.idx:offset]...)
			tfv.tags = append(tfv.tags, col)
		}
		bi.tagFamilies = append(bi.tagFamilies, tfv)
	}
	if len(bi.tagFamilies) == 0 {
		for _, tf := range b.tagFamilies {
			appendTagFamilies(tf)
		}
		return
	}

	tagFamilyMap := make(map[string]*tagFamily)
	for i := range bi.tagFamilies {
		tagFamilyMap[bi.tagFamilies[i].name] = &bi.tagFamilies[i]
	}

	for _, tf := range b.tagFamilies {
		if existingTagFamily, exists := tagFamilyMap[tf.name]; exists {
			columnMap := make(map[string]*tag)
			for i := range existingTagFamily.tags {
				columnMap[existingTagFamily.tags[i].name] = &existingTagFamily.tags[i]
			}

			for _, c := range tf.tags {
				if existingColumn, exists := columnMap[c.name]; exists {
					assertIdxAndOffset(c.name, len(c.values), b.idx, offset)
					existingColumn.values = append(existingColumn.values, c.values[b.idx:offset]...)
				} else {
					assertIdxAndOffset(c.name, len(c.values), b.idx, offset)
					col := tag{name: c.name, valueType: c.valueType}
					for j := 0; j < existDataSize; j++ {
						col.values = append(col.values, nil)
					}
					col.values = append(col.values, c.values[b.idx:offset]...)
					existingTagFamily.tags = append(existingTagFamily.tags, col)
				}
			}
		} else {
			appendTagFamilies(tf)
		}
	}
	for k := range tagFamilyMap {
		delete(tagFamilyMap, k)
	}
	for i := range b.tagFamilies {
		tagFamilyMap[b.tagFamilies[i].name] = &b.tagFamilies[i]
	}
	emptySize := offset - b.idx
	for _, tf := range bi.tagFamilies {
		if _, exists := tagFamilyMap[tf.name]; !exists {
			for i := range tf.tags {
				for j := 0; j < emptySize; j++ {
					tf.tags[i].values = append(tf.tags[i].values, nil)
				}
			}
		} else {
			existingTagFamily := tagFamilyMap[tf.name]
			columnMap := make(map[string]*tag)
			for i := range existingTagFamily.tags {
				columnMap[existingTagFamily.tags[i].name] = &existingTagFamily.tags[i]
			}
			for i := range tf.tags {
				if _, exists := columnMap[tf.tags[i].name]; !exists {
					for j := 0; j < emptySize; j++ {
						tf.tags[i].values = append(tf.tags[i].values, nil)
					}
				}
			}
		}
	}
}

func assertIdxAndOffset(name string, length int, idx int, offset int) {
	if idx >= offset {
		logger.Panicf("%q idx %d must be less than offset %d", name, idx, offset)
	}
	if offset > length {
		logger.Panicf("%q offset %d must be less than or equal to length %d", name, offset, length)
	}
}

func (bi *blockPointer) isFull() bool {
	return bi.bm.uncompressedSizeBytes >= maxUncompressedBlockSize
}

func (bi *blockPointer) reset() {
	bi.idx = 0
	bi.block.reset()
	bi.bm = blockMetadata{}
}

func generateBlockPointer() *blockPointer {
	v := blockPointerPool.Get()
	if v == nil {
		return &blockPointer{}
	}
	return v
}

func releaseBlockPointer(bi *blockPointer) {
	bi.reset()
	blockPointerPool.Put(bi)
}

var blockPointerPool = pool.Register[*blockPointer]("stream-blockPointer")
