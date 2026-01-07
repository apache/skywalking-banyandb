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
	"sort"

	"golang.org/x/exp/slices"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
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
		if !t.indexed {
			continue
		}
		if tags[j].uniqueValues == nil {
			tags[j].uniqueValues = make(map[string]struct{})
		}
		if t.valueArr != nil {
			for _, v := range t.valueArr {
				if v != nil {
					tags[j].uniqueValues[convert.BytesToString(v)] = struct{}{}
				}
			}
		} else if t.value != nil {
			tags[j].uniqueValues[convert.BytesToString(t.value)] = struct{}{}
		}
		if t.valueType == pbv1.ValueTypeInt64 {
			if len(tags[j].min) == 0 {
				tags[j].min = t.value
			} else if bytes.Compare(t.value, tags[j].min) == -1 {
				tags[j].min = t.value
			}
			if len(tags[j].max) == 0 {
				tags[j].max = t.value
			} else if bytes.Compare(t.value, tags[j].max) == 1 {
				tags[j].max = t.value
			}
		}
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
		for _, t := range tf.tags {
			if len(t.values) != itemsCount {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", t.name, len(t.values), itemsCount)
			}
		}
	}
}

func (b *block) marshalTagFamily(tf tagFamily, bm *blockMetadata, ww *writers) {
	hw, w, fw := ww.getWriters(tf.name)
	cc := tf.tags
	cfm := generateTagFamilyMetadata()
	cmm := cfm.resizeTagMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&cmm[i], w, fw)
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
	tagFamilyMetadataBlock *dataBlock, tagProjection []string, metaReader, valueReader fs.Reader, count int,
) {
	if len(tagProjection) < 1 {
		return
	}
	bb := bigValuePool.Generate()
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagFamilyMetadataBlock.size))
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
NEXT:
	for j := range tagProjection {
		for i := range tfm.tagMetadata {
			if tagProjection[j] == tfm.tagMetadata[i].name {
				cc[j].mustReadValues(decoder, valueReader, tfm.tagMetadata[i], uint64(b.Len()))
				continue NEXT
			}
		}
		cc[j].name = tagProjection[j]
		cc[j].valueType = pbv1.ValueTypeUnknown
		cc[j].resizeValues(count)
		for k := range cc[j].values {
			cc[j].values[k] = nil
		}
	}
}

func (b *block) unmarshalTagFamilyFromSeqReaders(decoder *encoding.BytesBlockDecoder, tfIndex int, name string,
	tagFamilyMetadataBlock *dataBlock, metaReader, valueReader *seqReader,
) {
	if tagFamilyMetadataBlock.offset != metaReader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", tagFamilyMetadataBlock.offset, metaReader.bytesRead)
	}
	bb := bigValuePool.Generate()
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagFamilyMetadataBlock.size))
	metaReader.mustReadFull(bb.Buf)
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
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
		n += uint64(len(tf.name))
		for _, c := range tf.tags {
			n += uint64(len(c.name))
			for _, v := range c.values {
				if len(v) > 0 {
					n += uint64(len(v))
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
			b.tagFamilies[i].name = name
			b.tagFamilies[i].resizeTags(len(bm.tagProjection[i].Names))
			for j := range bm.tagProjection[i].Names {
				b.tagFamilies[i].tags[j].name = bm.tagProjection[i].Names[j]
				b.tagFamilies[i].tags[j].valueType = pbv1.ValueTypeUnknown
				b.tagFamilies[i].tags[j].resizeValues(int(bm.count))
				for k := range bm.count {
					b.tagFamilies[i].tags[j].values[k] = nil
				}
			}
			continue
		}
		b.unmarshalTagFamily(decoder, i, name, block,
			bm.tagProjection[i].Names, p.tagFamilyMetadata[name],
			p.tagFamilies[name], int(bm.count))
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
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tm.size))
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
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tm.size))
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
	schemaTagTypes   map[string]pbv1.ValueType
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
	bc.schemaTagTypes = nil

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
	bc.schemaTagTypes = opts.schemaTagTypes
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

	for i, tf := range bc.tagFamilies {
		for j, t := range tf.tags {
			values := make([]*modelv1.TagValue, end-start)
			schemaType, hasSchemaType := bc.schemaTagTypes[t.name]
			for k := start; k < end; k++ {
				values[k-start] = decodeTagValue(t, k, hasSchemaType, schemaType)
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
	for i, tf := range bc.tagFamilies {
		if len(r.TagFamilies[i].Tags) != len(tf.tags) {
			logger.Panicf("unexpected number of tags: got %d; want %d", len(r.TagFamilies[i].Tags), len(bc.tagProjection[i].Names))
		}
		for j, t := range tf.tags {
			schemaType, hasSchemaType := bc.schemaTagTypes[t.name]
			r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, decodeTagValue(t, bc.idx, hasSchemaType, schemaType))
		}
	}
}

func decodeTagValue(t tag, idx int, hasSchemaType bool, schemaType pbv1.ValueType) *modelv1.TagValue {
	if len(t.values) <= idx {
		return pbv1.NullTagValue
	}
	valueType := t.valueType
	if t.valueType == pbv1.ValueTypeMixed && idx < len(t.types) {
		valueType = t.types[idx]
	}
	if hasSchemaType && valueType == schemaType {
		return mustDecodeTagValue(valueType, t.values[idx])
	}
	return pbv1.NullTagValue
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	bc.bm.tagProjection = bc.tagProjection
	var tf map[string]*dataBlock
	for _, tp := range bc.tagProjection {
		for tfName, block := range bc.bm.tagFamilies {
			if tp.Family == tfName {
				if tf == nil {
					tf = make(map[string]*dataBlock, len(bc.tagProjection))
				}
				tf[tfName] = block
			}
		}
	}
	if len(tf) == 0 {
		return false
	}

	bc.bm.tagFamilies = tf
	tmpBlock.mustReadFrom(&bc.tagValuesDecoder, bc.p, bc.bm)
	if len(tmpBlock.timestamps) == 0 {
		return false
	}

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

	for _, cf := range tmpBlock.tagFamilies {
		tf := tagFamily{
			name: cf.name,
		}
		for i := range cf.tags {
			t := tag{
				name:      cf.tags[i].name,
				valueType: cf.tags[i].valueType,
			}
			if len(cf.tags[i].values) == 0 {
				continue
			}
			if len(cf.tags[i].values) != len(tmpBlock.timestamps) {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d",
					cf.tags[i].name, len(cf.tags[i].values), len(tmpBlock.timestamps))
			}
			if len(idxList) > 0 {
				for _, idx := range idxList {
					t.values = append(t.values, cf.tags[i].values[idx])
					if cf.tags[i].valueType == pbv1.ValueTypeMixed && idx < len(cf.tags[i].types) {
						t.types = append(t.types, cf.tags[i].types[idx])
					}
				}
			} else {
				t.values = append(t.values, cf.tags[i].values[start:end+1]...)
				if cf.tags[i].valueType == pbv1.ValueTypeMixed && len(cf.tags[i].types) > 0 {
					t.types = append(t.types, cf.tags[i].types[start:end+1]...)
				}
			}
			tf.tags = append(tf.tags, t)
		}
		bc.tagFamilies = append(bc.tagFamilies, tf)
	}
	return len(bc.timestamps) > 0
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
			existingTag, srcTag := &bi.tagFamilies[i].tags[j], &b.tagFamilies[i].tags[j]
			if existingTag.name != srcTag.name {
				return fmt.Errorf("unexpected tag name for tag family %q: got %q; want %q",
					bi.tagFamilies[i].name, srcTag.name, existingTag.name)
			}
			if existingTag.valueType != srcTag.valueType &&
				existingTag.valueType != pbv1.ValueTypeUnknown &&
				srcTag.valueType != pbv1.ValueTypeUnknown {
				return fmt.Errorf("type mismatch for tag %q: existing=%d, source=%d",
					existingTag.name, existingTag.valueType, srcTag.valueType)
			}
			assertIdxAndOffset(srcTag.name, len(srcTag.values), b.idx, offset)
			if existingTag.valueType == pbv1.ValueTypeMixed || srcTag.valueType == pbv1.ValueTypeMixed {
				appendAsMixed(existingTag, srcTag, b.idx, offset)
			} else {
				existingTag.values = append(existingTag.values, srcTag.values[b.idx:offset]...)
			}
		}
	}
	return nil
}

func shouldAppendAsMixed(existing, src *tag) bool {
	if existing.valueType == pbv1.ValueTypeMixed || src.valueType == pbv1.ValueTypeMixed {
		return true
	}
	if existing.valueType == pbv1.ValueTypeUnknown || src.valueType == pbv1.ValueTypeUnknown {
		return false
	}
	return existing.valueType != src.valueType
}

func appendAsMixed(target, source *tag, srcIdx, offset int) {
	if target.valueType != pbv1.ValueTypeMixed {
		existingType := target.valueType
		target.resizeTypes(len(target.values))
		for i := range target.values {
			target.types[i] = existingType
		}
		target.valueType = pbv1.ValueTypeMixed
	}
	for i := srcIdx; i < offset; i++ {
		target.values = append(target.values, source.values[i])
		var valueType pbv1.ValueType
		if source.valueType == pbv1.ValueTypeMixed && len(source.types) > i {
			valueType = source.types[i]
		} else {
			valueType = source.valueType
		}
		target.types = append(target.types, valueType)
	}
}

func fullTagAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.timestamps)
	if len(bi.tagFamilies) == 0 {
		initTagFamilies(bi, b, offset, existDataSize)
		return
	}
	tagFamilyMap := make(map[string]*tagFamily)
	for i := range bi.tagFamilies {
		tagFamilyMap[bi.tagFamilies[i].name] = &bi.tagFamilies[i]
	}
	mergeTagFamilies(bi, b, offset, existDataSize, tagFamilyMap)
	padMissingTagsWithNils(bi, b, offset, tagFamilyMap)
}

func initTagFamilies(bi, b *blockPointer, offset, existDataSize int) {
	for _, tf := range b.tagFamilies {
		tfv := tagFamily{name: tf.name}
		for i := range tf.tags {
			assertIdxAndOffset(tf.tags[i].name, len(tf.tags[i].values), b.idx, offset)
			t := tag{name: tf.tags[i].name, valueType: tf.tags[i].valueType}
			for range existDataSize {
				t.values = append(t.values, nil)
			}
			t.values = append(t.values, tf.tags[i].values[b.idx:offset]...)
			if tf.tags[i].valueType == pbv1.ValueTypeMixed {
				assertIdxAndOffset(tf.tags[i].name, len(tf.tags[i].types), b.idx, offset)
				for range existDataSize {
					t.types = append(t.types, pbv1.ValueTypeUnknown)
				}
				t.types = append(t.types, tf.tags[i].types[b.idx:offset]...)
			}
			tfv.tags = append(tfv.tags, t)
		}
		bi.tagFamilies = append(bi.tagFamilies, tfv)
	}
}

func mergeTagFamilies(bi, b *blockPointer, offset, existDataSize int, tagFamilyMap map[string]*tagFamily) {
	for _, tf := range b.tagFamilies {
		existingTagFamily, exists := tagFamilyMap[tf.name]
		if !exists {
			tfv := tagFamily{name: tf.name}
			for i := range tf.tags {
				assertIdxAndOffset(tf.tags[i].name, len(tf.tags[i].values), b.idx, offset)
				t := tag{name: tf.tags[i].name, valueType: tf.tags[i].valueType}
				for range existDataSize {
					t.values = append(t.values, nil)
				}
				t.values = append(t.values, tf.tags[i].values[b.idx:offset]...)
				if tf.tags[i].valueType == pbv1.ValueTypeMixed {
					assertIdxAndOffset(tf.tags[i].name, len(tf.tags[i].types), b.idx, offset)
					for range existDataSize {
						t.types = append(t.types, pbv1.ValueTypeUnknown)
					}
					t.types = append(t.types, tf.tags[i].types[b.idx:offset]...)
				}
				tfv.tags = append(tfv.tags, t)
			}
			bi.tagFamilies = append(bi.tagFamilies, tfv)
			continue
		}

		tagMap := make(map[string]*tag)
		for i := range existingTagFamily.tags {
			tagMap[existingTagFamily.tags[i].name] = &existingTagFamily.tags[i]
		}

		for _, t := range tf.tags {
			existingTag, tagExists := tagMap[t.name]
			if tagExists {
				assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
				if shouldAppendAsMixed(existingTag, &t) {
					appendAsMixed(existingTag, &t, b.idx, offset)
				} else {
					existingTag.values = append(existingTag.values, t.values[b.idx:offset]...)
				}
			} else {
				assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
				newTag := tag{name: t.name, valueType: t.valueType}
				for range existDataSize {
					newTag.values = append(newTag.values, nil)
				}
				newTag.values = append(newTag.values, t.values[b.idx:offset]...)
				if t.valueType == pbv1.ValueTypeMixed {
					assertIdxAndOffset(t.name, len(t.types), b.idx, offset)
					for range existDataSize {
						newTag.types = append(newTag.types, pbv1.ValueTypeUnknown)
					}
					newTag.types = append(newTag.types, t.types[b.idx:offset]...)
				}
				existingTagFamily.tags = append(existingTagFamily.tags, newTag)
			}
		}
	}
}

func padMissingTagsWithNils(bi, b *blockPointer, offset int, tagFamilyMap map[string]*tagFamily) {
	clear(tagFamilyMap)
	for i := range b.tagFamilies {
		tagFamilyMap[b.tagFamilies[i].name] = &b.tagFamilies[i]
	}
	emptySize := offset - b.idx

	for _, tf := range bi.tagFamilies {
		if _, exists := tagFamilyMap[tf.name]; !exists {
			for i := range tf.tags {
				for range emptySize {
					tf.tags[i].values = append(tf.tags[i].values, nil)
				}
				if tf.tags[i].valueType == pbv1.ValueTypeMixed {
					for range emptySize {
						tf.tags[i].types = append(tf.tags[i].types, pbv1.ValueTypeUnknown)
					}
				}
			}
		} else {
			tagMap := make(map[string]*tag)
			for i := range tagFamilyMap[tf.name].tags {
				tagMap[tagFamilyMap[tf.name].tags[i].name] = &tagFamilyMap[tf.name].tags[i]
			}
			for i := range tf.tags {
				if _, tagExists := tagMap[tf.tags[i].name]; !tagExists {
					for range emptySize {
						tf.tags[i].values = append(tf.tags[i].values, nil)
					}
					if tf.tags[i].valueType == pbv1.ValueTypeMixed {
						for range emptySize {
							tf.tags[i].types = append(tf.tags[i].types, pbv1.ValueTypeUnknown)
						}
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
