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
	"fmt"
	"slices"
	"sort"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type block struct {
	spans [][]byte
	tags  []tag
	minTS int64
	maxTS int64
}

func (b *block) reset() {
	b.spans = b.spans[:0]
	b.tags = b.tags[:0]
	b.minTS = 0
	b.maxTS = 0
}

func (b *block) mustInitFromTrace(spans [][]byte, tags [][]*tagValue, timestamps []int64) {
	b.reset()
	size := len(spans)
	if size == 0 {
		return
	}
	if size != len(tags) {
		logger.Panicf("the number of spans %d must match the number of tags %d", size, len(tags))
	}

	b.spans = append(b.spans, spans...)
	b.minTS = timestamps[0]
	b.maxTS = timestamps[0]
	for _, ts := range timestamps {
		if ts < b.minTS {
			b.minTS = ts
		}
		if ts > b.maxTS {
			b.maxTS = ts
		}
	}
	b.mustInitFromTags(tags)
}

func (b *block) mustInitFromTags(tags [][]*tagValue) {
	spansLen := len(tags)
	if spansLen == 0 {
		return
	}

	b.resizeTags(len(tags[0]))
	for i, t := range tags {
		b.processTags(t, i, spansLen)
	}
}

func (b *block) processTags(tags []*tagValue, i, spansLen int) {
	for j, t := range tags {
		b.tags[j].name = t.tag
		b.tags[j].resizeValues(spansLen)
		b.tags[j].valueType = t.valueType
		b.tags[j].values[i] = t.marshal()
	}
}

func (b *block) resizeTags(tagsLen int) {
	tags := b.tags[:0]
	if n := tagsLen - cap(tags); n > 0 {
		tags = append(tags[:cap(tags)], make([]tag, n)...)
	}
	tags = tags[:tagsLen]
	b.tags = tags
}

func (b *block) Len() int {
	return len(b.spans)
}

func (b *block) mustWriteTo(tid string, bm *blockMetadata, ww *writers) {
	b.validate()
	bm.reset()

	bm.traceID = tid
	bm.uncompressedSpanSizeBytes = b.spanSize()
	bm.count = uint64(b.Len())
	bm.timestamps.min = b.minTS
	bm.timestamps.max = b.maxTS

	mustWriteSpansTo(bm.spans, b.spans, &ww.spanWriter)
	for ti := range b.tags {
		b.marshalTag(b.tags[ti], bm, ww)
	}
}

func (b *block) validate() {
	itemsCount := len(b.spans)
	for _, t := range b.tags {
		if len(t.values) != itemsCount {
			logger.Panicf("unexpected number of values for tags %q: got %d; want %d", t.name, len(t.values), itemsCount)
		}
	}
}

func (b *block) marshalTag(t tag, bm *blockMetadata, ww *writers) {
	mw, w := ww.getWriters(t.name)
	cm := generateTagMetadata()
	t.mustWriteTo(cm, w)
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = cm.marshal(bb.Buf)
	releaseTagMetadata(cm)
	tm := bm.getTagMetadata(t.name)
	tm.offset = mw.bytesWritten
	tm.size = uint64(len(bb.Buf))
	if tm.size > maxTagsMetadataSize {
		logger.Panicf("too big tagMetadataSize: %d bytes; mustn't exceed %d bytes", cm.size, maxTagsMetadataSize)
	}
	mw.MustWrite(bb.Buf)
	bm.tagType[t.name] = t.valueType
}

func (b *block) unmarshalTag(decoder *encoding.BytesBlockDecoder, i int,
	tagMetadataBlock *dataBlock, name string, tagType map[string]pbv1.ValueType, metaReader, valueReader fs.Reader,
) {
	bb := bigValuePool.Generate()
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagMetadataBlock.offset), bb.Buf)
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)
	err := tm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagMetadata: %v", metaReader.Path(), err)
	}
	tm.name = name
	bigValuePool.Release(bb)
	b.tags[i].name = name
	if valueType, ok := tagType[name]; ok {
		b.tags[i].valueType = valueType
		tm.valueType = valueType
	} else {
		b.tags[i].valueType = pbv1.ValueTypeUnknown
		for j := range b.tags[i].values {
			b.tags[i].values[j] = nil
		}
		return
	}
	b.tags[i].mustReadValues(decoder, valueReader, *tm, uint64(b.Len()))
}

func (b *block) unmarshalTagFromSeqReaders(decoder *encoding.BytesBlockDecoder, i int, tagMetadataBlock *dataBlock,
	tagType map[string]pbv1.ValueType, metaReader, valueReader *seqReader,
) {
	if tagMetadataBlock.offset != metaReader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", tagMetadataBlock.offset, metaReader.bytesRead)
	}
	bb := bigValuePool.Generate()
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagMetadataBlock.size))
	metaReader.mustReadFull(bb.Buf)
	tm := generateTagMetadata()
	defer releaseTagMetadata(tm)
	err := tm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)

	b.resizeTags(len(b.tags))
	// TODO: avoid sorting the tagType map
	keys := make([]string, 0, len(tagType))
	for k := range tagType {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.tags[i].name = keys[i]
	b.tags[i].valueType = tagType[keys[i]]
	tm.name = keys[i]
	tm.valueType = tagType[keys[i]]
	b.tags[i].mustSeqReadValues(decoder, valueReader, *tm, uint64(b.Len()))
}

func (b *block) spanSize() uint64 {
	n := 0
	for _, s := range b.spans {
		n += len(s)
	}
	return uint64(n)
}

func (b *block) mustReadFrom(decoder *encoding.BytesBlockDecoder, p *part, bm blockMetadata) {
	b.reset()

	b.spans = mustReadSpansFrom(b.spans, bm.spans, int(bm.count), p.spans)

	b.resizeTags(len(bm.tagProjection.Names))
	for i, name := range bm.tagProjection.Names {
		block, ok := bm.tags[name]
		if !ok {
			b.tags[i].name = name
			b.tags[i].valueType = pbv1.ValueTypeUnknown
			b.tags[i].resizeValues(int(bm.count))
			for j := range bm.count {
				b.tags[i].values[j] = nil
			}
			continue
		}
		b.unmarshalTag(decoder, i, block, name, bm.tagType, p.tagMetadata[name], p.tags[name])
	}
}

func (b *block) mustSeqReadFrom(decoder *encoding.BytesBlockDecoder, seqReaders *seqReaders, bm blockMetadata) {
	b.reset()

	b.spans = mustSeqReadSpansFrom(b.spans, bm.spans, int(bm.count), &seqReaders.spans)

	b.resizeTags(len(bm.tags))
	keys := make([]string, 0, len(bm.tags))
	for k := range bm.tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, name := range keys {
		block := bm.tags[name]
		b.unmarshalTagFromSeqReaders(decoder, i, block, bm.tagType, seqReaders.tagMetadata[name], seqReaders.tags[name])
	}
}

// For testing purpose only.
func (b *block) sortTags() {
	sort.Slice(b.tags, func(i, j int) bool {
		return b.tags[i].name < b.tags[j].name
	})
}

func mustWriteSpansTo(sm *dataBlock, spans [][]byte, spanWriter *writer) {
	if len(spans) == 0 {
		return
	}

	sm.reset()
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	sm.offset = spanWriter.bytesWritten
	for _, span := range spans {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(span)))
		bb.Buf = append(bb.Buf, span...)
	}
	sm.size = uint64(len(bb.Buf))

	spanWriter.MustWrite(bb.Buf)
}

func mustReadSpansFrom(spans [][]byte, sm *dataBlock, count int, reader fs.Reader) [][]byte {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(sm.size))
	fs.MustReadData(reader, int64(sm.offset), bb.Buf)

	src := bb.Buf
	spans = resizeSpans(spans, count)
	var spanLen uint64
	for i := 0; i < count; i++ {
		src, spanLen = encoding.BytesToVarUint64(src)
		if uint64(len(src)) < spanLen {
			logger.Panicf("insufficient data for span: need %d bytes, have %d", spanLen, len(src))
		}
		spans[i] = spans[i][:0]
		spans[i] = append(spans[i], src[:spanLen]...)
		src = src[spanLen:]
	}
	return spans
}

func mustSeqReadSpansFrom(spans [][]byte, sm *dataBlock, count int, reader *seqReader) [][]byte {
	if sm.offset != reader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", sm.offset, reader.bytesRead)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(sm.size))
	reader.mustReadFull(bb.Buf)

	src := bb.Buf
	spans = resizeSpans(spans, count)
	var spanLen uint64
	for i := 0; i < count; i++ {
		src, spanLen = encoding.BytesToVarUint64(src)
		if uint64(len(src)) < spanLen {
			logger.Panicf("insufficient data for span: need %d bytes, have %d", spanLen, len(src))
		}
		spans[i] = spans[i][:0]
		spans[i] = append(spans[i], src[:spanLen]...)
		src = src[spanLen:]
	}
	return spans
}

func resizeSpans(spans [][]byte, spansLen int) [][]byte {
	spans = spans[:0]
	if n := spansLen - cap(spans); n > 0 {
		spans = append(spans[:cap(spans)], make([][]byte, n)...)
	}
	spans = spans[:spansLen]
	return spans
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

var blockPool = pool.Register[*block]("trace-block")

type blockCursor struct {
	p                *part
	spans            [][]byte
	tags             []tag
	tagValuesDecoder encoding.BytesBlockDecoder
	tagProjection    *model.TagProjection
	bm               blockMetadata
	idx              int
}

func (bc *blockCursor) reset() {
	bc.idx = 0
	bc.p = nil
	bc.bm.reset()
	bc.tagProjection = nil

	bc.spans = bc.spans[:0]

	for i := range bc.tags {
		bc.tags[i].reset()
	}
	bc.tags = bc.tags[:0]
}

func (bc *blockCursor) init(p *part, bm *blockMetadata, opts queryOptions) {
	bc.reset()
	bc.p = p
	bc.bm.copyFrom(bm)
	bc.tagProjection = opts.TagProjection
}

func (bc *blockCursor) copyAllTo(r *model.TraceResult, desc bool) {
	start, end := 0, bc.idx+1
	if !desc {
		start, end = bc.idx, len(bc.spans)
	}
	if end <= start {
		return
	}

	r.TID = bc.bm.traceID
	r.Spans = append(r.Spans, bc.spans[start:end]...)

	if desc {
		slices.Reverse(r.Spans)
	}

	if len(r.Tags) != len(bc.tagProjection.Names) {
		r.Tags = make([]model.Tag, len(bc.tagProjection.Names))
		for i, name := range bc.tagProjection.Names {
			r.Tags[i] = model.Tag{Name: name}
		}
	}

	for i, t := range bc.tags {
		values := make([]*modelv1.TagValue, end-start)
		for k := start; k < end; k++ {
			if len(t.values) > k {
				values[k-start] = mustDecodeTagValue(t.valueType, t.values[k])
			} else {
				values[k-start] = pbv1.NullTagValue
			}
			if desc {
				slices.Reverse(values)
			}
		}
		r.Tags[i].Values = append(r.Tags[i].Values, values...)
	}
}

func (bc *blockCursor) copyTo(r *model.TraceResult) {
	r.Spans = append(r.Spans, bc.spans[bc.idx])
	r.TID = bc.bm.traceID
	if len(r.Tags) != len(bc.tagProjection.Names) {
		for _, name := range bc.tagProjection.Names {
			r.Tags = append(r.Tags, model.Tag{Name: name})
		}
	}
	if len(bc.tags) != len(r.Tags) {
		logger.Panicf("unexpected number of tags: got %d; want %d", len(bc.tags), len(r.Tags))
	}
	for i, t := range bc.tags {
		if len(t.values) > bc.idx {
			r.Tags[i].Values = append(r.Tags[i].Values, mustDecodeTagValue(t.valueType, t.values[bc.idx]))
		} else {
			r.Tags[i].Values = append(r.Tags[i].Values, pbv1.NullTagValue)
		}
	}
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	bc.bm.tagProjection = bc.tagProjection
	var t map[string]*dataBlock
	for _, name := range bc.tagProjection.Names {
		for tagName, block := range bc.bm.tags {
			if tagName == name {
				if t == nil {
					t = make(map[string]*dataBlock, len(bc.tagProjection.Names))
				}
				t[name] = block
			}
		}
	}
	if len(t) == 0 {
		return false
	}

	bc.bm.tags = t
	tmpBlock.mustReadFrom(&bc.tagValuesDecoder, bc.p, bc.bm)
	if len(tmpBlock.spans) == 0 {
		return false
	}

	bc.spans = append(bc.spans, tmpBlock.spans...)

	for _, tag := range tmpBlock.tags {
		if len(tag.values) == 0 {
			continue
		}
		if len(tag.values) != len(tmpBlock.spans) {
			logger.Panicf("unexpected number of values for tags %q: got %d; want %d",
				tag.name, len(tag.values), len(tmpBlock.spans))
		}
		bc.tags = append(bc.tags, tag)
	}
	return len(bc.spans) > 0
}

var blockCursorPool = pool.Register[*blockCursor]("trace-blockCursor")

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

func (bi *blockPointer) copyFrom(src *blockPointer) {
	bi.idx = 0
	bi.bm.copyFrom(&src.bm)
	bi.appendAll(src)
}

func (bi *blockPointer) appendAll(b *blockPointer) {
	if len(b.spans) == 0 {
		return
	}
	bi.append(b, len(b.spans))
}

var log = logger.GetLogger("trace").Named("block")

func (bi *blockPointer) append(b *blockPointer, offset int) {
	if offset <= b.idx {
		return
	}
	if len(bi.tags) == 0 && len(b.tags) > 0 {
		fullTagAppend(bi, b, offset)
	} else {
		if err := fastTagAppend(bi, b, offset); err != nil {
			if log.Debug().Enabled() {
				log.Debug().Msgf("fastTagMerge failed: %v; falling back to fullTagMerge", err)
			}
			fullTagAppend(bi, b, offset)
		}
	}

	assertIdxAndOffset("spans", len(b.spans), bi.idx, offset)
	bi.spans = append(bi.spans, b.spans[b.idx:offset]...)
}

func fastTagAppend(bi, b *blockPointer, offset int) error {
	if len(bi.tags) != len(b.tags) {
		return fmt.Errorf("unexpected number of tags: got %d; want %d", len(b.tags), len(bi.tags))
	}
	for i := range bi.tags {
		if bi.tags[i].name != b.tags[i].name {
			return fmt.Errorf("unexpected tag name for tag %q: got %q; want %q",
				bi.tags[i].name, b.tags[i].name, bi.tags[i].name)
		}
		assertIdxAndOffset(b.tags[i].name, len(b.tags[i].values), b.idx, offset)
		bi.tags[i].values = append(bi.tags[i].values, b.tags[i].values[b.idx:offset]...)
	}
	return nil
}

func fullTagAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.spans)

	if len(bi.tags) == 0 {
		for _, t := range b.tags {
			newTag := tag{name: t.name, valueType: t.valueType}
			for j := 0; j < existDataSize; j++ {
				newTag.values = append(newTag.values, nil)
			}
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			newTag.values = append(newTag.values, t.values[b.idx:offset]...)
			bi.tags = append(bi.tags, newTag)
		}
		return
	}

	tagMap := make(map[string]*tag)
	for i := range bi.tags {
		tagMap[bi.tags[i].name] = &bi.tags[i]
	}

	for _, t := range b.tags {
		if existingTag, exists := tagMap[t.name]; exists {
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			existingTag.values = append(existingTag.values, t.values[b.idx:offset]...)
		} else {
			newTag := tag{name: t.name, valueType: t.valueType}
			for j := 0; j < existDataSize; j++ {
				newTag.values = append(newTag.values, nil)
			}
			assertIdxAndOffset(t.name, len(t.values), b.idx, offset)
			newTag.values = append(newTag.values, t.values[b.idx:offset]...)
			bi.tags = append(bi.tags, newTag)
		}
	}

	sourceTags := make(map[string]struct{})
	for _, t := range b.tags {
		sourceTags[t.name] = struct{}{}
	}

	emptySize := offset - b.idx
	for i := range bi.tags {
		if _, exists := sourceTags[bi.tags[i].name]; !exists {
			for j := 0; j < emptySize; j++ {
				bi.tags[i].values = append(bi.tags[i].values, nil)
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
	return bi.bm.uncompressedSpanSizeBytes >= maxUncompressedSpanSize
}

func (bi *blockPointer) reset() {
	bi.idx = 0
	bi.block.reset()
	bi.bm.reset()
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

var blockPointerPool = pool.Register[*blockPointer]("trace-blockPointer")
