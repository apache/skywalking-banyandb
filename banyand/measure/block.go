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
	"fmt"
	"slices"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type block struct {
	timestamps []int64

	versions []int64

	tagFamilies []columnFamily

	field columnFamily
}

func (b *block) reset() {
	b.timestamps = b.timestamps[:0]
	b.versions = b.versions[:0]

	tff := b.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	b.tagFamilies = tff[:0]
	b.field.reset()
}

func (b *block) mustInitFromDataPoints(timestamps []int64, versions []int64, tagFamilies [][]nameValues, fields []nameValues) {
	b.reset()
	size := len(timestamps)
	if size == 0 {
		return
	}
	if size != len(tagFamilies) {
		logger.Panicf("the number of timestamps %d must match the number of tagFamilies %d", size, len(tagFamilies))
	}
	if size != len(fields) {
		logger.Panicf("the number of timestamps %d must match the number of fields %d", size, len(fields))
	}

	assertTimestampsSorted(timestamps)
	b.timestamps = append(b.timestamps, timestamps...)
	b.versions = append(b.versions, versions...)
	b.mustInitFromTagsAndFields(tagFamilies, fields)
}

func assertTimestampsSorted(timestamps []int64) {
	for i := range timestamps {
		if i > 0 && timestamps[i-1] > timestamps[i] {
			logger.Panicf("data points must be sorted by timestamp; got the previous data point with bigger timestamp %d than the current data point with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}
}

func (b *block) mustInitFromTagsAndFields(tagFamilies [][]nameValues, fields []nameValues) {
	dataPointsLen := len(tagFamilies)
	if dataPointsLen == 0 {
		return
	}
	for i, tff := range tagFamilies {
		b.processTagFamilies(tff, i, dataPointsLen)
	}
	for i, f := range fields {
		columns := b.field.resizeColumns(len(f.values))
		for j, t := range f.values {
			columns[j].name = t.name
			columns[j].resizeValues(dataPointsLen)
			columns[j].valueType = t.valueType
			columns[j].values[i] = t.marshal()
		}
	}
}

func (b *block) processTagFamilies(tff []nameValues, i int, dataPointsLen int) {
	tagFamilies := b.resizeTagFamilies(len(tff))
	for j, tf := range tff {
		tagFamilies[j].name = tf.name
		b.processTags(tf, j, i, dataPointsLen)
	}
}

func (b *block) processTags(tf nameValues, columnFamilyIdx, i int, dataPointsLen int) {
	columns := b.tagFamilies[columnFamilyIdx].resizeColumns(len(tf.values))
	for j, t := range tf.values {
		columns[j].name = t.name
		columns[j].resizeValues(dataPointsLen)
		columns[j].valueType = t.valueType
		columns[j].values[i] = t.marshal()
	}
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

func (b *block) mustWriteTo(sid common.SeriesID, bm *blockMetadata, ww *writers) {
	b.validate()
	bm.reset()

	bm.seriesID = sid
	bm.uncompressedSizeBytes = b.uncompressedSizeBytes()
	bm.count = uint64(b.Len())

	mustWriteTimestampsTo(&bm.timestamps, b.timestamps, b.versions, &ww.timestampsWriter)

	for ti := range b.tagFamilies {
		b.marshalTagFamily(b.tagFamilies[ti], bm, ww)
	}

	f := b.field
	cc := f.columns
	cmm := bm.field.resizeColumnMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&cmm[i], &ww.fieldValuesWriter)
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

func (b *block) marshalTagFamily(tf columnFamily, bm *blockMetadata, ww *writers) {
	hw, w := ww.getColumnMetadataWriterAndColumnWriter(tf.name)
	cc := tf.columns
	cfm := generateColumnFamilyMetadata()
	cmm := cfm.resizeColumnMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&cmm[i], w)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = cfm.marshal(bb.Buf)
	releaseColumnFamilyMetadata(cfm)
	tfm := bm.getTagFamilyMetadata(tf.name)
	tfm.offset = hw.bytesWritten
	tfm.size = uint64(len(bb.Buf))
	if tfm.size > maxTagFamiliesMetadataSize {
		logger.Panicf("too big columnFamilyMetadataSize: %d bytes; mustn't exceed %d bytes", tfm.size, maxTagFamiliesMetadataSize)
	}
	hw.MustWrite(bb.Buf)
}

func (b *block) unmarshalTagFamily(decoder *encoding.BytesBlockDecoder, tfIndex int, name string,
	columnFamilyMetadataBlock *dataBlock, tagProjection []string, metaReader, valueReader fs.Reader, count int,
) {
	if len(tagProjection) < 1 {
		return
	}
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(columnFamilyMetadataBlock.size))
	fs.MustReadData(metaReader, int64(columnFamilyMetadataBlock.offset), bb.Buf)
	cfm := generateColumnFamilyMetadata()
	defer releaseColumnFamilyMetadata(cfm)
	_, err := cfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal columnFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	b.tagFamilies[tfIndex].name = name
	cc := b.tagFamilies[tfIndex].resizeColumns(len(tagProjection))
NEXT:
	for j := range tagProjection {
		for i := range cfm.columnMetadata {
			if tagProjection[j] == cfm.columnMetadata[i].name {
				cc[j].mustReadValues(decoder, valueReader, cfm.columnMetadata[i], uint64(b.Len()))
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
	columnFamilyMetadataBlock *dataBlock, metaReader, valueReader *seqReader,
) {
	if columnFamilyMetadataBlock.offset != metaReader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", columnFamilyMetadataBlock.offset, metaReader.bytesRead)
	}
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(columnFamilyMetadataBlock.size))
	metaReader.mustReadFull(bb.Buf)
	cfm := generateColumnFamilyMetadata()
	defer releaseColumnFamilyMetadata(cfm)
	_, err := cfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal columnFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	b.tagFamilies[tfIndex].name = name

	cc := b.tagFamilies[tfIndex].resizeColumns(len(cfm.columnMetadata))
	for i := range cfm.columnMetadata {
		cc[i].mustSeqReadValues(decoder, valueReader, cfm.columnMetadata[i], uint64(b.Len()))
	}
}

func (b *block) uncompressedSizeBytes() uint64 {
	dataPointsCount := uint64(b.Len())

	n := dataPointsCount * (8 + 8) // 8 bytes for timestamp and 8 bytes for version

	tff := b.tagFamilies
	for i := range tff {
		tf := tff[i]
		n += uint64(len(tf.name))
		for _, c := range tf.columns {
			n += uint64(len(c.name))
			for _, v := range c.values {
				if len(v) > 0 {
					n += uint64(len(v))
				}
			}
		}
	}

	ff := b.field
	for i := range ff.columns {
		c := ff.columns[i]
		nameLen := uint64(len(c.name))
		for _, v := range c.values {
			if len(v) > 0 {
				n += nameLen + uint64(len(v))
			}
		}
	}
	return n
}

func (b *block) mustReadFrom(decoder *encoding.BytesBlockDecoder, p *part, bm blockMetadata) {
	b.reset()

	b.timestamps, b.versions = mustReadTimestampsFrom(b.timestamps, b.versions, &bm.timestamps, int(bm.count), p.timestamps)

	cc := b.field.resizeColumns(len(bm.field.columnMetadata))
	for i := range cc {
		cc[i].mustReadValues(decoder, p.fieldValues, bm.field.columnMetadata[i], bm.count)
	}

	_ = b.resizeTagFamilies(len(bm.tagProjection))
	for i := range bm.tagProjection {
		name := bm.tagProjection[i].Family
		block, ok := bm.tagFamilies[name]
		if !ok {
			b.tagFamilies[i].name = name
			b.tagFamilies[i].resizeColumns(len(bm.tagProjection[i].Names))
			for j := range bm.tagProjection[i].Names {
				b.tagFamilies[i].columns[j].name = bm.tagProjection[i].Names[j]
				b.tagFamilies[i].columns[j].valueType = pbv1.ValueTypeUnknown
				b.tagFamilies[i].columns[j].resizeValues(int(bm.count))
				for k := range bm.count {
					b.tagFamilies[i].columns[j].values[k] = nil
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

	b.timestamps, b.versions = mustSeqReadTimestampsFrom(b.timestamps, b.versions, &bm.timestamps, int(bm.count), &seqReaders.timestamps)

	cc := b.field.resizeColumns(len(bm.field.columnMetadata))
	for i := range cc {
		cc[i].mustSeqReadValues(decoder, &seqReaders.fieldValues, bm.field.columnMetadata[i], bm.count)
	}
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

func mustWriteTimestampsTo(tm *timestampsMetadata, timestamps, versions []int64, timestampsWriter *writer) {
	tm.reset()

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf, tm.encodeType, tm.min = encoding.Int64ListToBytes(bb.Buf[:0], timestamps)
	tm.encodeType = encoding.GetVersionType(tm.encodeType)
	if tm.encodeType == encoding.EncodeTypeUnknown {
		logger.Panicf("unexpected encodeType %d", tm.encodeType)
		return
	}
	tm.max = timestamps[len(timestamps)-1]
	tm.offset = timestampsWriter.bytesWritten
	tm.versionOffset = uint64(len(bb.Buf))
	timestampsWriter.MustWrite(bb.Buf)
	bb.Buf, tm.versionEncodeType, tm.versionFirst = encoding.Int64ListToBytes(bb.Buf[:0], versions)
	tm.size = tm.versionOffset + uint64(len(bb.Buf))
	timestampsWriter.MustWrite(bb.Buf)
}

func mustReadTimestampsFrom(timestamps, versions []int64, tm *timestampsMetadata, count int, reader fs.Reader) ([]int64, []int64) {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
	fs.MustReadData(reader, int64(tm.offset), bb.Buf)
	return mustDecodeTimestampsWithVersions(timestamps, versions, tm, count, reader.Path(), bb.Buf)
}

func mustSeqReadTimestampsFrom(timestamps, versions []int64, tm *timestampsMetadata, count int, reader *seqReader) ([]int64, []int64) {
	if tm.offset != reader.bytesRead {
		logger.Panicf("offset %d must be equal to bytesRead %d", tm.offset, reader.bytesRead)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tm.size))
	reader.mustReadFull(bb.Buf)
	return mustDecodeTimestampsWithVersions(timestamps, versions, tm, count, reader.Path(), bb.Buf)
}

func mustDecodeTimestampsWithVersions(timestamps, versions []int64, tm *timestampsMetadata, count int, path string, src []byte) ([]int64, []int64) {
	var err error
	t := encoding.GetCommonType(tm.encodeType)
	if t == encoding.EncodeTypeUnknown {
		logger.Panicf("unexpected encodeType %d", tm.encodeType)
	}
	if tm.size < tm.versionOffset {
		logger.Panicf("size %d must be greater than versionOffset %d", tm.size, tm.versionOffset)
	}
	timestamps, err = encoding.BytesToInt64List(timestamps, src[:tm.versionOffset], t, tm.min, count)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal timestamps with versions: %v", path, err)
	}
	versions, err = encoding.BytesToInt64List(versions, src[tm.versionOffset:], tm.versionEncodeType, tm.versionFirst, count)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal versions: %v", path, err)
	}
	return timestamps, versions
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

var blockPool = pool.Register[*block]("measure-block")

type blockCursor struct {
	p                   *part
	fields              columnFamily
	timestamps          []int64
	versions            []int64
	tagFamilies         []columnFamily
	columnValuesDecoder encoding.BytesBlockDecoder
	tagProjection       []model.TagProjection
	fieldProjection     []string
	schemaTagTypes      map[string]pbv1.ValueType
	bm                  blockMetadata
	idx                 int
	minTimestamp        int64
	maxTimestamp        int64
	shardID             common.ShardID
}

func (bc *blockCursor) reset() {
	bc.idx = 0
	bc.p = nil
	bc.bm.reset()
	bc.minTimestamp = 0
	bc.maxTimestamp = 0
	bc.tagProjection = bc.tagProjection[:0]
	bc.fieldProjection = bc.fieldProjection[:0]
	bc.schemaTagTypes = nil

	bc.timestamps = bc.timestamps[:0]
	bc.versions = bc.versions[:0]

	tff := bc.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	bc.tagFamilies = tff[:0]
	bc.fields.reset()
}

func (bc *blockCursor) init(p *part, bm *blockMetadata, queryOpts queryOptions) {
	bc.reset()
	bc.p = p
	bc.bm.copyFrom(bm)
	bc.minTimestamp = queryOpts.minTimestamp
	bc.maxTimestamp = queryOpts.maxTimestamp
	bc.tagProjection = queryOpts.TagProjection
	bc.fieldProjection = queryOpts.FieldProjection
	bc.schemaTagTypes = queryOpts.schemaTagTypes
}

func (bc *blockCursor) copyAllTo(r *model.MeasureResult, storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	tagProjection []model.TagProjection, desc bool,
) {
	var idx, offset int
	if desc {
		idx = 0
		offset = bc.idx + 1
	} else {
		idx = bc.idx
		offset = len(bc.timestamps)
	}
	if offset <= idx {
		return
	}
	size := offset - idx
	r.SID = bc.bm.seriesID
	r.Timestamps = append(r.Timestamps, bc.timestamps[idx:offset]...)
	r.Versions = append(r.Versions, bc.versions[idx:offset]...)
	for i := 0; i < size; i++ {
		r.ShardIDs = append(r.ShardIDs, bc.shardID)
	}
	if desc {
		slices.Reverse(r.Timestamps)
		slices.Reverse(r.Versions)
	}
	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[r.SID]
	}
OUTER:
	for _, tp := range tagProjection {
		tf := model.TagFamily{
			Name: tp.Family,
		}
		var cf *columnFamily
		for _, tagName := range tp.Names {
			t := model.Tag{
				Name: tagName,
			}
			if indexValue != nil && indexValue[tagName] != nil {
				t.Values = make([]*modelv1.TagValue, size)
				for i := 0; i < size; i++ {
					t.Values[i] = indexValue[tagName]
				}
				tf.Tags = append(tf.Tags, t)
				continue
			}
			if cf == nil {
				for i := range bc.tagFamilies {
					if bc.tagFamilies[i].name == tp.Family {
						cf = &bc.tagFamilies[i]
						break
					}
				}
			}
			if cf == nil {
				for _, n := range tp.Names {
					t = model.Tag{
						Name:   n,
						Values: make([]*modelv1.TagValue, size),
					}
					for i := 0; i < size; i++ {
						t.Values[i] = pbv1.NullTagValue
					}
					tf.Tags = append(tf.Tags, t)
				}
				r.TagFamilies = append(r.TagFamilies, tf)
				continue OUTER
			}
			var foundTag bool
			for i := range cf.columns {
				if cf.columns[i].name == tagName {
					schemaType, hasSchemaType := bc.schemaTagTypes[tagName]
					if hasSchemaType && cf.columns[i].valueType == schemaType {
						for _, v := range cf.columns[i].values[idx:offset] {
							t.Values = append(t.Values, mustDecodeTagValue(cf.columns[i].valueType, v))
						}
					} else {
						t.Values = make([]*modelv1.TagValue, len(cf.columns[i].values[idx:offset]))
						for j := range t.Values {
							t.Values[j] = pbv1.NullTagValue
						}
					}
					foundTag = true
					break
				}
			}
			if !foundTag {
				t.Values = make([]*modelv1.TagValue, size)
				for i := 0; i < size; i++ {
					t.Values[i] = pbv1.NullTagValue
				}
			} else if desc {
				slices.Reverse(t.Values)
			}
			tf.Tags = append(tf.Tags, t)
		}
		r.TagFamilies = append(r.TagFamilies, tf)
	}
	for _, c := range bc.fields.columns {
		f := model.Field{
			Name: c.name,
		}
		for _, v := range c.values[idx:offset] {
			f.Values = append(f.Values, mustDecodeFieldValue(c.valueType, v))
		}
		if desc {
			slices.Reverse(f.Values)
		}
		r.Fields = append(r.Fields, f)
	}
}

func (bc *blockCursor) copyTo(r *model.MeasureResult, storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	tagProjection []model.TagProjection,
) {
	r.SID = bc.bm.seriesID
	r.Timestamps = append(r.Timestamps, bc.timestamps[bc.idx])
	r.Versions = append(r.Versions, bc.versions[bc.idx])
	r.ShardIDs = append(r.ShardIDs, bc.shardID)
	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[r.SID]
	}
	if len(r.TagFamilies) == 0 {
		for _, tp := range tagProjection {
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
	for i := range r.TagFamilies {
		tfName := r.TagFamilies[i].Name
		var cf *columnFamily
		for j := range r.TagFamilies[i].Tags {
			tagName := r.TagFamilies[i].Tags[j].Name
			if indexValue != nil && indexValue[tagName] != nil {
				r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, indexValue[tagName])
				continue
			}
			if cf == nil {
				for i := range bc.tagFamilies {
					if bc.tagFamilies[i].name == tfName {
						cf = &bc.tagFamilies[i]
						break
					}
				}
			}
			if cf == nil {
				r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, pbv1.NullTagValue)
				continue
			}
			var foundTag bool
			for _, c := range cf.columns {
				if c.name == tagName {
					schemaType, hasSchemaType := bc.schemaTagTypes[tagName]
					if hasSchemaType && c.valueType == schemaType {
						r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, mustDecodeTagValue(c.valueType, c.values[bc.idx]))
					} else {
						r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, pbv1.NullTagValue)
					}
					foundTag = true
					break
				}
			}
			if !foundTag {
				r.TagFamilies[i].Tags[j].Values = append(r.TagFamilies[i].Tags[j].Values, pbv1.NullTagValue)
			}
		}
	}

	if len(r.Fields) == 0 {
		for _, n := range bc.fieldProjection {
			f := model.Field{
				Name: n,
			}
			r.Fields = append(r.Fields, f)
		}
	}
	for i, c := range bc.fields.columns {
		r.Fields[i].Values = append(r.Fields[i].Values, mustDecodeFieldValue(c.valueType, c.values[bc.idx]))
	}
}

func (bc *blockCursor) mergeTopNResult(r *model.MeasureResult, storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue,
	topNPostAggregator PostProcessor,
) {
	r.SID = bc.bm.seriesID
	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[r.SID]
	}
	for i := range r.TagFamilies {
		tfName := r.TagFamilies[i].Name
		var cf *columnFamily
		for j := range r.TagFamilies[i].Tags {
			tagName := r.TagFamilies[i].Tags[j].Name
			if indexValue != nil && indexValue[tagName] != nil {
				r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = indexValue[tagName]
				continue
			}
			if cf == nil {
				for i := range bc.tagFamilies {
					if bc.tagFamilies[i].name == tfName {
						cf = &bc.tagFamilies[i]
						break
					}
				}
			}
			for _, c := range cf.columns {
				if c.name == tagName {
					schemaType, hasSchemaType := bc.schemaTagTypes[tagName]
					if hasSchemaType && c.valueType == schemaType {
						r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = mustDecodeTagValue(c.valueType, c.values[bc.idx])
					} else {
						r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = pbv1.NullTagValue
					}
					break
				}
			}
		}
	}

	topNValue := GenerateTopNValue()
	defer ReleaseTopNValue(topNValue)
	decoder := GenerateTopNValuesDecoder()
	defer ReleaseTopNValuesDecoder(decoder)

	uTimestamps := uint64(bc.timestamps[bc.idx])

	for i, c := range bc.fields.columns {
		srcFieldValue := r.Fields[i].Values[len(r.Fields[i].Values)-1]
		destFieldValue := mustDecodeFieldValue(c.valueType, c.values[bc.idx])

		topNValue.Reset()

		if err := topNValue.Unmarshal(srcFieldValue.GetBinaryData(), decoder); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal topN value, skip current batch")
			continue
		}

		valueName := topNValue.valueName
		entityTagNames := topNValue.entityTagNames

		for j, entityList := range topNValue.entities {
			entityValues := make(pbv1.EntityValues, 0, len(entityList))
			for _, e := range entityList {
				entityValues = append(entityValues, e)
			}
			topNPostAggregator.Put(entityValues, topNValue.values[j], uTimestamps, r.Versions[len(r.Versions)-1])
		}

		topNValue.Reset()
		if err := topNValue.Unmarshal(destFieldValue.GetBinaryData(), decoder); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal topN value, skip current batch")
			continue
		}

		for j, entityList := range topNValue.entities {
			entityValues := make(pbv1.EntityValues, 0, len(entityList))
			for _, e := range entityList {
				entityValues = append(entityValues, e)
			}
			topNPostAggregator.Put(entityValues, topNValue.values[j], uTimestamps, bc.versions[bc.idx])
		}

		items, err := topNPostAggregator.Flush()
		if err != nil {
			log.Error().Err(err).Msg("failed to flush aggregator, skip current batch")
			continue
		}

		topNValue.Reset()
		topNValue.setMetadata(valueName, entityTagNames)

		for _, item := range items {
			topNValue.addValue(item.val, item.values)
		}

		buf, err := topNValue.marshal(make([]byte, 0, 128))
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal topN value, skip current batch")
			continue
		}

		r.Fields[i].Values[len(r.Fields[i].Values)-1] = &modelv1.FieldValue{
			Value: &modelv1.FieldValue_BinaryData{
				BinaryData: buf,
			},
		}
		r.Versions[len(r.Versions)-1] = bc.versions[bc.idx]
	}
}

func (bc *blockCursor) replace(r *model.MeasureResult, storedIndexValue map[common.SeriesID]map[string]*modelv1.TagValue) {
	r.SID = bc.bm.seriesID
	r.Versions[len(r.Versions)-1] = bc.versions[bc.idx]
	var indexValue map[string]*modelv1.TagValue
	if storedIndexValue != nil {
		indexValue = storedIndexValue[r.SID]
	}
	for i := range r.TagFamilies {
		tfName := r.TagFamilies[i].Name
		var cf *columnFamily
		for j := range r.TagFamilies[i].Tags {
			tagName := r.TagFamilies[i].Tags[j].Name
			if indexValue != nil && indexValue[tagName] != nil {
				r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = indexValue[tagName]
				continue
			}
			if cf == nil {
				for i := range bc.tagFamilies {
					if bc.tagFamilies[i].name == tfName {
						cf = &bc.tagFamilies[i]
						break
					}
				}
			}
			for _, c := range cf.columns {
				if c.name == tagName {
					schemaType, hasSchemaType := bc.schemaTagTypes[tagName]
					if hasSchemaType && c.valueType == schemaType {
						r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = mustDecodeTagValue(c.valueType, c.values[bc.idx])
					} else {
						r.TagFamilies[i].Tags[j].Values[len(r.TagFamilies[i].Tags[j].Values)-1] = pbv1.NullTagValue
					}
					break
				}
			}
		}
	}
	for i, c := range bc.fields.columns {
		r.Fields[i].Values[len(r.Fields[i].Values)-1] = mustDecodeFieldValue(c.valueType, c.values[bc.idx])
	}
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	cfm := make([]columnMetadata, 0, len(bc.fieldProjection))
NEXT_FIELD:
	for _, fp := range bc.fieldProjection {
		for _, cm := range bc.bm.field.columnMetadata {
			if cm.name == fp {
				cfm = append(cfm, cm)
				continue NEXT_FIELD
			}
		}
		cfm = append(cfm, columnMetadata{
			name:      fp,
			valueType: pbv1.ValueTypeUnknown,
		})
	}
	bc.bm.field.columnMetadata = cfm
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
	bc.bm.tagFamilies = tf
	tmpBlock.mustReadFrom(&bc.columnValuesDecoder, bc.p, bc.bm)

	start, end, ok := timestamp.FindRange(tmpBlock.timestamps, bc.minTimestamp, bc.maxTimestamp)
	if !ok {
		return false
	}
	bc.timestamps = append(bc.timestamps, tmpBlock.timestamps[start:end+1]...)
	bc.versions = append(bc.versions, tmpBlock.versions[start:end+1]...)

	for _, cf := range tmpBlock.tagFamilies {
		tf := columnFamily{
			name: cf.name,
		}
		for i := range cf.columns {
			column := column{
				name:      cf.columns[i].name,
				valueType: cf.columns[i].valueType,
			}
			if len(cf.columns[i].values) == 0 {
				continue
			}
			if len(cf.columns[i].values) != len(tmpBlock.timestamps) {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", cf.columns[i].name, len(cf.columns[i].values), len(tmpBlock.timestamps))
			}
			column.values = append(column.values, cf.columns[i].values[start:end+1]...)
			tf.columns = append(tf.columns, column)
		}
		bc.tagFamilies = append(bc.tagFamilies, tf)
	}
	bc.fields.name = tmpBlock.field.name
	for i := range tmpBlock.field.columns {
		if len(tmpBlock.field.columns[i].values) == 0 {
			continue
		}
		if len(tmpBlock.field.columns[i].values) != len(tmpBlock.timestamps) {
			logger.Panicf("unexpected number of values for fields %q: got %d; want %d",
				tmpBlock.field.columns[i].name, len(tmpBlock.field.columns[i].values), len(tmpBlock.timestamps))
		}
		c := column{
			name:      tmpBlock.field.columns[i].name,
			valueType: tmpBlock.field.columns[i].valueType,
		}

		c.values = append(c.values, tmpBlock.field.columns[i].values[start:end+1]...)
		bc.fields.columns = append(bc.fields.columns, c)
	}
	return true
}

var blockCursorPool = pool.Register[*blockCursor]("measure-blockCursor")

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
	bi.reset()
	bi.bm.copyFrom(&src.bm)
	bi.appendAll(src)
}

func (bi *blockPointer) appendAll(b *blockPointer) {
	if len(b.timestamps) == 0 {
		return
	}
	bi.append(b, len(b.timestamps))
}

var log = logger.GetLogger("measure").Named("block")

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

	if len(bi.field.columns) == 0 && len(b.field.columns) > 0 {
		fullFieldAppend(bi, b, offset)
	} else {
		if err := fastFieldAppend(bi, b, offset); err != nil {
			if log.Debug().Enabled() {
				log.Debug().Msgf("fastFieldAppend failed: %v; falling back to fullFieldAppend", err)
			}
			fullFieldAppend(bi, b, offset)
		}
	}

	assertIdxAndOffset("timestamps", len(b.timestamps), bi.idx, offset)
	bi.timestamps = append(bi.timestamps, b.timestamps[b.idx:offset]...)
	assertIdxAndOffset("versions", len(b.versions), bi.idx, offset)
	bi.versions = append(bi.versions, b.versions[b.idx:offset]...)
}

func fastTagAppend(bi, b *blockPointer, offset int) error {
	if len(bi.tagFamilies) != len(b.tagFamilies) {
		return fmt.Errorf("unexpected number of tag families: got %d; want %d", len(b.tagFamilies), len(bi.tagFamilies))
	}
	for i := range bi.tagFamilies {
		if bi.tagFamilies[i].name != b.tagFamilies[i].name {
			return fmt.Errorf("unexpected tag family name: got %q; want %q", b.tagFamilies[i].name, bi.tagFamilies[i].name)
		}
		if len(bi.tagFamilies[i].columns) != len(b.tagFamilies[i].columns) {
			return fmt.Errorf("unexpected number of tags for tag family %q: got %d; want %d",
				bi.tagFamilies[i].name, len(b.tagFamilies[i].columns), len(bi.tagFamilies[i].columns))
		}
		for j := range bi.tagFamilies[i].columns {
			if bi.tagFamilies[i].columns[j].name != b.tagFamilies[i].columns[j].name {
				return fmt.Errorf("unexpected tag name for tag family %q: got %q; want %q",
					bi.tagFamilies[i].name, b.tagFamilies[i].columns[j].name, bi.tagFamilies[i].columns[j].name)
			}
			assertIdxAndOffset(b.tagFamilies[i].columns[j].name, len(b.tagFamilies[i].columns[j].values), b.idx, offset)
			bi.tagFamilies[i].columns[j].values = append(bi.tagFamilies[i].columns[j].values, b.tagFamilies[i].columns[j].values[b.idx:offset]...)
		}
	}
	return nil
}

func fullTagAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.timestamps)
	appendTagFamilies := func(tf columnFamily) {
		tagFamily := columnFamily{name: tf.name}
		for i := range tf.columns {
			assertIdxAndOffset(tf.columns[i].name, len(tf.columns[i].values), b.idx, offset)
			col := column{name: tf.columns[i].name, valueType: tf.columns[i].valueType}
			for j := 0; j < existDataSize; j++ {
				col.values = append(col.values, nil)
			}
			col.values = append(col.values, tf.columns[i].values[b.idx:offset]...)
			tagFamily.columns = append(tagFamily.columns, col)
		}
		bi.tagFamilies = append(bi.tagFamilies, tagFamily)
	}
	if len(bi.tagFamilies) == 0 {
		for _, tf := range b.tagFamilies {
			appendTagFamilies(tf)
		}
		return
	}

	tagFamilyMap := make(map[string]*columnFamily)
	for i := range bi.tagFamilies {
		tagFamilyMap[bi.tagFamilies[i].name] = &bi.tagFamilies[i]
	}

	for _, tf := range b.tagFamilies {
		if existingTagFamily, exists := tagFamilyMap[tf.name]; exists {
			columnMap := make(map[string]*column)
			for i := range existingTagFamily.columns {
				columnMap[existingTagFamily.columns[i].name] = &existingTagFamily.columns[i]
			}

			for _, c := range tf.columns {
				if existingColumn, exists := columnMap[c.name]; exists {
					assertIdxAndOffset(c.name, len(c.values), b.idx, offset)
					existingColumn.values = append(existingColumn.values, c.values[b.idx:offset]...)
				} else {
					assertIdxAndOffset(c.name, len(c.values), b.idx, offset)
					col := column{name: c.name, valueType: c.valueType}
					for j := 0; j < existDataSize; j++ {
						col.values = append(col.values, nil)
					}
					col.values = append(col.values, c.values[b.idx:offset]...)
					existingTagFamily.columns = append(existingTagFamily.columns, col)
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
			for i := range tf.columns {
				for j := 0; j < emptySize; j++ {
					tf.columns[i].values = append(tf.columns[i].values, nil)
				}
			}
		} else {
			existingTagFamily := tagFamilyMap[tf.name]
			columnMap := make(map[string]*column)
			for i := range existingTagFamily.columns {
				columnMap[existingTagFamily.columns[i].name] = &existingTagFamily.columns[i]
			}
			for i := range tf.columns {
				if _, exists := columnMap[tf.columns[i].name]; !exists {
					for j := 0; j < emptySize; j++ {
						tf.columns[i].values = append(tf.columns[i].values, nil)
					}
				}
			}
		}
	}
}

func fastFieldAppend(bi, b *blockPointer, offset int) error {
	if len(bi.field.columns) != len(b.field.columns) {
		return fmt.Errorf("unexpected number of fields: got %d; want %d", len(bi.field.columns), len(b.field.columns))
	}
	for i := range bi.field.columns {
		if bi.field.columns[i].name != b.field.columns[i].name {
			return fmt.Errorf("unexpected field name: got %q; want %q", b.field.columns[i].name, bi.field.columns[i].name)
		}
		assertIdxAndOffset(b.field.columns[i].name, len(b.field.columns[i].values), b.idx, offset)
		bi.field.columns[i].values = append(bi.field.columns[i].values, b.field.columns[i].values[b.idx:offset]...)
	}
	return nil
}

func fullFieldAppend(bi, b *blockPointer, offset int) {
	existDataSize := len(bi.timestamps)
	appendFields := func(c column) {
		col := column{name: c.name, valueType: c.valueType}
		for j := 0; j < existDataSize; j++ {
			col.values = append(col.values, nil)
		}
		col.values = append(col.values, c.values[b.idx:offset]...)
		bi.field.columns = append(bi.field.columns, col)
	}
	if len(bi.field.columns) == 0 {
		for _, c := range b.field.columns {
			appendFields(c)
		}
		return
	}

	fieldMap := make(map[string]*column)
	for i := range bi.field.columns {
		fieldMap[bi.field.columns[i].name] = &bi.field.columns[i]
	}

	for _, c := range b.field.columns {
		if existingField, exists := fieldMap[c.name]; exists {
			assertIdxAndOffset(c.name, len(c.values), b.idx, offset)
			existingField.values = append(existingField.values, c.values[b.idx:offset]...)
		} else {
			appendFields(c)
		}
	}
	for k := range fieldMap {
		delete(fieldMap, k)
	}
	for i := range b.field.columns {
		fieldMap[b.field.columns[i].name] = &b.field.columns[i]
	}

	emptySize := offset - b.idx
	for i := range bi.field.columns {
		if _, exists := fieldMap[bi.field.columns[i].name]; !exists {
			for j := 0; j < emptySize; j++ {
				bi.field.columns[i].values = append(bi.field.columns[i].values, nil)
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
	return bi.bm.count >= maxBlockLength || bi.bm.uncompressedSizeBytes >= maxUncompressedBlockSize
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

var blockPointerPool = pool.Register[*blockPointer]("measure-blockPointer")
