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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type block struct {
	timestamps []int64

	tagFamilies []ColumnFamily

	field ColumnFamily
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

func (b *block) mustInitFromDataPoints(timestamps []int64, tagFamilies [][]nameValues, fields []nameValues) {
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
	b.mustInitFromTagsAndFields(tagFamilies, fields)
}

func assertTimestampsSorted(timestamps []int64) {
	for i := range timestamps {
		if i > 0 && timestamps[i-1] > timestamps[i] {
			logger.Panicf("log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
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
			columns[j].Name = t.name
			columns[j].resizeValues(dataPointsLen)
			columns[j].ValueType = t.valueType
			columns[j].Values[i] = t.marshal()
		}
	}
}

func (b *block) processTagFamilies(tff []nameValues, i int, dataPointsLen int) {
	tagFamilies := b.resizeTagFamilies(len(tff))
	for j, tf := range tff {
		tagFamilies[j].Name = tf.name
		b.processTags(tf, j, i, dataPointsLen)
	}
}

func (b *block) processTags(tf nameValues, columnFamilyIdx, i int, dataPointsLen int) {
	columns := b.tagFamilies[columnFamilyIdx].resizeColumns(len(tf.values))
	for j, t := range tf.values {
		columns[j].Name = t.name
		columns[j].resizeValues(dataPointsLen)
		columns[j].ValueType = t.valueType
		columns[j].Values[i] = t.marshal()
	}
}

func (b *block) resizeTagFamilies(tagFamiliesLen int) []ColumnFamily {
	tff := b.tagFamilies[:0]
	if n := tagFamiliesLen - cap(tff); n > 0 {
		tff = append(tff[:cap(tff)], make([]ColumnFamily, n)...)
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

	for ti := range b.tagFamilies {
		b.marshalTagFamily(b.tagFamilies[ti], bm, ww)
	}

	f := b.field
	cc := f.Columns
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
		for _, c := range tf.Columns {
			if len(c.Values) != itemsCount {
				logger.Panicf("unexpected number of values for tags %q: got %d; want %d", c.Name, len(c.Values), itemsCount)
			}
		}
	}
	ff := b.field
	for _, f := range ff.Columns {
		if len(f.Values) != itemsCount {
			logger.Panicf("unexpected number of values for fields %q: got %d; want %d", f.Name, len(f.Values), itemsCount)
		}
	}
}

func (b *block) marshalTagFamily(tf ColumnFamily, bm *blockMetadata, ww *writers) {
	hw, w := ww.getColumnMetadataWriterAndColumnWriter(tf.Name)
	cc := tf.Columns
	cfm := generateColumnFamilyMetadata()
	cmm := cfm.resizeColumnMetadata(len(cc))
	for i := range cc {
		cc[i].mustWriteTo(&cmm[i], w)
	}
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = cfm.marshal(bb.Buf)
	releaseColumnFamilyMetadata(cfm)
	tfm := bm.getTagFamilyMetadata(tf.Name)
	tfm.offset = hw.bytesWritten
	tfm.size = uint64(len(bb.Buf))
	if tfm.size > maxTagFamiliesMetadataSize {
		logger.Panicf("too big columnFamilyMetadataSize: %d bytes; mustn't exceed %d bytes", tfm.size, maxTagFamiliesMetadataSize)
	}
	hw.MustWrite(bb.Buf)
}

func (b *block) unmarshalTagFamily(decoder *encoding.BytesBlockDecoder, tfIndex int, name string, columnFamilyMetadataBlock *dataBlock, tagProjection []string, metaReader, valueReader fs.Reader) {
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
	b.tagFamilies[tfIndex].Name = name
	cc := b.tagFamilies[tfIndex].resizeColumns(len(tagProjection))

	for j := range tagProjection {
		for i := range cc {
			if tagProjection[j] == cfm.columnMetadata[i].name {
				cc[i].mustReadValues(decoder, valueReader, cfm.columnMetadata[i], uint64(b.Len()))
			}
		}
	}
}

func (b *block) uncompressedSizeBytes() uint64 {
	dataPointsCount := uint64(b.Len())

	n := dataPointsCount * 8

	tff := b.tagFamilies
	for i := range tff {
		tf := tff[i]
		nameLen := uint64(len(tf.Name))
		for _, c := range tf.Columns {
			nameLen += uint64(len(c.Name))
			for _, v := range c.Values {
				if len(v) > 0 {
					n += nameLen + uint64(len(v))
				}
			}
		}
	}

	ff := b.field
	for i := range ff.Columns {
		c := ff.Columns[i]
		nameLen := uint64(len(c.Name))
		for _, v := range c.Values {
			if len(v) > 0 {
				n += nameLen + uint64(len(v))
			}
		}
	}
	return n
}

func (b *block) mustReadFrom(decoder *encoding.BytesBlockDecoder, p *part, bm blockMetadata) {
	b.reset()

	b.timestamps = mustReadTimestampsFrom(b.timestamps, &bm.timestamps, int(bm.count), p.timestamps)

	_ = b.resizeTagFamilies(len(bm.tagProjection))
	for i := range bm.tagProjection {
		name := bm.tagProjection[i].Family
		block, ok := bm.tagFamilies[name]
		if !ok {
			continue
		}
		b.unmarshalTagFamily(decoder, i, name, block, bm.tagProjection[i].Names, p.tagFamilyMetadata[name], p.tagFamilies[name])
	}
	cc := b.field.resizeColumns(len(bm.field.columnMetadata))
	for i := range cc {
		cc[i].mustReadValues(decoder, p.fieldValues, bm.field.columnMetadata[i], bm.count)
	}
}

// For testing purpose only.
func (b *block) sortTagFamilies() {
	sort.Slice(b.tagFamilies, func(i, j int) bool {
		return b.tagFamilies[i].Name < b.tagFamilies[j].Name
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
	idx int

	timestamps []int64

	tagFamilies []ColumnFamily

	fields ColumnFamily

	columnValuesDecoder encoding.BytesBlockDecoder
	p                   *part
	bm                  blockMetadata
	minTimestamp        int64
	maxTimestamp        int64
	tagProjection       []pbv1.TagProjection
	fieldProjection     []string
}

func (bc *blockCursor) reset() {
	bc.idx = 0
	bc.p = nil
	bc.bm = blockMetadata{}
	bc.minTimestamp = 0
	bc.maxTimestamp = 0
	bc.tagProjection = bc.tagProjection[:0]
	bc.fieldProjection = bc.fieldProjection[:0]

	bc.timestamps = bc.timestamps[:0]

	tff := bc.tagFamilies
	for i := range tff {
		tff[i].reset()
	}
	bc.tagFamilies = tff[:0]
	bc.fields.reset()
}

func (bc *blockCursor) init(p *part, bm blockMetadata, queryOpts QueryOptions) {
	bc.reset()
	bc.p = p
	bc.bm = bm
	bc.minTimestamp = queryOpts.minTimestamp
	bc.maxTimestamp = queryOpts.maxTimestamp
	bc.tagProjection = queryOpts.TagProjection
	bc.fieldProjection = queryOpts.FieldProjection
}

func (bc *blockCursor) copyAllTo(r *pbv1.Result) {
	r.SID = bc.bm.seriesID
	r.Timestamps = bc.timestamps
	for _, cf := range bc.tagFamilies {
		tf := pbv1.TagFamily{
			Name: cf.Name,
		}
		for _, c := range cf.Columns {
			t := pbv1.Tag{
				Name: c.Name,
			}
			for _, v := range c.Values {
				t.Values = append(t.Values, mustDecodeTagValue(c.ValueType, v))
			}
			tf.Tags = append(tf.Tags, t)
		}
		r.TagFamilies = append(r.TagFamilies, tf)
	}
	for _, c := range bc.fields.Columns {
		f := pbv1.Field{
			Name: c.Name,
		}
		for _, v := range c.Values {
			f.Values = append(f.Values, mustDecodeFieldValue(c.ValueType, v))
		}
		r.Fields = append(r.Fields, f)
	}
}

func (bc *blockCursor) copyTo(r *pbv1.Result) {
	r.SID = bc.bm.seriesID
	r.Timestamps = append(r.Timestamps, bc.timestamps[bc.idx])
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
		if len(r.TagFamilies[i].Tags) != len(cf.Columns) {
			logger.Panicf("unexpected number of tags: got %d; want %d", len(r.TagFamilies[i].Tags), len(bc.tagProjection[i].Names))
		}
		for i2, c := range cf.Columns {
			r.TagFamilies[i].Tags[i2].Values = append(r.TagFamilies[i].Tags[i2].Values, mustDecodeTagValue(c.ValueType, c.Values[bc.idx]))
		}
	}

	if len(r.Fields) != len(bc.fieldProjection) {
		for _, n := range bc.fieldProjection {
			f := pbv1.Field{
				Name: n,
			}
			r.Fields = append(r.Fields, f)
		}
	}
	for i, c := range bc.fields.Columns {
		r.Fields[i].Values = append(r.Fields[i].Values, mustDecodeFieldValue(c.ValueType, c.Values[bc.idx]))
	}
}

func (bc *blockCursor) loadData(tmpBlock *block) bool {
	tmpBlock.reset()
	cfm := make([]columnMetadata, 0, len(bc.fieldProjection))
	for j := range bc.fieldProjection {
		for i := range bc.bm.field.columnMetadata {
			if bc.bm.field.columnMetadata[i].name == bc.fieldProjection[j] {
				cfm = append(cfm, bc.bm.field.columnMetadata[i])
			}
		}
	}
	bc.bm.field.columnMetadata = cfm
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
	tmpBlock.mustReadFrom(&bc.columnValuesDecoder, bc.p, bc.bm)

	start, end, ok := findRange(tmpBlock.timestamps, bc.minTimestamp, bc.maxTimestamp)
	if !ok {
		return false
	}
	bc.timestamps = append(bc.timestamps, tmpBlock.timestamps[start:end]...)

	for _, cf := range tmpBlock.tagFamilies {
		tf := ColumnFamily{
			Name: cf.Name,
		}
		for i := range cf.Columns {
			column := Column{
				Name:      cf.Columns[i].Name,
				ValueType: cf.Columns[i].ValueType,
			}
			column.Values = append(column.Values, cf.Columns[i].Values[start:end]...)
			tf.Columns = append(tf.Columns, column)
		}
		bc.tagFamilies = append(bc.tagFamilies, tf)
	}
	bc.fields.Name = tmpBlock.field.Name
	for i := range tmpBlock.field.Columns {
		column := Column{
			Name:      tmpBlock.field.Columns[i].Name,
			ValueType: tmpBlock.field.Columns[i].ValueType,
		}
		column.Values = append(column.Values, tmpBlock.field.Columns[i].Values[start:end]...)
		bc.fields.Columns = append(bc.fields.Columns, column)
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
