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
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type writer struct {
	w            fs.Writer
	bytesWritten uint64
}

func (w *writer) reset() {
	w.w = nil
	w.bytesWritten = 0
}

func (w *writer) init(wc fs.Writer) {
	w.reset()

	w.w = wc
}

func (w *writer) MustWrite(data []byte) {
	fs.MustWriteData(w.w, data)
	w.bytesWritten += uint64(len(data))
}

func (w *writer) MustClose() {
	fs.MustClose(w.w)
	w.reset()
}

type mustCreateTagFamilyWriters func(name string) (fs.Writer, fs.Writer)

type writers struct {
	mustCreateTagFamilyWriters mustCreateTagFamilyWriters
	metaWriter                 writer
	primaryWriter              writer
	tagFamilyMetadataWriters   map[string]*writer
	tagFamilyWriters           map[string]*writer
	timestampsWriter           writer
	fieldValuesWriter          writer
}

func (sw *writers) reset() {
	sw.mustCreateTagFamilyWriters = nil
	sw.metaWriter.reset()
	sw.primaryWriter.reset()
	sw.timestampsWriter.reset()
	sw.fieldValuesWriter.reset()

	for i, w := range sw.tagFamilyMetadataWriters {
		w.reset()
		delete(sw.tagFamilyMetadataWriters, i)
	}
	for i, w := range sw.tagFamilyWriters {
		w.reset()
		delete(sw.tagFamilyWriters, i)
	}
}

func (sw *writers) totalBytesWritten() uint64 {
	n := sw.metaWriter.bytesWritten + sw.primaryWriter.bytesWritten +
		sw.timestampsWriter.bytesWritten + sw.fieldValuesWriter.bytesWritten
	for _, w := range sw.tagFamilyMetadataWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagFamilyWriters {
		n += w.bytesWritten
	}
	return n
}

func (sw *writers) MustClose() {
	sw.metaWriter.MustClose()
	sw.primaryWriter.MustClose()
	sw.timestampsWriter.MustClose()
	sw.fieldValuesWriter.MustClose()

	for _, w := range sw.tagFamilyMetadataWriters {
		w.MustClose()
	}
	for _, w := range sw.tagFamilyWriters {
		w.MustClose()
	}
}

func (sw *writers) getColumnMetadataWriterAndColumnWriter(columnName string) (*writer, *writer) {
	chw, ok := sw.tagFamilyMetadataWriters[columnName]
	cw := sw.tagFamilyWriters[columnName]
	if ok {
		return chw, cw
	}
	hw, w := sw.mustCreateTagFamilyWriters(columnName)
	chw = &writer{
		w: hw,
	}
	cw = &writer{
		w: w,
	}
	sw.tagFamilyMetadataWriters[columnName] = chw
	sw.tagFamilyWriters[columnName] = cw
	return chw, cw
}

type blockWriter struct {
	streamWriters writers

	sidLast common.SeriesID

	sidFirst common.SeriesID

	minTimestampLast int64

	minTimestamp int64

	maxTimestamp int64

	hasWrittenBlocks bool

	totalUncompressedSizeBytes uint64

	totalCount uint64

	totalBlocksCount uint64

	totalMinTimestamp int64

	totalMaxTimestamp int64

	primaryBlockData []byte

	metaData []byte

	primaryBlockMetadata primaryBlockMetadata
}

func (bw *blockWriter) reset() {
	bw.streamWriters.reset()
	bw.sidLast = 0
	bw.sidFirst = 0
	bw.minTimestampLast = 0
	bw.minTimestamp = 0
	bw.maxTimestamp = 0
	bw.hasWrittenBlocks = false
	bw.totalUncompressedSizeBytes = 0
	bw.totalCount = 0
	bw.totalBlocksCount = 0
	bw.totalMinTimestamp = 0
	bw.totalMaxTimestamp = 0
	bw.primaryBlockData = bw.primaryBlockData[:0]
	bw.metaData = bw.metaData[:0]
	bw.primaryBlockMetadata.reset()
}

func (bsw *blockWriter) MustInitForMemPart(mp *memPart) {
	bsw.reset()
	bsw.streamWriters = writers{
		mustCreateTagFamilyWriters: mp.mustCreateMemTagFamilyWriters,
	}
	bsw.streamWriters.metaWriter.init(&mp.meta)
	bsw.streamWriters.primaryWriter.init(&mp.primary)
	bsw.streamWriters.timestampsWriter.init(&mp.timestamps)
	bsw.streamWriters.fieldValuesWriter.init(&mp.fieldValues)
}

func (bsw *blockWriter) MustWriteDataPoints(sid common.SeriesID, timestamps []int64, tagFamilies [][]nameValues, fields []nameValues) {
	if len(timestamps) == 0 {
		return
	}

	b := generateBlock()
	defer releaseBlock(b)
	b.mustInitFromDataPoints(timestamps, tagFamilies, fields)
	if b.Len() == 0 {
		return
	}
	if sid < bsw.sidLast {
		logger.Panicf("the sid=%d cannot be smaller than the previously written sid=%d", sid, &bsw.sidLast)
	}
	hasWrittenBlocks := bsw.hasWrittenBlocks
	if !hasWrittenBlocks {
		bsw.sidFirst = sid
		bsw.hasWrittenBlocks = true
	}
	isSeenSid := sid == bsw.sidLast
	bsw.sidLast = sid

	bh := generateBlockMetadata()
	b.mustWriteTo(sid, bh, &bsw.streamWriters)
	th := &bh.timestamps
	if bsw.totalCount == 0 || th.min < bsw.totalMinTimestamp {
		bsw.totalMinTimestamp = th.min
	}
	if bsw.totalCount == 0 || th.max > bsw.totalMaxTimestamp {
		bsw.totalMaxTimestamp = th.max
	}
	if !hasWrittenBlocks || th.min < bsw.minTimestamp {
		bsw.minTimestamp = th.min
	}
	if !hasWrittenBlocks || th.max > bsw.maxTimestamp {
		bsw.maxTimestamp = th.max
	}
	if isSeenSid && th.min < bsw.minTimestampLast {
		logger.Panicf("the block for sid=%d cannot contain timestamp smaller than %d, but it contains timestamp %d", sid, bsw.minTimestampLast, th.min)
	}
	bsw.minTimestampLast = th.min

	bsw.totalUncompressedSizeBytes += bh.uncompressedSizeBytes
	bsw.totalCount += bh.count
	bsw.totalBlocksCount++

	bsw.primaryBlockData = bh.marshal(bsw.primaryBlockData)
	releaseBlockMetadata(bh)
	if len(bsw.primaryBlockData) > maxUncompressedPrimaryBlockSize {
		bsw.mustFlushPrimaryBlock(bsw.primaryBlockData)
		bsw.primaryBlockData = bsw.primaryBlockData[:0]
	}
}

func (bsw *blockWriter) mustFlushPrimaryBlock(data []byte) {
	if len(data) > 0 {
		bsw.primaryBlockMetadata.mustWriteBlock(data, bsw.sidFirst, bsw.minTimestamp, bsw.maxTimestamp, &bsw.streamWriters)
		bsw.metaData = bsw.primaryBlockMetadata.marshal(bsw.metaData)
	}
	bsw.hasWrittenBlocks = false
	bsw.minTimestamp = 0
	bsw.maxTimestamp = 0
	bsw.sidFirst = 0
}

func (bsw *blockWriter) Flush(ph *partMetadata) {
	ph.UncompressedSizeBytes = bsw.totalUncompressedSizeBytes
	ph.TotalCount = bsw.totalCount
	ph.BlocksCount = bsw.totalBlocksCount
	ph.MinTimestamp = bsw.totalMinTimestamp
	ph.MaxTimestamp = bsw.totalMaxTimestamp

	bsw.mustFlushPrimaryBlock(bsw.primaryBlockData)

	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], bsw.metaData, 1)
	bsw.streamWriters.metaWriter.MustWrite(bb.Buf)
	bigValuePool.Release(bb)

	ph.CompressedSizeBytes = bsw.streamWriters.totalBytesWritten()

	bsw.streamWriters.MustClose()
	bsw.reset()
}

func generateBlockWriter() *blockWriter {
	v := blockWriterPool.Get()
	if v == nil {
		return &blockWriter{}
	}
	return v.(*blockWriter)
}

func releaseBlockWriter(bsw *blockWriter) {
	bsw.reset()
	blockWriterPool.Put(bsw)
}

var blockWriterPool sync.Pool
