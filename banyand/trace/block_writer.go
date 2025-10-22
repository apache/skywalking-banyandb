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
	"path/filepath"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type writer struct {
	sw           fs.SeqWriter
	w            fs.Writer
	bytesWritten uint64
}

func (w *writer) reset() {
	w.w = nil
	w.sw = nil
	w.bytesWritten = 0
}

func (w *writer) init(wc fs.Writer) {
	w.reset()

	w.w = wc
	w.sw = wc.SequentialWrite()
}

func (w *writer) MustWrite(data []byte) {
	fs.MustWriteData(w.sw, data)
	w.bytesWritten += uint64(len(data))
}

func (w *writer) MustClose() {
	fs.MustClose(w.sw)
	w.reset()
}

type mustCreateTagWriters func(name string) (fs.Writer, fs.Writer)

type writers struct {
	mustCreateTagWriters mustCreateTagWriters
	metaWriter           writer
	primaryWriter        writer
	tagMetadataWriters   map[string]*writer
	tagWriters           map[string]*writer
	spanWriter           writer
}

func (sw *writers) reset() {
	sw.mustCreateTagWriters = nil
	sw.metaWriter.reset()
	sw.primaryWriter.reset()
	sw.spanWriter.reset()

	for i, w := range sw.tagMetadataWriters {
		w.reset()
		delete(sw.tagMetadataWriters, i)
	}
	for i, w := range sw.tagWriters {
		w.reset()
		delete(sw.tagWriters, i)
	}
}

func (sw *writers) mustInitForMemPart(mp *memPart) {
	sw.reset()
	sw.mustCreateTagWriters = mp.mustCreateMemTagWriters
	sw.metaWriter.init(&mp.meta)
	sw.primaryWriter.init(&mp.primary)
	sw.spanWriter.init(&mp.spans)
}

func (sw *writers) totalBytesWritten() uint64 {
	n := sw.metaWriter.bytesWritten + sw.primaryWriter.bytesWritten +
		sw.spanWriter.bytesWritten
	for _, w := range sw.tagMetadataWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagWriters {
		n += w.bytesWritten
	}
	return n
}

func (sw *writers) MustClose() {
	sw.metaWriter.MustClose()
	sw.primaryWriter.MustClose()
	sw.spanWriter.MustClose()

	for _, w := range sw.tagMetadataWriters {
		w.MustClose()
	}
	for _, w := range sw.tagWriters {
		w.MustClose()
	}
}

func (sw *writers) getWriters(tagName string) (*writer, *writer) {
	tmw, ok := sw.tagMetadataWriters[tagName]
	tw := sw.tagWriters[tagName]
	if ok {
		return tmw, tw
	}
	mw, w := sw.mustCreateTagWriters(tagName)
	tmw = new(writer)
	tmw.init(mw)
	tw = new(writer)
	tw.init(w)
	sw.tagMetadataWriters[tagName] = tmw
	sw.tagWriters[tagName] = tw
	return tmw, tw
}

type blockWriter struct {
	writers                        writers
	traceIDs                       []string
	tagType                        tagType
	metaData                       []byte
	primaryBlockData               []byte
	primaryBlockMetadata           primaryBlockMetadata
	totalBlocksCount               uint64
	maxTimestamp                   int64
	totalUncompressedSpanSizeBytes uint64
	totalCount                     uint64
	minTimestamp                   int64
	totalMinTimestamp              int64
	totalMaxTimestamp              int64
	minTimestampLast               int64
}

func (bw *blockWriter) reset() {
	bw.writers.reset()
	bw.traceIDs = bw.traceIDs[:0]
	if bw.tagType == nil {
		bw.tagType = make(tagType)
	} else {
		bw.tagType.reset()
	}
	bw.minTimestampLast = 0
	bw.minTimestamp = 0
	bw.maxTimestamp = 0
	bw.totalUncompressedSpanSizeBytes = 0
	bw.totalCount = 0
	bw.totalBlocksCount = 0
	bw.totalMinTimestamp = 0
	bw.totalMaxTimestamp = 0
	bw.primaryBlockData = bw.primaryBlockData[:0]
	bw.metaData = bw.metaData[:0]
	bw.primaryBlockMetadata.reset()
}

func (bw *blockWriter) MustInitForMemPart(mp *memPart) {
	bw.reset()
	bw.writers.mustCreateTagWriters = mp.mustCreateMemTagWriters
	bw.writers.metaWriter.init(&mp.meta)
	bw.writers.primaryWriter.init(&mp.primary)
	bw.writers.spanWriter.init(&mp.spans)
}

func (bw *blockWriter) mustInitForFilePart(fileSystem fs.FileSystem, path string, shouldCache bool) {
	bw.reset()
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)
	bw.writers.mustCreateTagWriters = func(name string) (fs.Writer, fs.Writer) {
		metaPath := filepath.Join(path, name+tagsMetadataFilenameExt)
		dataPath := filepath.Join(path, name+tagsFilenameExt)
		return fs.MustCreateFile(fileSystem, metaPath, storage.FilePerm, shouldCache),
			fs.MustCreateFile(fileSystem, dataPath, storage.FilePerm, shouldCache)
	}

	bw.writers.metaWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, metaFilename), storage.FilePerm, shouldCache))
	bw.writers.primaryWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, primaryFilename), storage.FilePerm, shouldCache))
	bw.writers.spanWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, spansFilename), storage.FilePerm, shouldCache))
}

func (bw *blockWriter) MustWriteTrace(tid string, spans [][]byte, tags [][]*tagValue, timestamps []int64, spanIDs []string) {
	if len(spans) == 0 {
		return
	}

	b := generateBlock()
	defer releaseBlock(b)
	b.mustInitFromTrace(spans, tags, timestamps, spanIDs)
	bw.mustWriteBlock(tid, b)
}

func (bw *blockWriter) mustWriteBlock(tid string, b *block) {
	if b.Len() == 0 {
		return
	}
	hasWrittenBlocks := len(bw.traceIDs) > 0
	var tidLast string
	var isSeenTid bool

	if hasWrittenBlocks {
		tidLast = bw.traceIDs[len(bw.traceIDs)-1]
		if tid < tidLast {
			logger.Panicf("the tid=%s cannot be smaller than the previously written tid=%s", tid, tidLast)
		}
		isSeenTid = tid == tidLast
	}

	bm := generateBlockMetadata()
	b.mustWriteTo(tid, bm, &bw.writers)
	bw.traceIDs = append(bw.traceIDs, tid)
	bw.tagType.copyFrom(bm.tagType)
	tm := &bm.timestamps
	if bw.totalCount == 0 || tm.min < bw.totalMinTimestamp {
		bw.totalMinTimestamp = tm.min
	}
	if bw.totalCount == 0 || tm.max > bw.totalMaxTimestamp {
		bw.totalMaxTimestamp = tm.max
	}
	if !hasWrittenBlocks || tm.min < bw.minTimestamp {
		bw.minTimestamp = tm.min
	}
	if !hasWrittenBlocks || tm.max > bw.maxTimestamp {
		bw.maxTimestamp = tm.max
	}
	if isSeenTid && tm.min < bw.minTimestampLast {
		logger.Panicf("the block for tid=%s cannot contain timestamp smaller than %d, but it contains timestamp %d", tid, bw.minTimestampLast, tm.min)
	}
	bw.minTimestampLast = tm.min

	bw.totalUncompressedSpanSizeBytes += bm.uncompressedSpanSizeBytes
	bw.totalCount += bm.count
	bw.totalBlocksCount++

	bw.primaryBlockData = bm.marshal(bw.primaryBlockData)
	releaseBlockMetadata(bm)
	if len(bw.primaryBlockData) > maxUncompressedPrimaryBlockSize {
		bw.mustFlushPrimaryBlock(bw.primaryBlockData)
		bw.primaryBlockData = bw.primaryBlockData[:0]
	}
}

func (bw *blockWriter) mustFlushPrimaryBlock(data []byte) {
	if len(data) > 0 {
		bw.primaryBlockMetadata.mustWriteBlock(data, bw.traceIDs[0], &bw.writers)
		bw.metaData = bw.primaryBlockMetadata.marshal(bw.metaData)
	}
	bw.minTimestamp = 0
	bw.maxTimestamp = 0
}

func (bw *blockWriter) mustWriteRawBlock(r *rawBlock) {
	bm := r.bm
	if bm.count == 0 {
		return
	}
	hasWrittenBlocks := len(bw.traceIDs) > 0
	var tidLast string
	var isSeenTid bool

	if hasWrittenBlocks {
		tidLast = bw.traceIDs[len(bw.traceIDs)-1]
		if bm.traceID < tidLast {
			logger.Panicf("the tid=%s cannot be smaller than the previously written tid=%s", bm.traceID, tidLast)
		}
		isSeenTid = bm.traceID == tidLast
	}
	bw.traceIDs = append(bw.traceIDs, bm.traceID)
	bw.tagType.copyFrom(bm.tagType)

	tm := &bm.timestamps
	if bw.totalCount == 0 || tm.min < bw.totalMinTimestamp {
		bw.totalMinTimestamp = tm.min
	}
	if bw.totalCount == 0 || tm.max > bw.totalMaxTimestamp {
		bw.totalMaxTimestamp = tm.max
	}
	if !hasWrittenBlocks || tm.min < bw.minTimestamp {
		bw.minTimestamp = tm.min
	}
	if !hasWrittenBlocks || tm.max > bw.maxTimestamp {
		bw.maxTimestamp = tm.max
	}
	if isSeenTid && tm.min < bw.minTimestampLast {
		logger.Panicf("the block for tid=%s cannot contain timestamp smaller than %d, but it contains timestamp %d", bm.traceID, bw.minTimestampLast, tm.min)
	}
	bw.minTimestampLast = tm.min

	bw.totalUncompressedSpanSizeBytes += bm.uncompressedSpanSizeBytes
	bw.totalCount += bm.count
	bw.totalBlocksCount++

	newBm := generateBlockMetadata()
	newBm.copyFrom(bm)

	newBm.spans.offset = bw.writers.spanWriter.bytesWritten
	bw.writers.spanWriter.MustWrite(r.spans)

	for name := range bm.tags {
		tmw, tw := bw.writers.getWriters(name)
		newDB := newBm.getTagMetadata(name)

		// Unmarshal the tag metadata to update the tag value offset
		tm := generateTagMetadata()
		if err := tm.unmarshal(r.tagMetadata[name]); err != nil {
			logger.Panicf("cannot unmarshal tag metadata for %s: %v", name, err)
		}

		// Update the tag value offset to point to the correct location in the new file
		tm.offset = tw.bytesWritten
		tw.MustWrite(r.tags[name])

		// Now marshal the updated metadata and write it
		bb := bigValuePool.Generate()
		bb.Buf = tm.marshal(bb.Buf[:0])
		newDB.offset = tmw.bytesWritten
		newDB.size = uint64(len(bb.Buf))
		tmw.MustWrite(bb.Buf)
		bigValuePool.Release(bb)
		releaseTagMetadata(tm)
	}

	bw.primaryBlockData = newBm.marshal(bw.primaryBlockData)
	releaseBlockMetadata(newBm)

	if len(bw.primaryBlockData) > maxUncompressedPrimaryBlockSize {
		bw.mustFlushPrimaryBlock(bw.primaryBlockData)
		bw.primaryBlockData = bw.primaryBlockData[:0]
	}
}

func (bw *blockWriter) Flush(pm *partMetadata, tf *traceIDFilter, tt *tagType) {
	pm.UncompressedSpanSizeBytes = bw.totalUncompressedSpanSizeBytes
	pm.TotalCount = bw.totalCount
	pm.BlocksCount = bw.totalBlocksCount
	pm.MinTimestamp = bw.totalMinTimestamp
	pm.MaxTimestamp = bw.totalMaxTimestamp

	bw.mustFlushPrimaryBlock(bw.primaryBlockData)

	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], bw.metaData, 1)
	bw.writers.metaWriter.MustWrite(bb.Buf)
	bigValuePool.Release(bb)

	pm.CompressedSizeBytes = bw.writers.totalBytesWritten()

	if len(bw.traceIDs) > 0 {
		if tf.filter == nil {
			tf.filter = generateBloomFilter()
		}
		tf.filter.SetN(len(bw.traceIDs))
		tf.filter.ResizeBits((len(bw.traceIDs)*filter.B + 63) / 64)
		for _, traceID := range bw.traceIDs {
			tf.filter.Add(convert.StringToBytes(traceID))
		}
	}
	tt.copyFrom(bw.tagType)

	bw.writers.MustClose()
	bw.reset()
}

func generateBlockWriter() *blockWriter {
	v := blockWriterPool.Get()
	if v == nil {
		return &blockWriter{
			writers: writers{
				tagMetadataWriters: make(map[string]*writer),
				tagWriters:         make(map[string]*writer),
			},
		}
	}
	return v
}

func releaseBlockWriter(bsw *blockWriter) {
	bsw.reset()
	blockWriterPool.Put(bsw)
}

var blockWriterPool = pool.Register[*blockWriter]("trace-blockWriter")

func generateWriters() *writers {
	v := writersPool.Get()
	if v == nil {
		return &writers{
			tagMetadataWriters: make(map[string]*writer),
			tagWriters:         make(map[string]*writer),
		}
	}
	return v
}

func releaseWriters(sw *writers) {
	sw.reset()
	writersPool.Put(sw)
}

var writersPool = pool.Register[*writers]("trace-writers")
