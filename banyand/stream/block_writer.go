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
	"path/filepath"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
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

type mustCreateTagFamilyWriters func(name string) (fs.Writer, fs.Writer, fs.Writer)

type writers struct {
	mustCreateTagFamilyWriters mustCreateTagFamilyWriters
	metaWriter                 writer
	primaryWriter              writer
	tagFamilyMetadataWriters   map[string]*writer
	tagFamilyWriters           map[string]*writer
	tagFamilyFilterWriters     map[string]*writer
	timestampsWriter           writer
}

func (sw *writers) reset() {
	sw.mustCreateTagFamilyWriters = nil
	sw.metaWriter.reset()
	sw.primaryWriter.reset()
	sw.timestampsWriter.reset()

	for i, w := range sw.tagFamilyMetadataWriters {
		w.reset()
		delete(sw.tagFamilyMetadataWriters, i)
	}
	for i, w := range sw.tagFamilyWriters {
		w.reset()
		delete(sw.tagFamilyWriters, i)
	}
	for i, w := range sw.tagFamilyFilterWriters {
		w.reset()
		delete(sw.tagFamilyFilterWriters, i)
	}
}

func (sw *writers) totalBytesWritten() uint64 {
	n := sw.metaWriter.bytesWritten + sw.primaryWriter.bytesWritten +
		sw.timestampsWriter.bytesWritten
	for _, w := range sw.tagFamilyMetadataWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagFamilyWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagFamilyFilterWriters {
		n += w.bytesWritten
	}
	return n
}

func (sw *writers) MustClose() {
	sw.metaWriter.MustClose()
	sw.primaryWriter.MustClose()
	sw.timestampsWriter.MustClose()

	for _, w := range sw.tagFamilyMetadataWriters {
		w.MustClose()
	}
	for _, w := range sw.tagFamilyWriters {
		w.MustClose()
	}
	for _, w := range sw.tagFamilyFilterWriters {
		w.MustClose()
	}
}

func (sw *writers) getWriters(tagName string) (*writer, *writer, *writer) {
	thw, ok := sw.tagFamilyMetadataWriters[tagName]
	tw := sw.tagFamilyWriters[tagName]
	tfw := sw.tagFamilyFilterWriters[tagName]
	if ok {
		return thw, tw, tfw
	}
	hw, w, fw := sw.mustCreateTagFamilyWriters(tagName)
	thw = new(writer)
	thw.init(hw)
	tw = new(writer)
	tw.init(w)
	tfw = new(writer)
	tfw.init(fw)
	sw.tagFamilyMetadataWriters[tagName] = thw
	sw.tagFamilyWriters[tagName] = tw
	sw.tagFamilyFilterWriters[tagName] = tfw
	return thw, tw, tfw
}

type blockWriter struct {
	writers                    writers
	metaData                   []byte
	primaryBlockData           []byte
	primaryBlockMetadata       primaryBlockMetadata
	totalBlocksCount           uint64
	maxTimestamp               int64
	totalUncompressedSizeBytes uint64
	totalCount                 uint64
	minTimestamp               int64
	totalMinTimestamp          int64
	totalMaxTimestamp          int64
	minTimestampLast           int64
	sidFirst                   common.SeriesID
	sidLast                    common.SeriesID
	hasWrittenBlocks           bool
}

func (bw *blockWriter) reset() {
	bw.writers.reset()
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

func (bw *blockWriter) MustInitForMemPart(mp *memPart) {
	bw.reset()
	bw.writers.mustCreateTagFamilyWriters = mp.mustCreateMemTagFamilyWriters
	bw.writers.metaWriter.init(&mp.meta)
	bw.writers.primaryWriter.init(&mp.primary)
	bw.writers.timestampsWriter.init(&mp.timestamps)
}

func (bw *blockWriter) mustInitForFilePart(fileSystem fs.FileSystem, path string) {
	bw.reset()
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)
	bw.writers.mustCreateTagFamilyWriters = func(name string) (fs.Writer, fs.Writer, fs.Writer) {
		return fs.MustCreateFile(fileSystem, filepath.Join(path, name+tagFamiliesMetadataFilenameExt), storage.FilePerm),
			fs.MustCreateFile(fileSystem, filepath.Join(path, name+tagFamiliesFilenameExt), storage.FilePerm),
			fs.MustCreateFile(fileSystem, filepath.Join(path, name+tagFamiliesFilterFilenameExt), storage.FilePerm)
	}
	bw.writers.metaWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, metaFilename), storage.FilePerm))
	bw.writers.primaryWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, primaryFilename), storage.FilePerm))
	bw.writers.timestampsWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, timestampsFilename), storage.FilePerm))
}

func (bw *blockWriter) MustWriteElements(sid common.SeriesID, timestamps []int64, elementIDs []uint64, tagFamilies [][]tagValues,
	indexedTags []map[string]map[string]struct{},
) {
	if len(timestamps) == 0 {
		return
	}

	b := generateBlock()
	defer releaseBlock(b)
	b.mustInitFromElements(timestamps, elementIDs, tagFamilies, indexedTags)
	bw.mustWriteBlock(sid, b)
}

func (bw *blockWriter) mustWriteBlock(sid common.SeriesID, b *block) {
	if b.Len() == 0 {
		return
	}
	if sid < bw.sidLast {
		logger.Panicf("the sid=%d cannot be smaller than the previously written sid=%d", sid, &bw.sidLast)
	}
	hasWrittenBlocks := bw.hasWrittenBlocks
	if !hasWrittenBlocks {
		bw.sidFirst = sid
		bw.hasWrittenBlocks = true
	}
	isSeenSid := sid == bw.sidLast
	bw.sidLast = sid

	bm := generateBlockMetadata()
	b.mustWriteTo(sid, bm, &bw.writers)
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
	if isSeenSid && tm.min < bw.minTimestampLast {
		logger.Panicf("the block for sid=%d cannot contain timestamp smaller than %d, but it contains timestamp %d", sid, bw.minTimestampLast, tm.min)
	}
	bw.minTimestampLast = tm.min

	bw.totalUncompressedSizeBytes += bm.uncompressedSizeBytes
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
		bw.primaryBlockMetadata.mustWriteBlock(data, bw.sidFirst, bw.minTimestamp, bw.maxTimestamp, &bw.writers)
		bw.metaData = bw.primaryBlockMetadata.marshal(bw.metaData)
	}
	bw.hasWrittenBlocks = false
	bw.minTimestamp = 0
	bw.maxTimestamp = 0
	bw.sidFirst = 0
}

func (bw *blockWriter) Flush(pm *partMetadata) {
	pm.UncompressedSizeBytes = bw.totalUncompressedSizeBytes
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

	bw.writers.MustClose()
	bw.reset()
}

func generateBlockWriter() *blockWriter {
	v := blockWriterPool.Get()
	if v == nil {
		return &blockWriter{
			writers: writers{
				tagFamilyMetadataWriters: make(map[string]*writer),
				tagFamilyWriters:         make(map[string]*writer),
				tagFamilyFilterWriters:   make(map[string]*writer),
			},
		}
	}
	return v
}

func releaseBlockWriter(bsw *blockWriter) {
	bsw.reset()
	blockWriterPool.Put(bsw)
}

var blockWriterPool = pool.Register[*blockWriter]("stream-blockWriter")
