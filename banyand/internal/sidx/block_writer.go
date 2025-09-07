// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sidx

import (
	"path/filepath"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	maxUncompressedPrimaryBlockSize = 64 * 1024 // 64KB
)

// writer wraps a file writer and tracks bytes written.
type writer struct {
	sw           fs.SeqWriter
	w            fs.Writer
	bytesWritten uint64
}

// reset clears writer state for reuse.
func (w *writer) reset() {
	w.w = nil
	w.sw = nil
	w.bytesWritten = 0
}

// init initializes writer with file writer.
func (w *writer) init(wc fs.Writer) {
	w.reset()
	w.w = wc
	w.sw = wc.SequentialWrite()
}

// MustWrite writes data and tracks bytes written.
func (w *writer) MustWrite(data []byte) {
	fs.MustWriteData(w.sw, data)
	w.bytesWritten += uint64(len(data))
}

// MustClose closes the sequential writer.
func (w *writer) MustClose() {
	fs.MustClose(w.sw)
	w.reset()
}

// mustCreateTagWriters function type for creating tag file writers.
type mustCreateTagWriters func(name string) (fs.Writer, fs.Writer, fs.Writer)

// writers manages all file writers for a part.
type writers struct {
	mustCreateTagWriters mustCreateTagWriters
	tagMetadataWriters   map[string]*writer
	tagDataWriters       map[string]*writer
	tagFilterWriters     map[string]*writer
	metaWriter           writer
	primaryWriter        writer
	dataWriter           writer
	keysWriter           writer
}

// reset clears all writers for reuse.
func (sw *writers) reset() {
	sw.mustCreateTagWriters = nil
	sw.metaWriter.reset()
	sw.primaryWriter.reset()
	sw.dataWriter.reset()
	sw.keysWriter.reset()

	for i, w := range sw.tagMetadataWriters {
		w.reset()
		delete(sw.tagMetadataWriters, i)
	}
	for i, w := range sw.tagDataWriters {
		w.reset()
		delete(sw.tagDataWriters, i)
	}
	for i, w := range sw.tagFilterWriters {
		w.reset()
		delete(sw.tagFilterWriters, i)
	}
}

// mustInitForMemPart initializes writers for memory part.
func (sw *writers) mustInitForMemPart(mp *memPart) {
	sw.reset()
	sw.mustCreateTagWriters = mp.mustCreateTagWriters
	sw.metaWriter.init(&mp.meta)
	sw.primaryWriter.init(&mp.primary)
	sw.dataWriter.init(&mp.data)
	sw.keysWriter.init(&mp.keys)
}

// totalBytesWritten returns total bytes written across all writers.
func (sw *writers) totalBytesWritten() uint64 {
	n := sw.metaWriter.bytesWritten + sw.primaryWriter.bytesWritten +
		sw.dataWriter.bytesWritten + sw.keysWriter.bytesWritten

	for _, w := range sw.tagMetadataWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagDataWriters {
		n += w.bytesWritten
	}
	for _, w := range sw.tagFilterWriters {
		n += w.bytesWritten
	}
	return n
}

// MustClose closes all writers.
func (sw *writers) MustClose() {
	sw.metaWriter.MustClose()
	sw.primaryWriter.MustClose()
	sw.dataWriter.MustClose()
	sw.keysWriter.MustClose()

	for _, w := range sw.tagMetadataWriters {
		w.MustClose()
	}
	for _, w := range sw.tagDataWriters {
		w.MustClose()
	}
	for _, w := range sw.tagFilterWriters {
		w.MustClose()
	}
}

// getWriters returns writers for the specified tag name, creating them if needed.
func (sw *writers) getWriters(tagName string) (*writer, *writer, *writer) {
	tmw, ok := sw.tagMetadataWriters[tagName]
	tdw := sw.tagDataWriters[tagName]
	tfw := sw.tagFilterWriters[tagName]

	if ok {
		return tmw, tdw, tfw
	}

	// Create new writers
	mw, dw, fw := sw.mustCreateTagWriters(tagName)
	tmw = new(writer)
	tmw.init(mw)
	tdw = new(writer)
	tdw.init(dw)
	tfw = new(writer)
	tfw.init(fw)

	sw.tagMetadataWriters[tagName] = tmw
	sw.tagDataWriters[tagName] = tdw
	sw.tagFilterWriters[tagName] = tfw

	return tmw, tdw, tfw
}

// blockWriter handles writing blocks to files.
type blockWriter struct {
	writers                    writers
	metaData                   []byte
	primaryBlockData           []byte
	primaryBlockMetadata       primaryBlockMetadata
	totalBlocksCount           uint64
	maxKey                     int64
	totalUncompressedSizeBytes uint64
	totalCount                 uint64
	minKey                     int64
	totalMinKey                int64
	totalMaxKey                int64
	minKeyLast                 int64
	sidFirst                   common.SeriesID
	sidLast                    common.SeriesID
	hasWrittenBlocks           bool
}

// reset clears block writer state for reuse.
func (bw *blockWriter) reset() {
	bw.writers.reset()
	bw.sidLast = 0
	bw.sidFirst = 0
	bw.minKeyLast = 0
	bw.minKey = 0
	bw.maxKey = 0
	bw.hasWrittenBlocks = false
	bw.totalUncompressedSizeBytes = 0
	bw.totalCount = 0
	bw.totalBlocksCount = 0
	bw.totalMinKey = 0
	bw.totalMaxKey = 0
	bw.primaryBlockData = bw.primaryBlockData[:0]
	bw.metaData = bw.metaData[:0]
	bw.primaryBlockMetadata.reset()
}

// MustInitForMemPart initializes block writer for memory part.
func (bw *blockWriter) MustInitForMemPart(mp *memPart) {
	bw.reset()
	bw.writers.mustInitForMemPart(mp)
}

// mustInitForFilePart initializes block writer for file part.
func (bw *blockWriter) mustInitForFilePart(fileSystem fs.FileSystem, path string, shouldCache bool) {
	bw.reset()
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)

	bw.writers.mustCreateTagWriters = func(name string) (fs.Writer, fs.Writer, fs.Writer) {
		metaPath := filepath.Join(path, name+tagMetadataExtension)
		dataPath := filepath.Join(path, name+tagDataExtension)
		filterPath := filepath.Join(path, name+tagFilterExtension)
		return fs.MustCreateFile(fileSystem, metaPath, storage.FilePerm, shouldCache),
			fs.MustCreateFile(fileSystem, dataPath, storage.FilePerm, shouldCache),
			fs.MustCreateFile(fileSystem, filterPath, storage.FilePerm, shouldCache)
	}

	bw.writers.metaWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, metaFilename), storage.FilePerm, shouldCache))
	bw.writers.primaryWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, primaryFilename), storage.FilePerm, shouldCache))
	bw.writers.dataWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, dataFilename), storage.FilePerm, shouldCache))
	bw.writers.keysWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(path, keysFilename), storage.FilePerm, shouldCache))
}

// MustWriteElements writes elements to the block writer.
func (bw *blockWriter) MustWriteElements(sid common.SeriesID, userKeys []int64, data [][]byte, tags [][]*tag) {
	if len(userKeys) == 0 {
		return
	}

	b := generateBlock()
	defer releaseBlock(b)
	// Copy core data
	b.userKeys = append(b.userKeys, userKeys...)
	b.data = append(b.data, data...)

	// Process tags
	b.mustInitFromTags(tags)

	bw.mustWriteBlock(sid, b)
}

// mustWriteBlock writes a block with metadata tracking.
func (bw *blockWriter) mustWriteBlock(sid common.SeriesID, b *block) {
	if b.Len() == 0 {
		return
	}

	if sid < bw.sidLast {
		logger.Panicf("the sid=%d cannot be smaller than the previously written sid=%d", sid, bw.sidLast)
	}

	hasWrittenBlocks := bw.hasWrittenBlocks
	if !hasWrittenBlocks {
		bw.sidFirst = sid
		bw.hasWrittenBlocks = true
	}

	isSeenSid := sid == bw.sidLast
	bw.sidLast = sid

	bm := generateBlockMetadata()
	defer releaseBlockMetadata(bm)

	// Write block to files
	b.mustWriteTo(sid, bm, &bw.writers)

	// Update key ranges
	minKey, maxKey := b.getKeyRange()
	if bw.totalCount == 0 || minKey < bw.totalMinKey {
		bw.totalMinKey = minKey
	}
	if bw.totalCount == 0 || maxKey > bw.totalMaxKey {
		bw.totalMaxKey = maxKey
	}
	if !hasWrittenBlocks || minKey < bw.minKey {
		bw.minKey = minKey
	}
	if !hasWrittenBlocks || maxKey > bw.maxKey {
		bw.maxKey = maxKey
	}

	if isSeenSid && minKey < bw.minKeyLast {
		logger.Panicf("the block for sid=%d cannot contain key smaller than %d, but it contains key %d", sid, bw.minKeyLast, minKey)
	}
	bw.minKeyLast = minKey

	bw.totalUncompressedSizeBytes += b.uncompressedSizeBytes()
	bw.totalCount += uint64(b.Len())
	bw.totalBlocksCount++

	// Serialize block metadata
	bm.setSeriesID(sid)
	bm.setKeyRange(minKey, maxKey)
	bw.primaryBlockData = bm.marshal(bw.primaryBlockData)

	if len(bw.primaryBlockData) > maxUncompressedPrimaryBlockSize {
		bw.mustFlushPrimaryBlock(bw.primaryBlockData)
		bw.primaryBlockData = bw.primaryBlockData[:0]
	}
}

// mustFlushPrimaryBlock flushes accumulated primary block data.
func (bw *blockWriter) mustFlushPrimaryBlock(data []byte) {
	if len(data) > 0 {
		bw.primaryBlockMetadata.mustWriteBlock(data, bw.sidFirst, bw.minKey, bw.maxKey, &bw.writers)
		bmData := bw.primaryBlockMetadata.marshal(bw.metaData[:0])
		bw.metaData = append(bw.metaData, bmData...)
	}
	bw.hasWrittenBlocks = false
	bw.minKey = 0
	bw.maxKey = 0
	bw.sidFirst = 0
}

// Flush finalizes the block writer and updates part metadata.
func (bw *blockWriter) Flush(pm *partMetadata) {
	pm.UncompressedSizeBytes = bw.totalUncompressedSizeBytes
	pm.TotalCount = bw.totalCount
	pm.BlocksCount = bw.totalBlocksCount
	pm.MinKey = bw.totalMinKey
	pm.MaxKey = bw.totalMaxKey

	bw.mustFlushPrimaryBlock(bw.primaryBlockData)

	// Compress and write metadata
	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	bb.Buf = zstd.Compress(bb.Buf[:0], bw.metaData, 1)
	bw.writers.metaWriter.MustWrite(bb.Buf)
	bb.Buf = bb.Buf[:0] // Reset for reuse
	bigValuePool.Put(bb)

	pm.CompressedSizeBytes = bw.writers.totalBytesWritten()

	bw.writers.MustClose()
	bw.reset()
}

// Pool management for block writers and writers.
var (
	blockWriterPool = pool.Register[*blockWriter]("sidx-blockWriter")
	writersPool     = pool.Register[*writers]("sidx-writers")
	bigValuePool    = pool.Register[*bytes.Buffer]("sidx-bigValue")
)

// generateBlockWriter gets a block writer from pool or creates new.
func generateBlockWriter() *blockWriter {
	v := blockWriterPool.Get()
	if v == nil {
		return &blockWriter{
			writers: writers{
				tagMetadataWriters: make(map[string]*writer),
				tagDataWriters:     make(map[string]*writer),
				tagFilterWriters:   make(map[string]*writer),
			},
		}
	}
	return v
}

// releaseBlockWriter returns block writer to pool after reset.
func releaseBlockWriter(bw *blockWriter) {
	bw.reset()
	blockWriterPool.Put(bw)
}

// generateWriters gets writers from pool or creates new.
func generateWriters() *writers {
	v := writersPool.Get()
	if v == nil {
		return &writers{
			tagMetadataWriters: make(map[string]*writer),
			tagDataWriters:     make(map[string]*writer),
			tagFilterWriters:   make(map[string]*writer),
		}
	}
	return v
}

// releaseWriters returns writers to pool after reset.
func releaseWriters(sw *writers) {
	sw.reset()
	writersPool.Put(sw)
}
