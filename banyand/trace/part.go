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
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	// Streaming file names (without extensions).
	tracePrimaryName        = "primary"
	traceMetaName           = "meta"
	traceSpansName          = "spans"
	traceTagsPrefix         = "t:"
	traceTagMetadataPrefix  = "tm:"
	traceSeriesMetadataName = "smeta"
	metadataFilename        = "metadata.json"
	traceIDFilterFilename   = "traceID.filter"
	tagTypeFilename         = "tag.type"
	primaryFilename         = tracePrimaryName + ".bin"
	metaFilename            = traceMetaName + ".bin"
	spansFilename           = traceSpansName + ".bin"
	seriesMetadataFilename  = traceSeriesMetadataName + ".bin"
	tagsMetadataFilenameExt = ".tm"
	tagsFilenameExt         = ".t"
)

type part struct {
	primary              fs.Reader
	spans                fs.Reader
	fileSystem           fs.FileSystem
	tagMetadata          map[string]fs.Reader
	tags                 map[string]fs.Reader
	tagType              tagType
	traceIDFilter        traceIDFilter
	seriesMetadata       fs.Reader // Optional: series metadata reader
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

func (p *part) close() {
	fs.MustClose(p.primary)
	fs.MustClose(p.spans)
	if p.seriesMetadata != nil {
		fs.MustClose(p.seriesMetadata)
	}
	for _, t := range p.tags {
		fs.MustClose(t)
	}
	for _, tm := range p.tagMetadata {
		fs.MustClose(tm)
	}
	p.traceIDFilter.reset()
}

func (p *part) String() string {
	return fmt.Sprintf("part %d", p.partMetadata.ID)
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata
	p.tagType = mp.tagType
	p.traceIDFilter.filter = mp.traceIDFilter.filter

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], &mp.meta)

	// Open data files
	p.primary = &mp.primary
	p.spans = &mp.spans
	if len(mp.seriesMetadata.Buf) > 0 {
		p.seriesMetadata = &mp.seriesMetadata
	}
	if mp.tags != nil {
		p.tags = make(map[string]fs.Reader)
		p.tagMetadata = make(map[string]fs.Reader)
		for name, t := range mp.tags {
			p.tags[name] = t
			p.tagMetadata[name] = mp.tagMetadata[name]
		}
	}
	return &p
}

type memPart struct {
	tagMetadata    map[string]*bytes.Buffer
	tags           map[string]*bytes.Buffer
	tagType        tagType
	traceIDFilter  traceIDFilter
	spans          bytes.Buffer
	meta           bytes.Buffer
	primary        bytes.Buffer
	seriesMetadata bytes.Buffer
	partMetadata   partMetadata
	segmentID      int64
}

func (mp *memPart) mustCreateMemTagWriters(name string) (fs.Writer, fs.Writer) {
	if mp.tags == nil {
		mp.tags = make(map[string]*bytes.Buffer)
		mp.tagMetadata = make(map[string]*bytes.Buffer)
	}
	t, ok := mp.tags[name]
	tm := mp.tagMetadata[name]
	if ok {
		t.Reset()
		tm.Reset()
		return tm, t
	}
	mp.tags[name] = &bytes.Buffer{}
	mp.tagMetadata[name] = &bytes.Buffer{}
	return mp.tagMetadata[name], mp.tags[name]
}

func (mp *memPart) reset() {
	mp.partMetadata.reset()
	if mp.tagType == nil {
		mp.tagType = make(tagType)
	} else {
		mp.tagType.reset()
	}
	mp.traceIDFilter.reset()
	mp.meta.Reset()
	mp.primary.Reset()
	mp.spans.Reset()
	mp.seriesMetadata.Reset()
	mp.segmentID = 0
	if mp.tags != nil {
		for k, t := range mp.tags {
			t.Reset()
			delete(mp.tags, k)
		}
	}
	if mp.tagMetadata != nil {
		for k, tm := range mp.tagMetadata {
			tm.Reset()
			delete(mp.tagMetadata, k)
		}
	}
}

func (mp *memPart) mustInitFromPart(p *part) {
	mp.reset()

	// Copy part metadata, tagType and traceIDFilter
	mp.partMetadata = p.partMetadata
	mp.tagType = p.tagType
	mp.traceIDFilter.filter = p.traceIDFilter.filter

	// Read primary data
	sr := p.primary.SequentialRead()
	data, err := io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read primary data from %s: %s", p.primary.Path(), err)
	}
	fs.MustClose(sr)
	mp.primary.Buf = append(mp.primary.Buf[:0], data...)

	// Read spans data
	sr = p.spans.SequentialRead()
	data, err = io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read spans data from %s: %s", p.spans.Path(), err)
	}
	fs.MustClose(sr)
	mp.spans.Buf = append(mp.spans.Buf[:0], data...)

	// Read tag data
	if p.tags != nil {
		mp.tags = make(map[string]*bytes.Buffer)
		mp.tagMetadata = make(map[string]*bytes.Buffer)

		for name, reader := range p.tags {
			sr = reader.SequentialRead()
			data, err = io.ReadAll(sr)
			if err != nil {
				logger.Panicf("cannot read tag data from %s: %s", reader.Path(), err)
			}
			fs.MustClose(sr)

			mp.tags[name] = &bytes.Buffer{}
			mp.tags[name].Buf = append(mp.tags[name].Buf[:0], data...)
		}

		for name, reader := range p.tagMetadata {
			sr = reader.SequentialRead()
			data, err = io.ReadAll(sr)
			if err != nil {
				logger.Panicf("cannot read tag metadata from %s: %s", reader.Path(), err)
			}
			fs.MustClose(sr)

			mp.tagMetadata[name] = &bytes.Buffer{}
			mp.tagMetadata[name].Buf = append(mp.tagMetadata[name].Buf[:0], data...)
		}
	}

	// Encode primaryBlockMetadata to memPart.meta
	// This is the reverse process of mustReadPrimaryBlockMetadata
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// Marshal all primaryBlockMetadata entries
	for _, pbm := range p.primaryBlockMetadata {
		bb.Buf = pbm.marshal(bb.Buf)
	}

	// Compress the marshaled data
	mp.meta.Buf = zstd.Compress(mp.meta.Buf[:0], bb.Buf, 1)
}

func (mp *memPart) mustInitFromTraces(ts *traces) {
	mp.reset()

	if len(ts.traceIDs) == 0 {
		return
	}

	sort.Sort(ts)

	// Count unique trace IDs
	traceSize := 0
	var tidPrevCount string
	for _, tid := range ts.traceIDs {
		if tid != tidPrevCount {
			traceSize++
			tidPrevCount = tid
		}
	}

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp, traceSize)

	var tidPrev string
	uncompressedSpansSizeBytes := uint64(0)
	var indexPrev int
	for i := range ts.spans {
		tid := ts.traceIDs[i]
		if tidPrev == "" {
			tidPrev = tid
		}

		if uncompressedSpansSizeBytes >= maxUncompressedSpanSize || tid != tidPrev {
			bsw.MustWriteTrace(tidPrev, ts.spans[indexPrev:i], ts.tags[indexPrev:i], ts.timestamps[indexPrev:i], ts.spanIDs[indexPrev:i])
			tidPrev = tid
			indexPrev = i
			uncompressedSpansSizeBytes = 0
		}
		uncompressedSpansSizeBytes += uint64(len(ts.spans[i]))
	}
	bsw.MustWriteTrace(tidPrev, ts.spans[indexPrev:], ts.tags[indexPrev:], ts.timestamps[indexPrev:], ts.spanIDs[indexPrev:])
	bsw.Flush(&mp.partMetadata, &mp.traceIDFilter, &mp.tagType)
	releaseBlockWriter(bsw)
}

func (mp *memPart) mustFlush(fileSystem fs.FileSystem, path string) {
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)

	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(path, metaFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(path, primaryFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.spans.Buf, filepath.Join(path, spansFilename), storage.FilePerm)
	for name, t := range mp.tags {
		fs.MustFlush(fileSystem, t.Buf, filepath.Join(path, name+tagsFilenameExt), storage.FilePerm)
	}
	for name, tm := range mp.tagMetadata {
		fs.MustFlush(fileSystem, tm.Buf, filepath.Join(path, name+tagsMetadataFilenameExt), storage.FilePerm)
	}

	// Flush series metadata if available
	if len(mp.seriesMetadata.Buf) > 0 {
		fs.MustFlush(fileSystem, mp.seriesMetadata.Buf, filepath.Join(path, seriesMetadataFilename), storage.FilePerm)
	}

	mp.partMetadata.mustWriteMetadata(fileSystem, path)
	mp.tagType.mustWriteTagType(fileSystem, path)
	mp.traceIDFilter.mustWriteTraceIDFilter(fileSystem, path)

	fileSystem.SyncPath(path)
}

func generateMemPart() *memPart {
	v := memPartPool.Get()
	if v == nil {
		return &memPart{}
	}
	return v
}

func releaseMemPart(mp *memPart) {
	mp.reset()
	memPartPool.Put(mp)
}

var memPartPool = pool.Register[*memPart]("trace-memPart")

type partWrapper struct {
	mp        *memPart
	p         *part
	ref       int32
	removable atomic.Bool
}

func newPartWrapper(mp *memPart, p *part) *partWrapper {
	return &partWrapper{mp: mp, p: p, ref: 1}
}

func (pw *partWrapper) incRef() {
	atomic.AddInt32(&pw.ref, 1)
}

func (pw *partWrapper) decRef() {
	n := atomic.AddInt32(&pw.ref, -1)
	if n > 0 {
		return
	}
	if pw.mp != nil {
		releaseMemPart(pw.mp)
		pw.mp = nil
		pw.p = nil
		return
	}
	pw.p.close()
	if pw.removable.Load() && pw.p.fileSystem != nil {
		go func(pw *partWrapper) {
			pw.p.fileSystem.MustRMAll(pw.p.path)
		}(pw)
	}
}

func (pw *partWrapper) ID() uint64 {
	return pw.p.partMetadata.ID
}

func (pw *partWrapper) String() string {
	if pw.mp != nil {
		return fmt.Sprintf("mem part %v", pw.mp.partMetadata)
	}
	return fmt.Sprintf("part %v", pw.p.partMetadata)
}

func mustOpenFilePart(id uint64, root string, fileSystem fs.FileSystem) *part {
	var p part
	partPath := partPath(root, id)
	p.path = partPath
	p.fileSystem = fileSystem
	p.partMetadata.mustReadMetadata(fileSystem, partPath)
	p.partMetadata.ID = id

	p.tagType = make(tagType)
	p.tagType.mustReadTagType(fileSystem, partPath)
	p.traceIDFilter.mustReadTraceIDFilter(fileSystem, partPath)

	metaPath := path.Join(partPath, metaFilename)
	pr := mustOpenReader(metaPath, fileSystem)
	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], pr)
	fs.MustClose(pr)

	p.primary = mustOpenReader(path.Join(partPath, primaryFilename), fileSystem)
	p.spans = mustOpenReader(path.Join(partPath, spansFilename), fileSystem)

	// Try to open series metadata file (optional, for backward compatibility)
	seriesMetadataPath := path.Join(partPath, seriesMetadataFilename)
	reader, err := fileSystem.OpenFile(seriesMetadataPath)
	if err != nil {
		var fsErr *fs.FileSystemError
		// File does not exist is acceptable for backward compatibility
		if !errors.As(err, &fsErr) || fsErr.Code != fs.IsNotExistError {
			logger.Panicf("cannot open series metadata file %q: %s", seriesMetadataPath, err)
		}
	} else {
		p.seriesMetadata = reader
	}

	ee := fileSystem.ReadDir(partPath)
	for _, e := range ee {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == tagsMetadataFilenameExt {
			if p.tagMetadata == nil {
				p.tagMetadata = make(map[string]fs.Reader)
			}
			p.tagMetadata[removeExt(e.Name(), tagsMetadataFilenameExt)] = mustOpenReader(path.Join(partPath, e.Name()), fileSystem)
		}
		if filepath.Ext(e.Name()) == tagsFilenameExt {
			if p.tags == nil {
				p.tags = make(map[string]fs.Reader)
			}
			p.tags[removeExt(e.Name(), tagsFilenameExt)] = mustOpenReader(path.Join(partPath, e.Name()), fileSystem)
		}
	}
	return &p
}

func mustOpenReader(name string, fileSystem fs.FileSystem) fs.Reader {
	f, err := fileSystem.OpenFile(name)
	if err != nil {
		logger.Panicf("cannot open %q: %s", name, err)
	}
	return f
}

func removeExt(nameWithExt, ext string) string {
	return nameWithExt[:len(nameWithExt)-len(ext)]
}

func partPath(root string, epoch uint64) string {
	return filepath.Join(root, partName(epoch))
}

func partName(epoch uint64) string {
	return fmt.Sprintf("%016x", epoch)
}

// CreatePartFileReaderFromPath opens all files in a part directory and returns their FileInfo and a cleanup function.
func CreatePartFileReaderFromPath(partPath string, lfs fs.FileSystem) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo
	var readers []fs.Reader

	// Core trace files (required files)
	metaPath := path.Join(partPath, metaFilename)
	metaReader, err := lfs.OpenFile(metaPath)
	if err != nil {
		logger.Panicf("cannot open trace meta file %q: %s", metaPath, err)
	}
	readers = append(readers, metaReader)
	files = append(files, queue.FileInfo{
		Name:   traceMetaName,
		Reader: metaReader.SequentialRead(),
	})

	primaryPath := path.Join(partPath, primaryFilename)
	primaryReader, err := lfs.OpenFile(primaryPath)
	if err != nil {
		logger.Panicf("cannot open trace primary file %q: %s", primaryPath, err)
	}
	readers = append(readers, primaryReader)
	files = append(files, queue.FileInfo{
		Name:   tracePrimaryName,
		Reader: primaryReader.SequentialRead(),
	})

	spansPath := path.Join(partPath, spansFilename)
	if spansReader, err := lfs.OpenFile(spansPath); err == nil {
		readers = append(readers, spansReader)
		files = append(files, queue.FileInfo{
			Name:   traceSpansName,
			Reader: spansReader.SequentialRead(),
		})
	}

	// Special trace files: traceID.filter and tag.type
	traceIDFilterPath := path.Join(partPath, traceIDFilterFilename)
	if filterReader, err := lfs.OpenFile(traceIDFilterPath); err == nil {
		readers = append(readers, filterReader)
		files = append(files, queue.FileInfo{
			Name:   traceIDFilterFilename,
			Reader: filterReader.SequentialRead(),
		})
	}

	tagTypePath := path.Join(partPath, tagTypeFilename)
	if tagTypeReader, err := lfs.OpenFile(tagTypePath); err == nil {
		readers = append(readers, tagTypeReader)
		files = append(files, queue.FileInfo{
			Name:   tagTypeFilename,
			Reader: tagTypeReader.SequentialRead(),
		})
	}

	// Dynamic tag files (*.t and *.tm)
	ee := lfs.ReadDir(partPath)
	for _, e := range ee {
		if e.IsDir() {
			continue
		}

		// Tag metadata files (.tm)
		if filepath.Ext(e.Name()) == tagsMetadataFilenameExt {
			tmPath := path.Join(partPath, e.Name())
			tmReader, err := lfs.OpenFile(tmPath)
			if err != nil {
				logger.Panicf("cannot open trace tag metadata file %q: %s", tmPath, err)
			}
			readers = append(readers, tmReader)
			tagName := removeExt(e.Name(), tagsMetadataFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   traceTagMetadataPrefix + tagName,
				Reader: tmReader.SequentialRead(),
			})
		}

		// Tag data files (.t)
		if filepath.Ext(e.Name()) == tagsFilenameExt {
			tPath := path.Join(partPath, e.Name())
			tReader, err := lfs.OpenFile(tPath)
			if err != nil {
				logger.Panicf("cannot open trace tag file %q: %s", tPath, err)
			}
			readers = append(readers, tReader)
			tagName := removeExt(e.Name(), tagsFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   traceTagsPrefix + tagName,
				Reader: tReader.SequentialRead(),
			})
		}
	}

	cleanup := func() {
		for _, reader := range readers {
			fs.MustClose(reader)
		}
	}

	return files, cleanup
}
