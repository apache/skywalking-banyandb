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
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
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
	metadataFilename        = "metadata.json"
	traceIDFilterFilename   = "traceID.filter"
	tagTypeFilename         = "tag.type"
	primaryFilename         = tracePrimaryName + ".bin"
	metaFilename            = traceMetaName + ".bin"
	spansFilename           = traceSpansName + ".bin"
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
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

func (p *part) close() {
	fs.MustClose(p.primary)
	fs.MustClose(p.spans)
	for _, t := range p.tags {
		fs.MustClose(t)
	}
	for _, tm := range p.tagMetadata {
		fs.MustClose(tm)
	}
}

func (p *part) String() string {
	return fmt.Sprintf("part %d", p.partMetadata.ID)
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata
	p.tagType = mp.tagType
	p.traceIDFilter = mp.traceIDFilter

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], &mp.meta)

	// Open data files
	p.primary = &mp.primary
	p.spans = &mp.spans
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
	tagMetadata   map[string]*bytes.Buffer
	tags          map[string]*bytes.Buffer
	tagType       tagType
	traceIDFilter traceIDFilter
	spans         bytes.Buffer
	meta          bytes.Buffer
	primary       bytes.Buffer
	partMetadata  partMetadata
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

// Marshal serializes the memPart to []byte for network transmission.
// The format is:
// - partMetadata (JSON)
// - meta buffer length + data
// - primary buffer length + data
// - tags count + (name + buffer length + data) for each
// - tagMetadata count + (name + buffer length + data) for each.
func (mp *memPart) Marshal() ([]byte, error) {
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// Marshal partMetadata as JSON
	metadataBytes, err := json.Marshal(mp.partMetadata)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal partMetadata: %w", err)
	}
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(metadataBytes)))
	bb.Buf = append(bb.Buf, metadataBytes...)

	// Marshal tagType
	var tagTypeBuf []byte
	tagTypeBuf = mp.tagType.marshal(tagTypeBuf)
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(tagTypeBuf)))
	bb.Buf = append(bb.Buf, tagTypeBuf...)

	// Marshal traceIDFilter
	if mp.traceIDFilter.filter != nil {
		var filterBuf []byte
		filterBuf = encodeBloomFilter(filterBuf, mp.traceIDFilter.filter)
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(filterBuf)))
		bb.Buf = append(bb.Buf, filterBuf...)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	}

	// Marshal meta buffer
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.meta.Buf)))
	bb.Buf = append(bb.Buf, mp.meta.Buf...)

	// Marshal primary buffer
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.primary.Buf)))
	bb.Buf = append(bb.Buf, mp.primary.Buf...)

	// Marshal spans
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.spans.Buf)))
	bb.Buf = append(bb.Buf, mp.spans.Buf...)

	// Marshal tags
	if mp.tags == nil {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.tags)))
		for name, t := range mp.tags {
			// Write name length and name
			nameBytes := []byte(name)
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(nameBytes)))
			bb.Buf = append(bb.Buf, nameBytes...)
			// Write buffer length and data
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(t.Buf)))
			bb.Buf = append(bb.Buf, t.Buf...)
		}
	}

	// Marshal tagMetadata
	if mp.tagMetadata == nil {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.tagMetadata)))
		for name, tm := range mp.tagMetadata {
			// Write name length and name
			nameBytes := []byte(name)
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(nameBytes)))
			bb.Buf = append(bb.Buf, nameBytes...)
			// Write buffer length and data
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(tm.Buf)))
			bb.Buf = append(bb.Buf, tm.Buf...)
		}
	}

	result := make([]byte, len(bb.Buf))
	copy(result, bb.Buf)
	return result, nil
}

// Unmarshal deserializes the memPart from []byte.
func (mp *memPart) Unmarshal(data []byte) error {
	mp.reset()

	// Unmarshal partMetadata
	tail, metadataLen := encoding.BytesToVarUint64(data)
	if uint64(len(tail)) < metadataLen {
		return fmt.Errorf("insufficient data for partMetadata: need %d bytes, have %d", metadataLen, len(tail))
	}
	metadataBytes := tail[:metadataLen]
	tail = tail[metadataLen:]
	if err := json.Unmarshal(metadataBytes, &mp.partMetadata); err != nil {
		return fmt.Errorf("cannot unmarshal partMetadata: %w", err)
	}

	// Unmarshal tagType
	tail, tagTypeLen := encoding.BytesToVarUint64(tail)
	if uint64(len(tail)) < tagTypeLen {
		return fmt.Errorf("insufficient data for tagType: need %d bytes, have %d", tagTypeLen, len(tail))
	}
	tagTypeBytes := tail[:tagTypeLen]
	tail = tail[tagTypeLen:]
	if err := mp.tagType.unmarshal(tagTypeBytes); err != nil {
		return fmt.Errorf("cannot unmarshal tagType: %w", err)
	}

	// Unmarshal traceIDFilter
	tail, filterLen := encoding.BytesToVarUint64(tail)
	if filterLen > 0 {
		if uint64(len(tail)) < filterLen {
			return fmt.Errorf("insufficient data for traceIDFilter: need %d bytes, have %d", filterLen, len(tail))
		}
		filterBytes := tail[:filterLen]
		tail = tail[filterLen:]
		bf := generateBloomFilter()
		defer releaseBloomFilter(bf)
		mp.traceIDFilter.filter = decodeBloomFilter(filterBytes, bf)
	} else {
		mp.traceIDFilter.filter = nil
	}

	// Unmarshal meta buffer
	tail, metaLen := encoding.BytesToVarUint64(tail)
	if uint64(len(tail)) < metaLen {
		return fmt.Errorf("insufficient data for meta buffer: need %d bytes, have %d", metaLen, len(tail))
	}
	mp.meta.Buf = append(mp.meta.Buf[:0], tail[:metaLen]...)
	tail = tail[metaLen:]

	// Unmarshal primary buffer
	tail, primaryLen := encoding.BytesToVarUint64(tail)
	if uint64(len(tail)) < primaryLen {
		return fmt.Errorf("insufficient data for primary buffer: need %d bytes, have %d", primaryLen, len(tail))
	}
	mp.primary.Buf = append(mp.primary.Buf[:0], tail[:primaryLen]...)
	tail = tail[primaryLen:]

	// Unmarshal spans
	tail, spansLen := encoding.BytesToVarUint64(tail)
	if uint64(len(tail)) < spansLen {
		return fmt.Errorf("insufficient data for spans buffer: need %d bytes, have %d", spansLen, len(tail))
	}
	mp.spans.Buf = append(mp.spans.Buf[:0], tail[:spansLen]...)
	tail = tail[spansLen:]

	var nameLen, bufLen uint64
	// Unmarshal tags
	tail, tagsCount := encoding.BytesToVarUint64(tail)
	if tagsCount > 0 {
		mp.tags = make(map[string]*bytes.Buffer)
		for i := uint64(0); i < tagsCount; i++ {
			// Read name length and name
			tail, nameLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < nameLen {
				return fmt.Errorf("insufficient data for tag name: need %d bytes, have %d", nameLen, len(tail))
			}
			name := string(tail[:nameLen])
			tail = tail[nameLen:]

			// Read buffer length and data
			tail, bufLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < bufLen {
				return fmt.Errorf("insufficient data for tag buffer: need %d bytes, have %d", bufLen, len(tail))
			}
			t := &bytes.Buffer{}
			t.Buf = append(t.Buf[:0], tail[:bufLen]...)
			mp.tags[name] = t
			tail = tail[bufLen:]
		}
	}

	// Unmarshal tagMetadata
	tail, tagMetadataCount := encoding.BytesToVarUint64(tail)
	if tagMetadataCount > 0 {
		mp.tagMetadata = make(map[string]*bytes.Buffer)
		for i := uint64(0); i < tagMetadataCount; i++ {
			// Read name length and name
			tail, nameLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < nameLen {
				return fmt.Errorf("insufficient data for tag metadata name: need %d bytes, have %d", nameLen, len(tail))
			}
			name := string(tail[:nameLen])
			tail = tail[nameLen:]

			// Read buffer length and data
			tail, bufLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < bufLen {
				return fmt.Errorf("insufficient data for tag metadata buffer: need %d bytes, have %d", bufLen, len(tail))
			}
			tm := &bytes.Buffer{}
			tm.Buf = append(tm.Buf[:0], tail[:bufLen]...)
			mp.tagMetadata[name] = tm
			tail = tail[bufLen:]
		}
	}

	if len(tail) > 0 {
		return fmt.Errorf("unexpected trailing data: %d bytes", len(tail))
	}

	return nil
}

func (mp *memPart) mustInitFromPart(p *part) {
	mp.reset()

	// Copy part metadata, tagType and traceIDFilter
	mp.partMetadata = p.partMetadata
	mp.tagType = p.tagType
	mp.traceIDFilter = p.traceIDFilter

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

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	for _, tid := range ts.traceIDs {
		if len(tid) > int(bsw.traceIDLen) {
			bsw.traceIDLen = uint32(len(tid))
		}
	}

	var tidPrev string
	uncompressedSpansSizeBytes := uint64(0)
	var indexPrev int
	for i := range ts.spans {
		tid := ts.traceIDs[i]
		if tidPrev == "" {
			tidPrev = tid
		}

		if uncompressedSpansSizeBytes >= maxUncompressedSpanSize || tid != tidPrev {
			bsw.MustWriteTrace(tidPrev, ts.spans[indexPrev:i], ts.tags[indexPrev:i], ts.timestamps[indexPrev:i])
			tidPrev = tid
			indexPrev = i
			uncompressedSpansSizeBytes = 0
		}
		uncompressedSpansSizeBytes += uint64(len(ts.spans[i]))
	}
	bsw.MustWriteTrace(tidPrev, ts.spans[indexPrev:], ts.tags[indexPrev:], ts.timestamps[indexPrev:])
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

	mp.partMetadata.mustWriteMetadata(fileSystem, path)
	mp.tagType.mustWriteTagType(fileSystem, path)
	mp.traceIDFilter.mustWriteTraceIDFilter(fileSystem, path)
	if mp.traceIDFilter.filter != nil {
		releaseBloomFilter(mp.traceIDFilter.filter)
		mp.traceIDFilter.filter = nil
	}

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
