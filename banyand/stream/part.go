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
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	// Streaming file names (without extensions).
	streamPrimaryName       = "primary"
	streamMetaName          = "meta"
	streamTimestampsName    = "timestamps"
	streamTagFamiliesPrefix = "tf:"
	streamTagMetadataPrefix = "tfm:"
	streamTagFilterPrefix   = "tff:"

	metadataFilename               = "metadata.json"
	primaryFilename                = streamPrimaryName + ".bin"
	metaFilename                   = streamMetaName + ".bin"
	timestampsFilename             = streamTimestampsName + ".bin"
	elementIndexFilename           = "idx"
	tagFamiliesMetadataFilenameExt = ".tfm"
	tagFamiliesFilenameExt         = ".tf"
	tagFamiliesFilterFilenameExt   = ".tff"
)

type part struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	tagFamilyFilter      map[string]fs.Reader
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

func (p *part) close() {
	fs.MustClose(p.primary)
	fs.MustClose(p.timestamps)
	for _, tf := range p.tagFamilies {
		fs.MustClose(tf)
	}
	for _, tfh := range p.tagFamilyMetadata {
		fs.MustClose(tfh)
	}
	for _, tff := range p.tagFamilyFilter {
		fs.MustClose(tff)
	}
}

func (p *part) String() string {
	return fmt.Sprintf("part %d", p.partMetadata.ID)
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], &mp.meta)

	// Open data files
	p.primary = &mp.primary
	p.timestamps = &mp.timestamps
	if mp.tagFamilies != nil {
		p.tagFamilies = make(map[string]fs.Reader)
		p.tagFamilyMetadata = make(map[string]fs.Reader)
		p.tagFamilyFilter = make(map[string]fs.Reader)
		for name, tf := range mp.tagFamilies {
			p.tagFamilies[name] = tf
			p.tagFamilyMetadata[name] = mp.tagFamilyMetadata[name]
			p.tagFamilyFilter[name] = mp.tagFamilyFilter[name]
		}
	}
	return &p
}

type memPart struct {
	tagFamilyMetadata map[string]*bytes.Buffer
	tagFamilies       map[string]*bytes.Buffer
	tagFamilyFilter   map[string]*bytes.Buffer
	meta              bytes.Buffer
	primary           bytes.Buffer
	timestamps        bytes.Buffer
	partMetadata      partMetadata
}

func (mp *memPart) mustCreateMemTagFamilyWriters(name string) (fs.Writer, fs.Writer, fs.Writer) {
	if mp.tagFamilies == nil {
		mp.tagFamilies = make(map[string]*bytes.Buffer)
		mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
		mp.tagFamilyFilter = make(map[string]*bytes.Buffer)
	}
	tf, ok := mp.tagFamilies[name]
	tfh := mp.tagFamilyMetadata[name]
	tff := mp.tagFamilyFilter[name]
	if ok {
		tf.Reset()
		tfh.Reset()
		tff.Reset()
		return tfh, tf, tff
	}
	mp.tagFamilies[name] = &bytes.Buffer{}
	mp.tagFamilyMetadata[name] = &bytes.Buffer{}
	mp.tagFamilyFilter[name] = &bytes.Buffer{}
	return mp.tagFamilyMetadata[name], mp.tagFamilies[name], mp.tagFamilyFilter[name]
}

func (mp *memPart) reset() {
	mp.partMetadata.reset()
	mp.meta.Reset()
	mp.primary.Reset()
	mp.timestamps.Reset()
	if mp.tagFamilies != nil {
		for k, tf := range mp.tagFamilies {
			tf.Reset()
			delete(mp.tagFamilies, k)
		}
	}
	if mp.tagFamilyMetadata != nil {
		for k, tfh := range mp.tagFamilyMetadata {
			tfh.Reset()
			delete(mp.tagFamilyMetadata, k)
		}
	}
	if mp.tagFamilyFilter != nil {
		for k, tff := range mp.tagFamilyFilter {
			tff.Reset()
			delete(mp.tagFamilyFilter, k)
		}
	}
}

// Marshal serializes the memPart to []byte for network transmission.
// The format is:
// - partMetadata (JSON)
// - meta buffer length + data
// - primary buffer length + data
// - timestamps buffer length + data
// - tagFamilies count + (name + buffer length + data) for each
// - tagFamilyMetadata count + (name + buffer length + data) for each
// - tagFamilyFilter count + (name + buffer length + data) for each.
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

	// Marshal meta buffer
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.meta.Buf)))
	bb.Buf = append(bb.Buf, mp.meta.Buf...)

	// Marshal primary buffer
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.primary.Buf)))
	bb.Buf = append(bb.Buf, mp.primary.Buf...)

	// Marshal timestamps buffer
	bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.timestamps.Buf)))
	bb.Buf = append(bb.Buf, mp.timestamps.Buf...)

	// Marshal tagFamilies
	if mp.tagFamilies == nil {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.tagFamilies)))
		for name, tf := range mp.tagFamilies {
			// Write name length and name
			nameBytes := []byte(name)
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(nameBytes)))
			bb.Buf = append(bb.Buf, nameBytes...)
			// Write buffer length and data
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(tf.Buf)))
			bb.Buf = append(bb.Buf, tf.Buf...)
		}
	}

	// Marshal tagFamilyMetadata
	if mp.tagFamilyMetadata == nil {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.tagFamilyMetadata)))
		for name, tfh := range mp.tagFamilyMetadata {
			// Write name length and name
			nameBytes := []byte(name)
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(nameBytes)))
			bb.Buf = append(bb.Buf, nameBytes...)
			// Write buffer length and data
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(tfh.Buf)))
			bb.Buf = append(bb.Buf, tfh.Buf...)
		}
	}

	// Marshal tagFamilyFilter
	if mp.tagFamilyFilter == nil {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, 0)
	} else {
		bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(mp.tagFamilyFilter)))
		for name, tff := range mp.tagFamilyFilter {
			// Write name length and name
			nameBytes := []byte(name)
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(nameBytes)))
			bb.Buf = append(bb.Buf, nameBytes...)
			// Write buffer length and data
			bb.Buf = encoding.VarUint64ToBytes(bb.Buf, uint64(len(tff.Buf)))
			bb.Buf = append(bb.Buf, tff.Buf...)
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

	// Unmarshal timestamps buffer
	tail, timestampsLen := encoding.BytesToVarUint64(tail)
	if uint64(len(tail)) < timestampsLen {
		return fmt.Errorf("insufficient data for timestamps buffer: need %d bytes, have %d", timestampsLen, len(tail))
	}
	mp.timestamps.Buf = append(mp.timestamps.Buf[:0], tail[:timestampsLen]...)
	tail = tail[timestampsLen:]

	var nameLen, bufLen uint64
	// Unmarshal tagFamilies
	tail, tagFamiliesCount := encoding.BytesToVarUint64(tail)
	if tagFamiliesCount > 0 {
		mp.tagFamilies = make(map[string]*bytes.Buffer)
		for i := uint64(0); i < tagFamiliesCount; i++ {
			// Read name length and name
			tail, nameLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < nameLen {
				return fmt.Errorf("insufficient data for tag family name: need %d bytes, have %d", nameLen, len(tail))
			}
			name := string(tail[:nameLen])
			tail = tail[nameLen:]

			// Read buffer length and data
			tail, bufLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < bufLen {
				return fmt.Errorf("insufficient data for tag family buffer: need %d bytes, have %d", bufLen, len(tail))
			}
			tf := &bytes.Buffer{}
			tf.Buf = append(tf.Buf[:0], tail[:bufLen]...)
			mp.tagFamilies[name] = tf
			tail = tail[bufLen:]
		}
	}

	// Unmarshal tagFamilyMetadata
	tail, tagFamilyMetadataCount := encoding.BytesToVarUint64(tail)
	if tagFamilyMetadataCount > 0 {
		mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
		for i := uint64(0); i < tagFamilyMetadataCount; i++ {
			// Read name length and name
			tail, nameLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < nameLen {
				return fmt.Errorf("insufficient data for tag family metadata name: need %d bytes, have %d", nameLen, len(tail))
			}
			name := string(tail[:nameLen])
			tail = tail[nameLen:]

			// Read buffer length and data
			tail, bufLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < bufLen {
				return fmt.Errorf("insufficient data for tag family metadata buffer: need %d bytes, have %d", bufLen, len(tail))
			}
			tfh := &bytes.Buffer{}
			tfh.Buf = append(tfh.Buf[:0], tail[:bufLen]...)
			mp.tagFamilyMetadata[name] = tfh
			tail = tail[bufLen:]
		}
	}

	// Unmarshal tagFamilyFilter
	tail, tagFamilyFilterCount := encoding.BytesToVarUint64(tail)
	if tagFamilyFilterCount > 0 {
		mp.tagFamilyFilter = make(map[string]*bytes.Buffer)
		for i := uint64(0); i < tagFamilyFilterCount; i++ {
			// Read name length and name
			tail, nameLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < nameLen {
				return fmt.Errorf("insufficient data for tag family filter name: need %d bytes, have %d", nameLen, len(tail))
			}
			name := string(tail[:nameLen])
			tail = tail[nameLen:]

			// Read buffer length and data
			tail, bufLen = encoding.BytesToVarUint64(tail)
			if uint64(len(tail)) < bufLen {
				return fmt.Errorf("insufficient data for tag family filter buffer: need %d bytes, have %d", bufLen, len(tail))
			}
			tff := &bytes.Buffer{}
			tff.Buf = append(tff.Buf[:0], tail[:bufLen]...)
			mp.tagFamilyFilter[name] = tff
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

	// Copy part metadata
	mp.partMetadata = p.partMetadata

	// Read primary data
	sr := p.primary.SequentialRead()
	data, err := io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read primary data from %s: %s", p.primary.Path(), err)
	}
	fs.MustClose(sr)
	mp.primary.Buf = append(mp.primary.Buf[:0], data...)

	// Read timestamps data
	sr = p.timestamps.SequentialRead()
	data, err = io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read timestamps data from %s: %s", p.timestamps.Path(), err)
	}
	fs.MustClose(sr)
	mp.timestamps.Buf = append(mp.timestamps.Buf[:0], data...)

	// Read tag families data
	if p.tagFamilies != nil {
		mp.tagFamilies = make(map[string]*bytes.Buffer)
		mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
		mp.tagFamilyFilter = make(map[string]*bytes.Buffer)

		for name, reader := range p.tagFamilies {
			sr = reader.SequentialRead()
			data, err = io.ReadAll(sr)
			if err != nil {
				logger.Panicf("cannot read tag family data from %s: %s", reader.Path(), err)
			}
			fs.MustClose(sr)

			mp.tagFamilies[name] = &bytes.Buffer{}
			mp.tagFamilies[name].Buf = append(mp.tagFamilies[name].Buf[:0], data...)
		}

		for name, reader := range p.tagFamilyMetadata {
			sr = reader.SequentialRead()
			data, err = io.ReadAll(sr)
			if err != nil {
				logger.Panicf("cannot read tag family metadata from %s: %s", reader.Path(), err)
			}
			fs.MustClose(sr)

			mp.tagFamilyMetadata[name] = &bytes.Buffer{}
			mp.tagFamilyMetadata[name].Buf = append(mp.tagFamilyMetadata[name].Buf[:0], data...)
		}

		for name, reader := range p.tagFamilyFilter {
			sr = reader.SequentialRead()
			data, err = io.ReadAll(sr)
			if err != nil {
				logger.Panicf("cannot read tag family filter from %s: %s", reader.Path(), err)
			}
			fs.MustClose(sr)

			mp.tagFamilyFilter[name] = &bytes.Buffer{}
			mp.tagFamilyFilter[name].Buf = append(mp.tagFamilyFilter[name].Buf[:0], data...)
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

func (mp *memPart) mustInitFromElements(es *elements) {
	mp.reset()

	if len(es.timestamps) == 0 {
		return
	}

	sort.Sort(es)

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	var sidPrev common.SeriesID
	uncompressedBlockSizeBytes := uint64(0)
	var indexPrev int
	for i := range es.timestamps {
		sid := es.seriesIDs[i]
		if sidPrev == 0 {
			sidPrev = sid
		}

		if uncompressedBlockSizeBytes >= maxUncompressedBlockSize || sid != sidPrev {
			bsw.MustWriteElements(sidPrev, es.timestamps[indexPrev:i], es.elementIDs[indexPrev:i], es.tagFamilies[indexPrev:i])
			sidPrev = sid
			indexPrev = i
			uncompressedBlockSizeBytes = 0
		}
		uncompressedBlockSizeBytes += uncompressedElementSizeBytes(i, es)
	}
	bsw.MustWriteElements(sidPrev, es.timestamps[indexPrev:], es.elementIDs[indexPrev:], es.tagFamilies[indexPrev:])
	bsw.Flush(&mp.partMetadata)
	releaseBlockWriter(bsw)
}

func (mp *memPart) mustFlush(fileSystem fs.FileSystem, path string) {
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)

	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(path, metaFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(path, primaryFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.timestamps.Buf, filepath.Join(path, timestampsFilename), storage.FilePerm)
	for name, tf := range mp.tagFamilies {
		fs.MustFlush(fileSystem, tf.Buf, filepath.Join(path, name+tagFamiliesFilenameExt), storage.FilePerm)
	}
	for name, tfh := range mp.tagFamilyMetadata {
		fs.MustFlush(fileSystem, tfh.Buf, filepath.Join(path, name+tagFamiliesMetadataFilenameExt), storage.FilePerm)
	}
	for name, tfh := range mp.tagFamilyFilter {
		fs.MustFlush(fileSystem, tfh.Buf, filepath.Join(path, name+tagFamiliesFilterFilenameExt), storage.FilePerm)
	}

	mp.partMetadata.mustWriteMetadata(fileSystem, path)

	fileSystem.SyncPath(path)
}

func uncompressedElementSizeBytes(index int, es *elements) uint64 {
	// 8 bytes for timestamp
	// 8 bytes for elementID
	n := uint64(8 + 8)
	for i := range es.tagFamilies[index] {
		n += uint64(len(es.tagFamilies[index][i].tag))
		for j := range es.tagFamilies[index][i].values {
			n += uint64(es.tagFamilies[index][i].values[j].size())
		}
	}
	return n
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

var memPartPool = pool.Register[*memPart]("stream-memPart")

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

	metaPath := path.Join(partPath, metaFilename)
	pr := mustOpenReader(metaPath, fileSystem)
	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], pr)
	fs.MustClose(pr)

	p.primary = mustOpenReader(path.Join(partPath, primaryFilename), fileSystem)
	p.timestamps = mustOpenReader(path.Join(partPath, timestampsFilename), fileSystem)
	ee := fileSystem.ReadDir(partPath)
	for _, e := range ee {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == tagFamiliesMetadataFilenameExt {
			if p.tagFamilyMetadata == nil {
				p.tagFamilyMetadata = make(map[string]fs.Reader)
			}
			p.tagFamilyMetadata[removeExt(e.Name(), tagFamiliesMetadataFilenameExt)] = mustOpenReader(path.Join(partPath, e.Name()), fileSystem)
		}
		if filepath.Ext(e.Name()) == tagFamiliesFilenameExt {
			if p.tagFamilies == nil {
				p.tagFamilies = make(map[string]fs.Reader)
			}
			p.tagFamilies[removeExt(e.Name(), tagFamiliesFilenameExt)] = mustOpenReader(path.Join(partPath, e.Name()), fileSystem)
		}
		if filepath.Ext(e.Name()) == tagFamiliesFilterFilenameExt {
			if p.tagFamilyFilter == nil {
				p.tagFamilyFilter = make(map[string]fs.Reader)
			}
			p.tagFamilyFilter[removeExt(e.Name(), tagFamiliesFilterFilenameExt)] = mustOpenReader(path.Join(partPath, e.Name()), fileSystem)
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

// CreatePartFileReaderFromPath opens all files in a part directory and returns their FileInfo and a cleanup function.
func CreatePartFileReaderFromPath(partPath string, lfs fs.FileSystem) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo
	var readers []fs.Reader

	metaPath := path.Join(partPath, metaFilename)
	metaReader, err := lfs.OpenFile(metaPath)
	if err != nil {
		logger.Panicf("cannot open meta file %q: %s", metaPath, err)
	}
	readers = append(readers, metaReader)
	files = append(files, queue.FileInfo{
		Name:   streamMetaName,
		Reader: metaReader.SequentialRead(),
	})

	primaryPath := path.Join(partPath, primaryFilename)
	primaryReader, err := lfs.OpenFile(primaryPath)
	if err != nil {
		logger.Panicf("cannot open primary file %q: %s", primaryPath, err)
	}
	readers = append(readers, primaryReader)
	files = append(files, queue.FileInfo{
		Name:   streamPrimaryName,
		Reader: primaryReader.SequentialRead(),
	})

	timestampsPath := path.Join(partPath, timestampsFilename)
	timestampsReader, err := lfs.OpenFile(timestampsPath)
	if err != nil {
		logger.Panicf("cannot open timestamps file %q: %s", timestampsPath, err)
	}
	readers = append(readers, timestampsReader)
	files = append(files, queue.FileInfo{
		Name:   streamTimestampsName,
		Reader: timestampsReader.SequentialRead(),
	})

	ee := lfs.ReadDir(partPath)
	for _, e := range ee {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == tagFamiliesMetadataFilenameExt {
			tfmPath := path.Join(partPath, e.Name())
			tfmReader, err := lfs.OpenFile(tfmPath)
			if err != nil {
				logger.Panicf("cannot open tag family metadata file %q: %s", tfmPath, err)
			}
			readers = append(readers, tfmReader)
			tagName := removeExt(e.Name(), tagFamiliesMetadataFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   streamTagMetadataPrefix + tagName,
				Reader: tfmReader.SequentialRead(),
			})
		}
		if filepath.Ext(e.Name()) == tagFamiliesFilenameExt {
			tfPath := path.Join(partPath, e.Name())
			tfReader, err := lfs.OpenFile(tfPath)
			if err != nil {
				logger.Panicf("cannot open tag family file %q: %s", tfPath, err)
			}
			readers = append(readers, tfReader)
			tagName := removeExt(e.Name(), tagFamiliesFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   streamTagFamiliesPrefix + tagName,
				Reader: tfReader.SequentialRead(),
			})
		}
		if filepath.Ext(e.Name()) == tagFamiliesFilterFilenameExt {
			tffPath := path.Join(partPath, e.Name())
			tffReader, err := lfs.OpenFile(tffPath)
			if err != nil {
				logger.Panicf("cannot open tag family filter file %q: %s", tffPath, err)
			}
			readers = append(readers, tffReader)
			tagName := removeExt(e.Name(), tagFamiliesFilterFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   streamTagFilterPrefix + tagName,
				Reader: tffReader.SequentialRead(),
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

func partPath(root string, epoch uint64) string {
	return filepath.Join(root, partName(epoch))
}

func partName(epoch uint64) string {
	return fmt.Sprintf("%016x", epoch)
}
