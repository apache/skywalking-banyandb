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
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	// Streaming file names for measure data parts (without extensions).
	measurePrimaryName        = "primary"
	measureMetaName           = "meta"
	measureTimestampsName     = "timestamps"
	measureFieldValuesName    = "fv"
	measureTagFamiliesPrefix  = "tf:"
	measureTagMetadataPrefix  = "tfm:"
	measureSeriesMetadataName = "smeta"

	metadataFilename               = "metadata.json"
	primaryFilename                = measurePrimaryName + ".bin"
	metaFilename                   = measureMetaName + ".bin"
	timestampsFilename             = measureTimestampsName + ".bin"
	fieldValuesFilename            = measureFieldValuesName + ".bin"
	seriesMetadataFilename         = measureSeriesMetadataName + ".bin"
	tagFamiliesMetadataFilenameExt = ".tfm"
	tagFamiliesFilenameExt         = ".tf"
)

type part struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fieldValues          fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	seriesMetadata       fs.Reader // Optional: series metadata reader
	cache                storage.Cache
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
	shardID              common.ShardID
}

func (p *part) close() {
	fs.MustClose(p.primary)
	fs.MustClose(p.timestamps)
	fs.MustClose(p.fieldValues)
	if p.seriesMetadata != nil {
		fs.MustClose(p.seriesMetadata)
	}
	for _, tf := range p.tagFamilies {
		fs.MustClose(tf)
	}
	for _, tfh := range p.tagFamilyMetadata {
		fs.MustClose(tfh)
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
	p.fieldValues = &mp.fieldValues
	if mp.tagFamilies != nil {
		p.tagFamilies = make(map[string]fs.Reader)
		p.tagFamilyMetadata = make(map[string]fs.Reader)
		for name, tf := range mp.tagFamilies {
			p.tagFamilies[name] = tf
			p.tagFamilyMetadata[name] = mp.tagFamilyMetadata[name]
		}
	}
	return &p
}

type memPart struct {
	tagFamilyMetadata map[string]*bytes.Buffer
	tagFamilies       map[string]*bytes.Buffer
	meta              bytes.Buffer
	primary           bytes.Buffer
	timestamps        bytes.Buffer
	fieldValues       bytes.Buffer
	seriesMetadata    bytes.Buffer
	partMetadata      partMetadata
	segmentID         int64
}

func (mp *memPart) mustCreateMemTagFamilyWriters(name string) (fs.Writer, fs.Writer) {
	if mp.tagFamilies == nil {
		mp.tagFamilies = make(map[string]*bytes.Buffer)
		mp.tagFamilyMetadata = make(map[string]*bytes.Buffer)
	}
	tf, ok := mp.tagFamilies[name]
	tfh := mp.tagFamilyMetadata[name]
	if ok {
		tf.Reset()
		tfh.Reset()
		return tfh, tf
	}
	mp.tagFamilies[name] = &bytes.Buffer{}
	mp.tagFamilyMetadata[name] = &bytes.Buffer{}
	return mp.tagFamilyMetadata[name], mp.tagFamilies[name]
}

func (mp *memPart) reset() {
	mp.partMetadata.reset()
	mp.meta.Reset()
	mp.primary.Reset()
	mp.timestamps.Reset()
	mp.fieldValues.Reset()
	mp.seriesMetadata.Reset()
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
}

func (mp *memPart) mustInitFromDataPoints(dps *dataPoints) {
	mp.reset()

	if len(dps.timestamps) == 0 {
		return
	}

	sort.Sort(dps)

	bsw := generateBlockWriter()
	bsw.MustInitForMemPart(mp)
	var sidPrev common.SeriesID
	uncompressedBlockSizeBytes := uint64(0)
	var indexPrev int
	var tsPrev int64
	for i := 0; i < len(dps.timestamps); i++ {
		sid := dps.seriesIDs[i]
		if sidPrev == 0 {
			sidPrev = sid
		}

		if sid == sidPrev {
			if tsPrev == dps.timestamps[i] {
				dps.skip(i)
				i--
				continue
			}
			tsPrev = dps.timestamps[i]
		}

		if uncompressedBlockSizeBytes >= maxUncompressedBlockSize ||
			(i-indexPrev) > maxBlockLength || sid != sidPrev {
			bsw.MustWriteDataPoints(sidPrev, dps.timestamps[indexPrev:i], dps.versions[indexPrev:i], dps.tagFamilies[indexPrev:i], dps.fields[indexPrev:i])
			sidPrev = sid
			indexPrev = i
			tsPrev = dps.timestamps[indexPrev]
			uncompressedBlockSizeBytes = 0
		}
		uncompressedBlockSizeBytes += uncompressedDataPointSizeBytes(i, dps)
	}
	bsw.MustWriteDataPoints(sidPrev, dps.timestamps[indexPrev:], dps.versions[indexPrev:], dps.tagFamilies[indexPrev:], dps.fields[indexPrev:])
	bsw.Flush(&mp.partMetadata)
	releaseBlockWriter(bsw)
}

func (mp *memPart) mustFlush(fileSystem fs.FileSystem, path string) {
	fileSystem.MkdirPanicIfExist(path, storage.DirPerm)

	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(path, metaFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(path, primaryFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.timestamps.Buf, filepath.Join(path, timestampsFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.fieldValues.Buf, filepath.Join(path, fieldValuesFilename), storage.FilePerm)
	for name, tf := range mp.tagFamilies {
		fs.MustFlush(fileSystem, tf.Buf, filepath.Join(path, name+tagFamiliesFilenameExt), storage.FilePerm)
	}
	for name, tfh := range mp.tagFamilyMetadata {
		fs.MustFlush(fileSystem, tfh.Buf, filepath.Join(path, name+tagFamiliesMetadataFilenameExt), storage.FilePerm)
	}

	// Flush series metadata if available
	if len(mp.seriesMetadata.Buf) > 0 {
		fs.MustFlush(fileSystem, mp.seriesMetadata.Buf, filepath.Join(path, seriesMetadataFilename), storage.FilePerm)
	}

	mp.partMetadata.mustWriteMetadata(fileSystem, path)

	fileSystem.SyncPath(path)
}

func uncompressedDataPointSizeBytes(index int, dps *dataPoints) uint64 {
	// 8 bytes for timestamp
	// 8 bytes for version
	n := uint64(8 + 8)
	n += uint64(len(dps.fields[index].name))
	for i := range dps.fields[index].values {
		n += uint64(dps.fields[index].values[i].size())
	}
	for i := range dps.tagFamilies[index] {
		n += uint64(len(dps.tagFamilies[index][i].name))
		for j := range dps.tagFamilies[index][i].values {
			n += uint64(dps.tagFamilies[index][i].values[j].size())
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

var memPartPool = pool.Register[*memPart]("measure-memPart")

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
	p.fieldValues = mustOpenReader(path.Join(partPath, fieldValuesFilename), fileSystem)

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

// CreatePartFileReaderFromPath opens all files in a measure part directory and returns their FileInfo and a cleanup function.
// Similar to stream.CreatePartFileReaderFromPath but adapted for measure file structure.
func CreatePartFileReaderFromPath(partPath string, lfs fs.FileSystem) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo
	var readers []fs.Reader

	// Core measure files (required files)
	coreFiles := map[string]string{
		metaFilename:        measureMetaName,
		primaryFilename:     measurePrimaryName,
		timestampsFilename:  measureTimestampsName,
		fieldValuesFilename: measureFieldValuesName,
	}

	for filename, streamName := range coreFiles {
		filePath := path.Join(partPath, filename)
		reader, err := lfs.OpenFile(filePath)
		if err != nil {
			logger.Panicf("cannot open measure file %q: %s", filePath, err)
		}
		readers = append(readers, reader)
		files = append(files, queue.FileInfo{
			Name:   streamName,
			Reader: reader.SequentialRead(),
		})
	}

	// Dynamic tag family files (*.tf and *.tfm)
	ee := lfs.ReadDir(partPath)
	for _, e := range ee {
		if e.IsDir() {
			continue
		}

		// Tag family metadata files (.tfm)
		if filepath.Ext(e.Name()) == tagFamiliesMetadataFilenameExt {
			tfmPath := path.Join(partPath, e.Name())
			tfmReader, err := lfs.OpenFile(tfmPath)
			if err != nil {
				logger.Panicf("cannot open tag family metadata file %q: %s", tfmPath, err)
			}
			readers = append(readers, tfmReader)
			tagName := removeExt(e.Name(), tagFamiliesMetadataFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   measureTagMetadataPrefix + tagName,
				Reader: tfmReader.SequentialRead(),
			})
		}

		// Tag family data files (.tf)
		if filepath.Ext(e.Name()) == tagFamiliesFilenameExt {
			tfPath := path.Join(partPath, e.Name())
			tfReader, err := lfs.OpenFile(tfPath)
			if err != nil {
				logger.Panicf("cannot open tag family file %q: %s", tfPath, err)
			}
			readers = append(readers, tfReader)
			tagName := removeExt(e.Name(), tagFamiliesFilenameExt)
			files = append(files, queue.FileInfo{
				Name:   measureTagFamiliesPrefix + tagName,
				Reader: tfReader.SequentialRead(),
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
