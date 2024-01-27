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
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	metadataFilename               = "metadata.json"
	primaryFilename                = "primary.bin"
	metaFilename                   = "meta.bin"
	timestampsFilename             = "timestamps.bin"
	elementIDsFilename             = "elementIDs.bin"
	elementIndexFilename           = "idx"
	tagFamiliesMetadataFilenameExt = ".tfm"
	tagFamiliesFilenameExt         = ".tf"
)

type columnElements struct {
	elementID []string
	// TODO: change it to 1d array after refactoring low-level query
	tagFamilies [][]pbv1.TagFamily
	timestamp   []int64
}

func newColumnElements() *columnElements {
	ces := &columnElements{}
	ces.elementID = make([]string, 0)
	ces.tagFamilies = make([][]pbv1.TagFamily, 0)
	ces.timestamp = make([]int64, 0)
	return ces
}

func (ces *columnElements) BuildFromElement(e *element, tp []pbv1.TagProjection) {
	tagFamilies := make([]pbv1.TagFamily, 0)
	for i, tf := range e.tagFamilies {
		tagFamily := pbv1.TagFamily{
			Name: tf.name,
		}
		for j, t := range tf.tags {
			tag := pbv1.Tag{
				Name: t.name,
			}
			if tag.Name == "" {
				tag.Name = tp[i].Names[j]
				tag.Values = append(tag.Values, pbv1.NullTagValue)
			} else {
				tag.Values = append(tag.Values, mustDecodeTagValue(t.valueType, t.values[e.index]))
			}
			tagFamily.Tags = append(tagFamily.Tags, tag)
		}
		tagFamilies = append(tagFamilies, tagFamily)
	}
	ces.tagFamilies = append(ces.tagFamilies, tagFamilies)
	ces.elementID = append(ces.elementID, e.elementID)
	ces.timestamp = append(ces.timestamp, e.timestamp)
}

func (ces *columnElements) Pull() *pbv1.StreamColumnResult {
	r := &pbv1.StreamColumnResult{}
	r.Timestamps = make([]int64, 0)
	r.ElementIDs = make([]string, 0)
	r.TagFamilies = make([][]pbv1.TagFamily, len(ces.tagFamilies))
	r.Timestamps = append(r.Timestamps, ces.timestamp...)
	r.ElementIDs = append(r.ElementIDs, ces.elementID...)
	for i, tfs := range ces.tagFamilies {
		r.TagFamilies[i] = make([]pbv1.TagFamily, 0)
		r.TagFamilies[i] = append(r.TagFamilies[i], tfs...)
	}
	return r
}

type element struct {
	elementID   string
	tagFamilies []*tagFamily
	timestamp   int64
	index       int
}

type part struct {
	path                 string
	primary              fs.Reader
	timestamps           fs.Reader
	elementIDs           fs.Reader
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

func (p *part) containTimestamp(timestamp common.ItemID) bool {
	return timestamp >= common.ItemID(p.partMetadata.MinTimestamp) && timestamp <= common.ItemID(p.partMetadata.MaxTimestamp)
}

func (p *part) close() {
	fs.MustClose(p.primary)
	fs.MustClose(p.timestamps)
	fs.MustClose(p.elementIDs)
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

func (p *part) getElement(seriesID common.SeriesID, timestamp common.ItemID, tagProjection []pbv1.TagProjection) (*element, error) {
	// TODO: refactor to column-based query
	// TODO: cache blocks
	for i, primaryMeta := range p.primaryBlockMetadata {
		if i != len(p.primaryBlockMetadata)-1 && seriesID >= p.primaryBlockMetadata[i+1].seriesID {
			continue
		}
		if seriesID < p.primaryBlockMetadata[i].seriesID ||
			timestamp < common.ItemID(p.primaryBlockMetadata[i].minTimestamp) ||
			timestamp > common.ItemID(p.primaryBlockMetadata[i].maxTimestamp) {
			break
		}

		compressedPrimaryBuf := make([]byte, primaryMeta.size)
		fs.MustReadData(p.primary, int64(primaryMeta.offset), compressedPrimaryBuf)
		var err error
		primaryBuf := make([]byte, 0)
		primaryBuf, err = zstd.Decompress(primaryBuf[:0], compressedPrimaryBuf)
		if err != nil {
			return nil, fmt.Errorf("cannot decompress index block: %w", err)
		}
		bm := make([]blockMetadata, 0)
		bm, err = unmarshalBlockMetadata(bm, primaryBuf)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal index block: %w", err)
		}
		var targetBlockMetadata blockMetadata
		for _, blockMetadata := range bm {
			if blockMetadata.seriesID == seriesID {
				targetBlockMetadata = blockMetadata
				break
			}
		}

		timestamps := make([]int64, 0)
		timestamps = mustReadTimestampsFrom(timestamps, &targetBlockMetadata.timestamps, int(targetBlockMetadata.count), p.timestamps)
		for i, ts := range timestamps {
			if timestamp == common.ItemID(ts) {
				elementIDs := make([]string, 0)
				elementIDs = mustReadElementIDsFrom(elementIDs, &targetBlockMetadata.elementIDs, int(targetBlockMetadata.count), p.elementIDs)
				tfs := make([]*tagFamily, 0)
				for j := range tagProjection {
					name := tagProjection[j].Family
					block, ok := targetBlockMetadata.tagFamilies[name]
					if !ok {
						continue
					}
					decoder := &encoding.BytesBlockDecoder{}
					tf := unmarshalTagFamily(decoder, name, block, tagProjection[j].Names, p.tagFamilyMetadata[name], p.tagFamilies[name], len(timestamps))
					tfs = append(tfs, tf)
				}

				return &element{
					timestamp:   timestamps[i],
					elementID:   elementIDs[i],
					tagFamilies: tfs,
					index:       i,
				}, nil
			}
			if common.ItemID(ts) > timestamp {
				break
			}
		}
	}
	return nil, errors.New("element not found")
}

func unmarshalTagFamily(decoder *encoding.BytesBlockDecoder, name string,
	tagFamilyMetadataBlock *dataBlock, tagProjection []string, metaReader, valueReader fs.Reader, count int,
) *tagFamily {
	if len(tagProjection) < 1 {
		return &tagFamily{}
	}
	bb := bigValuePool.Generate()
	bb.Buf = bytes.ResizeExact(bb.Buf, int(tagFamilyMetadataBlock.size))
	fs.MustReadData(metaReader, int64(tagFamilyMetadataBlock.offset), bb.Buf)
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	err := tfm.unmarshal(bb.Buf)
	if err != nil {
		logger.Panicf("%s: cannot unmarshal tagFamilyMetadata: %v", metaReader.Path(), err)
	}
	bigValuePool.Release(bb)
	tf := tagFamily{}
	tf.name = name
	tf.tags = tf.resizeTags(len(tagProjection))

	for j := range tagProjection {
		for i := range tfm.tagMetadata {
			if tagProjection[j] == tfm.tagMetadata[i].name {
				tf.tags[j].mustReadValues(decoder, valueReader, tfm.tagMetadata[i], uint64(count))
				break
			}
		}
	}
	return &tf
}

func openMemPart(mp *memPart) *part {
	var p part
	p.partMetadata = mp.partMetadata

	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], &mp.meta)

	// Open data files
	p.primary = &mp.primary
	p.timestamps = &mp.timestamps
	p.elementIDs = &mp.elementIDs
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
	elementIDs        bytes.Buffer
	partMetadata      partMetadata
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
	mp.elementIDs.Reset()
	if mp.tagFamilies != nil {
		for _, tf := range mp.tagFamilies {
			tf.Reset()
		}
	}
	if mp.tagFamilyMetadata != nil {
		for _, tfh := range mp.tagFamilyMetadata {
			tfh.Reset()
		}
	}
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
	fileSystem.MkdirPanicIfExist(path, dirPermission)

	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(path, metaFilename), filePermission)
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(path, primaryFilename), filePermission)
	fs.MustFlush(fileSystem, mp.timestamps.Buf, filepath.Join(path, timestampsFilename), filePermission)
	fs.MustFlush(fileSystem, mp.elementIDs.Buf, filepath.Join(path, elementIDsFilename), filePermission)
	for name, tf := range mp.tagFamilies {
		fs.MustFlush(fileSystem, tf.Buf, filepath.Join(path, name+tagFamiliesFilenameExt), filePermission)
	}
	for name, tfh := range mp.tagFamilyMetadata {
		fs.MustFlush(fileSystem, tfh.Buf, filepath.Join(path, name+tagFamiliesMetadataFilenameExt), filePermission)
	}

	mp.partMetadata.mustWriteMetadata(fileSystem, path)

	fileSystem.SyncPath(path)
}

func uncompressedElementSizeBytes(index int, es *elements) uint64 {
	n := uint64(len(time.RFC3339Nano))
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
	return v.(*memPart)
}

func releaseMemPart(mp *memPart) {
	mp.reset()
	memPartPool.Put(mp)
}

var memPartPool sync.Pool

type partWrapper struct {
	mp  *memPart
	p   *part
	ref int32
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
}

func (pw *partWrapper) ID() uint64 {
	return pw.p.partMetadata.ID
}

func mustOpenFilePart(id uint64, root string, fileSystem fs.FileSystem) *part {
	var p part
	partPath := partPath(root, id)
	p.path = partPath
	p.partMetadata.mustReadMetadata(fileSystem, partPath)
	p.partMetadata.ID = id

	metaPath := path.Join(partPath, metaFilename)
	pr := mustOpenReader(metaPath, fileSystem)
	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], pr)
	fs.MustClose(pr)

	p.primary = mustOpenReader(path.Join(partPath, primaryFilename), fileSystem)
	p.timestamps = mustOpenReader(path.Join(partPath, timestampsFilename), fileSystem)
	p.elementIDs = mustOpenReader(path.Join(partPath, elementIDsFilename), fileSystem)
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
