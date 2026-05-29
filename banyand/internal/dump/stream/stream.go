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

// Package stream decodes the on-disk byte format of a stream part into rows.
// It is a pure parser: no filtering, projection or output formatting (those are
// CLI concerns).
package stream

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// PartMetadata is the on-disk metadata of a stream part.
type PartMetadata struct {
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`
	MinTimestamp          int64  `json:"minTimestamp"`
	MaxTimestamp          int64  `json:"maxTimestamp"`
	ID                    uint64 `json:"-"`
}

type primaryBlockMetadata struct {
	seriesID     common.SeriesID
	minTimestamp int64
	maxTimestamp int64
	offset       uint64
	size         uint64
}

type dataBlock struct {
	offset uint64
	size   uint64
}

type blockMetadata struct {
	tagFamilies           map[string]*dataBlock
	timestamps            timestampsMetadata
	elementIDs            elementIDsMetadata
	seriesID              common.SeriesID
	uncompressedSizeBytes uint64
	count                 uint64
}

type timestampsMetadata struct {
	dataBlock        dataBlock
	min              int64
	max              int64
	elementIDsOffset uint64
	encodeType       encoding.EncodeType
}

type elementIDsMetadata struct {
	dataBlock  dataBlock
	encodeType encoding.EncodeType
}

// PartReader provides read-only access to a stream part directory.
// Not safe for concurrent use.
type PartReader struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	tagFamilyFilter      map[string]fs.Reader
	seriesMap            map[common.SeriesID][]byte // part-level smeta.bin, nil if absent
	indexResolver        *dump.IndexResolver        // optional segment-level series-index fallback
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         PartMetadata
}

// Row is one decoded element from a stream part. The []byte / map values alias
// the iterator's block decode buffer and are valid only until the next
// Iterator.Next()/Close(); copy to retain. Tag keys are "family.tag" full names.
// Stream has no fields; the legacy element payload is never persisted, so it is
// not exposed here.
type Row struct {
	Tags         map[string][]byte
	TagTypes     map[string]pbv1.ValueType
	EntityValues []byte
	Timestamp    int64
	ElementID    uint64
	SeriesID     common.SeriesID
}

// OpenPart opens the stream part directory root/<id> for read-only access.
func OpenPart(id uint64, root string, fileSystem fs.FileSystem) (*PartReader, error) {
	var p PartReader
	partPath := filepath.Join(root, fmt.Sprintf("%016x", id))
	p.path = partPath
	p.fileSystem = fileSystem

	// Read metadata.json
	metadataPath := filepath.Join(partPath, "metadata.json")
	metadata, err := fileSystem.Read(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read metadata.json: %w", err)
	}
	if unmarshalErr := json.Unmarshal(metadata, &p.partMetadata); unmarshalErr != nil {
		return nil, fmt.Errorf("cannot parse metadata.json: %w", unmarshalErr)
	}
	p.partMetadata.ID = id

	// Read primary block metadata
	metaPath := filepath.Join(partPath, "meta.bin")
	metaFile, err := fileSystem.OpenFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open meta.bin: %w", err)
	}
	p.primaryBlockMetadata, err = readPrimaryBlockMetadata(metaFile)
	fs.MustClose(metaFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read primary block metadata: %w", err)
	}

	// Open data files
	p.primary, err = fileSystem.OpenFile(filepath.Join(partPath, "primary.bin"))
	if err != nil {
		return nil, fmt.Errorf("cannot open primary.bin: %w", err)
	}

	p.timestamps, err = fileSystem.OpenFile(filepath.Join(partPath, "timestamps.bin"))
	if err != nil {
		fs.MustClose(p.primary)
		return nil, fmt.Errorf("cannot open timestamps.bin: %w", err)
	}

	// Load the optional part-level series metadata (smeta.bin).
	p.seriesMap, err = dump.LoadPartSeriesMap(fileSystem, partPath, id)
	if err != nil {
		closePart(&p)
		return nil, fmt.Errorf("cannot load series metadata: %w", err)
	}

	// Open tag family files
	entries := fileSystem.ReadDir(partPath)
	p.tagFamilies = make(map[string]fs.Reader)
	p.tagFamilyMetadata = make(map[string]fs.Reader)
	p.tagFamilyFilter = make(map[string]fs.Reader)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".tfm") {
			tagFamilyName := name[:len(name)-4]
			reader, err := fileSystem.OpenFile(filepath.Join(partPath, name))
			if err == nil {
				p.tagFamilyMetadata[tagFamilyName] = reader
			}
		}
		if strings.HasSuffix(name, ".tf") {
			tagFamilyName := name[:len(name)-3]
			reader, err := fileSystem.OpenFile(filepath.Join(partPath, name))
			if err == nil {
				p.tagFamilies[tagFamilyName] = reader
			}
		}
		if strings.HasSuffix(name, ".tff") {
			tagFamilyName := name[:len(name)-4]
			reader, err := fileSystem.OpenFile(filepath.Join(partPath, name))
			if err == nil {
				p.tagFamilyFilter[tagFamilyName] = reader
			}
		}
	}

	return &p, nil
}

func closePart(p *PartReader) {
	if p.primary != nil {
		fs.MustClose(p.primary)
		p.primary = nil
	}
	if p.timestamps != nil {
		fs.MustClose(p.timestamps)
		p.timestamps = nil
	}
	for _, r := range p.tagFamilies {
		fs.MustClose(r)
	}
	p.tagFamilies = nil
	for _, r := range p.tagFamilyMetadata {
		fs.MustClose(r)
	}
	p.tagFamilyMetadata = nil
	for _, r := range p.tagFamilyFilter {
		fs.MustClose(r)
	}
	p.tagFamilyFilter = nil
}

// Metadata returns the part's metadata.json contents.
func (p *PartReader) Metadata() PartMetadata { return p.partMetadata }

// PartID returns the part's numeric ID.
func (p *PartReader) PartID() uint64 { return p.partMetadata.ID }

// SeriesMap returns the part-level SeriesID -> EntityValues map parsed from
// smeta.bin, or nil if smeta.bin is absent.
func (p *PartReader) SeriesMap() map[common.SeriesID][]byte { return p.seriesMap }

// SetIndexResolver attaches a segment-level series-index resolver. When the
// part has no smeta.bin (the standalone write path skips it), the iterator
// falls back to IndexResolver.PartSeriesMap to recover EntityValues scoped to
// just this part's distinct seriesIDs.
func (p *PartReader) SetIndexResolver(r *dump.IndexResolver) { p.indexResolver = r }

// Close releases all file handles. Safe to call multiple times.
func (p *PartReader) Close() error {
	closePart(p)
	return nil
}

func readPrimaryBlockMetadata(r fs.Reader) ([]primaryBlockMetadata, error) {
	sr := r.SequentialRead()
	data, err := io.ReadAll(sr)
	fs.MustClose(sr)
	if err != nil {
		return nil, fmt.Errorf("cannot read: %w", err)
	}

	decompressed, err := zstd.Decompress(nil, data)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress: %w", err)
	}

	var result []primaryBlockMetadata
	src := decompressed
	for len(src) > 0 {
		var pbm primaryBlockMetadata
		src, err = unmarshalPrimaryBlockMetadata(&pbm, src)
		if err != nil {
			return nil, err
		}
		result = append(result, pbm)
	}
	return result, nil
}

func unmarshalPrimaryBlockMetadata(pbm *primaryBlockMetadata, src []byte) ([]byte, error) {
	if len(src) < 40 {
		return nil, fmt.Errorf("insufficient data")
	}
	pbm.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.minTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.maxTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pbm.offset = encoding.BytesToUint64(src)
	src = src[8:]
	pbm.size = encoding.BytesToUint64(src)
	return src[8:], nil
}

func parseBlockMetadata(src []byte) ([]*blockMetadata, error) {
	var result []*blockMetadata
	for len(src) > 0 {
		bm, tail, err := unmarshalBlockMetadata(src)
		if err != nil {
			return nil, err
		}
		result = append(result, bm)
		src = tail
	}
	return result, nil
}

func unmarshalBlockMetadata(src []byte) (*blockMetadata, []byte, error) {
	var bm blockMetadata

	if len(src) < 8 {
		return nil, nil, fmt.Errorf("cannot unmarshal blockMetadata from less than 8 bytes")
	}
	bm.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]

	src, n := encoding.BytesToVarUint64(src)
	bm.uncompressedSizeBytes = n

	src, n = encoding.BytesToVarUint64(src)
	bm.count = n

	// Unmarshal timestamps metadata (includes dataBlock, min, max, encodeType, elementIDsOffset)
	src = bm.timestamps.unmarshal(src)

	// Unmarshal elementIDs metadata (includes dataBlock, encodeType)
	src = bm.elementIDs.unmarshal(src)

	// Unmarshal tag families
	src, tagFamilyCount := encoding.BytesToVarUint64(src)
	if tagFamilyCount > 0 {
		bm.tagFamilies = make(map[string]*dataBlock)
		for i := uint64(0); i < tagFamilyCount; i++ {
			var nameBytes []byte
			var err error
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot unmarshal tagFamily name: %w", err)
			}
			tf := &dataBlock{}
			src = tf.unmarshal(src)
			bm.tagFamilies[string(nameBytes)] = tf
		}
	}

	return &bm, src, nil
}

func (tm *timestampsMetadata) unmarshal(src []byte) []byte {
	src = tm.dataBlock.unmarshal(src)
	tm.min = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.max = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.encodeType = encoding.EncodeType(src[0])
	src = src[1:]
	src, n := encoding.BytesToVarUint64(src)
	tm.elementIDsOffset = n
	return src
}

func (em *elementIDsMetadata) unmarshal(src []byte) []byte {
	src = em.dataBlock.unmarshal(src)
	em.encodeType = encoding.EncodeType(src[0])
	return src[1:]
}

func (db *dataBlock) unmarshal(src []byte) []byte {
	src, n := encoding.BytesToVarUint64(src)
	db.offset = n
	src, n = encoding.BytesToVarUint64(src)
	db.size = n
	return src
}

func readTimestamps(tm timestampsMetadata, count int, reader fs.Reader) ([]int64, []uint64, error) {
	data := make([]byte, tm.dataBlock.size)
	if err := dump.ReadData(reader, int64(tm.dataBlock.offset), data); err != nil {
		return nil, nil, fmt.Errorf("cannot read timestamps: %w", err)
	}

	if tm.dataBlock.size < tm.elementIDsOffset {
		return nil, nil, fmt.Errorf("size %d must be greater than elementIDsOffset %d", tm.dataBlock.size, tm.elementIDsOffset)
	}

	// Decode timestamps (first part of the data)
	var timestamps []int64
	var err error
	// For stream, encodeType is already the common type (not version type)
	timestamps, err = encoding.BytesToInt64List(timestamps, data[:tm.elementIDsOffset], tm.encodeType, tm.min, count)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode timestamps: %w", err)
	}

	// Decode element IDs (second part of the data, starting from elementIDsOffset)
	elementIDs := make([]uint64, count)
	_, err = encoding.BytesToVarUint64s(elementIDs, data[tm.elementIDsOffset:])
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode element IDs: %w", err)
	}

	return timestamps, elementIDs, nil
}

type tagMetadata struct {
	name      string
	min       []byte
	max       []byte
	dataBlock dataBlock
	valueType pbv1.ValueType
}

func parseTagFamilyMetadata(src []byte) ([]tagMetadata, error) {
	src, tagMetadataLen := encoding.BytesToVarUint64(src)
	if tagMetadataLen < 1 {
		return nil, nil
	}

	var result []tagMetadata
	for i := uint64(0); i < tagMetadataLen; i++ {
		var tm tagMetadata
		var nameBytes []byte
		var err error
		src, nameBytes, err = encoding.DecodeBytes(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tagMetadata.name: %w", err)
		}
		tm.name = string(nameBytes)

		if len(src) < 1 {
			return nil, fmt.Errorf("cannot unmarshal tagMetadata.valueType: src is too short")
		}
		tm.valueType = pbv1.ValueType(src[0])
		src = src[1:]

		src = tm.dataBlock.unmarshal(src)

		src, tm.min, err = encoding.DecodeBytes(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tagMetadata.min: %w", err)
		}

		src, tm.max, err = encoding.DecodeBytes(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tagMetadata.max: %w", err)
		}

		// Skip filter block
		var filterBlock dataBlock
		src = filterBlock.unmarshal(src)

		result = append(result, tm)
	}

	return result, nil
}

func readTagValues(decoder *encoding.BytesBlockDecoder, tagBlock dataBlock, _ string, count int,
	valueReader fs.Reader, valueType pbv1.ValueType,
) ([][]byte, error) {
	// Read tag values
	bb := &bytes.Buffer{}
	bb.Buf = make([]byte, tagBlock.size)
	if err := dump.ReadData(valueReader, int64(tagBlock.offset), bb.Buf); err != nil {
		return nil, fmt.Errorf("cannot read tag values: %w", err)
	}

	// Decode values using the internal encoding package
	var err error
	var values [][]byte
	values, err = internalencoding.DecodeTagValues(values, decoder, bb, valueType, count)
	if err != nil {
		return nil, fmt.Errorf("cannot decode tag values: %w", err)
	}

	return values, nil
}

// DiscoverColumns scans the first part under shardPath and returns its tag
// column names. Only the first part is scanned (preserving the CLI's historical
// behavior).
func DiscoverColumns(shardPath string, fileSystem fs.FileSystem) ([]string, error) {
	partIDs, err := dump.DiscoverPartIDs(shardPath)
	if err != nil {
		return nil, err
	}
	if len(partIDs) == 0 {
		return nil, nil
	}

	p, err := OpenPart(partIDs[0], shardPath, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("failed to open first part: %w", err)
	}
	defer closePart(p)

	tagNames := make(map[string]bool)
	partID := partIDs[0]
	for tagFamilyName := range p.tagFamilies {
		// Read tag family metadata to get tag names
		if tagFamilyMetadataReader, ok := p.tagFamilyMetadata[tagFamilyName]; ok {
			sr := tagFamilyMetadataReader.SequentialRead()
			metaData, err := io.ReadAll(sr)
			fs.MustClose(sr)
			if err != nil {
				logger.GetLogger("dump-stream").Warn().Err(err).Str("tagFamily", tagFamilyName).
					Uint64("partID", partID).Msg("failed to read tag family metadata")
				continue
			}
			tagMetadatas, err := parseTagFamilyMetadata(metaData)
			if err != nil {
				logger.GetLogger("dump-stream").Warn().Err(err).Str("tagFamily", tagFamilyName).
					Uint64("partID", partID).Msg("failed to parse tag family metadata")
				continue
			}
			for _, tm := range tagMetadatas {
				fullTagName := tagFamilyName + "." + tm.name
				tagNames[fullTagName] = true
			}
		}
	}

	result := make([]string, 0, len(tagNames))
	for name := range tagNames {
		result = append(result, name)
	}
	sort.Strings(result)

	return result, nil
}
