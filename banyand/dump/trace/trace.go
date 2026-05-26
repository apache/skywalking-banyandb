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

// Package trace decodes the on-disk byte format of a trace part into rows.
// It is a pure parser: no filtering, projection or output formatting (those are
// CLI concerns).
package trace

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// PartMetadata is the on-disk metadata of a trace part.
type PartMetadata struct {
	CompressedSizeBytes       uint64 `json:"compressedSizeBytes"`
	UncompressedSpanSizeBytes uint64 `json:"uncompressedSpanSizeBytes"`
	TotalCount                uint64 `json:"totalCount"`
	BlocksCount               uint64 `json:"blocksCount"`
	MinTimestamp              int64  `json:"minTimestamp"`
	MaxTimestamp              int64  `json:"maxTimestamp"`
	ID                        uint64 `json:"-"`
}

type primaryBlockMetadata struct {
	traceID string
	offset  uint64
	size    uint64
}

type dataBlock struct {
	offset uint64
	size   uint64
}

type blockMetadata struct {
	tags                      map[string]*dataBlock
	tagType                   map[string]pbv1.ValueType
	spans                     *dataBlock
	traceID                   string
	uncompressedSpanSizeBytes uint64
	count                     uint64
}

// PartReader provides read-only access to a trace part directory.
// Not safe for concurrent use.
type PartReader struct {
	primary              fs.Reader
	spans                fs.Reader
	fileSystem           fs.FileSystem
	tagMetadata          map[string]fs.Reader
	tags                 map[string]fs.Reader
	tagType              map[string]pbv1.ValueType
	seriesMap            map[common.SeriesID][]byte // part-level smeta.bin, nil if absent
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         PartMetadata
}

// Row is one decoded span from a trace part. The []byte / map values alias the
// iterator's block decode buffer and are valid only until the next
// Iterator.Next()/Close(); copy to retain. Tag keys are full tag names.
// SeriesID is computed from the tags (calculateSeriesIDFromTags); Timestamp is
// read from the tag whose type is ValueTypeTimestamp. Trace has no version/fields.
type Row struct {
	Tags         map[string][]byte
	TagTypes     map[string]pbv1.ValueType
	TraceID      string
	SpanID       string
	EntityValues []byte
	Span         []byte
	Timestamp    int64
	SeriesID     common.SeriesID
}

// OpenPart opens the trace part directory root/<id> for read-only access.
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

	// Read tag types
	p.tagType = make(map[string]pbv1.ValueType)
	tagTypePath := filepath.Join(partPath, "tag.type")
	if tagTypeData, readErr := fileSystem.Read(tagTypePath); readErr == nil && len(tagTypeData) > 0 {
		if unmarshalErr := unmarshalTagType(tagTypeData, p.tagType); unmarshalErr != nil {
			return nil, fmt.Errorf("cannot parse tag.type: %w", unmarshalErr)
		}
	}

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

	p.spans, err = fileSystem.OpenFile(filepath.Join(partPath, "spans.bin"))
	if err != nil {
		fs.MustClose(p.primary)
		return nil, fmt.Errorf("cannot open spans.bin: %w", err)
	}

	// Load the optional part-level series metadata (smeta.bin).
	p.seriesMap = dump.LoadPartSeriesMap(fileSystem, partPath, id)

	// Open tag files
	entries := fileSystem.ReadDir(partPath)
	p.tags = make(map[string]fs.Reader)
	p.tagMetadata = make(map[string]fs.Reader)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".tm") {
			name := e.Name()[:len(e.Name())-3]
			reader, err := fileSystem.OpenFile(filepath.Join(partPath, e.Name()))
			if err == nil {
				p.tagMetadata[name] = reader
			}
		}
		if strings.HasSuffix(e.Name(), ".t") {
			name := e.Name()[:len(e.Name())-2]
			reader, err := fileSystem.OpenFile(filepath.Join(partPath, e.Name()))
			if err == nil {
				p.tags[name] = reader
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
	if p.spans != nil {
		fs.MustClose(p.spans)
		p.spans = nil
	}
	for _, r := range p.tags {
		fs.MustClose(r)
	}
	p.tags = nil
	for _, r := range p.tagMetadata {
		fs.MustClose(r)
	}
	p.tagMetadata = nil
}

// Metadata returns the part's metadata.json contents.
func (p *PartReader) Metadata() PartMetadata { return p.partMetadata }

// PartID returns the part's numeric ID.
func (p *PartReader) PartID() uint64 { return p.partMetadata.ID }

// SeriesMap returns the part-level SeriesID -> EntityValues map parsed from
// smeta.bin, or nil if smeta.bin is absent.
func (p *PartReader) SeriesMap() map[common.SeriesID][]byte { return p.seriesMap }

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
	if len(src) < 4 {
		return nil, fmt.Errorf("insufficient data")
	}
	src, traceIDBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal traceID: %w", err)
	}
	pbm.traceID = string(traceIDBytes)
	if len(src) < 16 {
		return nil, fmt.Errorf("insufficient data for offset and size")
	}
	pbm.offset = encoding.BytesToUint64(src)
	src = src[8:]
	pbm.size = encoding.BytesToUint64(src)
	return src[8:], nil
}

func unmarshalTagType(src []byte, tagType map[string]pbv1.ValueType) error {
	src, count := encoding.BytesToVarUint64(src)
	for i := uint64(0); i < count; i++ {
		var nameBytes []byte
		var err error
		src, nameBytes, err = encoding.DecodeBytes(src)
		if err != nil {
			return err
		}
		name := string(nameBytes)
		if len(src) < 1 {
			return fmt.Errorf("insufficient data for valueType")
		}
		valueType := pbv1.ValueType(src[0])
		src = src[1:]
		tagType[name] = valueType
	}
	return nil
}

func parseAllBlockMetadata(src []byte, tagType map[string]pbv1.ValueType) ([]*blockMetadata, error) {
	var result []*blockMetadata
	for len(src) > 0 {
		bm, tail, err := parseBlockMetadata(src, tagType)
		if err != nil {
			return nil, err
		}
		result = append(result, bm)
		src = tail
	}
	return result, nil
}

func parseBlockMetadata(src []byte, tagType map[string]pbv1.ValueType) (*blockMetadata, []byte, error) {
	var bm blockMetadata
	bm.tagType = make(map[string]pbv1.ValueType)
	for k, v := range tagType {
		bm.tagType[k] = v
	}

	src, traceIDBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot unmarshal traceID: %w", err)
	}
	bm.traceID = string(traceIDBytes)

	src, n := encoding.BytesToVarUint64(src)
	bm.uncompressedSpanSizeBytes = n

	src, n = encoding.BytesToVarUint64(src)
	bm.count = n

	bm.spans = &dataBlock{}
	src, n = encoding.BytesToVarUint64(src)
	bm.spans.offset = n
	src, n = encoding.BytesToVarUint64(src)
	bm.spans.size = n

	src, tagCount := encoding.BytesToVarUint64(src)
	if tagCount > 0 {
		bm.tags = make(map[string]*dataBlock)
		for i := uint64(0); i < tagCount; i++ {
			var nameBytes []byte
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot unmarshal tag name: %w", err)
			}
			name := string(nameBytes)

			db := &dataBlock{}
			src, n = encoding.BytesToVarUint64(src)
			db.offset = n
			src, n = encoding.BytesToVarUint64(src)
			db.size = n
			bm.tags[name] = db
		}
	}

	return &bm, src, nil
}

func readSpans(decoder *encoding.BytesBlockDecoder, sm *dataBlock, count int, reader fs.Reader) ([][]byte, []string, error) {
	data := make([]byte, sm.size)
	fs.MustReadData(reader, int64(sm.offset), data)

	var spanIDBytes [][]byte
	var tail []byte
	var err error
	spanIDBytes, tail, err = decoder.DecodeWithTail(spanIDBytes[:0], data, uint64(count))
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode spanIDs: %w", err)
	}

	spanIDs := make([]string, count)
	for i, idBytes := range spanIDBytes {
		spanIDs[i] = string(idBytes)
	}

	var spans [][]byte
	spans, err = decoder.Decode(spans[:0], tail, uint64(count))
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode spans: %w", err)
	}

	return spans, spanIDs, nil
}

type tagMetadata struct {
	min         []byte
	max         []byte
	offset      uint64
	size        uint64
	filterBlock dataBlock
}

func readTagValues(decoder *encoding.BytesBlockDecoder, tagBlock *dataBlock, _ string, count int,
	metaReader, valueReader fs.Reader, valueType pbv1.ValueType,
) ([][]byte, error) {
	// Read tag metadata
	metaData := make([]byte, tagBlock.size)
	fs.MustReadData(metaReader, int64(tagBlock.offset), metaData)

	var tm tagMetadata
	if err := unmarshalTagMetadata(&tm, metaData); err != nil {
		return nil, fmt.Errorf("cannot unmarshal tag metadata: %w", err)
	}

	// Check if there's actual data to read
	if tm.size == 0 {
		// Return nil values for all items
		values := make([][]byte, count)
		return values, nil
	}

	// Read tag values
	bb := &bytes.Buffer{}
	bb.Buf = make([]byte, tm.size)
	fs.MustReadData(valueReader, int64(tm.offset), bb.Buf)

	// Decode values using the internal encoding package
	var err error
	var values [][]byte
	values, err = internalencoding.DecodeTagValues(values, decoder, bb, valueType, count)
	if err != nil {
		return nil, fmt.Errorf("cannot decode tag values: %w", err)
	}

	return values, nil
}

func unmarshalTagMetadata(tm *tagMetadata, src []byte) error {
	var n uint64
	var err error

	// Unmarshal dataBlock (offset and size)
	src, n = encoding.BytesToVarUint64(src)
	tm.offset = n
	src, n = encoding.BytesToVarUint64(src)
	tm.size = n

	// Unmarshal min
	src, tm.min, err = encoding.DecodeBytes(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tagMetadata.min: %w", err)
	}

	// Unmarshal max
	src, tm.max, err = encoding.DecodeBytes(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tagMetadata.max: %w", err)
	}

	// Unmarshal filterBlock
	src, n = encoding.BytesToVarUint64(src)
	tm.filterBlock.offset = n
	_, n = encoding.BytesToVarUint64(src)
	tm.filterBlock.size = n

	return nil
}

func calculateSeriesIDFromTags(tags map[string][]byte) common.SeriesID {
	var entityValues []byte

	// Sort tag names for consistent hashing
	tagNames := make([]string, 0, len(tags))
	for name := range tags {
		tagNames = append(tagNames, name)
	}
	sort.Strings(tagNames)

	// Build entity values
	for _, name := range tagNames {
		value := tags[name]
		if value != nil {
			// Append tag name=value
			entityValues = append(entityValues, []byte(name)...)
			entityValues = append(entityValues, '=')
			entityValues = append(entityValues, value...)
			entityValues = append(entityValues, encoding.EntityDelimiter)
		}
	}

	if len(entityValues) == 0 {
		return 0
	}

	return common.SeriesID(convert.Hash(entityValues))
}

// DiscoverColumns scans the first part under shardPath and returns its tag
// column names (from tag.type). Only the first part is scanned (preserving the
// CLI's historical behavior).
func DiscoverColumns(shardPath string, fileSystem fs.FileSystem) ([]string, error) {
	partIDs, err := dump.DiscoverPartIDs(shardPath)
	if err != nil {
		return nil, err
	}
	if len(partIDs) == 0 {
		return nil, nil
	}

	// Open first part to discover tag columns
	p, err := OpenPart(partIDs[0], shardPath, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("failed to open first part: %w", err)
	}
	defer closePart(p)

	// Collect all tag names
	tagNames := make([]string, 0, len(p.tagType))
	for name := range p.tagType {
		tagNames = append(tagNames, name)
	}
	sort.Strings(tagNames)

	return tagNames, nil
}
