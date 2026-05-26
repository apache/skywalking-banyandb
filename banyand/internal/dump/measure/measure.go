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

// Package measure decodes the on-disk byte format of a measure part into rows.
// It is a pure parser: no filtering, projection or output formatting (those are
// CLI concerns).
package measure

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// PartMetadata is the on-disk metadata of a measure part.
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
	field                 columnFamilyMetadata
	timestamps            timestampsMetadata
	seriesID              common.SeriesID
	uncompressedSizeBytes uint64
	count                 uint64
}

type timestampsMetadata struct {
	dataBlock         dataBlock
	min               int64
	max               int64
	versionOffset     uint64
	versionFirst      int64
	encodeType        encoding.EncodeType
	versionEncodeType encoding.EncodeType
}

type columnFamilyMetadata struct {
	columns []columnMetadata
}

type columnMetadata struct {
	name      string
	dataBlock dataBlock
	valueType pbv1.ValueType
}

// PartReader provides read-only access to a measure part directory.
// Not safe for concurrent use.
type PartReader struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fieldValues          fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	seriesMap            map[common.SeriesID][]byte // part-level smeta.bin, nil if absent
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         PartMetadata
}

// Row is one decoded data point from a measure part. The []byte / map values
// alias the iterator's block decode buffer and are valid only until the next
// Iterator.Next()/Close(); copy to retain. Tag/Field keys are "family.tag" full names.
type Row struct {
	Tags         map[string][]byte
	Fields       map[string][]byte
	TagTypes     map[string]pbv1.ValueType
	FieldTypes   map[string]pbv1.ValueType
	EntityValues []byte
	Timestamp    int64
	Version      int64
	SeriesID     common.SeriesID
}

// OpenPart opens the measure part directory root/<id> for read-only access.
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

	p.fieldValues, err = fileSystem.OpenFile(filepath.Join(partPath, "fv.bin"))
	if err != nil {
		fs.MustClose(p.primary)
		fs.MustClose(p.timestamps)
		return nil, fmt.Errorf("cannot open fv.bin: %w", err)
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
	if p.fieldValues != nil {
		fs.MustClose(p.fieldValues)
		p.fieldValues = nil
	}
	for _, r := range p.tagFamilies {
		fs.MustClose(r)
	}
	p.tagFamilies = nil
	for _, r := range p.tagFamilyMetadata {
		fs.MustClose(r)
	}
	p.tagFamilyMetadata = nil
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

	// Unmarshal timestamps metadata
	src = bm.timestamps.unmarshal(src)

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

	// Unmarshal field (column family metadata)
	var err error
	src, err = bm.field.unmarshal(src)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot unmarshal columnFamilyMetadata: %w", err)
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
	tm.versionOffset = n
	tm.versionFirst = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.versionEncodeType = encoding.EncodeType(src[0])
	return src[1:]
}

func (db *dataBlock) unmarshal(src []byte) []byte {
	src, n := encoding.BytesToVarUint64(src)
	db.offset = n
	src, n = encoding.BytesToVarUint64(src)
	db.size = n
	return src
}

func (cfm *columnFamilyMetadata) unmarshal(src []byte) ([]byte, error) {
	src, columnMetadataLen := encoding.BytesToVarUint64(src)
	if columnMetadataLen < 1 {
		return src, nil
	}
	cfm.columns = make([]columnMetadata, columnMetadataLen)
	var err error
	for i := range cfm.columns {
		src, err = cfm.columns[i].unmarshal(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal columnMetadata %d: %w", i, err)
		}
	}
	return src, nil
}

func (cm *columnMetadata) unmarshal(src []byte) ([]byte, error) {
	src, nameBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal columnMetadata.name: %w", err)
	}
	cm.name = string(nameBytes)
	if len(src) < 1 {
		return nil, fmt.Errorf("cannot unmarshal columnMetadata.valueType: src is too short")
	}
	cm.valueType = pbv1.ValueType(src[0])
	src = src[1:]
	src = cm.dataBlock.unmarshal(src)
	return src, nil
}

func readTimestamps(tm timestampsMetadata, count int, reader fs.Reader) ([]int64, []int64, error) {
	data := make([]byte, tm.dataBlock.size)
	if err := dump.ReadData(reader, int64(tm.dataBlock.offset), data); err != nil {
		return nil, nil, fmt.Errorf("cannot read timestamps: %w", err)
	}

	if tm.dataBlock.size < tm.versionOffset {
		return nil, nil, fmt.Errorf("size %d must be greater than versionOffset %d", tm.dataBlock.size, tm.versionOffset)
	}

	// Get the common type from the version type (similar to mustDecodeTimestampsWithVersions)
	commonEncodeType := encoding.GetCommonType(tm.encodeType)
	if commonEncodeType == encoding.EncodeTypeUnknown {
		return nil, nil, fmt.Errorf("unexpected encodeType %d", tm.encodeType)
	}

	// Decode timestamps (first part of the data)
	var timestamps []int64
	var err error
	timestamps, err = encoding.BytesToInt64List(timestamps, data[:tm.versionOffset], commonEncodeType, tm.min, count)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode timestamps: %w", err)
	}

	// Decode versions (second part of the data, starting from versionOffset)
	var versions []int64
	versions, err = encoding.BytesToInt64List(versions, data[tm.versionOffset:], tm.versionEncodeType, tm.versionFirst, count)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode versions: %w", err)
	}

	return timestamps, versions, nil
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

func readFieldValues(decoder *encoding.BytesBlockDecoder, fieldBlock dataBlock, _ string, count int,
	valueReader fs.Reader, valueType pbv1.ValueType,
) ([][]byte, error) {
	// Read field values
	bb := &bytes.Buffer{}
	bb.Buf = make([]byte, fieldBlock.size)
	if err := dump.ReadData(valueReader, int64(fieldBlock.offset), bb.Buf); err != nil {
		return nil, fmt.Errorf("cannot read field values: %w", err)
	}

	// Decode values based on value type
	var values [][]byte
	var err error

	switch valueType {
	case pbv1.ValueTypeInt64:
		// Decode int64 values - similar to column.decodeInt64Column
		if len(bb.Buf) < 1 {
			return nil, fmt.Errorf("buffer too short for int64 field")
		}
		encodeType := encoding.EncodeType(bb.Buf[0])
		if encodeType == encoding.EncodeTypePlain {
			// Use default decoder for plain encoding
			bb.Buf = bb.Buf[1:]
			values, err = decoder.Decode(values[:0], bb.Buf, uint64(count))
			if err != nil {
				return nil, fmt.Errorf("cannot decode int64 field values (plain): %w", err)
			}
		} else {
			const expectedLen = 9
			if len(bb.Buf) < expectedLen {
				return nil, fmt.Errorf("buffer too short for int64 field: expected at least %d bytes", expectedLen)
			}
			firstValue := convert.BytesToInt64(bb.Buf[1:9])
			bb.Buf = bb.Buf[9:]
			intValues := make([]int64, count)
			intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, count)
			if err != nil {
				return nil, fmt.Errorf("cannot decode int64 field values: %w", err)
			}
			values = make([][]byte, count)
			for i, v := range intValues {
				values[i] = convert.Int64ToBytes(v)
			}
		}
	case pbv1.ValueTypeFloat64:
		// Decode float64 values - similar to column.decodeFloat64Column
		if len(bb.Buf) < 1 {
			return nil, fmt.Errorf("buffer too short for float64 field")
		}
		encodeType := encoding.EncodeType(bb.Buf[0])
		if encodeType == encoding.EncodeTypePlain {
			// Use default decoder for plain encoding
			bb.Buf = bb.Buf[1:]
			values, err = decoder.Decode(values[:0], bb.Buf, uint64(count))
			if err != nil {
				return nil, fmt.Errorf("cannot decode float64 field values (plain): %w", err)
			}
		} else {
			const expectedLen = 11
			if len(bb.Buf) < expectedLen {
				return nil, fmt.Errorf("buffer too short for float64 field: expected at least %d bytes", expectedLen)
			}
			exp := convert.BytesToInt16(bb.Buf[1:3])
			firstValue := convert.BytesToInt64(bb.Buf[3:11])
			bb.Buf = bb.Buf[11:]
			intValues := make([]int64, count)
			intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, count)
			if err != nil {
				return nil, fmt.Errorf("cannot decode int values for float64: %w", err)
			}
			floatValues := make([]float64, count)
			floatValues, err = encoding.DecimalIntListToFloat64List(floatValues[:0], intValues, exp, count)
			if err != nil {
				return nil, fmt.Errorf("cannot convert DecimalIntList to Float64List: %w", err)
			}
			values = make([][]byte, count)
			for i, v := range floatValues {
				values[i] = convert.Float64ToBytes(v)
			}
		}
	default:
		// Use default decoder for other types
		values, err = internalencoding.DecodeTagValues(values, decoder, bb, valueType, count)
		if err != nil {
			return nil, fmt.Errorf("cannot decode field values: %w", err)
		}
	}

	return values, nil
}

// DiscoverColumns scans the first part under shardPath and returns its tag and
// field column names. Only the first part is scanned (preserving the CLI's
// historical behavior).
func DiscoverColumns(shardPath string, fileSystem fs.FileSystem) ([]string, []string, error) {
	partIDs, err := dump.DiscoverPartIDs(shardPath)
	if err != nil {
		return nil, nil, err
	}
	if len(partIDs) == 0 {
		return nil, nil, nil
	}

	p, err := OpenPart(partIDs[0], shardPath, fileSystem)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open first part: %w", err)
	}
	defer closePart(p)

	tagNames := make(map[string]bool)
	fieldNames := make(map[string]bool)
	partID := partIDs[0]
	for tagFamilyName := range p.tagFamilies {
		// Read tag family metadata to get tag names
		if tagFamilyMetadataReader, ok := p.tagFamilyMetadata[tagFamilyName]; ok {
			sr := tagFamilyMetadataReader.SequentialRead()
			metaData, err := io.ReadAll(sr)
			fs.MustClose(sr)
			if err != nil {
				logger.GetLogger("dump-measure").Warn().Err(err).Str("tagFamily", tagFamilyName).
					Uint64("partID", partID).Msg("failed to read tag family metadata")
				continue
			}
			var cfm columnFamilyMetadata
			_, err = cfm.unmarshal(metaData)
			if err != nil {
				logger.GetLogger("dump-measure").Warn().Err(err).Str("tagFamily", tagFamilyName).
					Uint64("partID", partID).Msg("failed to parse tag family metadata")
				continue
			}
			for _, colMeta := range cfm.columns {
				fullTagName := tagFamilyName + "." + colMeta.name
				tagNames[fullTagName] = true
			}
		}
	}

	// Read primary block to discover field names
	if len(p.primaryBlockMetadata) > 0 {
		primaryData := make([]byte, p.primaryBlockMetadata[0].size)
		if readErr := dump.ReadData(p.primary, int64(p.primaryBlockMetadata[0].offset), primaryData); readErr != nil {
			return nil, nil, fmt.Errorf("cannot read primary block: %w", readErr)
		}
		decompressed, err := zstd.Decompress(nil, primaryData)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot decompress primary block: %w", err)
		}
		blockMetadatas, err := parseBlockMetadata(decompressed)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot parse block metadata: %w", err)
		}
		if len(blockMetadatas) > 0 {
			for _, colMeta := range blockMetadatas[0].field.columns {
				fieldNames[colMeta.name] = true
			}
		}
	}

	tagResult := make([]string, 0, len(tagNames))
	for name := range tagNames {
		tagResult = append(tagResult, name)
	}
	sort.Strings(tagResult)

	fieldResult := make([]string, 0, len(fieldNames))
	for name := range fieldNames {
		fieldResult = append(fieldResult, name)
	}
	sort.Strings(fieldResult)

	return tagResult, fieldResult, nil
}

// DecodeFieldValue converts a byte-encoded measure field value back to a typed
// modelv1.FieldValue. The returned value is self-contained (binary contents are
// copied), so it may be retained past the next Iterator.Next()/Close().
func DecodeFieldValue(valueType pbv1.ValueType, raw []byte) *modelv1.FieldValue {
	if raw == nil {
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: convert.BytesToInt64(raw)}}}
	case pbv1.ValueTypeFloat64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: convert.BytesToFloat64(raw)}}}
	case pbv1.ValueTypeStr:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: string(raw)}}}
	case pbv1.ValueTypeBinaryData:
		b := make([]byte, len(raw))
		copy(b, raw)
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: b}}
	default:
		b := make([]byte, len(raw))
		copy(b, raw)
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: b}}
	}
}
