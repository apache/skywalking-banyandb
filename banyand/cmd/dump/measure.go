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

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	dirNameSidx = "sidx"
	dirNameMeta = "meta"
)

type measureDumpOptions struct {
	shardPath      string
	segmentPath    string
	criteriaJSON   string
	projectionTags string
	verbose        bool
	csvOutput      bool
}

func newMeasureCmd() *cobra.Command {
	var shardPath string
	var segmentPath string
	var verbose bool
	var csvOutput bool
	var criteriaJSON string
	var projectionTags string

	cmd := &cobra.Command{
		Use:   "measure",
		Short: "Dump measure shard data",
		Long: `Dump and display contents of a measure shard directory (containing multiple parts).
Outputs measure data in human-readable format or CSV.

Supports filtering by criteria and projecting specific tags.`,
		Example: `  # Display measure data from shard in text format
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment

  # Display with verbose hex dumps
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment -v

  # Filter by criteria
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}'

  # Project specific tags
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --projection "tag1,tag2,tag3"

  # Output as CSV
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv

  # Save CSV to file
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv > output.csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if shardPath == "" {
				return fmt.Errorf("--shard-path flag is required")
			}
			if segmentPath == "" {
				return fmt.Errorf("--segment-path flag is required")
			}
			return dumpMeasureShard(measureDumpOptions{
				shardPath:      shardPath,
				segmentPath:    segmentPath,
				verbose:        verbose,
				csvOutput:      csvOutput,
				criteriaJSON:   criteriaJSON,
				projectionTags: projectionTags,
			})
		},
	}

	cmd.Flags().StringVar(&shardPath, "shard-path", "", "Path to the shard directory (required)")
	cmd.Flags().StringVarP(&segmentPath, "segment-path", "g", "", "Path to the segment directory (required)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output (show raw data)")
	cmd.Flags().BoolVar(&csvOutput, "csv", false, "Output as CSV format")
	cmd.Flags().StringVarP(&criteriaJSON, "criteria", "c", "", "Criteria filter as JSON string")
	cmd.Flags().StringVarP(&projectionTags, "projection", "p", "", "Comma-separated list of tags to include as columns (e.g., tag1,tag2,tag3)")
	_ = cmd.MarkFlagRequired("shard-path")
	_ = cmd.MarkFlagRequired("segment-path")

	return cmd
}

func dumpMeasureShard(opts measureDumpOptions) error {
	ctx, err := newMeasureDumpContext(opts)
	if err != nil || ctx == nil {
		return err
	}
	defer ctx.close()

	if err := ctx.processParts(); err != nil {
		return err
	}

	ctx.printSummary()
	return nil
}

type measurePartMetadata struct {
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`
	MinTimestamp          int64  `json:"minTimestamp"`
	MaxTimestamp          int64  `json:"maxTimestamp"`
	ID                    uint64 `json:"-"`
}

type measurePrimaryBlockMetadata struct {
	seriesID     common.SeriesID
	minTimestamp int64
	maxTimestamp int64
	offset       uint64
	size         uint64
}

type measureDataBlock struct {
	offset uint64
	size   uint64
}

type measureBlockMetadata struct {
	tagFamilies           map[string]*measureDataBlock
	field                 measureColumnFamilyMetadata
	timestamps            measureTimestampsMetadata
	seriesID              common.SeriesID
	uncompressedSizeBytes uint64
	count                 uint64
}

type measureTimestampsMetadata struct {
	dataBlock         measureDataBlock
	min               int64
	max               int64
	versionOffset     uint64
	versionFirst      int64
	encodeType        encoding.EncodeType
	versionEncodeType encoding.EncodeType
}

type measureColumnFamilyMetadata struct {
	columns []measureColumnMetadata
}

type measureColumnMetadata struct {
	name      string
	dataBlock measureDataBlock
	valueType pbv1.ValueType
}

type measurePart struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fieldValues          fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	path                 string
	primaryBlockMetadata []measurePrimaryBlockMetadata
	partMetadata         measurePartMetadata
}

type measureRowData struct {
	tags       map[string][]byte
	fields     map[string][]byte
	fieldTypes map[string]pbv1.ValueType
	timestamp  int64
	version    int64
	partID     uint64
	seriesID   common.SeriesID
}

type measureDumpContext struct {
	tagFilter      logical.TagFilter
	fileSystem     fs.FileSystem
	seriesMap      map[common.SeriesID]string
	writer         *csv.Writer
	opts           measureDumpOptions
	partIDs        []uint64
	projectionTags []string
	tagColumns     []string
	fieldColumns   []string
	rowNum         int
}

func newMeasureDumpContext(opts measureDumpOptions) (*measureDumpContext, error) {
	ctx := &measureDumpContext{
		opts:       opts,
		fileSystem: fs.NewLocalFileSystem(),
	}

	partIDs, err := discoverMeasurePartIDs(opts.shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to discover part IDs: %w", err)
	}
	if len(partIDs) == 0 {
		fmt.Println("No parts found in shard directory")
		return nil, nil
	}
	ctx.partIDs = partIDs
	fmt.Fprintf(os.Stderr, "Found %d parts in shard\n", len(partIDs))

	ctx.seriesMap, err = loadMeasureSeriesMap(opts.segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		ctx.seriesMap = nil
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from segment\n", len(ctx.seriesMap))
	}

	if opts.criteriaJSON != "" {
		var criteria *modelv1.Criteria
		criteria, err = parseMeasureCriteriaJSON(opts.criteriaJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to parse criteria: %w", err)
		}
		ctx.tagFilter, err = logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			return nil, fmt.Errorf("failed to build tag filter: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Applied criteria filter\n")
	}

	if opts.projectionTags != "" {
		ctx.projectionTags = parseMeasureProjectionTags(opts.projectionTags)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", ctx.projectionTags)
	}

	if opts.csvOutput {
		if len(ctx.projectionTags) > 0 {
			ctx.tagColumns = ctx.projectionTags
		} else {
			ctx.tagColumns, ctx.fieldColumns, err = discoverMeasureColumns(ctx.partIDs, opts.shardPath, ctx.fileSystem)
			if err != nil {
				return nil, fmt.Errorf("failed to discover columns: %w", err)
			}
		}
	}

	if err := ctx.initOutput(); err != nil {
		return nil, err
	}

	return ctx, nil
}

func (ctx *measureDumpContext) initOutput() error {
	if !ctx.opts.csvOutput {
		fmt.Printf("================================================================================\n")
		fmt.Fprintf(os.Stderr, "Processing parts...\n")
		return nil
	}

	ctx.writer = csv.NewWriter(os.Stdout)
	header := []string{"PartID", "Timestamp", "Version", "SeriesID", "Series"}
	header = append(header, ctx.fieldColumns...)
	header = append(header, ctx.tagColumns...)
	if err := ctx.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	return nil
}

func (ctx *measureDumpContext) close() {
	if ctx.writer != nil {
		ctx.writer.Flush()
	}
}

func (ctx *measureDumpContext) processParts() error {
	for partIdx, partID := range ctx.partIDs {
		fmt.Fprintf(os.Stderr, "Processing part %d/%d (0x%016x)...\n", partIdx+1, len(ctx.partIDs), partID)

		p, err := openMeasurePart(partID, ctx.opts.shardPath, ctx.fileSystem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open part %016x: %v\n", partID, err)
			continue
		}

		partRowCount, partErr := ctx.processPart(partID, p)
		closeMeasurePart(p)
		if partErr != nil {
			return partErr
		}

		fmt.Fprintf(os.Stderr, "  Part %d/%d: processed %d rows (total: %d)\n", partIdx+1, len(ctx.partIDs), partRowCount, ctx.rowNum)
	}
	return nil
}

func (ctx *measureDumpContext) processPart(partID uint64, p *measurePart) (int, error) {
	decoder := &encoding.BytesBlockDecoder{}
	partRowCount := 0

	for _, pbm := range p.primaryBlockMetadata {
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		decompressed, err := zstd.Decompress(nil, primaryData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error decompressing primary data in part %016x: %v\n", partID, err)
			continue
		}

		blockMetadatas, err := parseMeasureBlockMetadata(decompressed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error parsing block metadata in part %016x: %v\n", partID, err)
			continue
		}

		for _, bm := range blockMetadatas {
			rows, err := ctx.processBlock(partID, bm, p, decoder)
			if err != nil {
				return partRowCount, err
			}
			partRowCount += rows
		}
	}

	return partRowCount, nil
}

func (ctx *measureDumpContext) processBlock(partID uint64, bm *measureBlockMetadata, p *measurePart, decoder *encoding.BytesBlockDecoder) (int, error) {
	// Read timestamps and versions
	timestamps, versions, err := readMeasureTimestamps(bm.timestamps, int(bm.count), p.timestamps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Error reading timestamps/versions for series %d in part %016x: %v\n", bm.seriesID, partID, err)
		return 0, nil
	}

	// Read field values
	fieldsByDataPoint := ctx.readBlockFields(partID, bm, p, decoder)

	// Read tag families
	tagsByDataPoint := ctx.readBlockTagFamilies(partID, bm, p, decoder)

	rows := 0
	for i := 0; i < len(timestamps); i++ {
		dataPointTags := make(map[string][]byte)
		for tagName, tagValues := range tagsByDataPoint {
			if i < len(tagValues) {
				dataPointTags[tagName] = tagValues[i]
			}
		}

		dataPointFields := make(map[string][]byte)
		dataPointFieldTypes := make(map[string]pbv1.ValueType)
		for fieldName, fieldValues := range fieldsByDataPoint {
			if i < len(fieldValues) {
				dataPointFields[fieldName] = fieldValues[i]
				dataPointFieldTypes[fieldName] = bm.field.columns[0].valueType
			}
		}

		if ctx.shouldSkip(dataPointTags) {
			continue
		}

		row := measureRowData{
			partID:     partID,
			timestamp:  timestamps[i],
			version:    versions[i],
			tags:       dataPointTags,
			fields:     dataPointFields,
			fieldTypes: dataPointFieldTypes,
			seriesID:   bm.seriesID,
		}

		if err := ctx.writeRow(row); err != nil {
			return rows, err
		}

		rows++
	}

	return rows, nil
}

func (ctx *measureDumpContext) readBlockTagFamilies(partID uint64, bm *measureBlockMetadata, p *measurePart, decoder *encoding.BytesBlockDecoder) map[string][][]byte {
	tags := make(map[string][][]byte)
	for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
		// Read tag family metadata
		tagFamilyMetadataData := make([]byte, tagFamilyBlock.size)
		fs.MustReadData(p.tagFamilyMetadata[tagFamilyName], int64(tagFamilyBlock.offset), tagFamilyMetadataData)

		// Parse tag family metadata as columnFamilyMetadata (same format as fields)
		var cfm measureColumnFamilyMetadata
		_, err := cfm.unmarshal(tagFamilyMetadataData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error parsing tag family metadata %s for series %d in part %016x: %v\n", tagFamilyName, bm.seriesID, partID, err)
			continue
		}

		// Read each tag (column) in the tag family
		for _, colMeta := range cfm.columns {
			fullTagName := tagFamilyName + "." + colMeta.name
			tagValues, err := readMeasureTagValues(decoder, colMeta.dataBlock, fullTagName, int(bm.count), p.tagFamilies[tagFamilyName], colMeta.valueType)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error reading tag %s for series %d in part %016x: %v\n", fullTagName, bm.seriesID, partID, err)
				continue
			}
			tags[fullTagName] = tagValues
		}
	}
	return tags
}

func (ctx *measureDumpContext) readBlockFields(partID uint64, bm *measureBlockMetadata, p *measurePart, decoder *encoding.BytesBlockDecoder) map[string][][]byte {
	fields := make(map[string][][]byte)
	for _, colMeta := range bm.field.columns {
		fieldValues, err := readMeasureFieldValues(decoder, colMeta.dataBlock, colMeta.name, int(bm.count), p.fieldValues, colMeta.valueType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error reading field %s for series %d in part %016x: %v\n", colMeta.name, bm.seriesID, partID, err)
			continue
		}
		fields[colMeta.name] = fieldValues
	}
	return fields
}

func (ctx *measureDumpContext) shouldSkip(tags map[string][]byte) bool {
	if ctx.tagFilter == nil || ctx.tagFilter == logical.DummyFilter {
		return false
	}
	// Convert tags to modelv1.Tag format for filtering
	modelTags := make([]*modelv1.Tag, 0, len(tags))
	for name, value := range tags {
		if value == nil {
			continue
		}
		tagValue := convertMeasureTagValue(value)
		if tagValue != nil {
			modelTags = append(modelTags, &modelv1.Tag{
				Key:   name,
				Value: tagValue,
			})
		}
	}

	// Create a simple registry for tag filtering
	registry := &measureTagRegistry{
		tags: tags,
	}

	matcher := logical.NewTagFilterMatcher(ctx.tagFilter, registry, measureTagValueDecoder)
	match, _ := matcher.Match(modelTags)
	return !match
}

func (ctx *measureDumpContext) writeRow(row measureRowData) error {
	if ctx.opts.csvOutput {
		if err := writeMeasureRowAsCSV(ctx.writer, row, ctx.fieldColumns, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writeMeasureRowAsText(row, ctx.rowNum+1, ctx.projectionTags, ctx.seriesMap)
	}
	ctx.rowNum++
	return nil
}

func (ctx *measureDumpContext) printSummary() {
	if ctx.opts.csvOutput {
		fmt.Fprintf(os.Stderr, "Total rows written: %d\n", ctx.rowNum)
		return
	}
	fmt.Printf("\nTotal rows: %d\n", ctx.rowNum)
}

func openMeasurePart(id uint64, root string, fileSystem fs.FileSystem) (*measurePart, error) {
	var p measurePart
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
	p.primaryBlockMetadata, err = readMeasurePrimaryBlockMetadata(metaFile)
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

func closeMeasurePart(p *measurePart) {
	if p.primary != nil {
		fs.MustClose(p.primary)
	}
	if p.timestamps != nil {
		fs.MustClose(p.timestamps)
	}
	if p.fieldValues != nil {
		fs.MustClose(p.fieldValues)
	}
	for _, r := range p.tagFamilies {
		fs.MustClose(r)
	}
	for _, r := range p.tagFamilyMetadata {
		fs.MustClose(r)
	}
}

func readMeasurePrimaryBlockMetadata(r fs.Reader) ([]measurePrimaryBlockMetadata, error) {
	sr := r.SequentialRead()
	data, err := io.ReadAll(sr)
	if err != nil {
		return nil, fmt.Errorf("cannot read: %w", err)
	}
	fs.MustClose(sr)

	decompressed, err := zstd.Decompress(nil, data)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress: %w", err)
	}

	var result []measurePrimaryBlockMetadata
	src := decompressed
	for len(src) > 0 {
		var pbm measurePrimaryBlockMetadata
		src, err = unmarshalMeasurePrimaryBlockMetadata(&pbm, src)
		if err != nil {
			return nil, err
		}
		result = append(result, pbm)
	}
	return result, nil
}

func unmarshalMeasurePrimaryBlockMetadata(pbm *measurePrimaryBlockMetadata, src []byte) ([]byte, error) {
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

func parseMeasureBlockMetadata(src []byte) ([]*measureBlockMetadata, error) {
	var result []*measureBlockMetadata
	for len(src) > 0 {
		bm, tail, err := unmarshalMeasureBlockMetadata(src)
		if err != nil {
			return nil, err
		}
		result = append(result, bm)
		src = tail
	}
	return result, nil
}

func unmarshalMeasureBlockMetadata(src []byte) (*measureBlockMetadata, []byte, error) {
	var bm measureBlockMetadata

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
		bm.tagFamilies = make(map[string]*measureDataBlock)
		for i := uint64(0); i < tagFamilyCount; i++ {
			var nameBytes []byte
			var err error
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot unmarshal tagFamily name: %w", err)
			}
			tf := &measureDataBlock{}
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

func (tm *measureTimestampsMetadata) unmarshal(src []byte) []byte {
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

func (db *measureDataBlock) unmarshal(src []byte) []byte {
	src, n := encoding.BytesToVarUint64(src)
	db.offset = n
	src, n = encoding.BytesToVarUint64(src)
	db.size = n
	return src
}

func (cfm *measureColumnFamilyMetadata) unmarshal(src []byte) ([]byte, error) {
	src, columnMetadataLen := encoding.BytesToVarUint64(src)
	if columnMetadataLen < 1 {
		return src, nil
	}
	cfm.columns = make([]measureColumnMetadata, columnMetadataLen)
	var err error
	for i := range cfm.columns {
		src, err = cfm.columns[i].unmarshal(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal columnMetadata %d: %w", i, err)
		}
	}
	return src, nil
}

func (cm *measureColumnMetadata) unmarshal(src []byte) ([]byte, error) {
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

func readMeasureTimestamps(tm measureTimestampsMetadata, count int, reader fs.Reader) ([]int64, []int64, error) {
	data := make([]byte, tm.dataBlock.size)
	fs.MustReadData(reader, int64(tm.dataBlock.offset), data)

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

func readMeasureTagValues(decoder *encoding.BytesBlockDecoder, tagBlock measureDataBlock, _ string, count int,
	valueReader fs.Reader, valueType pbv1.ValueType,
) ([][]byte, error) {
	// Read tag values
	bb := &bytes.Buffer{}
	bb.Buf = make([]byte, tagBlock.size)
	fs.MustReadData(valueReader, int64(tagBlock.offset), bb.Buf)

	// Decode values using the internal encoding package
	var err error
	var values [][]byte
	values, err = internalencoding.DecodeTagValues(values, decoder, bb, valueType, count)
	if err != nil {
		return nil, fmt.Errorf("cannot decode tag values: %w", err)
	}

	return values, nil
}

func readMeasureFieldValues(decoder *encoding.BytesBlockDecoder, fieldBlock measureDataBlock, _ string, count int,
	valueReader fs.Reader, valueType pbv1.ValueType,
) ([][]byte, error) {
	// Read field values
	bb := &bytes.Buffer{}
	bb.Buf = make([]byte, fieldBlock.size)
	fs.MustReadData(valueReader, int64(fieldBlock.offset), bb.Buf)

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

func discoverMeasurePartIDs(shardPath string) ([]uint64, error) {
	entries, err := os.ReadDir(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard directory: %w", err)
	}

	var partIDs []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == dirNameSidx || name == dirNameMeta {
			continue
		}
		partID, err := strconv.ParseUint(name, 16, 64)
		if err == nil {
			partIDs = append(partIDs, partID)
		}
	}

	sort.Slice(partIDs, func(i, j int) bool {
		return partIDs[i] < partIDs[j]
	})

	return partIDs, nil
}

func loadMeasureSeriesMap(segmentPath string) (map[common.SeriesID]string, error) {
	seriesIndexPath := filepath.Join(segmentPath, dirNameSidx)

	l := logger.GetLogger("dump-measure")

	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   seriesIndexPath,
		Logger: l,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open series index: %w", err)
	}
	defer store.Close()

	ctx := context.Background()
	iter, err := store.SeriesIterator(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create series iterator: %w", err)
	}
	defer iter.Close()

	seriesMap := make(map[common.SeriesID]string)
	for iter.Next() {
		series := iter.Val()
		if len(series.EntityValues) > 0 {
			seriesID := common.SeriesID(convert.Hash(series.EntityValues))
			seriesText := string(series.EntityValues)
			seriesMap[seriesID] = seriesText
		}
	}

	return seriesMap, nil
}

func parseMeasureCriteriaJSON(criteriaJSON string) (*modelv1.Criteria, error) {
	criteria := &modelv1.Criteria{}
	err := protojson.Unmarshal([]byte(criteriaJSON), criteria)
	if err != nil {
		return nil, fmt.Errorf("invalid criteria JSON: %w", err)
	}
	return criteria, nil
}

func parseMeasureProjectionTags(projectionStr string) []string {
	if projectionStr == "" {
		return nil
	}

	tags := strings.Split(projectionStr, ",")
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			result = append(result, tag)
		}
	}
	return result
}

func discoverMeasureColumns(partIDs []uint64, shardPath string, fileSystem fs.FileSystem) ([]string, []string, error) {
	if len(partIDs) == 0 {
		return nil, nil, nil
	}

	p, err := openMeasurePart(partIDs[0], shardPath, fileSystem)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open first part: %w", err)
	}
	defer closeMeasurePart(p)

	tagNames := make(map[string]bool)
	fieldNames := make(map[string]bool)
	partID := partIDs[0]
	for tagFamilyName := range p.tagFamilies {
		// Read tag family metadata to get tag names
		if tagFamilyMetadataReader, ok := p.tagFamilyMetadata[tagFamilyName]; ok {
			metaData, err := io.ReadAll(tagFamilyMetadataReader.SequentialRead())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error reading tag family metadata %s in part %016x: %v\n", tagFamilyName, partID, err)
				continue
			}
			var cfm measureColumnFamilyMetadata
			_, err = cfm.unmarshal(metaData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error parsing tag family metadata %s in part %016x: %v\n", tagFamilyName, partID, err)
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
		fs.MustReadData(p.primary, int64(p.primaryBlockMetadata[0].offset), primaryData)

		decompressed, err := zstd.Decompress(nil, primaryData)
		if err == nil {
			blockMetadatas, err := parseMeasureBlockMetadata(decompressed)
			if err == nil && len(blockMetadatas) > 0 {
				for _, colMeta := range blockMetadatas[0].field.columns {
					fieldNames[colMeta.name] = true
				}
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

func writeMeasureRowAsText(row measureRowData, rowNum int, projectionTags []string, seriesMap map[common.SeriesID]string) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", row.partID, row.partID)
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.timestamp))
	fmt.Printf("  Version: %d\n", row.version)
	fmt.Printf("  SeriesID: %d\n", row.seriesID)

	if seriesMap != nil {
		if seriesText, ok := seriesMap[row.seriesID]; ok {
			fmt.Printf("  Series: %s\n", seriesText)
		}
	}

	if len(row.fields) > 0 {
		fmt.Printf("  Fields:\n")
		var fieldNames []string
		for name := range row.fields {
			fieldNames = append(fieldNames, name)
		}
		sort.Strings(fieldNames)
		for _, name := range fieldNames {
			value := row.fields[name]
			valueType := row.fieldTypes[name]
			if value == nil {
				fmt.Printf("    %s: <nil>\n", name)
			} else {
				fmt.Printf("    %s: %s\n", name, formatTagValueForDisplay(value, valueType))
			}
		}
	}

	if len(row.tags) > 0 {
		fmt.Printf("  Tags:\n")

		var tagsToShow []string
		if len(projectionTags) > 0 {
			tagsToShow = projectionTags
		} else {
			for name := range row.tags {
				tagsToShow = append(tagsToShow, name)
			}
			sort.Strings(tagsToShow)
		}

		for _, name := range tagsToShow {
			value, exists := row.tags[name]
			if !exists {
				continue
			}
			if value == nil {
				fmt.Printf("    %s: <nil>\n", name)
			} else {
				fmt.Printf("    %s: %s\n", name, formatTagValueForDisplay(value, pbv1.ValueTypeStr))
			}
		}
	}
	fmt.Printf("\n")
}

func writeMeasureRowAsCSV(writer *csv.Writer, row measureRowData, fieldColumns []string, tagColumns []string, seriesMap map[common.SeriesID]string) error {
	seriesText := ""
	if seriesMap != nil {
		if text, ok := seriesMap[row.seriesID]; ok {
			seriesText = text
		}
	}

	csvRow := []string{
		fmt.Sprintf("%d", row.partID),
		formatTimestamp(row.timestamp),
		fmt.Sprintf("%d", row.version),
		fmt.Sprintf("%d", row.seriesID),
		seriesText,
	}

	// Add field values
	for _, fieldName := range fieldColumns {
		value := ""
		if fieldValue, exists := row.fields[fieldName]; exists && fieldValue != nil {
			valueType := row.fieldTypes[fieldName]
			value = formatTagValueForDisplay(fieldValue, valueType)
			// Remove quotes if it's a string
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}
		}
		csvRow = append(csvRow, value)
	}

	// Add tag values
	for _, tagName := range tagColumns {
		value := ""
		if tagValue, exists := row.tags[tagName]; exists && tagValue != nil {
			value = string(tagValue)
		}
		csvRow = append(csvRow, value)
	}

	return writer.Write(csvRow)
}

func convertMeasureTagValue(value []byte) *modelv1.TagValue {
	if value == nil {
		return pbv1.NullTagValue
	}
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: string(value),
			},
		},
	}
}

type measureTagRegistry struct {
	tags map[string][]byte
}

func (r *measureTagRegistry) FindTagSpecByName(name string) *logical.TagSpec {
	return &logical.TagSpec{
		Spec: &databasev1.TagSpec{
			Name: name,
			Type: databasev1.TagType_TAG_TYPE_STRING,
		},
		TagFamilyIdx: 0,
		TagIdx:       0,
	}
}

func (r *measureTagRegistry) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *measureTagRegistry) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *measureTagRegistry) EntityList() []string {
	return nil
}

func (r *measureTagRegistry) CreateTagRef(_ ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return nil, fmt.Errorf("CreateTagRef not supported in dump tool")
}

func (r *measureTagRegistry) CreateFieldRef(_ ...*logical.Field) ([]*logical.FieldRef, error) {
	return nil, fmt.Errorf("CreateFieldRef not supported in dump tool")
}

func (r *measureTagRegistry) ProjTags(_ ...[]*logical.TagRef) logical.Schema {
	return r
}

func (r *measureTagRegistry) ProjFields(_ ...*logical.FieldRef) logical.Schema {
	return r
}

func (r *measureTagRegistry) Children() []logical.Schema {
	return nil
}

func measureTagValueDecoder(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
	if value == nil && valueArr == nil {
		return pbv1.NullTagValue
	}

	switch valueType {
	case pbv1.ValueTypeStr:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: string(value),
				},
			},
		}
	case pbv1.ValueTypeInt64:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: convert.BytesToInt64(value),
				},
			},
		}
	default:
		if value != nil {
			return &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: string(value),
					},
				},
			}
		}
		return pbv1.NullTagValue
	}
}
