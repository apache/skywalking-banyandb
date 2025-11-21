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

type streamDumpOptions struct {
	shardPath      string
	segmentPath    string
	criteriaJSON   string
	projectionTags string
	verbose        bool
	csvOutput      bool
}

func newStreamCmd() *cobra.Command {
	var shardPath string
	var segmentPath string
	var verbose bool
	var csvOutput bool
	var criteriaJSON string
	var projectionTags string

	cmd := &cobra.Command{
		Use:   "stream",
		Short: "Dump stream shard data",
		Long: `Dump and display contents of a stream shard directory (containing multiple parts).
Outputs stream data in human-readable format or CSV.

Supports filtering by criteria and projecting specific tags.`,
		Example: `  # Display stream data from shard in text format
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment

  # Display with verbose hex dumps
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment -v

  # Filter by criteria
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}

  # Project specific tags
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --projection "tag1,tag2,tag3"

  # Output as CSV
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv

  # Save CSV to file
  dump stream --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv > output.csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if shardPath == "" {
				return fmt.Errorf("--shard-path flag is required")
			}
			if segmentPath == "" {
				return fmt.Errorf("--segment-path flag is required")
			}
			return dumpStreamShard(streamDumpOptions{
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

func dumpStreamShard(opts streamDumpOptions) error {
	ctx, err := newStreamDumpContext(opts)
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

type streamPartMetadata struct {
	CompressedSizeBytes   uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes uint64 `json:"uncompressedSizeBytes"`
	TotalCount            uint64 `json:"totalCount"`
	BlocksCount           uint64 `json:"blocksCount"`
	MinTimestamp          int64  `json:"minTimestamp"`
	MaxTimestamp          int64  `json:"maxTimestamp"`
	ID                    uint64 `json:"-"`
}

type streamPrimaryBlockMetadata struct {
	seriesID     common.SeriesID
	minTimestamp int64
	maxTimestamp int64
	offset       uint64
	size         uint64
}

type streamDataBlock struct {
	offset uint64
	size   uint64
}

type streamBlockMetadata struct {
	tagFamilies   map[string]*streamDataBlock
	timestamps    streamTimestampsMetadata
	elementIDs    streamElementIDsMetadata
	seriesID      common.SeriesID
	uncompressedSizeBytes uint64
	count         uint64
}

type streamTimestampsMetadata struct {
	dataBlock      streamDataBlock
	min            int64
	max            int64
	elementIDsOffset uint64
	encodeType      encoding.EncodeType
}

type streamElementIDsMetadata struct {
	dataBlock  streamDataBlock
	encodeType encoding.EncodeType
}

type streamPart struct {
	primary              fs.Reader
	timestamps           fs.Reader
	fileSystem           fs.FileSystem
	tagFamilyMetadata    map[string]fs.Reader
	tagFamilies          map[string]fs.Reader
	tagFamilyFilter      map[string]fs.Reader
	path                 string
	primaryBlockMetadata []streamPrimaryBlockMetadata
	partMetadata         streamPartMetadata
}

type streamRowData struct {
	tags       map[string][]byte
	elementID  uint64
	timestamp  int64
	partID     uint64
	seriesID   common.SeriesID
	elementData []byte
}

type streamDumpContext struct {
	tagFilter      logical.TagFilter
	fileSystem     fs.FileSystem
	seriesMap      map[common.SeriesID]string
	writer         *csv.Writer
	opts           streamDumpOptions
	partIDs        []uint64
	projectionTags []string
	tagColumns     []string
	rowNum         int
}

func newStreamDumpContext(opts streamDumpOptions) (*streamDumpContext, error) {
	ctx := &streamDumpContext{
		opts:       opts,
		fileSystem: fs.NewLocalFileSystem(),
	}

	partIDs, err := discoverStreamPartIDs(opts.shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to discover part IDs: %w", err)
	}
	if len(partIDs) == 0 {
		fmt.Println("No parts found in shard directory")
		return nil, nil
	}
	ctx.partIDs = partIDs
	fmt.Fprintf(os.Stderr, "Found %d parts in shard\n", len(partIDs))

	ctx.seriesMap, err = loadStreamSeriesMap(opts.segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		ctx.seriesMap = nil
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from segment\n", len(ctx.seriesMap))
	}

	if opts.criteriaJSON != "" {
		var criteria *modelv1.Criteria
		criteria, err = parseStreamCriteriaJSON(opts.criteriaJSON)
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
		ctx.projectionTags = parseStreamProjectionTags(opts.projectionTags)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", ctx.projectionTags)
	}

	if opts.csvOutput {
		if len(ctx.projectionTags) > 0 {
			ctx.tagColumns = ctx.projectionTags
		} else {
			ctx.tagColumns, err = discoverStreamTagColumns(ctx.partIDs, opts.shardPath, ctx.fileSystem)
			if err != nil {
				return nil, fmt.Errorf("failed to discover tag columns: %w", err)
			}
		}
	}

	if err := ctx.initOutput(); err != nil {
		return nil, err
	}

	return ctx, nil
}

func (ctx *streamDumpContext) initOutput() error {
	if !ctx.opts.csvOutput {
		fmt.Printf("================================================================================\n")
		fmt.Fprintf(os.Stderr, "Processing parts...\n")
		return nil
	}

	ctx.writer = csv.NewWriter(os.Stdout)
	header := []string{"PartID", "ElementID", "Timestamp", "SeriesID", "Series", "ElementDataSize"}
	header = append(header, ctx.tagColumns...)
	if err := ctx.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	return nil
}

func (ctx *streamDumpContext) close() {
	if ctx.writer != nil {
		ctx.writer.Flush()
	}
}

func (ctx *streamDumpContext) processParts() error {
	for partIdx, partID := range ctx.partIDs {
		fmt.Fprintf(os.Stderr, "Processing part %d/%d (0x%016x)...\n", partIdx+1, len(ctx.partIDs), partID)

		p, err := openStreamPart(partID, ctx.opts.shardPath, ctx.fileSystem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open part %016x: %v\n", partID, err)
			continue
		}

		partRowCount, partErr := ctx.processPart(partID, p)
		closeStreamPart(p)
		if partErr != nil {
			return partErr
		}

		fmt.Fprintf(os.Stderr, "  Part %d/%d: processed %d rows (total: %d)\n", partIdx+1, len(ctx.partIDs), partRowCount, ctx.rowNum)
	}
	return nil
}

func (ctx *streamDumpContext) processPart(partID uint64, p *streamPart) (int, error) {
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

		blockMetadatas, err := parseStreamBlockMetadata(decompressed)
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

func (ctx *streamDumpContext) processBlock(partID uint64, bm *streamBlockMetadata, p *streamPart, decoder *encoding.BytesBlockDecoder) (int, error) {
	// Read timestamps and element IDs (they are stored together in timestamps.bin)
	timestamps, elementIDs, err := readStreamTimestamps(decoder, bm.timestamps, int(bm.count), p.timestamps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Error reading timestamps/elementIDs for series %d in part %016x: %v\n", bm.seriesID, partID, err)
		return 0, nil
	}

	// Read tag families
	tagsByElement := ctx.readBlockTagFamilies(partID, bm, p, decoder)

	rows := 0
	for i := 0; i < len(timestamps); i++ {
		elementTags := make(map[string][]byte)
		for tagName, tagValues := range tagsByElement {
			if i < len(tagValues) {
				elementTags[tagName] = tagValues[i]
			}
		}

		if ctx.shouldSkip(elementTags) {
			continue
		}

		// Read element data from primary (if available)
		var elementData []byte
		// Note: In stream, element data is typically stored in primary.bin
		// For now, we'll use a placeholder or empty data

		row := streamRowData{
			partID:      partID,
			elementID:   elementIDs[i],
			timestamp:   timestamps[i],
			tags:        elementTags,
			seriesID:    bm.seriesID,
			elementData: elementData,
		}

		if err := ctx.writeRow(row); err != nil {
			return rows, err
		}

		rows++
	}

	return rows, nil
}

func (ctx *streamDumpContext) readBlockTagFamilies(partID uint64, bm *streamBlockMetadata, p *streamPart, decoder *encoding.BytesBlockDecoder) map[string][][]byte {
	tags := make(map[string][][]byte)
	for tagFamilyName, tagFamilyBlock := range bm.tagFamilies {
		// Read tag family metadata
		tagFamilyMetadataData := make([]byte, tagFamilyBlock.size)
		fs.MustReadData(p.tagFamilyMetadata[tagFamilyName], int64(tagFamilyBlock.offset), tagFamilyMetadataData)

		// Parse tag family metadata to get individual tag metadata
		tagMetadatas, err := parseStreamTagFamilyMetadata(tagFamilyMetadataData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Error parsing tag family metadata %s for series %d in part %016x: %v\n", tagFamilyName, bm.seriesID, partID, err)
			continue
		}

		// Read each tag in the tag family
		for _, tagMeta := range tagMetadatas {
			fullTagName := tagFamilyName + "." + tagMeta.name
			tagValues, err := readStreamTagValues(decoder, tagMeta.dataBlock, fullTagName, int(bm.count), p.tagFamilies[tagFamilyName], tagMeta.valueType)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error reading tag %s for series %d in part %016x: %v\n", fullTagName, bm.seriesID, partID, err)
				continue
			}
			tags[fullTagName] = tagValues
		}
	}
	return tags
}

func (ctx *streamDumpContext) shouldSkip(tags map[string][]byte) bool {
	if ctx.tagFilter == nil || ctx.tagFilter == logical.DummyFilter {
		return false
	}
	// Convert tags to modelv1.Tag format for filtering
	modelTags := make([]*modelv1.Tag, 0, len(tags))
	for name, value := range tags {
		if value == nil {
			continue
		}
		// Try to infer value type from the tag name or use string as default
		tagValue := convertStreamTagValue(value)
		if tagValue != nil {
			modelTags = append(modelTags, &modelv1.Tag{
				Key:   name,
				Value: tagValue,
			})
		}
	}

	// Create a simple registry for tag filtering
	registry := &streamTagRegistry{
		tags: tags,
	}

	matcher := logical.NewTagFilterMatcher(ctx.tagFilter, registry, streamTagValueDecoder)
	match, _ := matcher.Match(modelTags)
	return !match
}

func (ctx *streamDumpContext) writeRow(row streamRowData) error {
	if ctx.opts.csvOutput {
		if err := writeStreamRowAsCSV(ctx.writer, row, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writeStreamRowAsText(row, ctx.rowNum+1, ctx.opts.verbose, ctx.projectionTags, ctx.seriesMap)
	}
	ctx.rowNum++
	return nil
}

func (ctx *streamDumpContext) printSummary() {
	if ctx.opts.csvOutput {
		fmt.Fprintf(os.Stderr, "Total rows written: %d\n", ctx.rowNum)
		return
	}
	fmt.Printf("\nTotal rows: %d\n", ctx.rowNum)
}

func openStreamPart(id uint64, root string, fileSystem fs.FileSystem) (*streamPart, error) {
	var p streamPart
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
	p.primaryBlockMetadata, err = readStreamPrimaryBlockMetadata(metaFile)
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

func closeStreamPart(p *streamPart) {
	if p.primary != nil {
		fs.MustClose(p.primary)
	}
	if p.timestamps != nil {
		fs.MustClose(p.timestamps)
	}
	for _, r := range p.tagFamilies {
		fs.MustClose(r)
	}
	for _, r := range p.tagFamilyMetadata {
		fs.MustClose(r)
	}
	for _, r := range p.tagFamilyFilter {
		fs.MustClose(r)
	}
}

func readStreamPrimaryBlockMetadata(r fs.Reader) ([]streamPrimaryBlockMetadata, error) {
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

	var result []streamPrimaryBlockMetadata
	src := decompressed
	for len(src) > 0 {
		var pbm streamPrimaryBlockMetadata
		src, err = unmarshalStreamPrimaryBlockMetadata(&pbm, src)
		if err != nil {
			return nil, err
		}
		result = append(result, pbm)
	}
	return result, nil
}

func unmarshalStreamPrimaryBlockMetadata(pbm *streamPrimaryBlockMetadata, src []byte) ([]byte, error) {
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

func parseStreamBlockMetadata(src []byte) ([]*streamBlockMetadata, error) {
	var result []*streamBlockMetadata
	for len(src) > 0 {
		bm, tail, err := unmarshalStreamBlockMetadata(src)
		if err != nil {
			return nil, err
		}
		result = append(result, bm)
		src = tail
	}
	return result, nil
}

func unmarshalStreamBlockMetadata(src []byte) (*streamBlockMetadata, []byte, error) {
	var bm streamBlockMetadata

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
		bm.tagFamilies = make(map[string]*streamDataBlock)
		for i := uint64(0); i < tagFamilyCount; i++ {
			var nameBytes []byte
			var err error
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot unmarshal tagFamily name: %w", err)
			}
			tf := &streamDataBlock{}
			src = tf.unmarshal(src)
			bm.tagFamilies[string(nameBytes)] = tf
		}
	}

	return &bm, src, nil
}

func (tm *streamTimestampsMetadata) unmarshal(src []byte) []byte {
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

func (em *streamElementIDsMetadata) unmarshal(src []byte) []byte {
	src = em.dataBlock.unmarshal(src)
	em.encodeType = encoding.EncodeType(src[0])
	return src[1:]
}

func (db *streamDataBlock) unmarshal(src []byte) []byte {
	src, n := encoding.BytesToVarUint64(src)
	db.offset = n
	src, n = encoding.BytesToVarUint64(src)
	db.size = n
	return src
}

func readStreamTimestamps(decoder *encoding.BytesBlockDecoder, tm streamTimestampsMetadata, count int, reader fs.Reader) ([]int64, []uint64, error) {
	data := make([]byte, tm.dataBlock.size)
	fs.MustReadData(reader, int64(tm.dataBlock.offset), data)

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

type streamTagMetadata struct {
	name      string
	min       []byte
	max       []byte
	dataBlock streamDataBlock
	valueType pbv1.ValueType
}

func parseStreamTagFamilyMetadata(src []byte) ([]streamTagMetadata, error) {
	src, tagMetadataLen := encoding.BytesToVarUint64(src)
	if tagMetadataLen < 1 {
		return nil, nil
	}

	var result []streamTagMetadata
	for i := uint64(0); i < tagMetadataLen; i++ {
		var tm streamTagMetadata
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
		var filterBlock streamDataBlock
		src = filterBlock.unmarshal(src)

		result = append(result, tm)
	}

	return result, nil
}

func readStreamTagValues(decoder *encoding.BytesBlockDecoder, tagBlock streamDataBlock, _ string, count int,
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

func discoverStreamPartIDs(shardPath string) ([]uint64, error) {
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
		if name == "sidx" || name == "meta" {
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

func loadStreamSeriesMap(segmentPath string) (map[common.SeriesID]string, error) {
	seriesIndexPath := filepath.Join(segmentPath, "sidx")

	l := logger.GetLogger("dump-stream")

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

func parseStreamCriteriaJSON(criteriaJSON string) (*modelv1.Criteria, error) {
	criteria := &modelv1.Criteria{}
	err := protojson.Unmarshal([]byte(criteriaJSON), criteria)
	if err != nil {
		return nil, fmt.Errorf("invalid criteria JSON: %w", err)
	}
	return criteria, nil
}

func parseStreamProjectionTags(projectionStr string) []string {
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

func discoverStreamTagColumns(partIDs []uint64, shardPath string, fileSystem fs.FileSystem) ([]string, error) {
	if len(partIDs) == 0 {
		return nil, nil
	}

	p, err := openStreamPart(partIDs[0], shardPath, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("failed to open first part: %w", err)
	}
	defer closeStreamPart(p)

	tagNames := make(map[string]bool)
	for tagFamilyName := range p.tagFamilies {
		// Read tag family metadata to get tag names
		if tagFamilyMetadataReader, ok := p.tagFamilyMetadata[tagFamilyName]; ok {
			metaData, err := io.ReadAll(tagFamilyMetadataReader.SequentialRead())
			if err == nil {
				tagMetadatas, err := parseStreamTagFamilyMetadata(metaData)
				if err == nil {
					for _, tm := range tagMetadatas {
						fullTagName := tagFamilyName + "." + tm.name
						tagNames[fullTagName] = true
					}
				}
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

func writeStreamRowAsText(row streamRowData, rowNum int, verbose bool, projectionTags []string, seriesMap map[common.SeriesID]string) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", row.partID, row.partID)
	fmt.Printf("  ElementID: %d\n", row.elementID)
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.timestamp))
	fmt.Printf("  SeriesID: %d\n", row.seriesID)

	if seriesMap != nil {
		if seriesText, ok := seriesMap[row.seriesID]; ok {
			fmt.Printf("  Series: %s\n", seriesText)
		}
	}

	fmt.Printf("  Element Data: %d bytes\n", len(row.elementData))
	if verbose && len(row.elementData) > 0 {
		fmt.Printf("  Element Content:\n")
		printHexDump(row.elementData, 4)
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

func writeStreamRowAsCSV(writer *csv.Writer, row streamRowData, tagColumns []string, seriesMap map[common.SeriesID]string) error {
	seriesText := ""
	if seriesMap != nil {
		if text, ok := seriesMap[row.seriesID]; ok {
			seriesText = text
		}
	}

	csvRow := []string{
		fmt.Sprintf("%d", row.partID),
		fmt.Sprintf("%d", row.elementID),
		formatTimestamp(row.timestamp),
		fmt.Sprintf("%d", row.seriesID),
		seriesText,
		strconv.Itoa(len(row.elementData)),
	}

	for _, tagName := range tagColumns {
		value := ""
		if tagValue, exists := row.tags[tagName]; exists && tagValue != nil {
			value = string(tagValue)
		}
		csvRow = append(csvRow, value)
	}

	return writer.Write(csvRow)
}

func convertStreamTagValue(value []byte) *modelv1.TagValue {
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

type streamTagRegistry struct {
	tags map[string][]byte
}

func (r *streamTagRegistry) FindTagSpecByName(name string) *logical.TagSpec {
	return &logical.TagSpec{
		Spec: &databasev1.TagSpec{
			Name: name,
			Type: databasev1.TagType_TAG_TYPE_STRING,
		},
		TagFamilyIdx: 0,
		TagIdx:       0,
	}
}

func (r *streamTagRegistry) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *streamTagRegistry) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *streamTagRegistry) EntityList() []string {
	return nil
}

func (r *streamTagRegistry) CreateTagRef(_ ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return nil, fmt.Errorf("CreateTagRef not supported in dump tool")
}

func (r *streamTagRegistry) CreateFieldRef(_ ...*logical.Field) ([]*logical.FieldRef, error) {
	return nil, fmt.Errorf("CreateFieldRef not supported in dump tool")
}

func (r *streamTagRegistry) ProjTags(_ ...[]*logical.TagRef) logical.Schema {
	return r
}

func (r *streamTagRegistry) ProjFields(_ ...*logical.FieldRef) logical.Schema {
	return r
}

func (r *streamTagRegistry) Children() []logical.Schema {
	return nil
}

func streamTagValueDecoder(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
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

