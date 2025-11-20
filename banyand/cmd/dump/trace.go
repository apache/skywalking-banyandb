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
	"time"

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

func newTraceCmd() *cobra.Command {
	var shardPath string
	var segmentPath string
	var verbose bool
	var csvOutput bool
	var criteriaJSON string
	var projectionTags string

	cmd := &cobra.Command{
		Use:   "trace",
		Short: "Dump trace shard data",
		Long: `Dump and display contents of a trace shard directory (containing multiple parts).
Outputs trace data in human-readable format or CSV.

Supports filtering by criteria and projecting specific tags.`,
		Example: `  # Display trace data from shard in text format
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment

  # Display with verbose hex dumps
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment -v

  # Filter by criteria
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}'

  # Project specific tags
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --projection "tag1,tag2,tag3"

  # Output as CSV
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv

  # Save CSV to file
  dump trace --shard-path /path/to/shard-0 --segment-path /path/to/segment --csv > output.csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if shardPath == "" {
				return fmt.Errorf("--shard-path flag is required")
			}
			if segmentPath == "" {
				return fmt.Errorf("--segment-path flag is required")
			}
			return dumpTraceShard(shardPath, segmentPath, verbose, csvOutput, criteriaJSON, projectionTags)
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

//nolint:gocyclo // dumpTraceShard has high complexity due to multiple output formats and filtering options
func dumpTraceShard(shardPath, segmentPath string, verbose bool, csvOutput bool, criteriaJSON, projectionTagsStr string) error {
	// Discover all part directories in the shard
	partIDs, err := discoverTracePartIDs(shardPath)
	if err != nil {
		return fmt.Errorf("failed to discover part IDs: %w", err)
	}

	if len(partIDs) == 0 {
		fmt.Println("No parts found in shard directory")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Found %d parts in shard\n", len(partIDs))

	// Load series information for human-readable output
	seriesMap, err := loadTraceSeriesMap(segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		seriesMap = nil // Continue without series names
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from segment\n", len(seriesMap))
	}

	// Parse criteria if provided
	var criteria *modelv1.Criteria
	var tagFilter logical.TagFilter
	if criteriaJSON != "" {
		criteria, err = parseTraceCriteriaJSON(criteriaJSON)
		if err != nil {
			return fmt.Errorf("failed to parse criteria: %w", err)
		}
		tagFilter, err = logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			return fmt.Errorf("failed to build tag filter: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Applied criteria filter\n")
	}

	// Parse projection tags
	var projectionTags []string
	if projectionTagsStr != "" {
		projectionTags = parseTraceProjectionTags(projectionTagsStr)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", projectionTags)
	}

	// Open the file system
	fileSystem := fs.NewLocalFileSystem()

	// Determine tag columns for CSV output
	var tagColumns []string
	if csvOutput {
		if len(projectionTags) > 0 {
			tagColumns = projectionTags
		} else {
			// Scan first part to determine all available tags
			tagColumns, err = discoverTagColumns(partIDs, shardPath, fileSystem)
			if err != nil {
				return fmt.Errorf("failed to discover tag columns: %w", err)
			}
		}
	}

	// Initialize output
	var writer *csv.Writer
	var rowNum int
	if csvOutput {
		writer = csv.NewWriter(os.Stdout)
		defer writer.Flush()

		// Write CSV header
		header := []string{"PartID", "TraceID", "SpanID", "SeriesID", "Series", "SpanDataSize"}
		header = append(header, tagColumns...)
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	} else {
		fmt.Printf("================================================================================\n")
		fmt.Fprintf(os.Stderr, "Processing parts...\n")
	}

	// Process each part and stream output
	for partIdx, partID := range partIDs {
		fmt.Fprintf(os.Stderr, "Processing part %d/%d (0x%016x)...\n", partIdx+1, len(partIDs), partID)

		p, err := openFilePart(partID, shardPath, fileSystem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open part %016x: %v\n", partID, err)
			continue
		}

		decoder := &encoding.BytesBlockDecoder{}
		partRowCount := 0

		// Process all blocks in this part
		for _, pbm := range p.primaryBlockMetadata {
			// Read primary data block
			primaryData := make([]byte, pbm.size)
			fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

			// Decompress
			decompressed, err := zstd.Decompress(nil, primaryData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error decompressing primary data in part %016x: %v\n", partID, err)
				continue
			}

			// Parse ALL block metadata entries from this primary block
			blockMetadatas, err := parseAllBlockMetadata(decompressed, p.tagType)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error parsing block metadata in part %016x: %v\n", partID, err)
				continue
			}

			// Process each trace block within this primary block
			for _, bm := range blockMetadatas {
				// Read spans
				spans, spanIDs, err := readSpans(decoder, bm.spans, int(bm.count), p.spans)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Warning: Error reading spans for trace %s in part %016x: %v\n", bm.traceID, partID, err)
					continue
				}

				// Read tags
				tags := make(map[string][][]byte)
				for tagName, tagBlock := range bm.tags {
					tagValues, err := readTagValues(decoder, tagBlock, tagName, int(bm.count), p.tagMetadata[tagName], p.tags[tagName], p.tagType[tagName])
					if err != nil {
						fmt.Fprintf(os.Stderr, "Warning: Error reading tag %s for trace %s in part %016x: %v\n", tagName, bm.traceID, partID, err)
						continue
					}
					tags[tagName] = tagValues
				}

				// Process each span as a row
				for i := 0; i < len(spans); i++ {
					// Build tag map for this span
					spanTags := make(map[string][]byte)
					for tagName, tagValues := range tags {
						if i < len(tagValues) {
							spanTags[tagName] = tagValues[i]
						}
					}

					// Calculate series ID from entity tags
					seriesID := calculateSeriesIDFromTags(spanTags)

					// Apply criteria filter if specified
					if tagFilter != nil && tagFilter != logical.DummyFilter {
						if !matchesCriteria(spanTags, p.tagType, tagFilter) {
							continue
						}
					}

					row := traceRowData{
						partID:   partID,
						traceID:  bm.traceID,
						spanID:   spanIDs[i],
						spanData: spans[i],
						tags:     spanTags,
						seriesID: seriesID,
					}

					// Stream output immediately
					if csvOutput {
						if err := writeTraceRowAsCSV(writer, row, tagColumns, seriesMap); err != nil {
							return err
						}
					} else {
						writeTraceRowAsText(row, rowNum+1, verbose, projectionTags, seriesMap)
					}

					rowNum++
					partRowCount++
				}
			}
		}

		closePart(p)
		fmt.Fprintf(os.Stderr, "  Part %d/%d: processed %d rows (total: %d)\n", partIdx+1, len(partIDs), partRowCount, rowNum)
	}

	if !csvOutput {
		fmt.Printf("\nTotal rows: %d\n", rowNum)
	} else {
		fmt.Fprintf(os.Stderr, "Total rows written: %d\n", rowNum)
	}

	return nil
}

func formatTimestamp(nanos int64) string {
	if nanos == 0 {
		return "N/A"
	}
	// Convert nanoseconds to time.Time
	t := time.Unix(0, nanos)
	return t.Format(time.RFC3339Nano)
}

func formatTagValueForDisplay(data []byte, vt pbv1.ValueType) string {
	if data == nil {
		return "<nil>"
	}
	switch vt {
	case pbv1.ValueTypeStr:
		return fmt.Sprintf("%q", string(data))
	case pbv1.ValueTypeInt64:
		if len(data) >= 8 {
			return fmt.Sprintf("%d", convert.BytesToInt64(data))
		}
		return fmt.Sprintf("(invalid int64 data: %d bytes)", len(data))
	case pbv1.ValueTypeFloat64:
		if len(data) >= 8 {
			return fmt.Sprintf("%f", convert.BytesToFloat64(data))
		}
		return fmt.Sprintf("(invalid float64 data: %d bytes)", len(data))
	case pbv1.ValueTypeTimestamp:
		if len(data) >= 8 {
			nanos := convert.BytesToInt64(data)
			return formatTimestamp(nanos)
		}
		return fmt.Sprintf("(invalid timestamp data: %d bytes)", len(data))
	case pbv1.ValueTypeBinaryData:
		if isPrintable(data) {
			return fmt.Sprintf("%q", string(data))
		}
		return fmt.Sprintf("(binary: %d bytes)", len(data))
	default:
		if isPrintable(data) {
			return fmt.Sprintf("%q", string(data))
		}
		return fmt.Sprintf("(binary: %d bytes)", len(data))
	}
}

func unmarshalVarArray(dest, src []byte) ([]byte, []byte, error) {
	if len(src) == 0 {
		return nil, nil, fmt.Errorf("empty entity value")
	}
	if src[0] == internalencoding.EntityDelimiter {
		return dest, src[1:], nil
	}
	for len(src) > 0 {
		switch {
		case src[0] == internalencoding.Escape:
			if len(src) < 2 {
				return nil, nil, fmt.Errorf("invalid escape character")
			}
			src = src[1:]
			dest = append(dest, src[0])
		case src[0] == internalencoding.EntityDelimiter:
			return dest, src[1:], nil
		default:
			dest = append(dest, src[0])
		}
		src = src[1:]
	}
	return nil, nil, fmt.Errorf("invalid variable array")
}

func isPrintable(data []byte) bool {
	for _, b := range data {
		if b < 32 && b != '\n' && b != '\r' && b != '\t' || b > 126 {
			return false
		}
	}
	return true
}

func printHexDump(data []byte, indent int) {
	indentStr := strings.Repeat(" ", indent)
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}
		fmt.Printf("%s%04x:", indentStr, i)
		for j := i; j < end; j++ {
			fmt.Printf(" %02x", data[j])
		}
		// Padding
		for j := end; j < i+16; j++ {
			fmt.Print("   ")
		}
		fmt.Print("  |")
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				fmt.Printf("%c", data[j])
			} else {
				fmt.Print(".")
			}
		}
		fmt.Println("|")
	}
}

// Unexported helper functions to access trace package internals.
// These mirror the internal structure definitions.

type partMetadata struct {
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

type part struct {
	primary              fs.Reader
	spans                fs.Reader
	fileSystem           fs.FileSystem
	tagMetadata          map[string]fs.Reader
	tags                 map[string]fs.Reader
	tagType              map[string]pbv1.ValueType
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
	partMetadata         partMetadata
}

type traceRowData struct {
	tags     map[string][]byte
	traceID  string
	spanID   string
	spanData []byte
	partID   uint64
	seriesID common.SeriesID
}

func openFilePart(id uint64, root string, fileSystem fs.FileSystem) (*part, error) {
	var p part
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

func closePart(p *part) {
	if p.primary != nil {
		fs.MustClose(p.primary)
	}
	if p.spans != nil {
		fs.MustClose(p.spans)
	}
	for _, r := range p.tags {
		fs.MustClose(r)
	}
	for _, r := range p.tagMetadata {
		fs.MustClose(r)
	}
}

func readPrimaryBlockMetadata(r fs.Reader) ([]primaryBlockMetadata, error) {
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

// Helper functions for new shard-level dump.

func discoverTracePartIDs(shardPath string) ([]uint64, error) {
	entries, err := os.ReadDir(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read shard directory: %w", err)
	}

	var partIDs []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Skip special directories
		name := entry.Name()
		if name == "sidx" || name == "meta" {
			continue
		}
		// Try to parse as hex part ID
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

func loadTraceSeriesMap(segmentPath string) (map[common.SeriesID]string, error) {
	seriesIndexPath := filepath.Join(segmentPath, "sidx")

	l := logger.GetLogger("dump-trace")

	// Create inverted index store
	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   seriesIndexPath,
		Logger: l,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open series index: %w", err)
	}
	defer store.Close()

	// Get series iterator
	ctx := context.Background()
	iter, err := store.SeriesIterator(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create series iterator: %w", err)
	}
	defer iter.Close()

	// Build map of SeriesID -> text representation
	seriesMap := make(map[common.SeriesID]string)
	for iter.Next() {
		series := iter.Val()
		if len(series.EntityValues) > 0 {
			seriesID := common.SeriesID(convert.Hash(series.EntityValues))
			// Convert EntityValues bytes to readable string
			seriesText := string(series.EntityValues)
			seriesMap[seriesID] = seriesText
		}
	}

	return seriesMap, nil
}

func parseTraceCriteriaJSON(criteriaJSON string) (*modelv1.Criteria, error) {
	criteria := &modelv1.Criteria{}
	err := protojson.Unmarshal([]byte(criteriaJSON), criteria)
	if err != nil {
		return nil, fmt.Errorf("invalid criteria JSON: %w", err)
	}
	return criteria, nil
}

func parseTraceProjectionTags(projectionStr string) []string {
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

func calculateSeriesIDFromTags(tags map[string][]byte) common.SeriesID {
	// Extract entity values from tags to calculate series ID
	// Entity tags typically follow naming conventions like "service_id", "instance_id", etc.
	// We'll collect all tag key-value pairs and hash them
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
			entityValues = append(entityValues, internalencoding.EntityDelimiter)
		}
	}

	if len(entityValues) == 0 {
		return 0
	}

	return common.SeriesID(convert.Hash(entityValues))
}

func matchesCriteria(tags map[string][]byte, tagTypes map[string]pbv1.ValueType, filter logical.TagFilter) bool {
	// Convert tags to modelv1.Tag format
	modelTags := make([]*modelv1.Tag, 0, len(tags))
	for name, value := range tags {
		if value == nil {
			continue
		}

		valueType := tagTypes[name]
		tagValue := convertTagValue(value, valueType)
		if tagValue != nil {
			modelTags = append(modelTags, &modelv1.Tag{
				Key:   name,
				Value: tagValue,
			})
		}
	}

	// Use TagFilterMatcher to check if tags match the filter
	// Create a simple registry for the available tags
	registry := &traceTagRegistry{
		tagTypes: tagTypes,
	}

	matcher := logical.NewTagFilterMatcher(filter, registry, traceTagValueDecoder)
	match, _ := matcher.Match(modelTags)
	return match
}

func convertTagValue(value []byte, valueType pbv1.ValueType) *modelv1.TagValue {
	if value == nil {
		return pbv1.NullTagValue
	}

	switch valueType {
	case pbv1.ValueTypeStr:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: string(value),
				},
			},
		}
	case pbv1.ValueTypeInt64:
		if len(value) >= 8 {
			return &modelv1.TagValue{
				Value: &modelv1.TagValue_Int{
					Int: &modelv1.Int{
						Value: convert.BytesToInt64(value),
					},
				},
			}
		}
	case pbv1.ValueTypeStrArr:
		// Decode string array
		var values []string
		var err error
		remaining := value
		for len(remaining) > 0 {
			var decoded []byte
			decoded, remaining, err = unmarshalVarArray(nil, remaining)
			if err != nil {
				break
			}
			if len(decoded) > 0 {
				values = append(values, string(decoded))
			}
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{
					Value: values,
				},
			},
		}
	case pbv1.ValueTypeBinaryData:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: value,
			},
		}
	case pbv1.ValueTypeUnknown:
		// Fall through to default
	case pbv1.ValueTypeFloat64:
		// Fall through to default
	case pbv1.ValueTypeInt64Arr:
		// Fall through to default
	case pbv1.ValueTypeTimestamp:
		// Fall through to default
	}

	// Default: try to return as string
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: string(value),
			},
		},
	}
}

// traceTagRegistry implements logical.Schema for tag filtering.
type traceTagRegistry struct {
	tagTypes map[string]pbv1.ValueType
}

func (r *traceTagRegistry) FindTagSpecByName(name string) *logical.TagSpec {
	valueType := r.tagTypes[name]
	var tagType databasev1.TagType
	switch valueType {
	case pbv1.ValueTypeStr:
		tagType = databasev1.TagType_TAG_TYPE_STRING
	case pbv1.ValueTypeInt64:
		tagType = databasev1.TagType_TAG_TYPE_INT
	case pbv1.ValueTypeStrArr:
		tagType = databasev1.TagType_TAG_TYPE_STRING_ARRAY
	default:
		tagType = databasev1.TagType_TAG_TYPE_STRING
	}

	return &logical.TagSpec{
		Spec: &databasev1.TagSpec{
			Name: name,
			Type: tagType,
		},
		TagFamilyIdx: 0,
		TagIdx:       0,
	}
}

func (r *traceTagRegistry) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *traceTagRegistry) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *traceTagRegistry) EntityList() []string {
	return nil
}

func (r *traceTagRegistry) CreateTagRef(_ ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return nil, fmt.Errorf("CreateTagRef not supported in dump tool")
}

func (r *traceTagRegistry) CreateFieldRef(_ ...*logical.Field) ([]*logical.FieldRef, error) {
	return nil, fmt.Errorf("CreateFieldRef not supported in dump tool")
}

func (r *traceTagRegistry) ProjTags(_ ...[]*logical.TagRef) logical.Schema {
	return r
}

func (r *traceTagRegistry) ProjFields(_ ...*logical.FieldRef) logical.Schema {
	return r
}

func (r *traceTagRegistry) Children() []logical.Schema {
	return nil
}

func traceTagValueDecoder(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
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
	case pbv1.ValueTypeStrArr:
		var values []string
		for _, v := range valueArr {
			values = append(values, string(v))
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{
					Value: values,
				},
			},
		}
	case pbv1.ValueTypeBinaryData:
		if value == nil {
			return pbv1.NullTagValue
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: value,
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

func discoverTagColumns(partIDs []uint64, shardPath string, fileSystem fs.FileSystem) ([]string, error) {
	if len(partIDs) == 0 {
		return nil, nil
	}

	// Open first part to discover tag columns
	p, err := openFilePart(partIDs[0], shardPath, fileSystem)
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

func writeTraceRowAsText(row traceRowData, rowNum int, verbose bool, projectionTags []string, seriesMap map[common.SeriesID]string) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", row.partID, row.partID)
	fmt.Printf("  TraceID: %s\n", row.traceID)
	fmt.Printf("  SpanID: %s\n", row.spanID)
	fmt.Printf("  SeriesID: %d\n", row.seriesID)

	// Add series text if available
	if seriesMap != nil {
		if seriesText, ok := seriesMap[row.seriesID]; ok {
			fmt.Printf("  Series: %s\n", seriesText)
		}
	}

	fmt.Printf("  Span Data: %d bytes\n", len(row.spanData))
	if verbose {
		fmt.Printf("  Span Content:\n")
		printHexDump(row.spanData, 4)
	} else {
		if isPrintable(row.spanData) {
			fmt.Printf("  Span: %s\n", string(row.spanData))
		} else {
			fmt.Printf("  Span: (binary data, %d bytes)\n", len(row.spanData))
		}
	}

	// Print projected tags or all tags
	if len(row.tags) > 0 {
		fmt.Printf("  Tags:\n")

		var tagsToShow []string
		if len(projectionTags) > 0 {
			tagsToShow = projectionTags
		} else {
			// Show all tags
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

func writeTraceRowAsCSV(writer *csv.Writer, row traceRowData, tagColumns []string, seriesMap map[common.SeriesID]string) error {
	seriesText := ""
	if seriesMap != nil {
		if text, ok := seriesMap[row.seriesID]; ok {
			seriesText = text
		}
	}

	csvRow := []string{
		fmt.Sprintf("%d", row.partID),
		row.traceID,
		row.spanID,
		fmt.Sprintf("%d", row.seriesID),
		seriesText,
		strconv.Itoa(len(row.spanData)),
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
