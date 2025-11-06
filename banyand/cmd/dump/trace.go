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

	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func newTraceCmd() *cobra.Command {
	var partPath string
	var verbose bool
	var csvOutput bool

	cmd := &cobra.Command{
		Use:   "trace",
		Short: "Dump trace part data",
		Long: `Dump and display contents of a trace part directory.
Outputs trace data in human-readable format or CSV.`,
		Example: `  # Display trace data in text format
  dump trace -path /path/to/part/0000000000004db4

  # Display with verbose hex dumps
  dump trace -path /path/to/part/0000000000004db4 -v

  # Output as CSV
  dump trace -path /path/to/part/0000000000004db4 -csv

  # Save CSV to file
  dump trace -path /path/to/part/0000000000004db4 -csv > output.csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if partPath == "" {
				return fmt.Errorf("-path flag is required")
			}
			return dumpTracePart(partPath, verbose, csvOutput)
		},
	}

	cmd.Flags().StringVarP(&partPath, "path", "p", "", "Path to the trace part directory (required)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output (show raw data)")
	cmd.Flags().BoolVarP(&csvOutput, "csv", "c", false, "Output as CSV format")
	_ = cmd.MarkFlagRequired("path")

	return cmd
}

func dumpTracePart(partPath string, verbose bool, csvOutput bool) error {
	// Get the part ID from the directory name
	partName := filepath.Base(partPath)
	partID, err := strconv.ParseUint(partName, 16, 64)
	if err != nil {
		return fmt.Errorf("invalid part name %q: %w", partName, err)
	}

	// Get the root path (parent directory)
	rootPath := filepath.Dir(partPath)

	// Open the file system
	fileSystem := fs.NewLocalFileSystem()

	// Open the part
	p, err := openFilePart(partID, rootPath, fileSystem)
	if err != nil {
		return fmt.Errorf("failed to open part: %w", err)
	}
	defer closePart(p)

	if csvOutput {
		return dumpPartAsCSV(p)
	}

	// Original text output
	fmt.Printf("Opening trace part: %s (ID: %d)\n", partPath, partID)
	fmt.Printf("================================================================================\n\n")

	// Print part metadata
	fmt.Printf("Part Metadata:\n")
	fmt.Printf("  ID: %d (0x%016x)\n", p.partMetadata.ID, p.partMetadata.ID)
	fmt.Printf("  Total Count: %d\n", p.partMetadata.TotalCount)
	fmt.Printf("  Blocks Count: %d\n", p.partMetadata.BlocksCount)
	fmt.Printf("  Min Timestamp: %s (%d)\n", formatTimestamp(p.partMetadata.MinTimestamp), p.partMetadata.MinTimestamp)
	fmt.Printf("  Max Timestamp: %s (%d)\n", formatTimestamp(p.partMetadata.MaxTimestamp), p.partMetadata.MaxTimestamp)
	fmt.Printf("  Compressed Size: %d bytes\n", p.partMetadata.CompressedSizeBytes)
	fmt.Printf("  Uncompressed Span Size: %d bytes\n", p.partMetadata.UncompressedSpanSizeBytes)
	fmt.Printf("\n")

	// Print tag types
	if len(p.tagType) > 0 {
		fmt.Printf("Tag Types:\n")
		tagNames := make([]string, 0, len(p.tagType))
		for name := range p.tagType {
			tagNames = append(tagNames, name)
		}
		sort.Strings(tagNames)
		for _, name := range tagNames {
			fmt.Printf("  %s: %s\n", name, valueTypeName(p.tagType[name]))
		}
		fmt.Printf("\n")
	}

	// Print primary block metadata
	fmt.Printf("Primary Block Metadata (Total: %d blocks):\n", len(p.primaryBlockMetadata))
	fmt.Printf("--------------------------------------------------------------------------------\n")
	for i, pbm := range p.primaryBlockMetadata {
		fmt.Printf("Block %d:\n", i)
		fmt.Printf("  TraceID: %s\n", pbm.traceID)
		fmt.Printf("  Offset: %d\n", pbm.offset)
		fmt.Printf("  Size: %d bytes\n", pbm.size)

		// Read and decompress the primary data for this block
		if verbose {
			primaryData := make([]byte, pbm.size)
			fs.MustReadData(p.primary, int64(pbm.offset), primaryData)
			decompressed, err := zstd.Decompress(nil, primaryData)
			if err == nil {
				fmt.Printf("  Primary Data (decompressed %d bytes):\n", len(decompressed))
				printHexDump(decompressed, 4)
			}
		}
	}
	fmt.Printf("\n")

	// Read and display trace data
	fmt.Printf("Trace Data:\n")
	fmt.Printf("================================================================================\n\n")

	decoder := &encoding.BytesBlockDecoder{}

	rowNum := 0
	for _, pbm := range p.primaryBlockMetadata {
		// Read primary data block
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		// Decompress
		decompressed, err := zstd.Decompress(nil, primaryData)
		if err != nil {
			fmt.Printf("Error decompressing primary data: %v\n", err)
			continue
		}

		// Parse ALL block metadata entries from this primary block
		blockMetadatas, err := parseAllBlockMetadata(decompressed, p.tagType)
		if err != nil {
			fmt.Printf("Error parsing block metadata: %v\n", err)
			continue
		}

		// Process each trace block within this primary block
		for _, bm := range blockMetadatas {
			// Read spans
			spans, spanIDs, err := readSpans(decoder, bm.spans, int(bm.count), p.spans)
			if err != nil {
				fmt.Printf("Error reading spans for trace %s: %v\n", bm.traceID, err)
				continue
			}

			// Read tags
			tags := make(map[string][][]byte)
			for tagName, tagBlock := range bm.tags {
				tagValues, err := readTagValues(decoder, tagBlock, tagName, int(bm.count), p.tagMetadata[tagName], p.tags[tagName], p.tagType[tagName])
				if err != nil {
					fmt.Printf("Error reading tag %s for trace %s: %v\n", tagName, bm.traceID, err)
					continue
				}
				tags[tagName] = tagValues
			}

			// Display each span as a row
			for i := 0; i < len(spans); i++ {
				rowNum++
				fmt.Printf("Row %d:\n", rowNum)
				fmt.Printf("  TraceID: %s\n", bm.traceID)
				fmt.Printf("  SpanID: %s\n", spanIDs[i])
				fmt.Printf("  Span Data: %d bytes\n", len(spans[i]))
				if verbose {
					fmt.Printf("  Span Content:\n")
					printHexDump(spans[i], 4)
				} else {
					// Try to print as string if it's printable
					if isPrintable(spans[i]) {
						fmt.Printf("  Span: %s\n", string(spans[i]))
					} else {
						fmt.Printf("  Span: (binary data, %d bytes)\n", len(spans[i]))
					}
				}

				// Print tags for this span
				if len(tags) > 0 {
					fmt.Printf("  Tags:\n")
					tagNames := make([]string, 0, len(tags))
					for name := range tags {
						tagNames = append(tagNames, name)
					}
					sort.Strings(tagNames)
					for _, name := range tagNames {
						if i < len(tags[name]) {
							tagValue := tags[name][i]
							if tagValue == nil {
								fmt.Printf("    %s: <nil>\n", name)
							} else {
								valueType := p.tagType[name]
								fmt.Printf("    %s (%s): %s\n", name, valueTypeName(valueType), formatTagValueForDisplay(tagValue, valueType))
							}
						}
					}
				}
				fmt.Printf("\n")
			}
		}
	}

	fmt.Printf("Total rows: %d\n", rowNum)
	return nil
}

func dumpPartAsCSV(p *part) error {
	decoder := &encoding.BytesBlockDecoder{}

	// Collect all tag names in sorted order
	allTagNames := make([]string, 0, len(p.tagType))
	for name := range p.tagType {
		allTagNames = append(allTagNames, name)
	}
	sort.Strings(allTagNames)

	// Create CSV writer
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	header := []string{"TraceID", "SpanID", "SpanDataSize"}
	header = append(header, allTagNames...)
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Process all blocks and write rows
	for _, pbm := range p.primaryBlockMetadata {
		// Read primary data block
		primaryData := make([]byte, pbm.size)
		fs.MustReadData(p.primary, int64(pbm.offset), primaryData)

		// Decompress
		decompressed, err := zstd.Decompress(nil, primaryData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decompressing primary data: %v\n", err)
			continue
		}

		// Parse ALL block metadata entries from this primary block
		blockMetadatas, err := parseAllBlockMetadata(decompressed, p.tagType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing block metadata: %v\n", err)
			continue
		}

		// Process each block
		for _, bm := range blockMetadatas {
			// Read spans
			spans, spanIDs, err := readSpans(decoder, bm.spans, int(bm.count), p.spans)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading spans for trace %s: %v\n", bm.traceID, err)
				continue
			}

			// Read tags
			tags := make(map[string][][]byte)
			for tagName, tagBlock := range bm.tags {
				tagValues, err := readTagValues(decoder, tagBlock, tagName, int(bm.count), p.tagMetadata[tagName], p.tags[tagName], p.tagType[tagName])
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error reading tag %s for trace %s: %v\n", tagName, bm.traceID, err)
					continue
				}
				tags[tagName] = tagValues
			}

			// Write each span as a CSV row
			for i := 0; i < len(spans); i++ {
				row := append(make([]string, 0, len(header)), bm.traceID, spanIDs[i], strconv.Itoa(len(spans[i])))

				// Add tag values in the same order as header
				for _, tagName := range allTagNames {
					var value string
					if i < len(tags[tagName]) && tags[tagName][i] != nil {
						valueType := p.tagType[tagName]
						value = formatTagValueForCSV(tags[tagName][i], valueType)
					}
					row = append(row, value)
				}

				if err := writer.Write(row); err != nil {
					return fmt.Errorf("failed to write CSV row: %w", err)
				}
			}
		}
	}

	return nil
}

func valueTypeName(vt pbv1.ValueType) string {
	switch vt {
	case pbv1.ValueTypeStr:
		return "STRING"
	case pbv1.ValueTypeInt64:
		return "INT64"
	case pbv1.ValueTypeFloat64:
		return "FLOAT64"
	case pbv1.ValueTypeStrArr:
		return "STRING_ARRAY"
	case pbv1.ValueTypeInt64Arr:
		return "INT64_ARRAY"
	case pbv1.ValueTypeBinaryData:
		return "BINARY_DATA"
	case pbv1.ValueTypeTimestamp:
		return "TIMESTAMP"
	case pbv1.ValueTypeUnknown:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", vt)
	}
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

func formatTagValueForCSV(data []byte, vt pbv1.ValueType) string {
	if data == nil {
		return ""
	}
	switch vt {
	case pbv1.ValueTypeStr:
		return string(data)
	case pbv1.ValueTypeInt64:
		if len(data) >= 8 {
			return strconv.FormatInt(convert.BytesToInt64(data), 10)
		}
		return ""
	case pbv1.ValueTypeFloat64:
		if len(data) >= 8 {
			return strconv.FormatFloat(convert.BytesToFloat64(data), 'f', -1, 64)
		}
		return ""
	case pbv1.ValueTypeTimestamp:
		if len(data) >= 8 {
			nanos := convert.BytesToInt64(data)
			return formatTimestamp(nanos)
		}
		return ""
	case pbv1.ValueTypeStrArr:
		// Decode string array - each element is separated by EntityDelimiter
		var values []string
		var err error
		remaining := data
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
		if len(values) > 0 {
			return strings.Join(values, ";")
		}
		return ""
	case pbv1.ValueTypeBinaryData:
		if isPrintable(data) {
			return string(data)
		}
		return fmt.Sprintf("(binary: %d bytes)", len(data))
	default:
		if isPrintable(data) {
			return string(data)
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
