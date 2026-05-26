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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/dump/trace"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type traceDumpOptions struct {
	shardPath      string
	segmentPath    string
	criteriaJSON   string
	projectionTags string
	verbose        bool
	csvOutput      bool
}

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
			return dumpTraceShard(traceDumpOptions{
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

// dumpTraceShard has high complexity due to multiple output formats and filtering options.
func dumpTraceShard(opts traceDumpOptions) error {
	ctx, err := newTraceDumpContext(opts)
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
	if src[0] == encoding.EntityDelimiter {
		return dest, src[1:], nil
	}
	for len(src) > 0 {
		switch {
		case src[0] == encoding.Escape:
			if len(src) < 2 {
				return nil, nil, fmt.Errorf("invalid escape character")
			}
			src = src[1:]
			dest = append(dest, src[0])
		case src[0] == encoding.EntityDelimiter:
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

type traceDumpContext struct {
	tagFilter      logical.TagFilter
	fileSystem     fs.FileSystem
	seriesMap      map[common.SeriesID]string
	writer         *csv.Writer
	opts           traceDumpOptions
	partIDs        []uint64
	projectionTags []string
	tagColumns     []string
	rowNum         int
}

func newTraceDumpContext(opts traceDumpOptions) (*traceDumpContext, error) {
	ctx := &traceDumpContext{
		opts:       opts,
		fileSystem: fs.NewLocalFileSystem(),
	}

	partIDs, err := dump.DiscoverPartIDs(opts.shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to discover part IDs: %w", err)
	}
	if len(partIDs) == 0 {
		fmt.Println("No parts found in shard directory")
		return nil, nil
	}
	ctx.partIDs = partIDs
	fmt.Fprintf(os.Stderr, "Found %d parts in shard\n", len(partIDs))

	ctx.seriesMap, err = dump.LoadSegmentSeriesMap(opts.segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		ctx.seriesMap = nil
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from segment\n", len(ctx.seriesMap))
	}

	if opts.criteriaJSON != "" {
		var criteria *modelv1.Criteria
		criteria, err = parseTraceCriteriaJSON(opts.criteriaJSON)
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
		ctx.projectionTags = parseTraceProjectionTags(opts.projectionTags)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", ctx.projectionTags)
	}

	if opts.csvOutput {
		if len(ctx.projectionTags) > 0 {
			ctx.tagColumns = ctx.projectionTags
		} else {
			ctx.tagColumns, err = dumptrace.DiscoverColumns(opts.shardPath, ctx.fileSystem)
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

func (ctx *traceDumpContext) initOutput() error {
	if !ctx.opts.csvOutput {
		fmt.Printf("================================================================================\n")
		fmt.Fprintf(os.Stderr, "Processing parts...\n")
		return nil
	}

	ctx.writer = csv.NewWriter(os.Stdout)
	header := []string{"PartID", "TraceID", "SpanID", "SeriesID", "Series", "SpanDataSize"}
	header = append(header, ctx.tagColumns...)
	if err := ctx.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	return nil
}

func (ctx *traceDumpContext) close() {
	if ctx.writer != nil {
		ctx.writer.Flush()
	}
}

func (ctx *traceDumpContext) processParts() error {
	for partIdx, partID := range ctx.partIDs {
		fmt.Fprintf(os.Stderr, "Processing part %d/%d (0x%016x)...\n", partIdx+1, len(ctx.partIDs), partID)

		reader, err := dumptrace.OpenPart(partID, ctx.opts.shardPath, ctx.fileSystem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open part %016x: %v\n", partID, err)
			continue
		}

		partRowCount := 0
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			if ctx.shouldSkip(row.Tags, row.TagTypes) {
				continue
			}
			if err := ctx.writeRow(partID, row); err != nil {
				_ = it.Close()
				_ = reader.Close()
				return err
			}
			partRowCount++
		}
		if err := it.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: error iterating part %016x: %v\n", partID, err)
		}
		_ = it.Close()
		_ = reader.Close()

		fmt.Fprintf(os.Stderr, "  Part %d/%d: processed %d rows (total: %d)\n", partIdx+1, len(ctx.partIDs), partRowCount, ctx.rowNum)
	}
	return nil
}

func (ctx *traceDumpContext) shouldSkip(tags map[string][]byte, tagTypes map[string]pbv1.ValueType) bool {
	if ctx.tagFilter == nil || ctx.tagFilter == logical.DummyFilter {
		return false
	}
	return !matchesCriteria(tags, tagTypes, ctx.tagFilter)
}

func (ctx *traceDumpContext) writeRow(partID uint64, row dumptrace.Row) error {
	if ctx.opts.csvOutput {
		if err := writeTraceRowAsCSV(ctx.writer, partID, row, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writeTraceRowAsText(partID, row, ctx.rowNum+1, ctx.opts.verbose, ctx.projectionTags, ctx.seriesMap)
	}
	ctx.rowNum++
	return nil
}

func (ctx *traceDumpContext) printSummary() {
	if ctx.opts.csvOutput {
		fmt.Fprintf(os.Stderr, "Total rows written: %d\n", ctx.rowNum)
		return
	}
	fmt.Printf("\nTotal rows: %d\n", ctx.rowNum)
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

	matcher := logical.NewTagFilterMatcher(filter, registry, dump.DecodeTagValue)
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

// traceSeriesText resolves a row's SeriesID to its entity-values text via the
// segment-level series map. It returns "" when the SeriesID is not present
// (e.g. the series map is absent, or the row's SeriesID does not match the
// stored series ID).
func traceSeriesText(row dumptrace.Row, seriesMap map[common.SeriesID]string) string {
	if seriesMap != nil {
		if text, ok := seriesMap[row.SeriesID]; ok {
			return text
		}
	}
	return ""
}

func writeTraceRowAsText(
	partID uint64,
	row dumptrace.Row,
	rowNum int,
	verbose bool,
	projectionTags []string,
	seriesMap map[common.SeriesID]string,
) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", partID, partID)
	fmt.Printf("  TraceID: %s\n", row.TraceID)
	fmt.Printf("  SpanID: %s\n", row.SpanID)
	fmt.Printf("  SeriesID: %d\n", row.SeriesID)

	if seriesText := traceSeriesText(row, seriesMap); seriesText != "" {
		fmt.Printf("  Series: %s\n", seriesText)
	}

	fmt.Printf("  Span Data: %d bytes\n", len(row.Span))
	if verbose {
		fmt.Printf("  Span Content:\n")
		printHexDump(row.Span, 4)
	} else {
		if isPrintable(row.Span) {
			fmt.Printf("  Span: %s\n", string(row.Span))
		} else {
			fmt.Printf("  Span: (binary data, %d bytes)\n", len(row.Span))
		}
	}

	// Print projected tags or all tags
	if len(row.Tags) > 0 {
		fmt.Printf("  Tags:\n")

		var tagsToShow []string
		if len(projectionTags) > 0 {
			tagsToShow = projectionTags
		} else {
			// Show all tags
			for name := range row.Tags {
				tagsToShow = append(tagsToShow, name)
			}
			sort.Strings(tagsToShow)
		}

		for _, name := range tagsToShow {
			value, exists := row.Tags[name]
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

func writeTraceRowAsCSV(
	writer *csv.Writer,
	partID uint64,
	row dumptrace.Row,
	tagColumns []string,
	seriesMap map[common.SeriesID]string,
) error {
	csvRow := []string{
		fmt.Sprintf("%d", partID),
		row.TraceID,
		row.SpanID,
		fmt.Sprintf("%d", row.SeriesID),
		traceSeriesText(row, seriesMap),
		strconv.Itoa(len(row.Span)),
	}

	for _, tagName := range tagColumns {
		value := ""
		if tagValue, exists := row.Tags[tagName]; exists && tagValue != nil {
			value = string(tagValue)
		}
		csvRow = append(csvRow, value)
	}

	return writer.Write(csvRow)
}
