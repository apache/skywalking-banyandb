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

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/dump/stream"
	"github.com/apache/skywalking-banyandb/pkg/fs"
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
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}'

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
			ctx.tagColumns, err = dumpstream.DiscoverColumns(opts.shardPath, ctx.fileSystem)
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

		reader, err := dumpstream.OpenPart(partID, ctx.opts.shardPath, ctx.fileSystem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open part %016x: %v\n", partID, err)
			continue
		}

		partRowCount := 0
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			if ctx.shouldSkip(row.Tags) {
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

func (ctx *streamDumpContext) shouldSkip(tags map[string][]byte) bool {
	if ctx.tagFilter == nil || ctx.tagFilter == logical.DummyFilter {
		return false
	}
	modelTags := make([]*modelv1.Tag, 0, len(tags))
	for name, value := range tags {
		if value == nil {
			continue
		}
		modelTags = append(modelTags, &modelv1.Tag{
			Key: name,
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(value)}},
			},
		})
	}

	// Create a simple registry for tag filtering
	registry := &streamTagRegistry{
		tags: tags,
	}

	matcher := logical.NewTagFilterMatcher(ctx.tagFilter, registry, dump.DecodeTagValue)
	match, _ := matcher.Match(modelTags)
	return !match
}

func (ctx *streamDumpContext) writeRow(partID uint64, row dumpstream.Row) error {
	if ctx.opts.csvOutput {
		if err := writeStreamRowAsCSV(ctx.writer, partID, row, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writeStreamRowAsText(partID, row, ctx.rowNum+1, ctx.projectionTags, ctx.seriesMap)
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

func streamSeriesText(row dumpstream.Row, seriesMap map[common.SeriesID]string) string {
	if len(row.EntityValues) > 0 {
		return string(row.EntityValues)
	}
	if seriesMap != nil {
		if text, ok := seriesMap[row.SeriesID]; ok {
			return text
		}
	}
	return ""
}

func writeStreamRowAsText(
	partID uint64,
	row dumpstream.Row,
	rowNum int,
	projectionTags []string,
	seriesMap map[common.SeriesID]string,
) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", partID, partID)
	fmt.Printf("  ElementID: %d\n", row.ElementID)
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.Timestamp))
	fmt.Printf("  SeriesID: %d\n", row.SeriesID)

	if seriesText := streamSeriesText(row, seriesMap); seriesText != "" {
		fmt.Printf("  Series: %s\n", seriesText)
	}

	// Stream element payload is not persisted in the part, so its size is always 0.
	fmt.Printf("  Element Data: %d bytes\n", 0)

	if len(row.Tags) > 0 {
		fmt.Printf("  Tags:\n")

		var tagsToShow []string
		if len(projectionTags) > 0 {
			tagsToShow = projectionTags
		} else {
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

func writeStreamRowAsCSV(
	writer *csv.Writer,
	partID uint64,
	row dumpstream.Row,
	tagColumns []string,
	seriesMap map[common.SeriesID]string,
) error {
	csvRow := []string{
		fmt.Sprintf("%d", partID),
		fmt.Sprintf("%d", row.ElementID),
		formatTimestamp(row.Timestamp),
		fmt.Sprintf("%d", row.SeriesID),
		streamSeriesText(row, seriesMap),
		strconv.Itoa(0),
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
