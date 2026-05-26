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
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/dump/measure"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type measureDumpOptions struct {
	shardPath        string
	segmentPath      string
	criteriaJSON     string
	projectionTags   string
	projectionFields string
	verbose          bool
	csvOutput        bool
}

func newMeasureCmd() *cobra.Command {
	var shardPath string
	var segmentPath string
	var verbose bool
	var csvOutput bool
	var criteriaJSON string
	var projectionTags string
	var projectionFields string

	cmd := &cobra.Command{
		Use:   "measure",
		Short: "Dump measure shard data",
		Long: `Dump and display contents of a measure shard directory (containing multiple parts).
Outputs measure data in human-readable format or CSV.

Supports filtering by criteria and projecting specific tags and fields.`,
		Example: `  # Display measure data from shard in text format
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment

  # Display with verbose hex dumps
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment -v

  # Filter by criteria
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}'

  # Project specific tags
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --projection-tags "tag1,tag2,tag3"

  # Project specific fields
  dump measure --shard-path /path/to/shard-0 --segment-path /path/to/segment \
    --projection-fields "field1,field2"

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
				shardPath:        shardPath,
				segmentPath:      segmentPath,
				verbose:          verbose,
				csvOutput:        csvOutput,
				criteriaJSON:     criteriaJSON,
				projectionTags:   projectionTags,
				projectionFields: projectionFields,
			})
		},
	}

	cmd.Flags().StringVar(&shardPath, "shard-path", "", "Path to the shard directory (required)")
	cmd.Flags().StringVarP(&segmentPath, "segment-path", "g", "", "Path to the segment directory (required)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output (show raw data)")
	cmd.Flags().BoolVar(&csvOutput, "csv", false, "Output as CSV format")
	cmd.Flags().StringVarP(&criteriaJSON, "criteria", "c", "", "Criteria filter as JSON string")
	cmd.Flags().StringVar(&projectionTags, "projection-tags", "", "Comma-separated list of tags to include as columns (e.g., tag1,tag2,tag3)")
	cmd.Flags().StringVar(&projectionFields, "projection-fields", "", "Comma-separated list of fields to include as columns (e.g., field1,field2)")
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

type measureDumpContext struct {
	tagFilter        logical.TagFilter
	fileSystem       fs.FileSystem
	seriesMap        map[common.SeriesID]string
	writer           *csv.Writer
	opts             measureDumpOptions
	partIDs          []uint64
	projectionTags   []string
	projectionFields []string
	tagColumns       []string
	fieldColumns     []string
	rowNum           int
}

func newMeasureDumpContext(opts measureDumpOptions) (*measureDumpContext, error) {
	ctx := &measureDumpContext{
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

	if opts.projectionFields != "" {
		ctx.projectionFields = parseMeasureProjectionTags(opts.projectionFields)
		fmt.Fprintf(os.Stderr, "Projection fields: %v\n", ctx.projectionFields)
	}

	if opts.csvOutput {
		// Discover or use projection for tags
		if len(ctx.projectionTags) > 0 {
			ctx.tagColumns = ctx.projectionTags
		} else {
			// Discover all tags if no projection specified
			ctx.tagColumns, ctx.fieldColumns, err = dumpmeasure.DiscoverColumns(opts.shardPath, ctx.fileSystem)
			if err != nil {
				return nil, fmt.Errorf("failed to discover columns: %w", err)
			}
			// If we discovered, we need to separate tags and fields
			// But discoverMeasureColumns already returns them separately, so we're good
		}

		// Discover or use projection for fields
		if len(ctx.projectionFields) > 0 {
			ctx.fieldColumns = ctx.projectionFields
		} else if len(ctx.projectionTags) > 0 {
			// If tags were projected but fields weren't, we still need to discover fields
			_, ctx.fieldColumns, err = dumpmeasure.DiscoverColumns(opts.shardPath, ctx.fileSystem)
			if err != nil {
				return nil, fmt.Errorf("failed to discover fields: %w", err)
			}
		}
		// If neither was projected, discoverMeasureColumns already set both
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

		reader, err := dumpmeasure.OpenPart(partID, ctx.opts.shardPath, ctx.fileSystem)
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
		modelTags = append(modelTags, &modelv1.Tag{
			Key: name,
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(value)}},
			},
		})
	}

	// Create a simple registry for tag filtering
	registry := &measureTagRegistry{
		tags: tags,
	}

	matcher := logical.NewTagFilterMatcher(ctx.tagFilter, registry, dump.DecodeTagValue)
	match, _ := matcher.Match(modelTags)
	return !match
}

func (ctx *measureDumpContext) writeRow(partID uint64, row dumpmeasure.Row) error {
	if ctx.opts.csvOutput {
		if err := writeMeasureRowAsCSV(ctx.writer, partID, row, ctx.fieldColumns, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writeMeasureRowAsText(partID, row, ctx.rowNum+1, ctx.projectionTags, ctx.projectionFields, ctx.seriesMap)
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

func measureSeriesText(row dumpmeasure.Row, seriesMap map[common.SeriesID]string) string {
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

func writeMeasureRowAsText(
	partID uint64,
	row dumpmeasure.Row,
	rowNum int,
	projectionTags []string,
	projectionFields []string,
	seriesMap map[common.SeriesID]string,
) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  PartID: %d (0x%016x)\n", partID, partID)
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.Timestamp))
	fmt.Printf("  Version: %d\n", row.Version)
	fmt.Printf("  SeriesID: %d\n", row.SeriesID)

	if seriesText := measureSeriesText(row, seriesMap); seriesText != "" {
		fmt.Printf("  Series: %s\n", seriesText)
	}

	if len(row.Fields) > 0 {
		fmt.Printf("  Fields:\n")
		var fieldsToShow []string
		if len(projectionFields) > 0 {
			fieldsToShow = projectionFields
		} else {
			for name := range row.Fields {
				fieldsToShow = append(fieldsToShow, name)
			}
			sort.Strings(fieldsToShow)
		}

		for _, name := range fieldsToShow {
			value, exists := row.Fields[name]
			if !exists {
				continue
			}
			valueType := row.FieldTypes[name]
			if value == nil {
				fmt.Printf("    %s: <nil>\n", name)
			} else {
				fmt.Printf("    %s: %s\n", name, formatTagValueForDisplay(value, valueType))
			}
		}
	}

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

func writeMeasureRowAsCSV(
	writer *csv.Writer,
	partID uint64,
	row dumpmeasure.Row,
	fieldColumns []string,
	tagColumns []string,
	seriesMap map[common.SeriesID]string,
) error {
	csvRow := []string{
		fmt.Sprintf("%d", partID),
		formatTimestamp(row.Timestamp),
		fmt.Sprintf("%d", row.Version),
		fmt.Sprintf("%d", row.SeriesID),
		measureSeriesText(row, seriesMap),
	}

	// Add field values
	for _, fieldName := range fieldColumns {
		value := ""
		if fieldValue, exists := row.Fields[fieldName]; exists && fieldValue != nil {
			valueType := row.FieldTypes[fieldName]
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
		if tagValue, exists := row.Tags[tagName]; exists && tagValue != nil {
			value = string(tagValue)
		}
		csvRow = append(csvRow, value)
	}

	return writer.Write(csvRow)
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
