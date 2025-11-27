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
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type propertyDumpOptions struct {
	shardPath      string
	criteriaJSON   string
	projectionTags string
	verbose        bool
	csvOutput      bool
}

func newPropertyCmd() *cobra.Command {
	var shardPath string
	var verbose bool
	var csvOutput bool
	var criteriaJSON string
	var projectionTags string

	cmd := &cobra.Command{
		Use:   "property",
		Short: "Dump property shard data",
		Long: `Dump and display contents of a property shard directory.
Outputs property data in human-readable format or CSV.

Supports filtering by criteria and projecting specific tags.`,
		Example: `  # Display property data from shard in text format
  dump property --shard-path /path/to/shard-0

  # Display with verbose hex dumps
  dump property --shard-path /path/to/shard-0 -v

  # Filter by criteria
  dump property --shard-path /path/to/shard-0 \
    --criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["tag1=value1","tag2=value2"]}}}}'

  # Project specific tags
  dump property --shard-path /path/to/shard-0 \
    --projection "tag1,tag2,tag3"

  # Output as CSV
  dump property --shard-path /path/to/shard-0 --csv

  # Save CSV to file
  dump property --shard-path /path/to/shard-0 --csv > output.csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if shardPath == "" {
				return fmt.Errorf("--shard-path flag is required")
			}
			return dumpPropertyShard(propertyDumpOptions{
				shardPath:      shardPath,
				verbose:        verbose,
				csvOutput:      csvOutput,
				criteriaJSON:   criteriaJSON,
				projectionTags: projectionTags,
			})
		},
	}

	cmd.Flags().StringVar(&shardPath, "shard-path", "", "Path to the shard directory (required)")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output (show raw data)")
	cmd.Flags().BoolVar(&csvOutput, "csv", false, "Output as CSV format")
	cmd.Flags().StringVarP(&criteriaJSON, "criteria", "c", "", "Criteria filter as JSON string")
	cmd.Flags().StringVarP(&projectionTags, "projection", "p", "", "Comma-separated list of tags to include as columns (e.g., tag1,tag2,tag3)")
	_ = cmd.MarkFlagRequired("shard-path")

	return cmd
}

func dumpPropertyShard(opts propertyDumpOptions) error {
	ctx, err := newPropertyDumpContext(opts)
	if err != nil || ctx == nil {
		return err
	}
	defer ctx.close()

	if err := ctx.processProperties(); err != nil {
		return err
	}

	ctx.printSummary()
	return nil
}

type propertyRowData struct {
	property   *propertyv1.Property
	id         []byte
	seriesID   common.SeriesID
	timestamp  int64
	deleteTime int64
}

type propertyDumpContext struct {
	tagFilter      logical.TagFilter
	store          index.SeriesStore
	seriesMap      map[common.SeriesID]string
	writer         *csv.Writer
	opts           propertyDumpOptions
	projectionTags []string
	tagColumns     []string
	rowNum         int
}

func newPropertyDumpContext(opts propertyDumpOptions) (*propertyDumpContext, error) {
	ctx := &propertyDumpContext{
		opts: opts,
	}

	// Open inverted index store
	l := logger.GetLogger("dump-property")
	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   opts.shardPath,
		Logger: l,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open property shard: %w", err)
	}
	ctx.store = store

	// Load series map
	ctx.seriesMap, err = loadPropertySeriesMap(opts.shardPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		ctx.seriesMap = nil
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from shard\n", len(ctx.seriesMap))
	}

	// Parse criteria if provided
	if opts.criteriaJSON != "" {
		var criteria *modelv1.Criteria
		criteria, err = parsePropertyCriteriaJSON(opts.criteriaJSON)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("failed to parse criteria: %w", err)
		}
		ctx.tagFilter, err = logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("failed to build tag filter: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Applied criteria filter\n")
	}

	// Parse projection tags
	if opts.projectionTags != "" {
		ctx.projectionTags = parsePropertyProjectionTags(opts.projectionTags)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", ctx.projectionTags)
	}

	// Discover tag columns for CSV output
	if opts.csvOutput {
		if len(ctx.projectionTags) > 0 {
			ctx.tagColumns = ctx.projectionTags
		} else {
			ctx.tagColumns, err = discoverPropertyTagColumns(ctx.store)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Failed to discover tag columns: %v\n", err)
				ctx.tagColumns = []string{}
			}
		}
	}

	if err := ctx.initOutput(); err != nil {
		store.Close()
		return nil, err
	}

	return ctx, nil
}

func (ctx *propertyDumpContext) initOutput() error {
	if !ctx.opts.csvOutput {
		fmt.Printf("================================================================================\n")
		fmt.Fprintf(os.Stderr, "Processing properties...\n")
		return nil
	}

	ctx.writer = csv.NewWriter(os.Stdout)
	header := []string{"ID", "Timestamp", "SeriesID", "Series", "Group", "Name", "EntityID", "Deleted", "ModRevision"}
	header = append(header, ctx.tagColumns...)
	if err := ctx.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	return nil
}

func (ctx *propertyDumpContext) close() {
	if ctx.store != nil {
		ctx.store.Close()
	}
	if ctx.writer != nil {
		ctx.writer.Flush()
	}
}

func (ctx *propertyDumpContext) processProperties() error {
	// Use SeriesIterator to iterate through all series and query properties for each
	searchCtx := context.Background()
	iter, err := ctx.store.SeriesIterator(searchCtx)
	if err != nil {
		return fmt.Errorf("failed to create series iterator: %w", err)
	}
	defer iter.Close()

	projection := []index.FieldKey{
		{TagName: "_id"},
		{TagName: "_timestamp"},
		{TagName: "_source"},
		{TagName: "_deleted"},
	}

	var allResults []index.SeriesDocument
	seriesCount := 0

	// Iterate through all series
	for iter.Next() {
		series := iter.Val()
		if len(series.EntityValues) == 0 {
			continue
		}

		seriesCount++
		// Build query for this specific series
		seriesMatchers := []index.SeriesMatcher{
			{
				Match: series.EntityValues,
				Type:  index.SeriesMatcherTypeExact,
			},
		}

		iq, err := ctx.store.BuildQuery(seriesMatchers, nil, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to build query for series: %v\n", err)
			continue
		}

		// Search properties for this series
		results, err := ctx.store.Search(searchCtx, projection, iq, 10000)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to search properties for series: %v\n", err)
			continue
		}

		allResults = append(allResults, results...)
	}

	fmt.Fprintf(os.Stderr, "Found %d properties across %d series\n", len(allResults), seriesCount)

	// Process each result
	for _, result := range allResults {
		sourceBytes := result.Fields["_source"]
		if sourceBytes == nil {
			continue
		}

		var property propertyv1.Property
		if err := protojson.Unmarshal(sourceBytes, &property); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to unmarshal property: %v\n", err)
			continue
		}

		var deleteTime int64
		if result.Fields["_deleted"] != nil {
			deleteTime = convert.BytesToInt64(result.Fields["_deleted"])
		}

		seriesID := common.SeriesID(0)
		if len(result.Key.EntityValues) > 0 {
			seriesID = common.SeriesID(convert.Hash(result.Key.EntityValues))
		}

		row := propertyRowData{
			id:         result.Key.EntityValues,
			property:   &property,
			timestamp:  result.Timestamp,
			deleteTime: deleteTime,
			seriesID:   seriesID,
		}

		// Apply tag filter if specified
		if ctx.shouldSkip(row) {
			continue
		}

		if err := ctx.writeRow(row); err != nil {
			return err
		}
	}

	return nil
}

func (ctx *propertyDumpContext) shouldSkip(row propertyRowData) bool {
	if ctx.tagFilter == nil || ctx.tagFilter == logical.DummyFilter {
		return false
	}

	// Convert property tags to modelv1.Tag format for filtering
	modelTags := make([]*modelv1.Tag, 0, len(row.property.Tags))
	for _, tag := range row.property.Tags {
		modelTags = append(modelTags, &modelv1.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	// Create a simple registry for tag filtering
	registry := &propertyTagRegistry{
		property: row.property,
	}

	matcher := logical.NewTagFilterMatcher(ctx.tagFilter, registry, propertyTagValueDecoder)
	match, _ := matcher.Match(modelTags)
	return !match
}

func (ctx *propertyDumpContext) writeRow(row propertyRowData) error {
	if ctx.opts.csvOutput {
		if err := writePropertyRowAsCSV(ctx.writer, row, ctx.tagColumns, ctx.seriesMap); err != nil {
			return err
		}
	} else {
		writePropertyRowAsText(row, ctx.rowNum+1, ctx.opts.verbose, ctx.projectionTags, ctx.seriesMap)
	}
	ctx.rowNum++
	return nil
}

func (ctx *propertyDumpContext) printSummary() {
	if ctx.opts.csvOutput {
		fmt.Fprintf(os.Stderr, "Total rows written: %d\n", ctx.rowNum)
		return
	}
	fmt.Printf("\nTotal rows: %d\n", ctx.rowNum)
}

func loadPropertySeriesMap(shardPath string) (map[common.SeriesID]string, error) {
	l := logger.GetLogger("dump-property")

	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   shardPath,
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

func parsePropertyCriteriaJSON(criteriaJSON string) (*modelv1.Criteria, error) {
	criteria := &modelv1.Criteria{}
	err := protojson.Unmarshal([]byte(criteriaJSON), criteria)
	if err != nil {
		return nil, fmt.Errorf("invalid criteria JSON: %w", err)
	}
	return criteria, nil
}

func parsePropertyProjectionTags(projectionStr string) []string {
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

func discoverPropertyTagColumns(store index.SeriesStore) ([]string, error) {
	// Build query to get a sample property
	queryReq := &propertyv1.QueryRequest{
		Limit: 1,
	}

	iq, err := inverted.BuildPropertyQuery(queryReq, "_group", "_entity_id")
	if err != nil {
		return nil, fmt.Errorf("failed to build property query: %w", err)
	}

	projection := []index.FieldKey{
		{TagName: "_source"},
	}

	searchCtx := context.Background()
	results, err := store.Search(searchCtx, projection, iq, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to search properties: %w", err)
	}

	if len(results) == 0 {
		return []string{}, nil
	}

	sourceBytes := results[0].Fields["_source"]
	if sourceBytes == nil {
		return []string{}, nil
	}

	var property propertyv1.Property
	if err := protojson.Unmarshal(sourceBytes, &property); err != nil {
		return nil, fmt.Errorf("failed to unmarshal property: %w", err)
	}

	tagNames := make(map[string]bool)
	for _, tag := range property.Tags {
		tagNames[tag.Key] = true
	}

	result := make([]string, 0, len(tagNames))
	for name := range tagNames {
		result = append(result, name)
	}
	sort.Strings(result)

	return result, nil
}

func writePropertyRowAsText(row propertyRowData, rowNum int, verbose bool, projectionTags []string, seriesMap map[common.SeriesID]string) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  ID: %s\n", string(row.id))
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.timestamp))
	fmt.Printf("  SeriesID: %d\n", row.seriesID)

	if seriesMap != nil {
		if seriesText, ok := seriesMap[row.seriesID]; ok {
			fmt.Printf("  Series: %s\n", seriesText)
		}
	}

	if row.property.Metadata != nil {
		fmt.Printf("  Group: %s\n", row.property.Metadata.Group)
		fmt.Printf("  Name: %s\n", row.property.Metadata.Name)
		fmt.Printf("  EntityID: %s\n", row.property.Id)
		fmt.Printf("  ModRevision: %d\n", row.property.Metadata.ModRevision)
	}

	if row.deleteTime > 0 {
		fmt.Printf("  Deleted: true (deleteTime: %s)\n", formatTimestamp(row.deleteTime))
	} else {
		fmt.Printf("  Deleted: false\n")
	}

	if len(row.property.Tags) > 0 {
		fmt.Printf("  Tags:\n")

		var tagsToShow []string
		if len(projectionTags) > 0 {
			tagsToShow = projectionTags
		} else {
			for _, tag := range row.property.Tags {
				tagsToShow = append(tagsToShow, tag.Key)
			}
			sort.Strings(tagsToShow)
		}

		for _, name := range tagsToShow {
			var tag *modelv1.Tag
			for _, t := range row.property.Tags {
				if t.Key == name {
					tag = t
					break
				}
			}
			if tag == nil {
				continue
			}
			fmt.Printf("    %s: %s\n", name, formatPropertyTagValue(tag.Value))
		}
	}

	if verbose {
		// Print raw JSON
		jsonBytes, err := protojson.Marshal(row.property)
		if err == nil {
			fmt.Printf("  Raw JSON:\n")
			printHexDump(jsonBytes, 4)
		}
	}
	fmt.Printf("\n")
}

func writePropertyRowAsCSV(writer *csv.Writer, row propertyRowData, tagColumns []string, seriesMap map[common.SeriesID]string) error {
	seriesText := ""
	if seriesMap != nil {
		if text, ok := seriesMap[row.seriesID]; ok {
			seriesText = text
		}
	}

	group := ""
	name := ""
	entityID := ""
	modRevision := int64(0)
	if row.property.Metadata != nil {
		group = row.property.Metadata.Group
		name = row.property.Metadata.Name
		entityID = row.property.Id
		modRevision = row.property.Metadata.ModRevision
	}

	deleted := "false"
	if row.deleteTime > 0 {
		deleted = "true"
	}

	csvRow := []string{
		string(row.id),
		formatTimestamp(row.timestamp),
		fmt.Sprintf("%d", row.seriesID),
		seriesText,
		group,
		name,
		entityID,
		deleted,
		fmt.Sprintf("%d", modRevision),
	}

	// Add tag values
	for _, tagName := range tagColumns {
		value := ""
		for _, tag := range row.property.Tags {
			if tag.Key == tagName {
				value = formatPropertyTagValue(tag.Value)
				break
			}
		}
		csvRow = append(csvRow, value)
	}

	return writer.Write(csvRow)
}

func formatPropertyTagValue(value *modelv1.TagValue) string {
	if value == nil {
		return "<nil>"
	}
	switch v := value.Value.(type) {
	case *modelv1.TagValue_Str:
		return fmt.Sprintf("%q", v.Str.Value)
	case *modelv1.TagValue_Int:
		return fmt.Sprintf("%d", v.Int.Value)
	case *modelv1.TagValue_StrArray:
		return fmt.Sprintf("[%s]", strings.Join(v.StrArray.Value, ","))
	case *modelv1.TagValue_IntArray:
		values := make([]string, len(v.IntArray.Value))
		for i, val := range v.IntArray.Value {
			values[i] = fmt.Sprintf("%d", val)
		}
		return fmt.Sprintf("[%s]", strings.Join(values, ","))
	case *modelv1.TagValue_BinaryData:
		return fmt.Sprintf("(binary: %d bytes)", len(v.BinaryData))
	default:
		return fmt.Sprintf("%v", value)
	}
}

type propertyTagRegistry struct {
	property *propertyv1.Property
}

func (r *propertyTagRegistry) FindTagSpecByName(name string) *logical.TagSpec {
	// Try to find the tag in the property
	for _, tag := range r.property.Tags {
		if tag.Key == name {
			// Infer type from TagValue
			tagType := databasev1.TagType_TAG_TYPE_STRING
			if tag.Value != nil {
				switch tag.Value.Value.(type) {
				case *modelv1.TagValue_Int:
					tagType = databasev1.TagType_TAG_TYPE_INT
				case *modelv1.TagValue_StrArray:
					tagType = databasev1.TagType_TAG_TYPE_STRING_ARRAY
				case *modelv1.TagValue_IntArray:
					tagType = databasev1.TagType_TAG_TYPE_INT_ARRAY
				}
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
	}
	// Return default string type if not found
	return &logical.TagSpec{
		Spec: &databasev1.TagSpec{
			Name: name,
			Type: databasev1.TagType_TAG_TYPE_STRING,
		},
		TagFamilyIdx: 0,
		TagIdx:       0,
	}
}

func (r *propertyTagRegistry) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *propertyTagRegistry) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

func (r *propertyTagRegistry) EntityList() []string {
	return nil
}

func (r *propertyTagRegistry) CreateTagRef(_ ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return nil, fmt.Errorf("CreateTagRef not supported in dump tool")
}

func (r *propertyTagRegistry) CreateFieldRef(_ ...*logical.Field) ([]*logical.FieldRef, error) {
	return nil, fmt.Errorf("CreateFieldRef not supported in dump tool")
}

func (r *propertyTagRegistry) ProjTags(_ ...[]*logical.TagRef) logical.Schema {
	return r
}

func (r *propertyTagRegistry) ProjFields(_ ...*logical.FieldRef) logical.Schema {
	return r
}

func (r *propertyTagRegistry) Children() []logical.Schema {
	return nil
}

func propertyTagValueDecoder(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
	// This decoder is used for filtering, but property tags are already in TagValue format
	// So we'll convert from bytes if needed
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
