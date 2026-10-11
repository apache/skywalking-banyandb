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
	"errors"
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
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// Property-document field names mirror the unexported layout
// banyand/property/db writes (banyand/property/db/shard.go).
const (
	propertySourceField = "_source"
	propertyDeleteField = "_deleted"
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
	generation     *native.ReadOnlyGeneration
	tagFilter      logical.TagFilter
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

	// Open the shard's native index read-only: a read-only generation, never
	// a writable owner, so no lock is acquired on a directory a live node may
	// still have open.
	generation, err := native.OpenReadOnlyGeneration(opts.shardPath)
	if err != nil {
		if errors.Is(err, native.ErrNoSnapshot) {
			fmt.Fprintf(os.Stderr, "Warning: shard has no committed data yet\n")
			generation = nil
		} else {
			return nil, fmt.Errorf("failed to open property shard: %w", err)
		}
	}
	ctx.generation = generation

	// Parse criteria if provided
	if opts.criteriaJSON != "" {
		var criteria *modelv1.Criteria
		criteria, err = parsePropertyCriteriaJSON(opts.criteriaJSON)
		if err != nil {
			ctx.close()
			return nil, fmt.Errorf("failed to parse criteria: %w", err)
		}
		ctx.tagFilter, err = logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			ctx.close()
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
			ctx.tagColumns, err = discoverPropertyTagColumns(ctx.generation)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Failed to discover tag columns: %v\n", err)
				ctx.tagColumns = []string{}
			}
		}
	}

	if err := ctx.initOutput(); err != nil {
		ctx.close()
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
	if ctx.generation != nil {
		_ = ctx.generation.Close()
	}
	if ctx.writer != nil {
		ctx.writer.Flush()
	}
}

// walkPropertyRows visits every live document in the shard's native index and
// decodes it into a propertyRowData. It is factored out so tests can drive the
// same decode path the dump tool uses.
func walkPropertyRows(ctx context.Context, generation *native.ReadOnlyGeneration, visit func(propertyRowData) error) error {
	if generation == nil {
		return nil
	}
	return generation.VisitLiveDocuments(ctx, func(doc native.StoredDocument) error {
		row, ok, err := decodePropertyRow(doc)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		return visit(row)
	})
}

// decodePropertyRow extracts a propertyRowData from one native stored
// document. The identifier (_id) and timestamp (_timestamp) are written by
// the native encoder for every document regardless of IdentifierDocValues
// (NIDX-03 §2.2); _source and _deleted are the only other fields
// banyand/property/db/shard.go stores (buildUpdateDocument). A document
// without a _source value is not a property row -- ok is false -- so callers
// never try to unmarshal an empty payload.
//
// A malformed _source or _timestamp is reported as a warning on stderr, not
// a hard error: one corrupt row must not abort the rest of the dump, the
// same tolerance the previous dump tool had. walkPropertyRows
// still aborts on a genuine read-path failure (VisitStoredFields itself
// erroring, which signals the underlying segment is unreadable, not that one
// document's payload is malformed).
func decodePropertyRow(doc native.StoredDocument) (propertyRowData, bool, error) {
	var row propertyRowData
	var sourceBytes []byte
	visitErr := doc.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case identifierFieldName:
			row.id = append([]byte(nil), value...)
		case timestampFieldName:
			ts, decodeErr := native.DecodeTimestamp(value)
			if decodeErr != nil {
				fmt.Fprintf(os.Stderr, "warning: skipping _timestamp for document %x: %v\n", value, decodeErr)
				return true
			}
			row.timestamp = ts
		case propertySourceField:
			sourceBytes = append([]byte(nil), value...)
		case propertyDeleteField:
			if len(value) > 0 {
				row.deleteTime = convert.BytesToInt64(value)
			}
		}
		return true
	})
	if visitErr != nil {
		return propertyRowData{}, false, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	if len(sourceBytes) == 0 {
		return propertyRowData{}, false, nil
	}
	var property propertyv1.Property
	if err := protojson.Unmarshal(sourceBytes, &property); err != nil {
		fmt.Fprintf(os.Stderr, "warning: skipping document %x with malformed _source: %v\n", row.id, err)
		return propertyRowData{}, false, nil
	}
	row.property = &property
	if len(row.id) > 0 {
		row.seriesID = common.SeriesID(convert.Hash(row.id))
	}
	return row, true, nil
}

const (
	identifierFieldName = "_id"
	timestampFieldName  = "_timestamp"
)

func (ctx *propertyDumpContext) processProperties() error {
	searchCtx := context.Background()
	var allResults []propertyRowData
	if walkErr := walkPropertyRows(searchCtx, ctx.generation, func(row propertyRowData) error {
		allResults = append(allResults, row)
		return nil
	}); walkErr != nil {
		return fmt.Errorf("failed to walk property shard: %w", walkErr)
	}

	fmt.Fprintf(os.Stderr, "Found %d properties\n", len(allResults))

	for _, row := range allResults {
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
		if err := writePropertyRowAsCSV(ctx.writer, row, ctx.tagColumns); err != nil {
			return err
		}
	} else {
		writePropertyRowAsText(row, ctx.rowNum+1, ctx.opts.verbose, ctx.projectionTags)
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

// discoverPropertyTagColumns samples the first live property row to discover
// the tag names CSV output should carry as columns.
func discoverPropertyTagColumns(generation *native.ReadOnlyGeneration) ([]string, error) {
	var sample *propertyv1.Property
	errStop := errors.New("dump: stop walk")
	walkErr := walkPropertyRows(context.Background(), generation, func(row propertyRowData) error {
		sample = row.property
		return errStop
	})
	if walkErr != nil && !errors.Is(walkErr, errStop) {
		return nil, fmt.Errorf("failed to sample properties: %w", walkErr)
	}
	if sample == nil {
		return []string{}, nil
	}

	tagNames := make(map[string]bool)
	for _, tag := range sample.Tags {
		tagNames[tag.Key] = true
	}

	result := make([]string, 0, len(tagNames))
	for name := range tagNames {
		result = append(result, name)
	}
	sort.Strings(result)

	return result, nil
}

func writePropertyRowAsText(row propertyRowData, rowNum int, verbose bool, projectionTags []string) {
	fmt.Printf("Row %d:\n", rowNum)
	fmt.Printf("  ID: %s\n", string(row.id))
	fmt.Printf("  Timestamp: %s\n", formatTimestamp(row.timestamp))
	fmt.Printf("  SeriesID: %d\n", row.seriesID)

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

func writePropertyRowAsCSV(writer *csv.Writer, row propertyRowData, tagColumns []string) error {
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
		string(row.id),
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
