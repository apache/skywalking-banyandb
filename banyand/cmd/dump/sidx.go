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
	"math"
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
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func newSidxCmd() *cobra.Command {
	var sidxPath string
	var segmentPath string
	var criteriaJSON string
	var csvOutput bool
	var timeBegin string
	var timeEnd string
	var fullScan bool
	var projectionTags string
	var dataFilter string

	cmd := &cobra.Command{
		Use:   "sidx",
		Short: "Dump sidx (secondary index) data",
		Long: `Dump and display contents of a sidx directory with filtering.
Query results include which part each row belongs to.

The tool automatically discovers series IDs from the segment's series index.`,
		Example: `  # Display sidx data in text format
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment

  # Query with time range filter
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment \
    --time-begin "2025-11-23T05:05:00Z" --time-end "2025-11-23T05:20:00Z"

  # Query with criteria filter
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment \
    -criteria '{"condition":{"name":"query","op":"BINARY_OP_HAVING","value":{"strArray":{"value":["val1","val2"]}}}}'

  # Full scan mode (scans all series without requiring series ID discovery)
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment --full-scan

  # Project specific tags as columns
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment \
    --projection "tag1,tag2,tag3"

  # Filter by data content
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment \
    --data-filter "2dd1624d5719fdfedc526f3f24d00342-4423100"

  # Output as CSV
  dump sidx -sidx-path /path/to/sidx/timestamp_millis -segment-path /path/to/segment -csv`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if sidxPath == "" {
				return fmt.Errorf("-sidx-path flag is required")
			}
			if segmentPath == "" {
				return fmt.Errorf("-segment-path flag is required")
			}
			return dumpSidx(sidxPath, segmentPath, criteriaJSON, csvOutput, timeBegin, timeEnd, fullScan, projectionTags, dataFilter)
		},
	}

	cmd.Flags().StringVarP(&sidxPath, "sidx-path", "s", "", "Path to the sidx directory (required)")
	cmd.Flags().StringVarP(&segmentPath, "segment-path", "g", "", "Path to the segment directory (required)")
	cmd.Flags().StringVarP(&criteriaJSON, "criteria", "c", "", "Criteria filter as JSON string")
	cmd.Flags().BoolVar(&csvOutput, "csv", false, "Output as CSV format")
	cmd.Flags().StringVar(&timeBegin, "time-begin", "", "Begin time in RFC3339 format (e.g., 2025-11-23T05:05:00Z)")
	cmd.Flags().StringVar(&timeEnd, "time-end", "", "End time in RFC3339 format (e.g., 2025-11-23T05:20:00Z)")
	cmd.Flags().BoolVar(&fullScan, "full-scan", false, "Scan all series without requiring series ID discovery")
	cmd.Flags().StringVarP(&projectionTags, "projection", "p", "", "Comma-separated list of tags to include as columns (e.g., tag1,tag2,tag3)")
	cmd.Flags().StringVarP(&dataFilter, "data-filter", "d", "", "Filter rows by data content (substring match)")
	_ = cmd.MarkFlagRequired("sidx-path")
	_ = cmd.MarkFlagRequired("segment-path")

	return cmd
}

func dumpSidx(sidxPath, segmentPath, criteriaJSON string, csvOutput bool, timeBegin, timeEnd string, fullScan bool, projectionTags string, dataFilter string) error {
	// Parse time range if provided
	var minKeyNanos, maxKeyNanos int64
	var hasTimeRange bool

	if timeBegin != "" || timeEnd != "" {
		var err error
		minKeyNanos, maxKeyNanos, err = parseTimeRange(timeBegin, timeEnd)
		if err != nil {
			return fmt.Errorf("failed to parse time range: %w", err)
		}
		hasTimeRange = true
		fmt.Fprintf(os.Stderr, "Time range filter: %s to %s\n",
			time.Unix(0, minKeyNanos).Format(time.RFC3339),
			time.Unix(0, maxKeyNanos).Format(time.RFC3339))
	}

	// Parse projection tags
	var tagNames []string
	if projectionTags != "" {
		tagNames = parseProjectionTags(projectionTags)
		fmt.Fprintf(os.Stderr, "Projection tags: %v\n", tagNames)
	}

	// Log data filter if provided
	if dataFilter != "" {
		fmt.Fprintf(os.Stderr, "Data filter: %s\n", dataFilter)
	}

	// Discover available part IDs from segment directory
	allPartIDs, err := discoverPartIDs(segmentPath)
	if err != nil {
		return fmt.Errorf("failed to discover part IDs: %w", err)
	}

	if len(allPartIDs) == 0 {
		fmt.Println("No parts found in segment directory")
		return nil
	}

	// Filter parts by time range if specified
	var partIDs []uint64
	if hasTimeRange {
		partIDs, err = filterPartsByTimeRange(sidxPath, allPartIDs, minKeyNanos, maxKeyNanos)
		if err != nil {
			return fmt.Errorf("failed to filter parts by time range: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Filtered to %d parts (out of %d) based on time range\n", len(partIDs), len(allPartIDs))
	} else {
		partIDs = allPartIDs
	}

	if len(partIDs) == 0 {
		fmt.Println("No parts match the specified time range")
		return nil
	}

	// Parse criteria if provided
	var criteria *modelv1.Criteria
	if criteriaJSON != "" {
		criteria, err = parseCriteriaJSON(criteriaJSON)
		if err != nil {
			return fmt.Errorf("failed to parse criteria: %w", err)
		}
	}

	// Use full-scan mode if requested
	if fullScan {
		return dumpSidxFullScan(sidxPath, segmentPath, criteria, csvOutput, hasTimeRange, minKeyNanos, maxKeyNanos, allPartIDs, partIDs, tagNames, dataFilter)
	}

	// Open the file system
	fileSystem := fs.NewLocalFileSystem()

	// Create sidx options
	opts, err := sidx.NewOptions(sidxPath, &protector.Nop{})
	if err != nil {
		return fmt.Errorf("failed to create sidx options: %w", err)
	}
	opts.AvailablePartIDs = partIDs

	// Open SIDX instance
	sidxInstance, err := sidx.NewSIDX(fileSystem, opts)
	if err != nil {
		return fmt.Errorf("failed to open sidx: %w", err)
	}
	defer sidxInstance.Close()

	// Discover series IDs from the segment's series index
	seriesIDs, err := discoverSeriesIDs(segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not discover series IDs: %v. Using default range.\n", err)
		seriesIDs = nil
	}
	if len(seriesIDs) > 0 {
		fmt.Fprintf(os.Stderr, "Discovered %d series IDs from segment\n", len(seriesIDs))
	}

	// Create dynamic tag registry if criteria is provided
	var tagRegistry *dynamicTagRegistry
	if criteria != nil {
		tagRegistry, err = newDynamicTagRegistry(sidxPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to create tag registry: %v. Criteria filtering may not work properly.\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "Discovered %d tags from sidx\n", len(tagRegistry.tagSpecs))
		}
	}

	// Build query request
	req, err := buildQueryRequest(criteria, seriesIDs, hasTimeRange, minKeyNanos, maxKeyNanos, tagRegistry)
	if err != nil {
		return fmt.Errorf("failed to build query request: %w", err)
	}

	// Execute streaming query
	// Use a longer timeout for dump operations since they can process large datasets
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	resultsCh, errCh := sidxInstance.StreamingQuery(ctx, req)

	// Process and output results
	if csvOutput {
		return outputSidxResultsAsCSV(resultsCh, errCh, dataFilter)
	}
	return outputSidxResultsAsText(resultsCh, errCh, sidxPath, dataFilter)
}

func parseTimeRange(timeBegin, timeEnd string) (int64, int64, error) {
	var minKeyNanos, maxKeyNanos int64

	if timeBegin != "" {
		t, err := time.Parse(time.RFC3339, timeBegin)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid time-begin format: %w (expected RFC3339, e.g., 2025-11-23T05:05:00Z)", err)
		}
		minKeyNanos = t.UnixNano()
	} else {
		minKeyNanos = math.MinInt64
	}

	if timeEnd != "" {
		t, err := time.Parse(time.RFC3339, timeEnd)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid time-end format: %w (expected RFC3339, e.g., 2025-11-23T05:20:00Z)", err)
		}
		maxKeyNanos = t.UnixNano()
	} else {
		maxKeyNanos = math.MaxInt64
	}

	if minKeyNanos > maxKeyNanos {
		return 0, 0, fmt.Errorf("time-begin must be before time-end")
	}

	return minKeyNanos, maxKeyNanos, nil
}

func filterPartsByTimeRange(sidxPath string, partIDs []uint64, minKey, maxKey int64) ([]uint64, error) {
	fileSystem := fs.NewLocalFileSystem()
	var filteredParts []uint64

	for _, partID := range partIDs {
		partPath := filepath.Join(sidxPath, fmt.Sprintf("%016x", partID))
		manifestPath := filepath.Join(partPath, "manifest.json")

		// Read manifest.json
		manifestData, err := fileSystem.Read(manifestPath)
		if err != nil {
			// Skip parts that don't have a manifest
			continue
		}

		// Parse manifest to get minKey and maxKey
		var manifest struct {
			MinKey int64 `json:"minKey"`
			MaxKey int64 `json:"maxKey"`
		}

		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to parse manifest for part %016x: %v\n", partID, err)
			continue
		}

		// Check if part overlaps with the requested time range
		if manifest.MaxKey >= minKey && manifest.MinKey <= maxKey {
			filteredParts = append(filteredParts, partID)
		}
	}

	return filteredParts, nil
}

func parseCriteriaJSON(criteriaJSON string) (*modelv1.Criteria, error) {
	criteria := &modelv1.Criteria{}
	err := protojson.Unmarshal([]byte(criteriaJSON), criteria)
	if err != nil {
		return nil, fmt.Errorf("invalid criteria JSON: %w", err)
	}
	return criteria, nil
}

func discoverPartIDs(segmentPath string) ([]uint64, error) {
	// Look for shard directories in the segment path
	shardDirs, err := filepath.Glob(filepath.Join(segmentPath, "shard-*"))
	if err != nil {
		return nil, fmt.Errorf("failed to glob shard directories: %w", err)
	}

	if len(shardDirs) == 0 {
		return nil, fmt.Errorf("no shard directories found in segment path")
	}

	// Collect all part IDs from all shards
	partIDMap := make(map[uint64]bool)

	for _, shardDir := range shardDirs {
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to read shard directory %s: %v\n", shardDir, err)
			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			// Part directories are named as hex IDs (e.g., "0000000000004db4")
			name := entry.Name()
			// Skip special directories
			if name == "sidx" || name == "meta" {
				continue
			}
			// Try to parse as hex
			partID, err := strconv.ParseUint(name, 16, 64)
			if err == nil {
				partIDMap[partID] = true
			}
		}
	}

	// Convert map to sorted slice
	partIDs := make([]uint64, 0, len(partIDMap))
	for partID := range partIDMap {
		partIDs = append(partIDs, partID)
	}
	sort.Slice(partIDs, func(i, j int) bool {
		return partIDs[i] < partIDs[j]
	})

	return partIDs, nil
}

func discoverSeriesIDs(segmentPath string) ([]common.SeriesID, error) {
	// Open the series index (sidx directory under segment)
	seriesIndexPath := filepath.Join(segmentPath, "sidx")

	l := logger.GetLogger("dump-sidx")

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

	// Collect all series IDs
	var seriesIDs []common.SeriesID
	for iter.Next() {
		series := iter.Val()
		// Compute series ID from EntityValues using hash
		if len(series.EntityValues) > 0 {
			seriesID := common.SeriesID(convert.Hash(series.EntityValues))
			seriesIDs = append(seriesIDs, seriesID)
		}
	}

	return seriesIDs, nil
}

func buildQueryRequest(criteria *modelv1.Criteria, seriesIDs []common.SeriesID, hasTimeRange bool, minKeyNanos, maxKeyNanos int64, tagRegistry *dynamicTagRegistry) (sidx.QueryRequest, error) {
	// Use discovered series IDs if available, otherwise fall back to default range
	if len(seriesIDs) == 0 {
		// Use a range of series IDs to increase chance of finding data
		// Since we don't know what series IDs exist, we'll use a large range
		// Series IDs are uint64 hash values, so they can be quite large
		// We'll sample the space to cover likely values
		seriesIDs = make([]common.SeriesID, 0, 10000)

		// Add first 10000 series IDs (for simple/test data)
		for i := common.SeriesID(1); i <= 10000; i++ {
			seriesIDs = append(seriesIDs, i)
		}
	}

	req := sidx.QueryRequest{
		SeriesIDs:    seriesIDs,
		MaxBatchSize: 100, // Smaller batch size for better responsiveness
	}

	// Set min/max key based on time range if provided, otherwise full range
	var minKey, maxKey int64
	if hasTimeRange {
		minKey = minKeyNanos
		maxKey = maxKeyNanos
	} else {
		minKey = math.MinInt64
		maxKey = math.MaxInt64
	}
	req.MinKey = &minKey
	req.MaxKey = &maxKey

	// If criteria is provided, build tag filter
	if criteria != nil && tagRegistry != nil {
		tagFilter, err := logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			return req, fmt.Errorf("failed to build tag filter from criteria: %w", err)
		}

		// Create a tag filter matcher if filter is not dummy
		if tagFilter != nil && tagFilter != logical.DummyFilter {
			// Create tag filter matcher with the dynamic registry and decoder
			tagFilterMatcher := logical.NewTagFilterMatcher(tagFilter, tagRegistry, simpleTagValueDecoder)
			req.TagFilter = tagFilterMatcher
			fmt.Fprintf(os.Stderr, "Applied criteria filter: %v\n", tagFilter)
		}
	}

	return req, nil
}

func outputSidxResultsAsText(resultsCh <-chan *sidx.QueryResponse, errCh <-chan error, sidxPath string, dataFilter string) error {
	fmt.Printf("Opening sidx: %s\n", sidxPath)
	fmt.Printf("================================================================================\n\n")

	totalRows := 0
	filteredRows := 0
	batchNum := 0

	// Process results and errors
	for resultsCh != nil || errCh != nil {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return fmt.Errorf("query error: %w", err)
			}
		case resp, ok := <-resultsCh:
			if !ok {
				resultsCh = nil
				continue
			}

			if resp.Error != nil {
				return fmt.Errorf("response error: %w", resp.Error)
			}

			if resp.Len() > 0 {
				batchNum++
				batchPrinted := false

				for i := 0; i < resp.Len(); i++ {
					dataStr := string(resp.Data[i])

					// Apply data filter if specified
					if dataFilter != "" && !strings.Contains(dataStr, dataFilter) {
						filteredRows++
						continue
					}

					// Print batch header only when we have matching rows
					if !batchPrinted {
						fmt.Printf("Batch %d:\n", batchNum)
						fmt.Printf("--------------------------------------------------------------------------------\n")
						batchPrinted = true
					}

					totalRows++
					fmt.Printf("Row %d:\n", totalRows)
					fmt.Printf("  PartID: %d (0x%016x)\n", resp.PartIDs[i], resp.PartIDs[i])
					fmt.Printf("  Key: %d\n", resp.Keys[i])
					fmt.Printf("  SeriesID: %d\n", resp.SIDs[i])
					fmt.Printf("  Data: %s\n", dataStr)
					fmt.Printf("\n")
				}
			}
		}
	}

	if dataFilter != "" {
		fmt.Printf("\nTotal rows: %d (filtered out: %d)\n", totalRows, filteredRows)
	} else {
		fmt.Printf("\nTotal rows: %d\n", totalRows)
	}
	return nil
}

func outputSidxResultsAsCSV(resultsCh <-chan *sidx.QueryResponse, errCh <-chan error, dataFilter string) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	header := []string{"PartID", "Key", "SeriesID", "Data"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Process results and errors
	for resultsCh != nil || errCh != nil {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return fmt.Errorf("query error: %w", err)
			}
		case resp, ok := <-resultsCh:
			if !ok {
				resultsCh = nil
				continue
			}

			if resp.Error != nil {
				return fmt.Errorf("response error: %w", resp.Error)
			}

			for i := 0; i < resp.Len(); i++ {
				dataStr := string(resp.Data[i])

				// Apply data filter if specified
				if dataFilter != "" && !strings.Contains(dataStr, dataFilter) {
					continue
				}

				row := []string{
					fmt.Sprintf("%d", resp.PartIDs[i]),
					fmt.Sprintf("%d", resp.Keys[i]),
					fmt.Sprintf("%d", resp.SIDs[i]),
					dataStr,
				}
				if err := writer.Write(row); err != nil {
					return fmt.Errorf("failed to write CSV row: %w", err)
				}
			}
		}
	}

	return nil
}

// simpleTagValueDecoder is a simple decoder for tag values used in filtering
// It handles basic tag value types without needing full schema information
func simpleTagValueDecoder(valueType pbv1.ValueType, value []byte, valueArr [][]byte) *modelv1.TagValue {
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
		// For unknown types, try to return as string
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

// dynamicTagRegistry implements TagSpecRegistry by discovering tags from the sidx
type dynamicTagRegistry struct {
	tagSpecs map[string]*logical.TagSpec
}

// newDynamicTagRegistry creates a registry by discovering tag names from the sidx directory
func newDynamicTagRegistry(sidxPath string) (*dynamicTagRegistry, error) {
	registry := &dynamicTagRegistry{
		tagSpecs: make(map[string]*logical.TagSpec),
	}

	fileSystem := fs.NewLocalFileSystem()

	// List all directories in sidxPath to find parts
	entries := fileSystem.ReadDir(sidxPath)
	if len(entries) == 0 {
		return nil, fmt.Errorf("failed to read sidx directory or directory is empty")
	}

	tagNamesMap := make(map[string]bool)

	// Scan first part to discover tag names
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		partPath := filepath.Join(sidxPath, entry.Name())
		partEntries := fileSystem.ReadDir(partPath)

		// Look for .td (tag data) files
		for _, partEntry := range partEntries {
			if partEntry.IsDir() {
				continue
			}
			name := partEntry.Name()
			// Tag data files are named like "tagname.td"
			if strings.HasSuffix(name, ".td") {
				tagName := strings.TrimSuffix(name, ".td")
				tagNamesMap[tagName] = true
			}
		}

		// Only scan first part to save time
		if len(tagNamesMap) > 0 {
			break
		}
	}

	// Create tag specs for discovered tags
	// We assume all tags are string arrays since we don't have schema info
	tagFamilyIdx := 0
	tagIdx := 0
	for tagName := range tagNamesMap {
		registry.tagSpecs[tagName] = &logical.TagSpec{
			Spec: &databasev1.TagSpec{
				Name: tagName,
				Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY,
			},
			TagFamilyIdx: tagFamilyIdx,
			TagIdx:       tagIdx,
		}
		tagIdx++
	}

	return registry, nil
}

func (d *dynamicTagRegistry) FindTagSpecByName(name string) *logical.TagSpec {
	if spec, ok := d.tagSpecs[name]; ok {
		return spec
	}
	// If tag not found, return a default spec for it
	// This allows filtering on any tag name
	return &logical.TagSpec{
		Spec: &databasev1.TagSpec{
			Name: name,
			Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY,
		},
		TagFamilyIdx: 0,
		TagIdx:       0,
	}
}

// IndexDefined implements IndexChecker interface (stub implementation)
func (d *dynamicTagRegistry) IndexDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

// IndexRuleDefined implements IndexChecker interface (stub implementation)
func (d *dynamicTagRegistry) IndexRuleDefined(_ string) (bool, *databasev1.IndexRule) {
	return false, nil
}

// EntityList implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) EntityList() []string {
	return nil
}

// CreateTagRef implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return nil, fmt.Errorf("CreateTagRef not supported in dump tool")
}

// CreateFieldRef implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	return nil, fmt.Errorf("CreateFieldRef not supported in dump tool")
}

// ProjTags implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
	return d
}

// ProjFields implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) ProjFields(refs ...*logical.FieldRef) logical.Schema {
	return d
}

// Children implements Schema interface (stub implementation)
func (d *dynamicTagRegistry) Children() []logical.Schema {
	return nil
}

func dumpSidxFullScan(sidxPath, segmentPath string, criteria *modelv1.Criteria, csvOutput bool,
	hasTimeRange bool, minKeyNanos, maxKeyNanos int64, allPartIDs, partIDs []uint64, projectionTagNames []string, dataFilter string) error {

	fmt.Fprintf(os.Stderr, "Using full-scan mode (no series ID filtering)\n")

	// Load series information for human-readable output
	seriesMap, err := loadSeriesMap(segmentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load series information: %v\n", err)
		seriesMap = nil // Continue without series names
	} else {
		fmt.Fprintf(os.Stderr, "Loaded %d series from segment\n", len(seriesMap))
	}

	// Build tag projection
	var tagProjection []model.TagProjection
	if len(projectionTagNames) > 0 {
		// Create a single TagProjection with all the requested tag names
		tagProjection = []model.TagProjection{
			{
				Family: "", // Empty family for SIDX tags
				Names:  projectionTagNames,
			},
		}
	}

	// Create dynamic tag registry if criteria is provided
	var tagRegistry *dynamicTagRegistry
	if criteria != nil {
		var err error
		tagRegistry, err = newDynamicTagRegistry(sidxPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to create tag registry: %v\n", err)
		}
	}

	// Build scan request
	var minKey, maxKey int64
	if hasTimeRange {
		minKey = minKeyNanos
		maxKey = maxKeyNanos
	} else {
		minKey = math.MinInt64
		maxKey = math.MaxInt64
	}

	// Track progress
	startTime := time.Now()

	scanReq := sidx.ScanQueryRequest{
		MinKey:        &minKey,
		MaxKey:        &maxKey,
		MaxBatchSize:  1000,
		TagProjection: tagProjection,
		OnProgress: func(currentPart, totalParts int, rowsFound int) {
			elapsed := time.Since(startTime)
			fmt.Fprintf(os.Stderr, "\rScanning part %d/%d... Found %d rows (elapsed: %s)",
				currentPart, totalParts, rowsFound, elapsed.Round(time.Second))
			os.Stderr.Sync() // Flush immediately to show progress in real-time
		},
	}

	// Apply criteria filter if provided
	if criteria != nil && tagRegistry != nil {
		fmt.Fprintf(os.Stderr, "Discovered %d tags from sidx\n", len(tagRegistry.tagSpecs))

		tagFilter, err := logical.BuildSimpleTagFilter(criteria)
		if err != nil {
			return fmt.Errorf("failed to build tag filter: %w", err)
		}
		if tagFilter != nil && tagFilter != logical.DummyFilter {
			scanReq.TagFilter = logical.NewTagFilterMatcher(tagFilter, tagRegistry, simpleTagValueDecoder)
			fmt.Fprintf(os.Stderr, "Applied criteria filter: %v\n", tagFilter)
		}
	}

	// Open SIDX
	fileSystem := fs.NewLocalFileSystem()
	opts, err := sidx.NewOptions(sidxPath, &protector.Nop{})
	if err != nil {
		return fmt.Errorf("failed to create sidx options: %w", err)
	}
	opts.AvailablePartIDs = partIDs

	sidxInstance, err := sidx.NewSIDX(fileSystem, opts)
	if err != nil {
		return fmt.Errorf("failed to open sidx: %w", err)
	}
	defer sidxInstance.Close()

	// Execute scan query
	// Use a longer timeout for dump operations since they can process large datasets
	// Full-scan mode can be slower when scanning many parts with filtering
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	results, err := sidxInstance.ScanQuery(ctx, scanReq)
	if err != nil {
		return fmt.Errorf("scan query failed: %w", err)
	}

	// Print newline after progress output and flush to ensure clean output
	fmt.Fprintf(os.Stderr, "\nScan complete.\n")
	os.Stderr.Sync() // Ensure all stderr output is flushed

	// Output results
	if csvOutput {
		return outputScanResultsAsCSV(results, seriesMap, projectionTagNames, dataFilter)
	}
	return outputScanResultsAsText(results, sidxPath, seriesMap, projectionTagNames, dataFilter)
}

// parseProjectionTags parses a comma-separated list of tag names
func parseProjectionTags(projectionStr string) []string {
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

// loadSeriesMap loads all series from the segment's series index and creates a map from SeriesID to text representation
func loadSeriesMap(segmentPath string) (map[common.SeriesID]string, error) {
	seriesIndexPath := filepath.Join(segmentPath, "sidx")

	l := logger.GetLogger("dump-sidx")

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

func outputScanResultsAsText(results []*sidx.QueryResponse, sidxPath string, seriesMap map[common.SeriesID]string, projectionTagNames []string, dataFilter string) error {
	fmt.Printf("Opening sidx: %s\n", sidxPath)
	fmt.Printf("================================================================================\n\n")

	totalRows := 0
	filteredRows := 0
	for batchNum, resp := range results {
		if resp.Error != nil {
			return fmt.Errorf("response error in batch %d: %w", batchNum+1, resp.Error)
		}

		batchPrinted := false
		batchStartRow := totalRows

		for i := 0; i < resp.Len(); i++ {
			dataStr := string(resp.Data[i])

			// Apply data filter if specified
			if dataFilter != "" && !strings.Contains(dataStr, dataFilter) {
				filteredRows++
				continue
			}

			// Print batch header only when we have matching rows
			if !batchPrinted {
				fmt.Printf("Batch %d:\n", batchNum+1)
				fmt.Printf("--------------------------------------------------------------------------------\n")
				batchPrinted = true
			}

			fmt.Printf("Row %d:\n", totalRows+1)
			fmt.Printf("  PartID: %d (0x%016x)\n", resp.PartIDs[i], resp.PartIDs[i])
			fmt.Printf("  Key: %d\n", resp.Keys[i])
			fmt.Printf("  SeriesID: %d\n", resp.SIDs[i])

			// Add series text if available
			if seriesMap != nil {
				if seriesText, ok := seriesMap[resp.SIDs[i]]; ok {
					fmt.Printf("  Series: %s\n", seriesText)
				}
			}

			// Add projected tags if available
			if len(projectionTagNames) > 0 && resp.Tags != nil {
				for _, tagName := range projectionTagNames {
					if tagValues, ok := resp.Tags[tagName]; ok && i < len(tagValues) {
						tagValue := tagValues[i]
						// Calculate size of the tag value
						tagSize := len(tagValue)
						fmt.Printf("  %s: %s (size: %d bytes)\n", tagName, tagValue, tagSize)
					}
				}
			}

			fmt.Printf("  Data: %s\n", dataStr)
			fmt.Printf("\n")
			totalRows++
		}

		// Add newline after batch if we printed anything
		if batchPrinted && totalRows > batchStartRow {
			// Already added newline after each row
		}
	}

	if dataFilter != "" {
		fmt.Printf("\nTotal rows: %d (filtered out: %d)\n", totalRows, filteredRows)
	} else {
		fmt.Printf("\nTotal rows: %d\n", totalRows)
	}
	return nil
}

func outputScanResultsAsCSV(results []*sidx.QueryResponse, seriesMap map[common.SeriesID]string, projectionTagNames []string, dataFilter string) error {
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	// Write header
	header := []string{"PartID", "Key", "SeriesID", "Series"}
	// Add projected tag columns (with size)
	for _, tagName := range projectionTagNames {
		header = append(header, tagName, tagName+"_size")
	}
	header = append(header, "Data")

	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, resp := range results {
		if resp.Error != nil {
			return fmt.Errorf("response error: %w", resp.Error)
		}

		for i := 0; i < resp.Len(); i++ {
			dataStr := string(resp.Data[i])

			// Apply data filter if specified
			if dataFilter != "" && !strings.Contains(dataStr, dataFilter) {
				continue
			}

			seriesText := ""
			if seriesMap != nil {
				if text, ok := seriesMap[resp.SIDs[i]]; ok {
					seriesText = text
				}
			}

			row := []string{
				fmt.Sprintf("%d", resp.PartIDs[i]),
				fmt.Sprintf("%d", resp.Keys[i]),
				fmt.Sprintf("%d", resp.SIDs[i]),
				seriesText,
			}

			// Add projected tag values with size
			for _, tagName := range projectionTagNames {
				tagValue := ""
				tagSize := ""

				if resp.Tags != nil {
					if tagValues, ok := resp.Tags[tagName]; ok && i < len(tagValues) {
						tagValue = tagValues[i]
						// Calculate size of the tag value
						tagSize = fmt.Sprintf("%d", len(tagValue))
					}
				}

				row = append(row, tagValue, tagSize)
			}

			row = append(row, dataStr)

			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
	}

	return nil
}
