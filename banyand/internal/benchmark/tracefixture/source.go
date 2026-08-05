// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracefixture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/sourcecatalog"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Row is one deeply copied source span retained by the fixture generator.
type Row struct {
	IndexSeries map[string]common.SeriesID
	IndexTags   map[string][]sidx.Tag
	Tags        map[string][]byte
	TagTypes    map[string]pbv1.ValueType
	TraceID     string
	SpanID      string
	Span        []byte
	Timestamp   int64
	SeriesID    common.SeriesID
}

// LoadedFragment pairs physical fragment metadata with its complete source rows.
type LoadedFragment struct {
	Rows     []Row
	Fragment Fragment
}

// LoadedTrace is a complete logical source trace assembled across allowed parts.
type LoadedTrace struct {
	SourceID  string
	Fragments []LoadedFragment
}

// Source contains the two disjoint real-data populations used by the fixture.
type Source struct {
	IndexCompressedBytes map[string]uint64
	Mature               []LoadedTrace
	Small                []LoadedTrace
	Catalog              sourcecatalog.Catalog
}

// LoadOptions configures immutable source loading.
type LoadOptions struct {
	SourcePath  string
	CatalogPath string
	Format      dumptrace.PartFormat
}

type fragmentKey struct {
	traceID  string
	partID   uint64
	blockIdx int
}

// LoadSource assembles only catalog-allowlisted complete traces from the immutable shard.
func LoadSource(ctx context.Context, options LoadOptions) (Source, error) {
	catalogData, readErr := os.ReadFile(options.CatalogPath)
	if readErr != nil {
		return Source{}, fmt.Errorf("cannot read source catalog %q: %w", options.CatalogPath, readErr)
	}
	var catalog sourcecatalog.Catalog
	if unmarshalErr := json.Unmarshal(catalogData, &catalog); unmarshalErr != nil {
		return Source{}, fmt.Errorf("cannot decode source catalog %q: %w", options.CatalogPath, unmarshalErr)
	}
	sourceManifest, manifestErr := benchmark.TreeManifest(options.SourcePath)
	if manifestErr != nil {
		return Source{}, fmt.Errorf("cannot recompute immutable source manifest: %w", manifestErr)
	}
	if sourceManifest.SHA256 != catalog.SourceManifestSHA256 {
		return Source{}, fmt.Errorf("immutable source manifest changed: got %s, want %s", sourceManifest.SHA256, catalog.SourceManifestSHA256)
	}
	populationByTrace := make(map[string]TraceClass, len(catalog.Mature.TraceIDs)+len(catalog.Small.TraceIDs))
	addPopulationIDs(populationByTrace, catalog.Mature.TraceIDs, TraceClassMature)
	addPopulationIDs(populationByTrace, catalog.Mature.ClosureTraceIDs, TraceClassMature)
	addPopulationIDs(populationByTrace, catalog.Small.TraceIDs, TraceClassSmall)
	addPopulationIDs(populationByTrace, catalog.Small.ClosureTraceIDs, TraceClassSmall)
	partIDs, partErr := sourcePartIDs(catalog)
	if partErr != nil {
		return Source{}, fmt.Errorf("cannot resolve source part population: %w", partErr)
	}
	loaded, scanErr := scanSourceParts(ctx, options.SourcePath, options.Format, partIDs, populationByTrace)
	if scanErr != nil {
		return Source{}, scanErr
	}
	mature := orderLoadedPopulation(catalog.Mature.TraceIDs, loaded)
	small := orderLoadedPopulation(catalog.Small.TraceIDs, loaded)
	if indexErr := attachIndexSeries(ctx, options.SourcePath, populationByTrace, mature, small); indexErr != nil {
		return Source{}, indexErr
	}
	indexCompressedBytes, indexBytesErr := sourceIndexCompressedBytes(options.SourcePath, catalog)
	if indexBytesErr != nil {
		return Source{}, indexBytesErr
	}
	matureRows := catalog.Mature.RowCount + carrierRows(catalog.Mature.Carriers)
	if validateErr := validateLoadedPopulation("mature", mature, catalog.Mature.TraceCount, matureRows); validateErr != nil {
		return Source{}, validateErr
	}
	smallRows := catalog.Small.RowCount + carrierRows(catalog.Small.Carriers)
	if validateErr := validateLoadedPopulation("small", small, catalog.Small.TraceCount, smallRows); validateErr != nil {
		return Source{}, validateErr
	}
	return Source{Catalog: catalog, IndexCompressedBytes: indexCompressedBytes, Mature: mature, Small: small}, nil
}

func sourceIndexCompressedBytes(sourcePath string, catalog sourcecatalog.Catalog) (map[string]uint64, error) {
	result := make(map[string]uint64, len(catalog.Indexes))
	fileSystem := fs.NewLocalFileSystem()
	for indexName := range catalog.Indexes {
		root := filepath.Join(sourcePath, "sidx", indexName)
		partIDs, discoverErr := dump.DiscoverPartIDs(root)
		if discoverErr != nil {
			return nil, fmt.Errorf("cannot discover source index %q for compressed-size calibration: %w", indexName, discoverErr)
		}
		for _, partID := range partIDs {
			metadata, metadataErr := sidx.ParsePartMetadata(fileSystem, filepath.Join(root, fmt.Sprintf("%016x", partID)))
			if metadataErr != nil {
				return nil, fmt.Errorf("cannot parse source index %q part %016x for compressed-size calibration: %w", indexName, partID, metadataErr)
			}
			result[indexName] += metadata.CompressedSizeBytes
		}
	}
	return result, nil
}

func carrierRows(carriers []sourcecatalog.CarrierCatalog) uint64 {
	var rows uint64
	for carrierIdx := range carriers {
		rows += carriers[carrierIdx].RowCount
	}
	return rows
}

func addPopulationIDs(target map[string]TraceClass, traceIDs []string, class TraceClass) {
	for _, traceID := range traceIDs {
		target[traceID] = class
	}
}

func sourcePartIDs(catalog sourcecatalog.Catalog) ([]uint64, error) {
	partSet := make(map[uint64]struct{})
	addIDs := func(partIDs []string) error {
		for _, textID := range partIDs {
			partID, parseErr := strconv.ParseUint(textID, 16, 64)
			if parseErr != nil {
				return fmt.Errorf("cannot parse source part ID %q: %w", textID, parseErr)
			}
			partSet[partID] = struct{}{}
		}
		return nil
	}
	if addErr := addIDs(catalog.Mature.PartIDs); addErr != nil {
		return nil, addErr
	}
	if addErr := addIDs(catalog.Small.PartIDs); addErr != nil {
		return nil, addErr
	}
	for _, population := range []sourcecatalog.PopulationCatalog{catalog.Mature, catalog.Small} {
		for carrierIdx := range population.Carriers {
			if addErr := addIDs([]string{population.Carriers[carrierIdx].PartID}); addErr != nil {
				return nil, addErr
			}
		}
	}
	partIDs := make([]uint64, 0, len(partSet))
	for partID := range partSet {
		partIDs = append(partIDs, partID)
	}
	sort.Slice(partIDs, func(leftIdx, rightIdx int) bool { return partIDs[leftIdx] < partIDs[rightIdx] })
	return partIDs, nil
}

func scanSourceParts(ctx context.Context, root string, format dumptrace.PartFormat, partIDs []uint64,
	populationByTrace map[string]TraceClass,
) (map[string][]LoadedFragment, error) {
	fileSystem := fs.NewLocalFileSystem()
	fragments := make(map[fragmentKey]*LoadedFragment)
	for _, partID := range partIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("source scan canceled: %w", contextErr)
		}
		reader, openErr := dumptrace.OpenPartWithFormat(partID, root, fileSystem, format)
		if openErr != nil {
			return nil, fmt.Errorf("cannot open source trace part %016x: %w", partID, openErr)
		}
		iterator := reader.Iterator()
		for iterator.Next() {
			position := iterator.Position()
			row := iterator.Row()
			if _, selected := populationByTrace[row.TraceID]; !selected {
				continue
			}
			key := fragmentKey{traceID: row.TraceID, partID: partID, blockIdx: position.BlockIdx}
			fragment := fragments[key]
			if fragment == nil {
				fragment = &LoadedFragment{Fragment: Fragment{
					SourcePartID: partID,
					MinTimestamp: row.Timestamp,
					MaxTimestamp: row.Timestamp,
				}}
				fragments[key] = fragment
			}
			fragment.Fragment.Rows++
			fragment.Fragment.MinTimestamp = min(fragment.Fragment.MinTimestamp, row.Timestamp)
			fragment.Fragment.MaxTimestamp = max(fragment.Fragment.MaxTimestamp, row.Timestamp)
			fragment.Rows = append(fragment.Rows, cloneSourceRow(row))
		}
		iterationErr := iterator.Err()
		closeIteratorErr := iterator.Close()
		closeReaderErr := reader.Close()
		if combinedErr := errors.Join(iterationErr, closeIteratorErr, closeReaderErr); combinedErr != nil {
			return nil, fmt.Errorf("cannot scan source trace part %016x: %w", partID, combinedErr)
		}
	}
	byTrace := make(map[string][]LoadedFragment)
	for key, fragment := range fragments {
		byTrace[key.traceID] = append(byTrace[key.traceID], *fragment)
	}
	for traceID := range byTrace {
		sort.SliceStable(byTrace[traceID], func(leftIdx, rightIdx int) bool {
			left := &byTrace[traceID][leftIdx].Fragment
			right := &byTrace[traceID][rightIdx].Fragment
			if left.MinTimestamp != right.MinTimestamp {
				return left.MinTimestamp < right.MinTimestamp
			}
			return left.SourcePartID < right.SourcePartID
		})
	}
	return byTrace, nil
}

func cloneSourceRow(row dumptrace.Row) Row {
	tags := make(map[string][]byte, len(row.Tags))
	for tagName, value := range row.Tags {
		tags[tagName] = append([]byte(nil), value...)
	}
	tagTypes := make(map[string]pbv1.ValueType, len(row.TagTypes))
	for tagName, valueType := range row.TagTypes {
		tagTypes[tagName] = valueType
	}
	return Row{
		IndexSeries: make(map[string]common.SeriesID, 2), IndexTags: make(map[string][]sidx.Tag, 2), Tags: tags, TagTypes: tagTypes,
		TraceID: row.TraceID, SpanID: row.SpanID,
		Span: append([]byte(nil), row.Span...), Timestamp: row.Timestamp, SeriesID: row.SeriesID,
	}
}

type sourceIndexKey struct {
	traceID string
	key     int64
}

type sourceIndexValue struct {
	tags     []sidx.Tag
	seriesID common.SeriesID
}

func attachIndexSeries(ctx context.Context, sourcePath string, selected map[string]TraceClass, populations ...[]LoadedTrace) error {
	fileSystem := fs.NewLocalFileSystem()
	for _, indexName := range fixtureIndexNames {
		indexRoot := filepath.Join(sourcePath, "sidx", indexName)
		partIDs, discoverErr := dump.DiscoverPartIDs(indexRoot)
		if discoverErr != nil {
			return fmt.Errorf("cannot discover source index %q parts: %w", indexName, discoverErr)
		}
		seriesByRow := make(map[sourceIndexKey][]sourceIndexValue)
		scanErr := sidx.ScanRawParts(ctx, fileSystem, indexRoot, partIDs, func(indexRow sidx.RawRow) error {
			if len(indexRow.Data) < 2 || indexRow.Data[0] != 1 {
				return fmt.Errorf("source index %q has unsupported trace ID encoding", indexName)
			}
			traceID := string(indexRow.Data[1:])
			if _, ok := selected[traceID]; !ok {
				return nil
			}
			key := sourceIndexKey{traceID: traceID, key: indexRow.Key}
			seriesByRow[key] = append(seriesByRow[key], sourceIndexValue{seriesID: indexRow.SeriesID, tags: cloneIndexTags(indexRow.Tags)})
			return nil
		})
		if scanErr != nil {
			return fmt.Errorf("cannot scan source index %q: %w", indexName, scanErr)
		}
		for populationIdx := range populations {
			population := populations[populationIdx]
			for traceIdx := range population {
				trace := &population[traceIdx]
				for fragmentIdx := range trace.Fragments {
					fragment := &trace.Fragments[fragmentIdx]
					for rowIdx := range fragment.Rows {
						row := &fragment.Rows[rowIdx]
						key := sourceIndexKey{traceID: row.TraceID, key: sourceIndexKeyValue(indexName, row)}
						indexValues := seriesByRow[key]
						if len(indexValues) == 0 {
							return fmt.Errorf("source index %q has no series for trace %q key %d", indexName, row.TraceID, key.key)
						}
						row.IndexSeries[indexName] = indexValues[0].seriesID
						row.IndexTags[indexName] = indexValues[0].tags
						seriesByRow[key] = indexValues[1:]
					}
				}
			}
		}
		for key, seriesIDs := range seriesByRow {
			if len(seriesIDs) != 0 {
				return fmt.Errorf("source index %q left %d unmatched rows for trace %q key %d", indexName, len(seriesIDs), key.traceID, key.key)
			}
		}
	}
	return nil
}

func cloneIndexTags(tags []sidx.Tag) []sidx.Tag {
	cloned := make([]sidx.Tag, len(tags))
	for tagIdx := range tags {
		cloned[tagIdx] = tags[tagIdx]
		cloned[tagIdx].Value = append([]byte(nil), tags[tagIdx].Value...)
		if tags[tagIdx].ValueArr != nil {
			cloned[tagIdx].ValueArr = make([][]byte, len(tags[tagIdx].ValueArr))
			for valueIdx := range tags[tagIdx].ValueArr {
				cloned[tagIdx].ValueArr[valueIdx] = append([]byte(nil), tags[tagIdx].ValueArr[valueIdx]...)
			}
		}
	}
	return cloned
}

func sourceIndexKeyValue(indexName string, row *Row) int64 {
	if indexName == "start_time" {
		return row.Timestamp
	}
	value := row.Tags[indexName]
	if len(value) < 8 {
		return 0
	}
	return convert.BytesToInt64(value)
}

func orderLoadedPopulation(traceIDs []string, fragments map[string][]LoadedFragment) []LoadedTrace {
	ordered := append([]string(nil), traceIDs...)
	sort.Strings(ordered)
	population := make([]LoadedTrace, 0, len(ordered))
	for _, traceID := range ordered {
		population = append(population, LoadedTrace{SourceID: traceID, Fragments: fragments[traceID]})
	}
	return population
}

func validateLoadedPopulation(name string, population []LoadedTrace, expectedTraces, expectedRows uint64) error {
	if uint64(len(population)) != expectedTraces {
		return fmt.Errorf("%s source trace count mismatch: got %d, want %d", name, len(population), expectedTraces)
	}
	var rows uint64
	for traceIdx := range population {
		trace := &population[traceIdx]
		if len(trace.Fragments) == 0 {
			return fmt.Errorf("%s source trace %q has no allowlisted fragments", name, trace.SourceID)
		}
		for fragmentIdx := range trace.Fragments {
			rows += uint64(len(trace.Fragments[fragmentIdx].Rows))
		}
	}
	if rows != expectedRows {
		return fmt.Errorf("%s source row count mismatch: got %d, want %d", name, rows, expectedRows)
	}
	return nil
}

func planningTraces(loaded []LoadedTrace) []Trace {
	result := make([]Trace, 0, len(loaded))
	for traceIdx := range loaded {
		trace := &loaded[traceIdx]
		fragments := make([]Fragment, len(trace.Fragments))
		for fragmentIdx := range trace.Fragments {
			fragments[fragmentIdx] = trace.Fragments[fragmentIdx].Fragment
		}
		result = append(result, Trace{SourceID: trace.SourceID, Fragments: fragments})
	}
	return result
}

// BuildSourcePlan schedules a previously loaded immutable source.
func BuildSourcePlan(source Source, options Options) (Plan, error) {
	if len(source.Mature) != MatureTraceCount || len(source.Small) != SmallTraceCount || options.CopyCount != CopyTraceCount {
		return Plan{}, fmt.Errorf("reference population mismatch: mature=%d small=%d copies=%d", len(source.Mature), len(source.Small), options.CopyCount)
	}
	plan, planErr := BuildPlan(planningTraces(source.Mature), planningTraces(source.Small), options)
	if planErr != nil {
		return Plan{}, fmt.Errorf("cannot build reference fixture plan: %w", planErr)
	}
	expectedInstances := GeneratedTraceCount * plan.WriteIntensity
	if len(plan.Instances) != expectedInstances {
		return Plan{}, fmt.Errorf("generated trace count mismatch: got %d, want %d", len(plan.Instances), expectedInstances)
	}
	return plan, nil
}
