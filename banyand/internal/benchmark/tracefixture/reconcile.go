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
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func reconcileFixture(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, source Source, plan Plan,
	lookup sourceLookup, offsets []int64, expectedRows uint64,
) error {
	expectedByTrace, schemaTagTypes, buildErr := expectedTraceRows(plan, lookup)
	if buildErr != nil {
		return fmt.Errorf("cannot build expected trace ledger: %w", buildErr)
	}
	actualByTrace, scanErr := scanGeneratedCore(ctx, receiver.Root())
	if scanErr != nil {
		return fmt.Errorf("cannot scan generated core ledger: %w", scanErr)
	}
	if compareErr := compareTraceCounts(expectedByTrace, actualByTrace); compareErr != nil {
		return fmt.Errorf("generated core count ledger differs: %w", compareErr)
	}
	if ledgerErr := reconcileLogicalLedgers(ctx, receiver, plan, lookup, offsets); ledgerErr != nil {
		return fmt.Errorf("generated fixture ledger reconciliation failed: %w", ledgerErr)
	}
	if queryErr := reconcileTraceQueries(ctx, receiver, plan, expectedByTrace, schemaTagTypes); queryErr != nil {
		return fmt.Errorf("generated trace query reconciliation failed: %w", queryErr)
	}
	for _, indexName := range fixtureIndexNames {
		if indexErr := reconcileIndexQueries(ctx, receiver, indexName, plan, lookup, offsets, expectedRows); indexErr != nil {
			return fmt.Errorf("generated index %q reconciliation failed: %w", indexName, indexErr)
		}
	}
	if uint64(sumCounts(actualByTrace)) != expectedRows {
		return fmt.Errorf("generated core row count mismatch after reopen: got %d, want %d", sumCounts(actualByTrace), expectedRows)
	}
	if uint64(len(actualByTrace)) != source.Catalog.Core.TraceCount {
		return fmt.Errorf("generated core trace count mismatch after reopen: got %d, want %d", len(actualByTrace), source.Catalog.Core.TraceCount)
	}
	return nil
}

func expectedTraceRows(plan Plan, lookup sourceLookup) (map[string]int, map[string]pbv1.ValueType, error) {
	counts := make(map[string]int, len(plan.Instances))
	tagTypes := make(map[string]pbv1.ValueType)
	for instanceIdx := range plan.Instances {
		instance := &plan.Instances[instanceIdx]
		sourceTrace, ok := lookup[instance.SourceID]
		if !ok {
			return nil, nil, fmt.Errorf("generated trace %q source %q is missing", instance.GeneratedID, instance.SourceID)
		}
		for fragmentIdx := range sourceTrace.Fragments {
			fragment := &sourceTrace.Fragments[fragmentIdx]
			counts[instance.GeneratedID] += len(fragment.Rows)
			for rowIdx := range fragment.Rows {
				for tagName, valueType := range fragment.Rows[rowIdx].TagTypes {
					tagTypes[tagName] = valueType
				}
			}
		}
	}
	return counts, tagTypes, nil
}

func scanGeneratedCore(ctx context.Context, root string) (map[string]int, error) {
	partIDs, discoverErr := dump.DiscoverPartIDs(root)
	if discoverErr != nil {
		return nil, fmt.Errorf("cannot discover generated core parts: %w", discoverErr)
	}
	fileSystem := fs.NewLocalFileSystem()
	counts := make(map[string]int)
	for _, partID := range partIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("generated core scan canceled: %w", contextErr)
		}
		reader, openErr := dumptrace.OpenPart(partID, root, fileSystem)
		if openErr != nil {
			return nil, fmt.Errorf("cannot reopen generated core part %016x: %w", partID, openErr)
		}
		iterator := reader.Iterator()
		for iterator.Next() {
			counts[iterator.Row().TraceID]++
		}
		iterationErr := iterator.Err()
		closeIteratorErr := iterator.Close()
		closeErr := reader.Close()
		if partErr := errors.Join(iterationErr, closeIteratorErr, closeErr); partErr != nil {
			return nil, fmt.Errorf("cannot scan and close generated core part %016x: %w", partID, partErr)
		}
	}
	return counts, nil
}

func compareTraceCounts(expected, actual map[string]int) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("generated trace ledger count mismatch: got %d, want %d", len(actual), len(expected))
	}
	for traceID, expectedRows := range expected {
		if actualRows := actual[traceID]; actualRows != expectedRows {
			return fmt.Errorf("generated trace %q rows mismatch: got %d, want %d", traceID, actualRows, expectedRows)
		}
	}
	return nil
}

func reconcileTraceQueries(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, plan Plan, expected map[string]int,
	schemaTagTypes map[string]pbv1.ValueType,
) error {
	if len(plan.Instances) == 0 {
		return fmt.Errorf("generated fixture has no traces to query")
	}
	minTimestamp := plan.DayStart.UnixNano()
	maxTimestamp := plan.DayStart.Add(plan.DayDuration).UnixNano() - 1
	for instanceIdx := range plan.Instances {
		instance := &plan.Instances[instanceIdx]
		results, queryErr := receiver.QueryTrace(ctx, instance.GeneratedID, minTimestamp, maxTimestamp, schemaTagTypes)
		if queryErr != nil {
			return fmt.Errorf("generated trace query failed for %q: %w", instance.GeneratedID, queryErr)
		}
		rows := 0
		for resultIdx := range results {
			rows += len(results[resultIdx].SpanIDs)
		}
		if rows != expected[instance.GeneratedID] {
			return fmt.Errorf("generated trace query %q returned %d rows, want %d", instance.GeneratedID, rows, expected[instance.GeneratedID])
		}
	}
	return nil
}

func reconcileIndexQueries(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, name string, plan Plan,
	lookup sourceLookup, offsets []int64, expectedRows uint64,
) error {
	var rawRows uint64
	rawTraceIDs := make(map[string]struct{})
	expectedScanRows := make(map[indexQueryRow]uint64)
	seenScanData := make(map[indexBlockID]map[string]struct{})
	rawErr := receiver.ScanRawIndex(ctx, name, func(row sidx.RawRow) error {
		rawRows++
		if len(row.Data) > 1 && row.Data[0] == traceIDEncodingV1 {
			rawTraceIDs[string(row.Data[1:])] = struct{}{}
		}
		blockID := indexBlockID{partID: row.PartID, blockID: row.BlockID}
		seenData := seenScanData[blockID]
		if seenData == nil {
			seenData = make(map[string]struct{})
			seenScanData[blockID] = seenData
		}
		encodedData := string(row.Data)
		if _, seen := seenData[encodedData]; !seen {
			expectedScanRows[hashIndexQueryRow(uint64(row.SeriesID), row.Key, row.Data)]++
			seenData[encodedData] = struct{}{}
		}
		return nil
	})
	if rawErr != nil {
		return fmt.Errorf("generated index %q raw reconciliation failed: %w", name, rawErr)
	}
	if rawRows != expectedRows || len(rawTraceIDs) != len(plan.Instances) {
		return fmt.Errorf("generated index %q physical ledger mismatch: got rows=%d traces=%d, want rows=%d traces=%d",
			name, rawRows, len(rawTraceIDs), expectedRows, len(plan.Instances))
	}
	expectedQueryRows, expectedQueryData := expectedIndexQueryRows(name, plan, lookup, offsets)
	scanResults, scanErr := receiver.ScanIndex(ctx, name, sidx.ScanQueryRequest{})
	if scanErr != nil {
		return fmt.Errorf("generated index %q scan query failed: %w", name, scanErr)
	}
	scanRows, scanData, scanRowErr := indexQueryRows(scanResults)
	if scanRowErr != nil {
		return fmt.Errorf("generated index %q scan rows are invalid: %w", name, scanRowErr)
	}
	if compareErr := compareIndexScanRows(expectedScanRows, scanRows, expectedQueryData, scanData); compareErr != nil {
		return fmt.Errorf("generated index %q scan differs: %w", name, compareErr)
	}
	seriesSet := make(map[common.SeriesID]struct{})
	for sourceID := range lookup {
		trace := lookup[sourceID]
		for fragmentIdx := range trace.Fragments {
			for rowIdx := range trace.Fragments[fragmentIdx].Rows {
				seriesSet[trace.Fragments[fragmentIdx].Rows[rowIdx].IndexSeries[name]] = struct{}{}
			}
		}
	}
	seriesIDs := make([]common.SeriesID, 0, len(seriesSet))
	for seriesID := range seriesSet {
		seriesIDs = append(seriesIDs, seriesID)
	}
	sort.Slice(seriesIDs, func(leftIdx, rightIdx int) bool { return seriesIDs[leftIdx] < seriesIDs[rightIdx] })
	queryResults, queryErr := receiver.QueryIndex(ctx, name, sidx.QueryRequest{SeriesIDs: seriesIDs})
	if queryErr != nil {
		return fmt.Errorf("generated index %q keyed query failed: %w", name, queryErr)
	}
	queryRows, queryData, queryRowErr := indexQueryRows(queryResults)
	if queryRowErr != nil {
		return fmt.Errorf("generated index %q keyed rows are invalid: %w", name, queryRowErr)
	}
	if compareErr := compareIndexKeyedRows(expectedQueryRows, queryRows, expectedQueryData, queryData); compareErr != nil {
		return fmt.Errorf("generated index %q keyed query differs: %w", name, compareErr)
	}
	return nil
}

type indexQueryRow [32]byte

type indexBlockID struct {
	partID  uint64
	blockID uint64
}

func expectedIndexQueryRows(name string, plan Plan, lookup sourceLookup, offsets []int64) (map[indexQueryRow]uint64, map[string]struct{}) {
	rows := make(map[indexQueryRow]uint64)
	expectedData := make(map[string]struct{}, len(plan.Instances))
	for instanceIdx := range plan.Instances {
		instance := &plan.Instances[instanceIdx]
		sourceTrace := lookup[instance.SourceID]
		for fragmentIdx := range sourceTrace.Fragments {
			for rowIdx := range sourceTrace.Fragments[fragmentIdx].Rows {
				encoded := remapRow(sourceTrace.Fragments[fragmentIdx].Rows[rowIdx], instance.GeneratedID, offsets[instanceIdx])
				key := encoded.Timestamp
				if name == "latency" {
					key = 0
					for tagIdx := range encoded.Tags {
						if encoded.Tags[tagIdx].Name == name && len(encoded.Tags[tagIdx].RawValue) >= 8 {
							key = convert.BytesToInt64(encoded.Tags[tagIdx].RawValue)
						}
					}
				}
				encodedData := append([]byte{traceIDEncodingV1}, instance.GeneratedID...)
				rows[hashIndexQueryRow(uint64(encoded.IndexSeries[name]), key, encodedData)]++
				expectedData[string(encodedData)] = struct{}{}
			}
		}
	}
	return rows, expectedData
}

func indexQueryRows(results []*sidx.QueryResponse) (map[indexQueryRow]uint64, map[string]struct{}, error) {
	rows := make(map[indexQueryRow]uint64)
	data := make(map[string]struct{})
	for resultIdx := range results {
		result := results[resultIdx]
		if result.Error != nil {
			return nil, nil, result.Error
		}
		if validateErr := result.Validate(); validateErr != nil {
			return nil, nil, validateErr
		}
		for rowIdx := range result.Keys {
			rows[hashIndexQueryRow(uint64(result.SIDs[rowIdx]), result.Keys[rowIdx], result.Data[rowIdx])]++
			if len(result.Data[rowIdx]) < 2 || result.Data[rowIdx][0] != traceIDEncodingV1 {
				return nil, nil, fmt.Errorf("unsupported trace ID encoding in query result")
			}
			data[string(result.Data[rowIdx])] = struct{}{}
		}
	}
	return rows, data, nil
}

func hashIndexQueryRow(seriesID uint64, key int64, data []byte) indexQueryRow {
	digest := sha256.New()
	writeLedgerUint64(digest, seriesID)
	writeLedgerUint64(digest, uint64(key))
	writeLedgerBytes(digest, data)
	var result indexQueryRow
	copy(result[:], digest.Sum(nil))
	return result
}

func compareIndexScanRows(expected, actual map[indexQueryRow]uint64, expectedData, actualData map[string]struct{}) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("query distinct row count mismatch: got %d, want %d", len(actual), len(expected))
	}
	for row, expectedCount := range expected {
		if actualCount := actual[row]; actualCount != expectedCount {
			return fmt.Errorf("query row %x count mismatch: got %d, want %d", row, actualCount, expectedCount)
		}
	}
	return compareIndexQueryData(expectedData, actualData)
}

func compareIndexKeyedRows(expectedPhysical, actual map[indexQueryRow]uint64, expectedData, actualData map[string]struct{}) error {
	for row, actualCount := range actual {
		expectedCount := expectedPhysical[row]
		if expectedCount == 0 {
			return fmt.Errorf("unexpected keyed query row %x", row)
		}
		if actualCount > expectedCount {
			return fmt.Errorf("keyed query row %x was returned %d times, exceeding its physical count %d", row, actualCount, expectedCount)
		}
	}
	return compareIndexQueryData(expectedData, actualData)
}

func compareIndexQueryData(expected, actual map[string]struct{}) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("query data count mismatch: got %d, want %d", len(actual), len(expected))
	}
	for data := range expected {
		if _, exists := actual[data]; !exists {
			return fmt.Errorf("query data %x is missing", data)
		}
	}
	return nil
}

func sumCounts(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}
