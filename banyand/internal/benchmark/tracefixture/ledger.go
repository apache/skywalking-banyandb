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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type ledgerHashes map[string][][sha256.Size]byte

func reconcileLogicalLedgers(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, plan Plan, lookup sourceLookup, offsets []int64) error {
	expectedCore := make(ledgerHashes)
	expectedIndexes := map[string]ledgerHashes{"latency": {}, "start_time": {}}
	for instanceIdx := range plan.Instances {
		instance := &plan.Instances[instanceIdx]
		sourceTrace := lookup[instance.SourceID]
		for fragmentIdx := range sourceTrace.Fragments {
			for rowIdx := range sourceTrace.Fragments[fragmentIdx].Rows {
				encodedRow := remapRow(sourceTrace.Fragments[fragmentIdx].Rows[rowIdx], instance.GeneratedID, offsets[instanceIdx])
				expectedCore[instance.GeneratedID] = append(expectedCore[instance.GeneratedID], hashEncodedCoreRow(encodedRow))
				for _, indexName := range fixtureIndexNames {
					key := int64(0)
					if indexName == "start_time" {
						key = encodedRow.Timestamp
					} else {
						for tagIdx := range encodedRow.Tags {
							if encodedRow.Tags[tagIdx].Name == indexName && len(encodedRow.Tags[tagIdx].RawValue) >= 8 {
								key = convert.BytesToInt64(encodedRow.Tags[tagIdx].RawValue)
							}
						}
					}
					data := append([]byte{traceIDEncodingV1}, instance.GeneratedID...)
					expectedIndexes[indexName][instance.GeneratedID] = append(expectedIndexes[indexName][instance.GeneratedID],
						hashIndexLedgerRow(uint64(encodedRow.IndexSeries[indexName]), key, data, encodedRow.IndexTags[indexName]))
				}
			}
		}
	}
	actualCore, coreErr := scanCoreLedger(ctx, receiver.Root())
	if coreErr != nil {
		return coreErr
	}
	if compareErr := compareLedger("core", expectedCore, actualCore); compareErr != nil {
		return compareErr
	}
	for _, indexName := range fixtureIndexNames {
		actualIndex := make(ledgerHashes)
		scanErr := receiver.ScanRawIndex(ctx, indexName, func(row sidx.RawRow) error {
			if len(row.Data) < 2 || row.Data[0] != traceIDEncodingV1 {
				return fmt.Errorf("index %q returned unsupported trace ID encoding", indexName)
			}
			traceID := string(row.Data[1:])
			actualIndex[traceID] = append(actualIndex[traceID], hashIndexLedgerRow(uint64(row.SeriesID), row.Key, row.Data, row.Tags))
			return nil
		})
		if scanErr != nil {
			return fmt.Errorf("cannot scan generated index %q ledger: %w", indexName, scanErr)
		}
		if compareErr := compareLedger(indexName, expectedIndexes[indexName], actualIndex); compareErr != nil {
			return compareErr
		}
	}
	return nil
}

func scanCoreLedger(ctx context.Context, root string) (ledgerHashes, error) {
	partIDs, discoverErr := dump.DiscoverPartIDs(root)
	if discoverErr != nil {
		return nil, fmt.Errorf("cannot discover generated core ledger parts: %w", discoverErr)
	}
	fileSystem := fs.NewLocalFileSystem()
	rows := make(ledgerHashes)
	for _, partID := range partIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("generated core ledger scan canceled: %w", contextErr)
		}
		reader, openErr := dumptrace.OpenPart(partID, root, fileSystem)
		if openErr != nil {
			return nil, fmt.Errorf("cannot open generated core ledger part %016x: %w", partID, openErr)
		}
		iterator := reader.Iterator()
		for iterator.Next() {
			row := iterator.Row()
			rows[row.TraceID] = append(rows[row.TraceID], hashDecodedCoreRow(row))
		}
		partErr := errors.Join(iterator.Err(), iterator.Close(), reader.Close())
		if partErr != nil {
			return nil, fmt.Errorf("cannot scan generated core ledger part %016x: %w", partID, partErr)
		}
	}
	return rows, nil
}

func hashEncodedCoreRow(row storagetrace.PartEncoderRow) [sha256.Size]byte {
	tags := make(map[string][]byte, len(row.Tags))
	types := make(map[string]byte, len(row.Tags))
	for tagIdx := range row.Tags {
		tag := &row.Tags[tagIdx]
		tags[tag.Name] = tag.RawValue
		types[tag.Name] = byte(tag.ValueType)
	}
	return hashCoreRow(row.SpanID, row.Span, row.Timestamp, tags, types)
}

func hashDecodedCoreRow(row dumptrace.Row) [sha256.Size]byte {
	types := make(map[string]byte, len(row.TagTypes))
	for tagName, valueType := range row.TagTypes {
		types[tagName] = byte(valueType)
	}
	return hashCoreRow(row.SpanID, row.Span, row.Timestamp, row.Tags, types)
}

func hashCoreRow(spanID string, span []byte, timestamp int64, tags map[string][]byte, types map[string]byte) [sha256.Size]byte {
	digest := sha256.New()
	writeLedgerBytes(digest, []byte(spanID))
	writeLedgerBytes(digest, span)
	writeLedgerUint64(digest, uint64(timestamp))
	tagNames := make([]string, 0, len(tags))
	for tagName := range tags {
		tagNames = append(tagNames, tagName)
	}
	sort.Strings(tagNames)
	for _, tagName := range tagNames {
		writeLedgerBytes(digest, []byte(tagName))
		writeLedgerBytes(digest, []byte{types[tagName]})
		writeLedgerBytes(digest, tags[tagName])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func hashIndexLedgerRow(seriesID uint64, key int64, data []byte, tags []sidx.Tag) [sha256.Size]byte {
	digest := sha256.New()
	writeLedgerUint64(digest, seriesID)
	writeLedgerUint64(digest, uint64(key))
	writeLedgerBytes(digest, data)
	orderedTags := cloneIndexTags(tags)
	sort.Slice(orderedTags, func(leftIdx, rightIdx int) bool { return orderedTags[leftIdx].Name < orderedTags[rightIdx].Name })
	for tagIdx := range orderedTags {
		tag := &orderedTags[tagIdx]
		writeLedgerBytes(digest, []byte(tag.Name))
		writeLedgerBytes(digest, []byte{byte(tag.ValueType)})
		writeLedgerBytes(digest, tag.Value)
		for valueIdx := range tag.ValueArr {
			writeLedgerBytes(digest, tag.ValueArr[valueIdx])
		}
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func compareLedger(name string, expected, actual ledgerHashes) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("%s ledger trace count mismatch: got %d, want %d", name, len(actual), len(expected))
	}
	for traceID, expectedRows := range expected {
		actualRows := actual[traceID]
		sort.Slice(expectedRows, func(leftIdx, rightIdx int) bool {
			return string(expectedRows[leftIdx][:]) < string(expectedRows[rightIdx][:])
		})
		sort.Slice(actualRows, func(leftIdx, rightIdx int) bool {
			return string(actualRows[leftIdx][:]) < string(actualRows[rightIdx][:])
		})
		if len(expectedRows) != len(actualRows) {
			return fmt.Errorf("%s ledger trace %q row count mismatch: got %d, want %d", name, traceID, len(actualRows), len(expectedRows))
		}
		for rowIdx := range expectedRows {
			if expectedRows[rowIdx] != actualRows[rowIdx] {
				return fmt.Errorf("%s ledger trace %q checksum mismatch at row %d: got %s, want %s", name, traceID, rowIdx,
					hex.EncodeToString(actualRows[rowIdx][:]), hex.EncodeToString(expectedRows[rowIdx][:]))
			}
		}
	}
	return nil
}

func writeLedgerBytes(digest interface{ Write([]byte) (int, error) }, value []byte) {
	writeLedgerUint64(digest, uint64(len(value)))
	written, writeErr := digest.Write(value)
	if writeErr != nil || written != len(value) {
		panic(fmt.Sprintf("cannot write in-memory ledger hash: wrote=%d want=%d err=%v", written, len(value), writeErr))
	}
}

func writeLedgerUint64(digest interface{ Write([]byte) (int, error) }, value uint64) {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], value)
	written, writeErr := digest.Write(buffer[:])
	if writeErr != nil || written != len(buffer) {
		panic(fmt.Sprintf("cannot write in-memory ledger uint64: wrote=%d want=%d err=%v", written, len(buffer), writeErr))
	}
}
