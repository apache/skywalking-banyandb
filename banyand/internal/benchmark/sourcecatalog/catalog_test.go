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

package sourcecatalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/sourcecatalog"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func TestBuildValidatesAndCatalogsImmutableSource(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.MkdirAll(root, 0o755))
	fileSystem := fs.NewLocalFileSystem()
	partRows := map[uint64][]storagetrace.DumpRow{
		1: {
			{TraceID: "trace-small", SpanID: "span-small", Span: []byte("payload-small"), Timestamp: 10},
			{TraceID: "trace-small-boundary", SpanID: "span-small-1", Span: []byte("payload-small-1"), Timestamp: 20},
		},
		2: {
			{TraceID: "trace-small-boundary", SpanID: "span-small-2", Span: []byte("payload-small-2"), Timestamp: 30},
		},
		3: {
			{TraceID: "trace-mature", SpanID: "span-mature", Span: []byte("payload-mature"), Timestamp: 40},
			{TraceID: "trace-mature-boundary", SpanID: "span-mature-1", Span: []byte("payload-mature-1"), Timestamp: 50},
		},
		4: {
			{TraceID: "trace-mature-boundary", SpanID: "span-mature-2", Span: []byte("payload-mature-2"), Timestamp: 60},
		},
	}
	for partID, rows := range partRows {
		_, _, cleanup := storagetrace.BuildPartForDump(root, fileSystem, partID, rows)
		t.Cleanup(cleanup)
	}
	for _, indexName := range []string{"latency", "start_time"} {
		buildIndexParts(t, fileSystem, filepath.Join(root, "sidx", indexName), partRows)
	}

	manifest, manifestErr := benchmark.TreeManifest(root)
	require.NoError(t, manifestErr)
	partIDs := []uint64{1, 2, 3, 4}
	coreBytes, coreBlocks := coreMetadataTotals(t, root, partIDs)
	latencyBytes := directoryBytes(t, filepath.Join(root, "sidx", "latency"))
	startTimeBytes := directoryBytes(t, filepath.Join(root, "sidx", "start_time"))
	expectations := sourcecatalog.Expectations{
		ManifestSHA256: manifest.SHA256,
		PartCount:      4,
		TraceCount:     4,
		RowCount:       6,
		CoreBytes:      coreBytes,
		Indexes: map[string]sourcecatalog.ExpectedIndex{
			"latency":    {PartCount: 4, RowCount: 6, Bytes: latencyBytes},
			"start_time": {PartCount: 4, RowCount: 6, Bytes: startTimeBytes},
		},
		Small: sourcecatalog.ExpectedPopulation{
			PartIDs:    []uint64{1},
			TraceCount: 2,
			RowCount:   2,
			BlockCount: coreBlocks[1],
			CoreBytes:  corePartBytes(t, root, 1),
			Carriers: map[uint64]sourcecatalog.ExpectedCarrier{
				2: {TraceCount: 1, RowCount: 1},
			},
		},
		Mature: sourcecatalog.ExpectedPopulation{
			PartIDs:    []uint64{3},
			TraceCount: 2,
			RowCount:   2,
			BlockCount: coreBlocks[3],
			CoreBytes:  corePartBytes(t, root, 3),
			Carriers: map[uint64]sourcecatalog.ExpectedCarrier{
				4: {TraceCount: 1, RowCount: 1},
			},
		},
	}
	output := filepath.Join(filepath.Dir(root), "catalog")
	catalog, buildErr := sourcecatalog.Build(context.Background(), sourcecatalog.Options{
		SourcePath:   root,
		OutputPath:   output,
		Format:       dumptrace.PartFormatCurrent,
		Expectations: expectations,
	})
	require.NoError(t, buildErr)
	assert.Equal(t, manifest.SHA256, catalog.SourceManifestSHA256)
	assert.Equal(t, uint64(4), catalog.Core.TraceCount)
	assert.Equal(t, uint64(6), catalog.Core.RowCount)
	assert.Equal(t, []string{"trace-small-boundary"}, catalog.Small.ClosureTraceIDs)
	assert.Equal(t, []string{"trace-mature-boundary"}, catalog.Mature.ClosureTraceIDs)
	assert.Equal(t, uint64(1), catalog.Small.Carriers[0].RowCount)
	assert.Equal(t, uint64(1), catalog.Mature.Carriers[0].RowCount)
	require.Len(t, catalog.Small.PartTemplates, 1)
	assert.Equal(t, "0000000000000001", catalog.Small.PartTemplates[0].PartID)
	assert.Equal(t, uint64(2), catalog.Small.PartTemplates[0].Rows)
	assert.Equal(t, coreBlocks[1], catalog.Small.PartTemplates[0].Blocks)
	assert.Positive(t, catalog.Small.PartTemplates[0].CompressedCoreBytes)
	assert.Positive(t, catalog.Small.PartTemplates[0].UncompressedSpanBytes)

	for _, name := range []string{"catalog.json", "core-ledger.jsonl", "sidx-latency-ledger.jsonl", "sidx-start_time-ledger.jsonl"} {
		fileInfo, statErr := os.Stat(filepath.Join(output, name))
		require.NoError(t, statErr)
		assert.Positive(t, fileInfo.Size())
	}
	after, afterErr := benchmark.TreeManifest(root)
	require.NoError(t, afterErr)
	assert.Equal(t, manifest, after)
}

func TestBuildRejectsOverlappingSmallAndMatureCatalogs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.MkdirAll(root, 0o755))
	fileSystem := fs.NewLocalFileSystem()
	partRows := map[uint64][]storagetrace.DumpRow{
		1: {{TraceID: "trace-overlap", SpanID: "span-1", Span: []byte("payload-1"), Timestamp: 10}},
		2: {{TraceID: "trace-overlap", SpanID: "span-2", Span: []byte("payload-2"), Timestamp: 20}},
	}
	for partID, rows := range partRows {
		_, _, cleanup := storagetrace.BuildPartForDump(root, fileSystem, partID, rows)
		t.Cleanup(cleanup)
	}
	manifest, manifestErr := benchmark.TreeManifest(root)
	require.NoError(t, manifestErr)
	coreBytes, coreBlocks := coreMetadataTotals(t, root, []uint64{1, 2})
	_, buildErr := sourcecatalog.Build(context.Background(), sourcecatalog.Options{
		SourcePath: root,
		OutputPath: filepath.Join(filepath.Dir(root), "catalog"),
		Format:     dumptrace.PartFormatCurrent,
		Expectations: sourcecatalog.Expectations{
			ManifestSHA256: manifest.SHA256,
			PartCount:      2,
			TraceCount:     1,
			RowCount:       2,
			CoreBytes:      coreBytes,
			Indexes:        map[string]sourcecatalog.ExpectedIndex{},
			Small: sourcecatalog.ExpectedPopulation{
				PartIDs:    []uint64{1},
				TraceCount: 1,
				RowCount:   1,
				BlockCount: coreBlocks[1],
				CoreBytes:  corePartBytes(t, root, 1),
				Carriers:   map[uint64]sourcecatalog.ExpectedCarrier{2: {TraceCount: 1, RowCount: 1}},
			},
			Mature: sourcecatalog.ExpectedPopulation{
				PartIDs:    []uint64{2},
				TraceCount: 1,
				RowCount:   1,
				BlockCount: coreBlocks[2],
				CoreBytes:  corePartBytes(t, root, 2),
				Carriers:   map[uint64]sourcecatalog.ExpectedCarrier{1: {TraceCount: 1, RowCount: 1}},
			},
		},
	})
	require.ErrorContains(t, buildErr, "overlap")
}

func TestBuildRejectsChangedSourceManifest(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "source"), []byte("changed"), 0o600))
	_, buildErr := sourcecatalog.Build(context.Background(), sourcecatalog.Options{
		SourcePath: root,
		OutputPath: filepath.Join(filepath.Dir(root), "catalog"),
		Expectations: sourcecatalog.Expectations{
			ManifestSHA256: "unexpected",
		},
	})
	require.ErrorContains(t, buildErr, "source manifest")
}

func buildIndexParts(t *testing.T, fileSystem fs.FileSystem, root string, partRows map[uint64][]storagetrace.DumpRow) {
	t.Helper()
	options, optionsErr := sidx.NewOptions(root, protector.Nop{})
	require.NoError(t, optionsErr)
	index, indexErr := sidx.NewSIDX(fileSystem, options)
	require.NoError(t, indexErr)
	for partID, rows := range partRows {
		requests := make([]sidx.WriteRequest, 0, len(rows))
		for rowIdx, row := range rows {
			requests = append(requests, sidx.WriteRequest{
				SeriesID: common.SeriesID(rowIdx + 1),
				Key:      row.Timestamp,
				Data:     append([]byte{1}, row.TraceID...),
			})
		}
		memPart, convertErr := index.ConvertToMemPart(requests, 0, nil, nil)
		require.NoError(t, convertErr)
		memPart.MustFlush(fileSystem, filepath.Join(root, formatPartID(partID)))
		sidx.ReleaseMemPart(memPart)
	}
	require.NoError(t, index.Close())
}

func coreMetadataTotals(t *testing.T, root string, partIDs []uint64) (uint64, map[uint64]uint64) {
	t.Helper()
	var totalBytes uint64
	blocks := make(map[uint64]uint64, len(partIDs))
	for _, partID := range partIDs {
		metadata := readCoreMetadata(t, root, partID)
		totalBytes += metadata.CompressedSizeBytes
		blocks[partID] = metadata.BlocksCount
	}
	return totalBytes, blocks
}

func corePartBytes(t *testing.T, root string, partID uint64) uint64 {
	t.Helper()
	return readCoreMetadata(t, root, partID).CompressedSizeBytes
}

func readCoreMetadata(t *testing.T, root string, partID uint64) dumptrace.PartMetadata {
	t.Helper()
	data, readErr := os.ReadFile(filepath.Join(root, formatPartID(partID), "metadata.json"))
	require.NoError(t, readErr)
	var metadata dumptrace.PartMetadata
	require.NoError(t, json.Unmarshal(data, &metadata))
	return metadata
}

func directoryBytes(t *testing.T, root string) uint64 {
	t.Helper()
	manifest, manifestErr := benchmark.TreeManifest(root)
	require.NoError(t, manifestErr)
	return manifest.Bytes
}

func formatPartID(partID uint64) string {
	return fmt.Sprintf("%016x", partID)
}
