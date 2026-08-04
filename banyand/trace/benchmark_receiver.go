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

package trace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const benchmarkTransferChunkSize = 64 * 1024

// BenchmarkPartReceiver exposes the production part-receipt and query paths to the trace merge benchmark.
type BenchmarkPartReceiver struct {
	table      *tsTable
	fileSystem fs.FileSystem
	root       string
}

// BenchmarkConsolidatedSizes reports production-merged compressed sizes without changing the received fixture.
type BenchmarkConsolidatedSizes struct {
	Indexes map[string]uint64
	Core    uint64
}

// NewBenchmarkPartReceiver opens a merge-disabled table for deterministic fixture ingestion.
func NewBenchmarkPartReceiver(root string) (*BenchmarkPartReceiver, error) {
	absoluteRoot, absoluteErr := filepath.Abs(root)
	if absoluteErr != nil {
		return nil, fmt.Errorf("cannot resolve benchmark trace table root %q: %w", root, absoluteErr)
	}
	root = absoluteRoot
	fileSystem := fs.NewLocalFileSystem()
	if mkdirErr := os.MkdirAll(root, 0o755); mkdirErr != nil {
		return nil, fmt.Errorf("cannot create benchmark trace table root %q: %w", root, mkdirErr)
	}
	table, tableErr := newTSTable(fileSystem, root, common.Position{}, logger.GetLogger("trace-benchmark-receiver"), timestamp.TimeRange{}, option{
		flushTimeout: 0,
		mergePolicy:  newMergePolicy(15, 1.7, run.Bytes(0)),
		protector:    protector.Nop{},
	}, nil)
	if tableErr != nil {
		return nil, fmt.Errorf("cannot open benchmark trace table %q: %w", root, tableErr)
	}
	return &BenchmarkPartReceiver{table: table, fileSystem: fileSystem, root: root}, nil
}

// Receive streams index parts followed by one core part through the normal data-node receipt callback.
func (bpr *BenchmarkPartReceiver) Receive(ctx context.Context, corePath string, indexPaths map[string]string) (receiveErr error) {
	partContext := &syncPartContext{tsTable: bpr.table, l: logger.GetLogger("trace-benchmark-part")}
	defer func() {
		if receiveErr != nil {
			receiveErr = errors.Join(receiveErr, partContext.Close())
		}
	}()
	indexNames := make([]string, 0, len(indexPaths))
	for indexName := range indexPaths {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)
	for _, indexName := range indexNames {
		indexPath := indexPaths[indexName]
		metadata, parseErr := sidx.ParsePartMetadata(bpr.fileSystem, indexPath)
		if parseErr != nil {
			return fmt.Errorf("cannot parse benchmark index part %q: %w", indexPath, parseErr)
		}
		chunkContext := &queue.ChunkedSyncPartContext{
			ID: metadata.ID, CompressedSizeBytes: metadata.CompressedSizeBytes, UncompressedSizeBytes: metadata.UncompressedSizeBytes,
			TotalCount: metadata.TotalCount, BlocksCount: metadata.BlocksCount, MinKey: metadata.MinKey, MaxKey: metadata.MaxKey, PartType: indexName,
		}
		if newPartErr := partContext.NewPartType(chunkContext); newPartErr != nil {
			return fmt.Errorf("cannot start benchmark index receipt %q: %w", indexName, newPartErr)
		}
		chunkContext.Handler = partContext
		if transferErr := bpr.transferFiles(ctx, sidx.CreatePartFileReaderFromPath, indexPath, chunkContext); transferErr != nil {
			return fmt.Errorf("cannot receive benchmark index part %q: %w", indexName, transferErr)
		}
	}
	metadata, parseErr := ParsePartMetadata(bpr.fileSystem, corePath)
	if parseErr != nil {
		return fmt.Errorf("cannot parse benchmark core part %q: %w", corePath, parseErr)
	}
	chunkContext := &queue.ChunkedSyncPartContext{
		ID: metadata.ID, CompressedSizeBytes: metadata.CompressedSizeBytes, UncompressedSizeBytes: metadata.UncompressedSizeBytes,
		TotalCount: metadata.TotalCount, BlocksCount: metadata.BlocksCount, MinTimestamp: metadata.MinTimestamp,
		MaxTimestamp: metadata.MaxTimestamp, PartType: PartTypeCore,
	}
	if newPartErr := partContext.NewPartType(chunkContext); newPartErr != nil {
		return fmt.Errorf("cannot start benchmark core receipt: %w", newPartErr)
	}
	chunkContext.Handler = partContext
	if transferErr := bpr.transferFiles(ctx, CreatePartFileReaderFromPath, corePath, chunkContext); transferErr != nil {
		return fmt.Errorf("cannot receive benchmark core part: %w", transferErr)
	}
	if finishErr := partContext.FinishSync(); finishErr != nil {
		return fmt.Errorf("cannot finish benchmark part receipt: %w", finishErr)
	}
	return nil
}

type partFileReader func(string, fs.FileSystem) ([]queue.FileInfo, func())

func (bpr *BenchmarkPartReceiver) transferFiles(ctx context.Context, open partFileReader, path string, chunkContext *queue.ChunkedSyncPartContext) error {
	files, cleanup := open(path, bpr.fileSystem)
	defer cleanup()
	handler := &syncChunkCallback{l: logger.GetLogger("trace-benchmark-transfer")}
	buffer := make([]byte, benchmarkTransferChunkSize)
	for fileIdx := range files {
		fileInfo := &files[fileIdx]
		chunkContext.FileName = fileInfo.Name
		for {
			if contextErr := ctx.Err(); contextErr != nil {
				return fmt.Errorf("benchmark part transfer canceled: %w", contextErr)
			}
			readCount, readErr := fileInfo.Reader.Read(buffer)
			if readCount > 0 {
				if handleErr := handler.HandleFileChunk(chunkContext, buffer[:readCount]); handleErr != nil {
					return fmt.Errorf("cannot handle file %q chunk: %w", fileInfo.Name, handleErr)
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return fmt.Errorf("cannot read file %q: %w", fileInfo.Name, readErr)
			}
		}
	}
	return nil
}

// Reopen closes and reloads the table, exercising persisted core and index snapshots.
func (bpr *BenchmarkPartReceiver) Reopen() error {
	oldTable := bpr.table
	bpr.table = nil
	if oldTable == nil {
		return fmt.Errorf("benchmark table is not open")
	}
	if closeErr := oldTable.Close(); closeErr != nil {
		return fmt.Errorf("cannot close benchmark table before reopen: %w", closeErr)
	}
	table, tableErr := newTSTable(bpr.fileSystem, bpr.root, common.Position{}, logger.GetLogger("trace-benchmark-reopen"), timestamp.TimeRange{}, option{
		flushTimeout: 0,
		mergePolicy:  newMergePolicy(15, 1.7, run.Bytes(0)),
		protector:    protector.Nop{},
	}, nil)
	if tableErr != nil {
		return fmt.Errorf("cannot reopen benchmark table %q: %w", bpr.root, tableErr)
	}
	bpr.table = table
	return nil
}

// ConsolidatedCompressedSizes rewrites the received parts into the requested number of temporary production merge outputs.
func (bpr *BenchmarkPartReceiver) ConsolidatedCompressedSizes(ctx context.Context, targetPartCount int) (BenchmarkConsolidatedSizes, error) {
	if targetPartCount <= 0 {
		return BenchmarkConsolidatedSizes{}, fmt.Errorf("target part count must be positive")
	}
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil || len(snapshot.parts) == 0 {
		return BenchmarkConsolidatedSizes{}, fmt.Errorf("benchmark table has no received parts")
	}
	defer snapshot.decRef()
	parts := append([]*partWrapper(nil), snapshot.parts...)
	sort.Slice(parts, func(leftIdx, rightIdx int) bool { return parts[leftIdx].ID() < parts[rightIdx].ID() })
	targetPartCount = min(targetPartCount, len(parts))
	indexes := bpr.table.getAllSidx()
	result := BenchmarkConsolidatedSizes{Indexes: make(map[string]uint64, len(indexes))}
	for groupIdx := 0; groupIdx < targetPartCount; groupIdx++ {
		if contextErr := ctx.Err(); contextErr != nil {
			return BenchmarkConsolidatedSizes{}, fmt.Errorf("benchmark consolidation canceled: %w", contextErr)
		}
		first := groupIdx * len(parts) / targetPartCount
		last := (groupIdx + 1) * len(parts) / targetPartCount
		group := parts[first:last]
		partIDs := make(map[uint64]struct{}, len(group))
		for _, part := range group {
			partIDs[part.ID()] = struct{}{}
		}
		calibrationPartID := math.MaxUint64 - uint64(groupIdx)
		if _, exists := partIDs[calibrationPartID]; exists {
			return BenchmarkConsolidatedSizes{}, fmt.Errorf("calibration part ID %016x conflicts with received input", calibrationPartID)
		}
		corePart, _, mergeErr := bpr.table.mergeParts(bpr.fileSystem, ctx.Done(), group, calibrationPartID, bpr.root, nil, nil)
		if mergeErr != nil {
			corePath := partPath(bpr.root, calibrationPartID)
			return BenchmarkConsolidatedSizes{}, errors.Join(
				fmt.Errorf("cannot consolidate benchmark core group %d: %w", groupIdx, mergeErr),
				removeCalibrationPath(corePath, "core", groupIdx),
			)
		}
		result.Core += corePart.p.partMetadata.CompressedSizeBytes
		corePath := corePart.p.path
		corePart.decRef()
		if removeErr := os.RemoveAll(corePath); removeErr != nil {
			return BenchmarkConsolidatedSizes{}, fmt.Errorf("cannot remove consolidated benchmark core group %d: %w", groupIdx, removeErr)
		}
		for indexName, index := range indexes {
			introduction, indexMergeErr := index.Merge(ctx.Done(), partIDs, calibrationPartID, nil)
			if indexMergeErr != nil {
				indexPath := sidxPartPath(bpr.root, indexName, calibrationPartID)
				return BenchmarkConsolidatedSizes{}, errors.Join(
					fmt.Errorf("cannot consolidate benchmark index %q group %d: %w", indexName, groupIdx, indexMergeErr),
					removeCalibrationPath(indexPath, "index "+indexName, groupIdx),
				)
			}
			if introduction == nil {
				return BenchmarkConsolidatedSizes{}, fmt.Errorf("benchmark index %q group %d has no matching parts", indexName, groupIdx)
			}
			indexPath := sidxPartPath(bpr.root, indexName, calibrationPartID)
			metadata, metadataErr := sidx.ParsePartMetadata(bpr.fileSystem, indexPath)
			introduction.ReleaseNewPart()
			introduction.Release()
			if metadataErr != nil {
				removeErr := os.RemoveAll(indexPath)
				if removeErr != nil {
					removeErr = fmt.Errorf("cannot remove invalid consolidated benchmark index %q group %d: %w", indexName, groupIdx, removeErr)
				}
				return BenchmarkConsolidatedSizes{}, errors.Join(
					fmt.Errorf("cannot parse consolidated benchmark index %q group %d: %w", indexName, groupIdx, metadataErr),
					removeErr,
				)
			}
			result.Indexes[indexName] += metadata.CompressedSizeBytes
			if removeErr := os.RemoveAll(indexPath); removeErr != nil {
				return BenchmarkConsolidatedSizes{}, fmt.Errorf("cannot remove consolidated benchmark index %q group %d: %w", indexName, groupIdx, removeErr)
			}
		}
	}
	return result, nil
}

func removeCalibrationPath(path, partType string, groupIdx int) error {
	if removeErr := os.RemoveAll(path); removeErr != nil {
		return fmt.Errorf("cannot remove consolidated benchmark %s group %d: %w", partType, groupIdx, removeErr)
	}
	return nil
}

// QueryIndex executes the keyed secondary-index query path.
func (bpr *BenchmarkPartReceiver) QueryIndex(ctx context.Context, name string, request sidx.QueryRequest) ([]*sidx.QueryResponse, error) {
	instance, ok := bpr.table.getSidx(name)
	if !ok {
		return nil, fmt.Errorf("benchmark index %q does not exist", name)
	}
	return instance.QuerySync(ctx, request)
}

// ScanIndex executes the secondary-index full-scan query path.
func (bpr *BenchmarkPartReceiver) ScanIndex(ctx context.Context, name string, request sidx.ScanQueryRequest) ([]*sidx.QueryResponse, error) {
	instance, ok := bpr.table.getSidx(name)
	if !ok {
		return nil, fmt.Errorf("benchmark index %q does not exist", name)
	}
	return instance.ScanQuery(ctx, request)
}

// ScanRawIndex visits every physical secondary-index row without query deduplication.
func (bpr *BenchmarkPartReceiver) ScanRawIndex(ctx context.Context, name string, visit func(sidx.RawRow) error) error {
	instance, ok := bpr.table.getSidx(name)
	if !ok {
		return fmt.Errorf("benchmark index %q does not exist", name)
	}
	return sidx.ScanRaw(ctx, instance, visit)
}

// QueryTrace executes the native trace block query path for one trace ID.
func (bpr *BenchmarkPartReceiver) QueryTrace(ctx context.Context, traceID string, minTimestamp, maxTimestamp int64,
	schemaTagTypes map[string]pbv1.ValueType,
) ([]model.TraceResult, error) {
	snapshot := bpr.table.currentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	defer snapshot.decRef()
	partPointers, _ := snapshot.getParts(nil, minTimestamp, maxTimestamp, []string{traceID})
	metadataArray := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(metadataArray)
	iterator := &tstIter{}
	groupedTraceIDs := make([][]string, len(partPointers))
	for groupIdx := range groupedTraceIDs {
		groupedTraceIDs[groupIdx] = []string{traceID}
	}
	iterator.init(metadataArray, partPointers, groupedTraceIDs)
	queryOpts := queryOptions{schemaTagTypes: schemaTagTypes}
	tagNames := make([]string, 0, len(schemaTagTypes))
	for tagName := range schemaTagTypes {
		tagNames = append(tagNames, tagName)
	}
	sort.Strings(tagNames)
	tagProjection := &model.TagProjection{Names: tagNames}
	var cursors []*blockCursor
	for iterator.nextBlock() {
		cursor := generateBlockCursor()
		partIterator := iterator.piPool[iterator.idx]
		cursorOpts := queryOpts
		cursorOpts.TagProjection = tagProjection
		cursor.init(partIterator.p, partIterator.curBlock, cursorOpts)
		cursors = append(cursors, cursor)
	}
	if iteratorErr := iterator.Error(); iteratorErr != nil {
		for _, cursor := range cursors {
			releaseBlockCursor(cursor)
		}
		return nil, fmt.Errorf("cannot locate benchmark trace %q blocks: %w", traceID, iteratorErr)
	}
	cursorChannel := make(chan scanCursorResult, len(cursors))
	for _, cursor := range cursors {
		cursorChannel <- scanCursorResult{cursor: cursor}
	}
	close(cursorChannel)
	batchChannel := make(chan *scanBatch, 1)
	traceIDMap := map[uint64][]string{0: {traceID}}
	batchChannel <- &scanBatch{traceBatch: traceBatch{
		traceIDs: traceIDMap, traceIDsOrder: []string{traceID}, keys: map[string]int64{traceID: 0},
	}, cursorCh: cursorChannel}
	close(batchChannel)
	result := queryResult{ctx: ctx, tagProjection: tagProjection, cursorBatchCh: batchChannel, keys: map[string]int64{traceID: 0}}
	defer result.Release()
	var traces []model.TraceResult
	for {
		traceResult := result.Pull()
		if traceResult == nil {
			break
		}
		if traceResult.Error != nil {
			return nil, fmt.Errorf("cannot query benchmark trace %q: %w", traceID, traceResult.Error)
		}
		traces = append(traces, *traceResult)
	}
	return traces, nil
}

// Root returns the receiver's trace-table directory.
func (bpr *BenchmarkPartReceiver) Root() string {
	return filepath.Clean(bpr.root)
}

// Close releases the benchmark table.
func (bpr *BenchmarkPartReceiver) Close() error {
	if bpr.table == nil {
		return nil
	}
	closeErr := bpr.table.Close()
	bpr.table = nil
	return closeErr
}
