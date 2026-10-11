// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	banyanfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// lfs performs this builder's own lock-file management. It is stateless, so
// one package-level instance suffices (matching pkg/index/native's own
// package-level fileSystem).
var lfs = banyanfs.NewLocalFileSystem()

// The union sidx is rebuilt read-only via a native owner opened directly on
// stagingPath — no banyandb TSDB/segment lifecycle is needed, just the
// lock-backed root lease every native.Owner requires.
const (
	// Larger batches mean fewer owner admissions, which otherwise dominate
	// CPU at small batch sizes. 50k keeps per-admission overhead off the hot
	// path while still fitting in the per-process heap.
	unionSidxBatchSize = 50000

	segPrefix   = "seg-"
	sidxDirName = "sidx"

	// unionSidxLockFilename is the lock file this builder creates to back
	// its own native.FileRootLease; it is removed once the build finishes.
	unionSidxLockFilename = "lock"

	// Stored field names of sidx (series-index) documents, mirroring the
	// (unexported) reserved names pkg/index/native keeps private, so this
	// builder can re-emit docs read via native.ReadOnlyGeneration.
	sidxDocIDField     = "_id"
	sidxTimestampField = "_timestamp"
	sidxVersionField   = "_version"
)

// BuildGroupUnionSidx walks every srcGroupRoot/seg-*/sidx/ directory
// across every supplied group root, scans every series-index doc,
// deduplicates by SeriesID, and re-emits the surviving docs into a
// fresh series index rooted at stagingPath.
//
// The returned path is stagingPath when at least one doc was written;
// it is "" (without error) when no source sidx contained any doc — the
// caller treats this as "no sidx to broadcast" and skips the per-target
// copy. SeriesID is recovered by unmarshaling the source doc's EntityValues.
// logf, when non-nil, receives progress lines (the scan runs minutes-long
// with no other output on large groups).
func BuildGroupUnionSidx(ctx context.Context, srcGroupRoots []string, stagingPath string, logf func(format string, args ...any)) (string, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if err := os.MkdirAll(stagingPath, storage.DirPerm); err != nil {
		return "", fmt.Errorf("mkdir staging %q: %w", stagingPath, err)
	}

	// native.FileRootLease requires its owner's Path to be a proper
	// subdirectory of the lease root (ValidatePath rejects root==Path), the
	// same relationship a TSDB's lock (at the segment root) and its sidx
	// subdirectory have. stagingPath is the directory this function's
	// contract hands back as the series index itself (callers open it
	// directly), so the lock lives one level up instead.
	leaseRoot := filepath.Dir(stagingPath)
	lockPath := filepath.Join(leaseRoot, unionSidxLockFilename)
	lock, err := lfs.CreateLockFile(lockPath, storage.FilePerm)
	if err != nil {
		return "", fmt.Errorf("create union sidx lock %q: %w", lockPath, err)
	}
	defer func() {
		_ = lock.Close()
		_ = os.Remove(lockPath)
	}()
	lease, err := native.NewFileRootLease(lock, leaseRoot)
	if err != nil {
		return "", fmt.Errorf("create union sidx root lease: %w", err)
	}
	// NewOwner is a synchronous constructor and has no context-bearing API.
	//nolint:contextcheck // construction does not perform cancellable I/O
	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: stagingPath, IdentifierDocValues: true})
	if err != nil {
		return "", fmt.Errorf("open union sidx owner at %q: %w", stagingPath, err)
	}
	closed := false
	published := false
	defer func() {
		if !closed {
			_ = owner.Close()
		}
		if !published {
			_ = os.RemoveAll(stagingPath)
		}
	}()

	// Collect every source sidx directory across all node + segment roots up
	// front so the worker pool can fan out cleanly.
	sidxPaths, pathsErr := collectSourceSidxPaths(ctx, srcGroupRoots)
	if pathsErr != nil {
		return "", pathsErr
	}

	seen := make(map[common.SeriesID]struct{}, 1_000_000)
	var seenMu sync.Mutex
	var writerMu sync.Mutex
	var insertedAtomic, scannedAtomic, doneAtomic atomic.Int64
	var firstErr atomic.Pointer[error]

	// GOMAXPROCS(0) honors the container CPU quota (automaxprocs); NumCPU
	// reports the node's physical cores and would over-fan readers, each
	// holding decompressed stored-field blocks.
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount > len(sidxPaths) {
		workerCount = len(sidxPaths)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	logf("union sidx: scanning %d source sidx dir(s) under %d root(s) with %d worker(s)",
		len(sidxPaths), len(srcGroupRoots), workerCount)

	pathCh := make(chan string)
	var wg sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	workerLogger := logger.GetLogger("migration")
	workerTasks := make([]*run.Task, 0, workerCount)
	recordWorkerPanic := func(panicValue any) {
		panicErr := fmt.Errorf("union sidx source reader panicked: %v", panicValue)
		if firstErr.CompareAndSwap(nil, &panicErr) {
			cancelWorkers()
		}
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		workerTask := run.Go(workerCtx, "union sidx source reader", workerLogger, func(taskCtx context.Context) {
			defer wg.Done()
			defer func() {
				if panicValue := recover(); panicValue != nil {
					recordWorkerPanic(panicValue)
					panic(panicValue)
				}
			}()
			for srcSidxPath := range pathCh {
				if taskCtx.Err() != nil {
					return
				}
				logf("union sidx: start scanning %s", srcSidxPath)
				count, scanned, mergeErr := mergeOneSourceSidxInto(taskCtx, srcSidxPath, owner, seen, &seenMu, &writerMu)
				if mergeErr != nil {
					e := fmt.Errorf("merge %s: %w", srcSidxPath, mergeErr)
					if firstErr.CompareAndSwap(nil, &e) {
						cancelWorkers()
					}
					return
				}
				insertedAtomic.Add(int64(count))
				scannedAtomic.Add(int64(scanned))
				done := doneAtomic.Add(1)
				logf("union sidx: scanned %d/%d sidx dir(s) (%.1f%%): %s",
					done, len(sidxPaths), float64(done)*100/float64(len(sidxPaths)), srcSidxPath)
			}
		})
		workerTasks = append(workerTasks, workerTask)
	}

dispatch:
	for _, srcSidxPath := range sidxPaths {
		select {
		case pathCh <- srcSidxPath:
		case <-workerCtx.Done():
			break dispatch
		}
	}
	close(pathCh)
	wg.Wait()
	for _, workerTask := range workerTasks {
		if outcome := workerTask.Wait(); outcome != nil && outcome.Panicked {
			recordWorkerPanic(outcome.PanicValue)
		}
	}
	if errPtr := firstErr.Load(); errPtr != nil {
		return "", *errPtr
	}
	inserted := int(insertedAtomic.Load())
	scanned := int(scannedAtomic.Load())
	logf("union sidx: scan finished — scanned=%d uniqueSeries=%d dedup-skipped=%d; closing owner (final persist)",
		scanned, inserted, scanned-inserted)

	closed = true
	if closeErr := owner.Close(); closeErr != nil {
		return "", fmt.Errorf("close union sidx owner: %w", closeErr)
	}
	if inserted == 0 {
		return "", nil
	}
	published = true
	return stagingPath, nil
}

func collectSourceSidxPaths(ctx context.Context, srcGroupRoots []string) ([]string, error) {
	var sidxPaths []string
	for _, srcGroupRoot := range srcGroupRoots {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		segmentEntries, readErr := os.ReadDir(srcGroupRoot)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				continue
			}
			return nil, fmt.Errorf("read src group root %q: %w", srcGroupRoot, readErr)
		}
		for _, segmentEntry := range segmentEntries {
			if !segmentEntry.IsDir() || !strings.HasPrefix(segmentEntry.Name(), segPrefix) {
				continue
			}
			sourceSidxPath := filepath.Join(srcGroupRoot, segmentEntry.Name(), sidxDirName)
			info, statErr := os.Stat(sourceSidxPath)
			if statErr != nil || !info.IsDir() {
				continue
			}
			sidxPaths = append(sidxPaths, sourceSidxPath)
		}
	}
	return sidxPaths, nil
}

func mergeOneSourceSidxInto(
	ctx context.Context,
	srcPath string,
	dst *native.Owner,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
	writerMu *sync.Mutex,
) (inserted, scanned int, err error) {
	generation, openErr := native.OpenReadOnlyGeneration(srcPath)
	if openErr != nil {
		if errors.Is(openErr, native.ErrNoSnapshot) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("open source %s: %w", srcPath, openErr)
	}
	defer func() { _ = generation.Close() }()

	batch := make([]native.Document, 0, unionSidxBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		writerMu.Lock()
		defer writerMu.Unlock()
		return dst.Batch(ctx, native.Batch{Documents: batch})
	}

	walkErr := generation.VisitLiveDocuments(ctx, func(source native.StoredDocument) error {
		scanned++

		doc, dup, buildErr := buildNativeDocumentLocked(source, seen, seenMu)
		if buildErr != nil {
			return buildErr
		}
		if dup || doc == nil {
			return nil
		}
		batch = append(batch, *doc)
		inserted++
		if len(batch) >= unionSidxBatchSize {
			if flushErr := flush(); flushErr != nil {
				return fmt.Errorf("flush batch: %w", flushErr)
			}
			batch = make([]native.Document, 0, unionSidxBatchSize)
		}
		return nil
	})
	if walkErr != nil {
		return inserted, scanned, fmt.Errorf("walk source %s: %w", srcPath, walkErr)
	}
	if flushErr := flush(); flushErr != nil {
		return inserted, scanned, fmt.Errorf("flush tail batch: %w", flushErr)
	}
	return inserted, scanned, nil
}

// buildNativeDocumentLocked rebuilds one series-index doc from its stored
// fields, deduplicating by SeriesID under seenMu.
func buildNativeDocumentLocked(
	source native.StoredDocument,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
) (*native.Document, bool, error) {
	var entityValues []byte
	type storedField struct {
		name  string
		value []byte
	}
	var fields []storedField
	visitErr := source.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case sidxDocIDField:
			entityValues = append([]byte(nil), value...)
		default:
			fields = append(fields, storedField{
				name:  field,
				value: append([]byte(nil), value...),
			})
		}
		return true
	})
	if visitErr != nil {
		return nil, false, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	if len(entityValues) == 0 {
		return nil, false, nil
	}
	var series pbv1.Series
	if err := series.Unmarshal(entityValues); err != nil {
		return nil, false, nil
	}
	seenMu.Lock()
	if _, dup := seen[series.ID]; dup {
		seenMu.Unlock()
		return nil, true, nil
	}
	seen[series.ID] = struct{}{}
	seenMu.Unlock()

	doc := &native.Document{Identifier: entityValues}
	for _, f := range fields {
		switch f.name {
		case sidxTimestampField:
			ts, decErr := native.DecodeTimestamp(f.value)
			if decErr != nil {
				return nil, false, fmt.Errorf("decode timestamp on series %d: %w", series.ID, decErr)
			}
			doc.Timestamp = ts
		case sidxVersionField:
			// _version is stored-only (storage.EncodeSeriesDocument never
			// indexes it): Store must be set explicitly here, or the
			// field carries neither an index entry nor a stored value and
			// is silently dropped (NIDX-03 §6.1).
			doc.Fields = append(doc.Fields, native.Field{Name: f.name, Value: f.value, Store: true})
		default:
			// Remaining stored fields are the series' entity-tag fields
			// (e.g. "service"), written by the production series index as
			// indexed, stored, sortable keyword fields
			// (storage.EncodeSeriesDocument). VisitStoredFields only returns
			// the stored name+value, not the original Index/NoSort/Analyzer
			// flags, so they are re-emitted the same way: indexed, stored and
			// sortable, with no analyzer. This preserves exact-match series
			// lookups -- the only way the union sidx is queried for the
			// supported catalogs (entity tags are keyword, not analyzed) --
			// but would NOT preserve a non-keyword analyzer. Such fields
			// don't occur in the current series index; supporting them would
			// require threading the schema's field metadata in.
			doc.Fields = append(doc.Fields, native.Field{Name: f.name, Value: f.value, Index: true, Store: true, Sort: true})
		}
	}
	return doc, false, nil
}
