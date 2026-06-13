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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blugelabs/bluge"
	blugesearch "github.com/blugelabs/bluge/search"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// The union sidx is rebuilt read-only via raw bluge — no banyandb store
// lifecycle is needed.
const (
	// Larger batches mean fewer bluge merger ticks; the merger loop
	// dominates CPU at small batch sizes. 50k keeps the merger work
	// off the hot path while still fitting in the per-process heap.
	unionSidxBatchSize = 50000

	segPrefix   = "seg-"
	sidxDirName = "sidx"

	// Stored bluge field names of sidx (inverted-index) documents, mirroring
	// the (unexported) layout written by pkg/index/inverted so this builder
	// can re-emit docs read via raw bluge.
	sidxDocIDField     = "_id"
	sidxTimestampField = "_timestamp"
	sidxVersionField   = "_version"
)

// BuildGroupUnionSidx walks every srcGroupRoot/seg-*/sidx/ directory
// across every supplied group root, scans every series-index doc,
// deduplicates by SeriesID, and re-emits the surviving docs into a
// fresh bluge index rooted at stagingPath.
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

	writer, err := bluge.OpenWriter(bluge.DefaultConfig(stagingPath))
	if err != nil {
		return "", fmt.Errorf("open union sidx writer at %q: %w", stagingPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()

	// Collect every source sidx directory across all node + segment
	// roots up front so the worker pool can fan out cleanly.
	var sidxPaths []string
	for _, srcGroupRoot := range srcGroupRoots {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		segEntries, err := os.ReadDir(srcGroupRoot)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read src group root %q: %w", srcGroupRoot, err)
		}
		for _, se := range segEntries {
			if !se.IsDir() || !strings.HasPrefix(se.Name(), segPrefix) {
				continue
			}
			srcSidxPath := filepath.Join(srcGroupRoot, se.Name(), sidxDirName)
			info, statErr := os.Stat(srcSidxPath)
			if statErr != nil || !info.IsDir() {
				continue
			}
			sidxPaths = append(sidxPaths, srcSidxPath)
		}
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

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for srcSidxPath := range pathCh {
				if workerCtx.Err() != nil {
					return
				}
				logf("union sidx: start scanning %s", srcSidxPath)
				count, scanned, mergeErr := mergeOneSourceSidxInto(workerCtx, srcSidxPath, writer, seen, &seenMu, &writerMu)
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
		}()
	}
	for _, p := range sidxPaths {
		select {
		case pathCh <- p:
		case <-workerCtx.Done():
		}
	}
	close(pathCh)
	wg.Wait()
	if errPtr := firstErr.Load(); errPtr != nil {
		return "", *errPtr
	}
	inserted := int(insertedAtomic.Load())
	scanned := int(scannedAtomic.Load())
	logf("union sidx: scan finished — scanned=%d uniqueSeries=%d dedup-skipped=%d; closing writer (final bluge merge)",
		scanned, inserted, scanned-inserted)

	closed = true
	if closeErr := writer.Close(); closeErr != nil {
		return "", fmt.Errorf("close union sidx writer: %w", closeErr)
	}
	if inserted == 0 {
		_ = os.RemoveAll(stagingPath)
		return "", nil
	}
	return stagingPath, nil
}

func mergeOneSourceSidxInto(
	ctx context.Context,
	srcPath string,
	dst *bluge.Writer,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
	writerMu *sync.Mutex,
) (inserted, scanned int, err error) {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(srcPath))
	if err != nil {
		// A sidx directory may exist on disk while carrying no committed
		// bluge snapshot — e.g. a fresh segment created by the runtime
		// whose sidx writer never received a doc. Treat that as "no
		// sidx to merge for this seg" instead of aborting the whole
		// per-group union build.
		if strings.Contains(err.Error(), "unable to find a usable snapshot") {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	dmi, err := reader.Search(ctx, bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		return 0, 0, fmt.Errorf("search: %w", err)
	}

	batch := bluge.NewBatch()
	batched := 0

	flush := func() error {
		writerMu.Lock()
		defer writerMu.Unlock()
		return dst.Batch(batch)
	}

	for {
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return inserted, scanned, fmt.Errorf("iterate docs: %w", nextErr)
		}
		if next == nil {
			break
		}
		scanned++

		doc, dup, buildErr := buildDocFromMatchLocked(next, seen, seenMu)
		if buildErr != nil {
			return inserted, scanned, buildErr
		}
		if dup || doc == nil {
			continue
		}
		batch.Insert(doc)
		batched++
		inserted++
		if batched >= unionSidxBatchSize {
			if err := flush(); err != nil {
				return inserted, scanned, fmt.Errorf("flush batch: %w", err)
			}
			batch = bluge.NewBatch()
			batched = 0
		}
	}
	if batched > 0 {
		if err := flush(); err != nil {
			return inserted, scanned, fmt.Errorf("flush tail batch: %w", err)
		}
	}
	return inserted, scanned, nil
}

// buildDocFromMatchLocked rebuilds one series-index doc from its stored
// fields, deduplicating by SeriesID under seenMu.
func buildDocFromMatchLocked(
	match *blugesearch.DocumentMatch,
	seen map[common.SeriesID]struct{},
	seenMu *sync.Mutex,
) (*bluge.Document, bool, error) {
	var entityValues []byte
	type storedField struct {
		name  string
		value []byte
	}
	var fields []storedField
	visitErr := match.VisitStoredFields(func(field string, value []byte) bool {
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

	doc := bluge.NewDocument(string(entityValues))
	for _, f := range fields {
		switch f.name {
		case sidxTimestampField:
			ts, decErr := bluge.DecodeDateTime(f.value)
			if decErr != nil {
				return nil, false, fmt.Errorf("decode timestamp on series %d: %w", series.ID, decErr)
			}
			doc.AddField(bluge.NewDateTimeField(f.name, ts).StoreValue())
		case sidxVersionField:
			doc.AddField(bluge.NewStoredOnlyField(f.name, f.value))
		default:
			// Remaining stored fields are the series' entity-tag fields
			// (e.g. "service"), written by the production series index as
			// keyword fields (pkg/index/inverted/inverted_series.go toDoc).
			// VisitStoredFields only returns the stored name+value, not the
			// original Index/NoSort/Analyzer flags, so they are re-emitted as
			// keyword+stored. This preserves exact-match (keyword) series
			// lookups — the only way the union sidx is queried for the
			// supported catalogs (entity tags are keyword, not analyzed) — but
			// would NOT preserve a non-keyword analyzer or doc-values sorting.
			// Such fields don't occur in the current series index; supporting
			// them would require threading the schema's field metadata in.
			doc.AddField(bluge.NewKeywordFieldBytes(f.name, f.value).StoreValue())
		}
	}
	return doc, false, nil
}
