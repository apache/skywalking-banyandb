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

package stream

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// ── Element-index decision registry (per source seg, shard) ──────────────────.

// streamElementIndexState tracks, for one source (seg, shard), the source shard
// directory and the set of distinct target seg names its rows landed in.
type streamElementIndexState struct {
	targetSegs map[string]struct{}
	shardDir   string
	srcSegName string
	shardName  string
}

// streamElementIndexRegistry collects, per source (seg, shard), which target segs
// received its rows, so the finalize step can decide byte-copy vs rebuild.
type streamElementIndexRegistry struct {
	m  map[string]*streamElementIndexState
	mu sync.Mutex
}

func newStreamElementIndexRegistry() *streamElementIndexRegistry {
	return &streamElementIndexRegistry{m: map[string]*streamElementIndexState{}}
}

// recordTargetSeg notes that a row from source (srcSegName, shardName) landed in
// the given target seg. The registry is keyed by the full source shard dir so two
// source roots (e.g. replica nodes) that share the same seg/shard name stay
// distinct and each gets its own rebuild over the right parts.
func (r *streamElementIndexRegistry) recordTargetSeg(srcSegName, shardName, shardDir, targetSeg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := shardDir
	st, ok := r.m[key]
	if !ok {
		st = &streamElementIndexState{
			shardDir:   shardDir,
			srcSegName: srcSegName,
			shardName:  shardName,
			targetSegs: map[string]struct{}{},
		}
		r.m[key] = st
	}
	st.targetSegs[targetSeg] = struct{}{}
}

func (r *streamElementIndexRegistry) snapshot() []*streamElementIndexState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*streamElementIndexState, 0, len(r.m))
	for _, st := range r.m {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].srcSegName != out[j].srcSegName {
			return out[i].srcSegName < out[j].srcSegName
		}
		return out[i].shardName < out[j].shardName
	})
	return out
}

// ── Finalize: byte-copy or rebuild per source (seg, shard) ───────────────────.

//nolint:govet // internal-only helper, readability > minor padding savings
type finalizeStreamElementIndexInput struct {
	fileSystem    fs.FileSystem
	decoder       *encoding.BytesBlockDecoder
	registry      *streamElementIndexRegistry
	indexLocators map[string]*streamIndexLocator
	entryTag      string
	stage         string
	group         string
	dstGroupRoot  string
	tagProjection []model.TagProjection
	ir            storage.IntervalRule
}

// targetIdxStore lazily opens one inverted store per target idx path
// (BatchWaitSec:0) and closes them all at the end, so multiple source segs feeding
// the same target idx share a single writer.
type targetIdxStore struct {
	stores map[string]index.SeriesStore
}

func newTargetIdxStore() *targetIdxStore {
	return &targetIdxStore{stores: map[string]index.SeriesStore{}}
}

func (t *targetIdxStore) get(path string) (index.SeriesStore, error) {
	if s, ok := t.stores[path]; ok {
		return s, nil
	}
	if err := os.MkdirAll(path, storage.DirPerm); err != nil {
		return nil, fmt.Errorf("mkdir idx %s: %w", path, err)
	}
	s, err := inverted.NewStore(inverted.StoreOpts{Path: path, BatchWaitSec: 0})
	if err != nil {
		return nil, fmt.Errorf("open idx store %s: %w", path, err)
	}
	t.stores[path] = s
	return s, nil
}

func (t *targetIdxStore) closeAll() error {
	var firstErr error
	for path, s := range t.stores {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close idx store %s: %w", path, err)
		}
	}
	t.stores = map[string]index.SeriesStore{}
	return firstErr
}

// finalizeStreamElementIndex resolves every source (seg, shard)'s element index
// into the target tree. Runs sequentially over source (seg, shard).
func finalizeStreamElementIndex(ctx context.Context, in finalizeStreamElementIndexInput) (totalBytes int64, err error) {
	states := in.registry.snapshot()
	if len(states) == 0 {
		return 0, nil
	}
	idxStores := newTargetIdxStore()
	// Close() flushes buffered batches; a close failure means an incomplete
	// index on disk, so it must surface even on the happy path.
	defer func() {
		if cerr := idxStores.closeAll(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	rebuiltTargets := map[string]struct{}{}
	for stateIdx, st := range states {
		if ctx.Err() != nil {
			return totalBytes, ctx.Err()
		}
		progress := fmt.Sprintf("[%d/%d (%.1f%%)]", stateIdx+1, len(states), float64(stateIdx+1)*100/float64(len(states)))
		srcIdxDir := filepath.Join(st.shardDir, elementIndexFilename)
		if info, srcStatErr := os.Stat(srcIdxDir); srcStatErr != nil || !info.IsDir() {
			// No source element index for this (seg, shard) — nothing to do.
			continue
		}
		singleTarget := ""
		if len(st.targetSegs) == 1 {
			for s := range st.targetSegs {
				singleTarget = s
			}
		}
		// Byte-copy only when this source (seg, shard) maps to a single target seg
		// AND no other source seg already wrote that target idx (neither a prior
		// byte-copy nor a rebuild).
		if singleTarget != "" {
			dstIdxDir := filepath.Join(in.dstGroupRoot, singleTarget, st.shardName, elementIndexFilename)
			_, statErr := os.Stat(dstIdxDir)
			existsOnDisk := statErr == nil
			_, alreadyRebuilt := rebuiltTargets[dstIdxDir]
			if existsOnDisk || alreadyRebuilt {
				logStreamStep("%s stage=%s group %s: idx %s %s/%s -> rebuild (target seg %s idx already written by another source shard)",
					in.entryTag, in.stage, in.group, progress, st.srcSegName, st.shardName, singleTarget)
				rb, rbErr := rebuildStreamElementIndexForState(ctx, in, st, idxStores, rebuiltTargets)
				if rbErr != nil {
					return totalBytes, rbErr
				}
				totalBytes += rb
				continue
			}
			logStreamStep("%s stage=%s group %s: idx %s %s/%s -> byte-copy into seg %s",
				in.entryTag, in.stage, in.group, progress, st.srcSegName, st.shardName, singleTarget)
			n, copyErr := migration.CopyDir(srcIdxDir, dstIdxDir)
			if copyErr != nil {
				return totalBytes, fmt.Errorf("byte-copy idx %s -> %s: %w", srcIdxDir, dstIdxDir, copyErr)
			}
			totalBytes += n
			continue
		}
		// Rows landed in >1 target seg: rebuild per target seg from rows.
		logStreamStep("%s stage=%s group %s: idx %s %s/%s -> rebuild from rows into %d target seg(s)",
			in.entryTag, in.stage, in.group, progress, st.srcSegName, st.shardName, len(st.targetSegs))
		rb, rbErr := rebuildStreamElementIndexForState(ctx, in, st, idxStores, rebuiltTargets)
		if rbErr != nil {
			return totalBytes, rbErr
		}
		totalBytes += rb
	}
	return totalBytes, nil
}

// resolveStreamIndexLocatorsForGroup returns the per-stream locator map for the
// group. A shard's element index mixes docs from every stream of the group; the
// offline rebuild therefore resolves each row's owning stream from its seriesID
// (see rebuildStreamElementIndexFromRows). The only hard failure is having no
// locators at all, since then no row can be indexed.
func resolveStreamIndexLocatorsForGroup(group string, locators map[string]*streamIndexLocator) (map[string]*streamIndexLocator, error) {
	if len(locators) == 0 {
		return nil, fmt.Errorf("group %s: no stream index locators loaded; cannot rebuild element index", group)
	}
	return locators, nil
}

// rebuildStreamElementIndexForState decodes the source parts of one (seg, shard),
// regenerates element index docs from rows + index rules, routes each doc to its
// target seg by timestamp and writes via the shared idx stores.
func rebuildStreamElementIndexForState(ctx context.Context, in finalizeStreamElementIndexInput,
	st *streamElementIndexState, idxStores *targetIdxStore, rebuiltTargets map[string]struct{},
) (int64, error) {
	locators, err := resolveStreamIndexLocatorsForGroup(in.group, in.indexLocators)
	if err != nil {
		return 0, err
	}
	return rebuildStreamElementIndexFromRows(ctx, rebuildStreamElementIndexInput{
		ir:             in.ir,
		decoder:        in.decoder,
		fileSystem:     in.fileSystem,
		tagProjection:  in.tagProjection,
		entryTag:       in.entryTag,
		stage:          in.stage,
		group:          in.group,
		srcSegName:     st.srcSegName,
		shardDir:       st.shardDir,
		shardName:      st.shardName,
		dstGroupRoot:   in.dstGroupRoot,
		locators:       locators,
		idxStores:      idxStores,
		rebuiltTargets: rebuiltTargets,
	})
}

// ── Row-level rebuild (never reads old idx) ──────────────────────────────────.

//nolint:govet // internal-only helper, readability > minor padding savings
type rebuildStreamElementIndexInput struct {
	fileSystem     fs.FileSystem
	decoder        *encoding.BytesBlockDecoder
	locators       map[string]*streamIndexLocator
	idxStores      *targetIdxStore
	rebuiltTargets map[string]struct{}
	entryTag       string
	stage          string
	group          string
	srcSegName     string
	shardDir       string
	shardName      string
	dstGroupRoot   string
	tagProjection  []model.TagProjection
	ir             storage.IntervalRule
}

const (
	// streamRebuildIdxBatchSize bounds one bluge batch. Smaller batches cap
	// the per-worker ice segment-build working set (~0.65GB at 50k docs);
	// the extra segments are compacted by banyandb's own merger once the
	// migrated target serves.
	streamRebuildIdxBatchSize = 25_000
	// streamRebuildIdxFlushWorkers bounds concurrent store.Batch calls.
	// bluge's Writer.Batch is safe for concurrent use: analysis runs on a
	// shared worker pool, the ice segment build runs on the calling
	// goroutine (this is the parallelism we're after — it dominated the
	// rebuild at 78% CPU single-threaded), and segment introduction is
	// serialized internally.
	streamRebuildIdxFlushWorkers = 4
)

// streamRowLocator is the per-stream rebuild context resolved once per stream
// name: the stream's index locator plus the 0-based entity-tag ordinal map used
// to recover entity-tag values from a row's decoded EntityValues.
type streamRowLocator struct {
	locator            *streamIndexLocator
	entityTagToOrdinal map[string]int
}

// streamSeriesResolution maps each seriesID found in the shard's parts to its
// owning stream's rebuild context and the decoded entity-tag values. It is built
// from the source segment's series index when the group has multiple streams or
// any stream indexes an entity tag.
type streamSeriesResolution struct {
	byLocator    map[common.SeriesID]*streamRowLocator
	entityValues map[common.SeriesID][]*modelv1.TagValue
	missing      int
}

func rebuildStreamElementIndexFromRows(ctx context.Context, in rebuildStreamElementIndexInput) (int64, error) {
	if len(in.locators) == 0 {
		return 0, fmt.Errorf("no index locators for shard %s", in.shardName)
	}
	partIDs, err := discoverStreamShardPartIDs(in.shardDir)
	if err != nil {
		return 0, err
	}
	if len(partIDs) == 0 {
		return 0, nil
	}

	multiStream := len(in.locators) > 1
	rowLocators := make(map[string]*streamRowLocator, len(in.locators))
	needEntityResolution := false
	var singleLocator *streamRowLocator
	for name, locator := range in.locators {
		rl := &streamRowLocator{locator: locator, entityTagToOrdinal: buildStreamEntityOrdinalMap(locator)}
		rowLocators[name] = rl
		singleLocator = rl
		if locator.HasEntityIndexRule() {
			needEntityResolution = true
		}
	}

	// Resolution via the source segment's series index is required when the group
	// holds multiple streams (each row's owning stream is unknown from the row
	// alone) or when an index rule references an entity tag (the value lives only
	// in the series index, not in the row's columns).
	var resolver *dump.IndexResolver
	if multiStream || needEntityResolution {
		srcSegPath := filepath.Dir(in.shardDir)
		resolver, err = dump.NewIndexResolver(srcSegPath, dump.DefaultIndexCacheSize, nil)
		if err != nil {
			return 0, fmt.Errorf("open index resolver for %s: %w", srcSegPath, err)
		}
		defer func() {
			if cerr := resolver.Close(); cerr != nil {
				logStreamStep("warning: close index resolver for %s: %v", srcSegPath, cerr)
			}
		}()
	}

	pending := map[string]index.Documents{}
	var segCache streamAlignedSegCache
	// WithContext lets a failed Batch cancel flushCtx so the part loop stops
	// decoding instead of feeding more batches to a doomed rebuild.
	flushG, flushCtx := errgroup.WithContext(ctx)
	flushG.SetLimit(streamRebuildIdxFlushWorkers)

	// flush hands full batches to the worker pool and keeps decoding; Go()
	// blocks once streamRebuildIdxFlushWorkers batches are in flight, which
	// doubles as back-pressure on memory. Stores are resolved on this
	// goroutine (idxStores is not goroutine-safe); concurrent Batch calls on
	// the resolved stores are safe (see streamRebuildIdxFlushWorkers).
	flush := func(force bool) error {
		for path, docs := range pending {
			if len(docs) == 0 {
				continue
			}
			if !force && len(docs) < streamRebuildIdxBatchSize {
				continue
			}
			store, getErr := in.idxStores.get(path)
			if getErr != nil {
				return getErr
			}
			if in.rebuiltTargets != nil {
				in.rebuiltTargets[path] = struct{}{}
			}
			batchDocs := docs
			batchPath := path
			flushG.Go(func() error {
				if batchErr := store.Batch(index.Batch{Documents: batchDocs}); batchErr != nil {
					return fmt.Errorf("batch idx %s: %w", batchPath, batchErr)
				}
				return nil
			})
			// Ownership of the docs slice moved to the worker; the next
			// batch for this path starts from a fresh slice.
			pending[path] = nil
		}
		return nil
	}

	missingRows, missingSeries := 0, 0
	for partIdx, partID := range partIDs {
		if flushCtx.Err() != nil {
			// Either the parent ctx was canceled or a flush worker failed;
			// Wait() surfaces the worker error when there is one.
			if waitErr := flushG.Wait(); waitErr != nil {
				return 0, waitErr
			}
			return 0, ctx.Err()
		}
		partMissingRows, partMissingSeries, procErr := rebuildOneStreamPartIntoDocs(rebuildOnePartInput{ //nolint:contextcheck // PartSeriesMap does only local file reads.
			partID:        partID,
			in:            in,
			resolver:      resolver,
			multiStream:   multiStream,
			rowLocators:   rowLocators,
			singleLocator: singleLocator,
			segCache:      &segCache,
			pending:       pending,
			flush:         flush,
		})
		if procErr != nil {
			_ = flushG.Wait()
			return 0, procErr
		}
		missingRows += partMissingRows
		missingSeries += partMissingSeries
		logStreamStep("%s stage=%s group %s: idx %s/%s rebuild part %d/%d (%.1f%%) done",
			in.entryTag, in.stage, in.group, in.srcSegName, in.shardName,
			partIdx+1, len(partIDs), float64(partIdx+1)*100/float64(len(partIDs)))
	}
	if err := flush(true); err != nil {
		_ = flushG.Wait()
		return 0, err
	}
	if err := flushG.Wait(); err != nil {
		return 0, err
	}
	if missingRows > 0 || missingSeries > 0 {
		// Rows whose seriesID has no doc in the source segment's series index
		// cannot be re-indexed (their owning stream / entity values are
		// unrecoverable); the row data itself is still copied. Observed on
		// lifecycle-produced warm segments whose sidx omitted a few series.
		logger.GetLogger("stream-migration").Warn().
			Str("stage", in.stage).Str("group", in.group).Str("seg", in.srcSegName).Str("shard", in.shardName).
			Int("rows", missingRows).Int("series", missingSeries).
			Msg("element-index rebuild skipped rows whose seriesID resolved to no stream locator")
	}
	// Stores are closed by the caller (shared across source segs); the bluge
	// segment size is not known until close, so report 0 bytes here.
	return 0, nil
}

//nolint:govet // internal-only helper
type rebuildOnePartInput struct {
	resolver      *dump.IndexResolver
	rowLocators   map[string]*streamRowLocator
	singleLocator *streamRowLocator
	segCache      *streamAlignedSegCache
	pending       map[string]index.Documents
	flush         func(force bool) error
	in            rebuildStreamElementIndexInput
	partID        uint64
	multiStream   bool
}

// resolvePartStreams maps the part's seriesIDs to their owning stream's rebuild
// context plus decoded entity-tag values by scanning the source series index once
// and matching each series' Subject to a stream locator.
func resolvePartStreams(r *rebuildOnePartInput, p *part) (*streamSeriesResolution, error) {
	seriesIDs, err := collectStreamPartSeriesIDs(p)
	if err != nil {
		return nil, fmt.Errorf("collect seriesIDs for part %016x: %w", r.partID, err)
	}
	entityValuesBySeries, err := r.resolver.PartSeriesMap(seriesIDs) //nolint:contextcheck // PartSeriesMap performs only local file reads; no ctx in its signature.
	if err != nil {
		return nil, fmt.Errorf("resolve series for part %016x: %w", r.partID, err)
	}
	res := &streamSeriesResolution{
		byLocator:    make(map[common.SeriesID]*streamRowLocator, len(entityValuesBySeries)),
		entityValues: make(map[common.SeriesID][]*modelv1.TagValue, len(entityValuesBySeries)),
	}
	for seriesID, buf := range entityValuesBySeries {
		var series pbv1.Series
		if uErr := series.Unmarshal(buf); uErr != nil {
			res.missing++
			continue
		}
		rl := r.rowLocators[series.Subject]
		if rl == nil {
			// Single-stream group: the row's seriesID may resolve before the
			// Subject is known to match; fall back to the only locator.
			if !r.multiStream {
				rl = r.singleLocator
			}
		}
		if rl == nil {
			res.missing++
			continue
		}
		res.byLocator[seriesID] = rl
		res.entityValues[seriesID] = series.EntityValues
	}
	return res, nil
}

// rebuildOneStreamPartIntoDocs decodes one part's rows into element-index docs
// and reports how many rows and distinct series could not be resolved.
func rebuildOneStreamPartIntoDocs(r rebuildOnePartInput) (missingRows, missingSeries int, err error) {
	in := r.in
	p := mustOpenFilePart(r.partID, in.shardDir, in.fileSystem)
	defer p.close()

	var resolution *streamSeriesResolution
	if r.resolver != nil {
		var resErr error
		resolution, resErr = resolvePartStreams(&r, p)
		if resErr != nil {
			return 0, 0, resErr
		}
	}

	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)
	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		compressed = bytes.ResizeOver(compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressed)
		var decErr error
		raw, decErr = zstd.Decompress(raw[:0], compressed)
		if decErr != nil {
			return 0, 0, fmt.Errorf("decompress primary block: %w", decErr)
		}
		bms, decErr = unmarshalBlockMetadata(bms[:0], raw)
		if decErr != nil {
			return 0, 0, fmt.Errorf("unmarshal block metadata: %w", decErr)
		}
		for j := range bms {
			bm := &bms[j]
			rl := r.singleLocator
			var entityTagValues []*modelv1.TagValue
			if resolution != nil {
				rl = resolution.byLocator[bm.seriesID]
				entityTagValues = resolution.entityValues[bm.seriesID]
			}
			if rl == nil {
				missingRows += int(bm.count)
				continue
			}
			bm.tagProjection = in.tagProjection
			b := generateBlock()
			b.mustReadFrom(in.decoder, p, *bm)
			for k := uint64(0); k < bm.count; k++ {
				appendStreamElementIndexDoc(streamElementIndexRowInput{
					in:              in,
					b:               b,
					rowLocator:      rl,
					seriesID:        bm.seriesID,
					k:               k,
					entityTagValues: entityTagValues,
					segCache:        r.segCache,
					pending:         r.pending,
				})
			}
			releaseBlock(b)
			// All rows of this block are now self-contained docs in pending
			// (string/binary tag values are copied during decode), so the
			// decoder's internal buffer can be reclaimed. Without this the
			// buffer accumulates every block of every part (2.4GB observed
			// on a 308-part group).
			in.decoder.Reset()
			if flushErr := r.flush(false); flushErr != nil {
				return 0, 0, flushErr
			}
		}
	}
	if resolution != nil {
		missingSeries = resolution.missing
	}
	return missingRows, missingSeries, nil
}

//nolint:govet // internal-only helper
type streamElementIndexRowInput struct {
	b               *block
	rowLocator      *streamRowLocator
	segCache        *streamAlignedSegCache
	pending         map[string]index.Documents
	entityTagValues []*modelv1.TagValue
	in              rebuildStreamElementIndexInput
	seriesID        common.SeriesID
	k               uint64
}

// appendStreamElementIndexDoc builds the element index doc for one row, mirroring
// the write path (processElements + appendField), and routes it to the target
// seg's idx path by timestamp.
func appendStreamElementIndexDoc(r streamElementIndexRowInput) {
	in := r.in
	ts := r.b.timestamps[r.k]
	elementID := r.b.elementIDs[r.k]

	entityTagValues := r.entityTagValues
	entityTagToOrdinal := r.rowLocator.entityTagToOrdinal

	var fields []index.Field
	locator := r.rowLocator.locator
	for fi := range locator.Families {
		fam := locator.Families[fi]
		if fi >= len(locator.Locators.TagFamilyTRule) {
			break
		}
		tfr := locator.Locators.TagFamilyTRule[fi]
		for ti := range fam.Tags {
			tagSpec := fam.Tags[ti]
			rule, ok := tfr[tagSpec.GetName()]
			if !ok || rule.GetType() != databasev1.IndexRule_TYPE_INVERTED {
				continue
			}
			tagVal := pbv1.NullTagValue
			if ord, isEntity := entityTagToOrdinal[tagSpec.GetName()]; isEntity {
				if ord >= 0 && ord < len(entityTagValues) {
					tagVal = entityTagValues[ord]
				}
			} else if rawVal, found := streamColumnValue(r.b, fam.GetName(), tagSpec.GetName(), r.k); found {
				tagVal = dump.DecodeTagValue(streamTagTypeToValueType(tagSpec.GetType()), rawVal, nil)
			}
			if tagVal == nil || tagVal == pbv1.NullTagValue {
				continue
			}
			fields = appendField(fields, index.FieldKey{
				IndexRuleID: rule.GetMetadata().GetId(),
				Analyzer:    rule.GetAnalyzer(),
				SeriesID:    r.seriesID,
			}, tagSpec.GetType(), tagVal, rule.GetNoSort())
		}
	}
	if len(fields) == 0 {
		return
	}

	targetSeg := r.segCache.segNameFor(in.ir, ts)
	idxPath := filepath.Join(in.dstGroupRoot, targetSeg, in.shardName, elementIndexFilename)
	r.pending[idxPath] = append(r.pending[idxPath], index.Document{
		DocID:     elementID,
		Fields:    fields,
		Timestamp: ts,
	})
}

// ── Helpers ──────────────────────────────────────────────────.

// streamColumnValue returns the raw column-stored value for (family, tag) at row k.
func streamColumnValue(b *block, family, tagName string, k uint64) ([]byte, bool) {
	for fi := range b.tagFamilies {
		cf := &b.tagFamilies[fi]
		if cf.name != family {
			continue
		}
		for ci := range cf.tags {
			c := &cf.tags[ci]
			if c.name != tagName {
				continue
			}
			if uint64(len(c.values)) > k {
				return c.values[k], true
			}
			return nil, true
		}
	}
	return nil, false
}

// buildStreamEntityOrdinalMap maps each entity tag name to its 0-based ordinal in
// the entity's EntityValues, returning nil when no index rule references an entity tag.
func buildStreamEntityOrdinalMap(locator *streamIndexLocator) map[string]int {
	if !locator.HasEntityIndexRule() {
		return nil
	}
	out := make(map[string]int, len(locator.Locators.EntitySet))
	for name, oneBased := range locator.Locators.EntitySet {
		out[name] = oneBased - 1
	}
	return out
}

func discoverStreamShardPartIDs(shardDir string) ([]uint64, error) {
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, fmt.Errorf("read shard %s: %w", shardDir, err)
	}
	var partIDs []uint64
	for _, e := range entries {
		if !e.IsDir() || !directStreamCopyPartDirPattern.MatchString(e.Name()) {
			continue
		}
		id, parseErr := strconv.ParseUint(e.Name(), 16, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse partID %s: %w", e.Name(), parseErr)
		}
		partIDs = append(partIDs, id)
	}
	sort.Slice(partIDs, func(i, j int) bool { return partIDs[i] < partIDs[j] })
	return partIDs, nil
}

// collectStreamPartSeriesIDs reads block metadata of a part and returns its set of
// distinct seriesIDs (used to scope entity resolution). A decode failure must
// surface: a partial set would silently drop element-index docs for the
// unresolved series while their rows are still copied.
func collectStreamPartSeriesIDs(p *part) (map[common.SeriesID]struct{}, error) {
	out := map[common.SeriesID]struct{}{}
	var (
		compressed []byte
		raw        []byte
		bms        []blockMetadata
	)
	for i := range p.primaryBlockMetadata {
		pbm := &p.primaryBlockMetadata[i]
		compressed = bytes.ResizeOver(compressed, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressed)
		var err error
		raw, err = zstd.Decompress(raw[:0], compressed)
		if err != nil {
			return nil, fmt.Errorf("decompress primary block metadata: %w", err)
		}
		bms, err = unmarshalBlockMetadata(bms[:0], raw)
		if err != nil {
			return nil, fmt.Errorf("unmarshal block metadata: %w", err)
		}
		for j := range bms {
			out[bms[j].seriesID] = struct{}{}
		}
	}
	return out, nil
}

// streamTagTypeToValueType maps the schema TagType to the storage ValueType used
// by the column encoder, so DecodeTagValue picks the matching decoder.
func streamTagTypeToValueType(tagType databasev1.TagType) pbv1.ValueType {
	switch tagType {
	case databasev1.TagType_TAG_TYPE_INT:
		return pbv1.ValueTypeInt64
	case databasev1.TagType_TAG_TYPE_STRING:
		return pbv1.ValueTypeStr
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return pbv1.ValueTypeBinaryData
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return pbv1.ValueTypeInt64Arr
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return pbv1.ValueTypeStrArr
	default:
		return pbv1.ValueTypeStr
	}
}
