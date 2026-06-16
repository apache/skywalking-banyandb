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

package lifecycle

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	measureReplayBatchSize    = 2000
	measureReplayBatchTimeout = 30 * time.Second
)

// cachedMeasureSchema bundles a measure's schema with its derived entity and
// sharding-key routers so each row only requires one schema lookup per
// measure. shardingKey is non-nil only when the measure declares a non-empty
// ShardingKey; otherwise the entity-based shardID wins.
type cachedMeasureSchema struct {
	schema        *databasev1.Measure
	shardingKey   partition.Router
	entityLocator partition.Locator
}

// measureRowReplayer replays a group's measure parts row-by-row through the
// real Write API, resolving each row's schema on demand via EntityValues.
type measureRowReplayer struct {
	selector        node.Selector
	archiver        *orphanArchiver
	sender          *batchSender
	fs              fs.FileSystem
	logger          *logger.Logger
	measureSchemas  map[string]*databasev1.Measure
	schemaCache     map[string]*cachedMeasureSchema
	irResolver      *dump.IndexResolver
	mergedRuleToTag map[uint32]dump.IndexedTagSpec
	rebuildIdx      *entityRebuildIndex
	counter         *uint64
	group           string
	irPath          string
	srcStage        string
	rebuildEV       []*modelv1.TagValue
	rebuildSeries   pbv1.Series
	schemaCacheMu   sync.Mutex
	irMu            sync.Mutex
	targetShardNum  uint32
}

// newMeasureRowReplayer constructs a measure replayer, eagerly enumerating the
// group's measures and index rules for IndexResolver decoding.
func newMeasureRowReplayer(
	ctx context.Context,
	group string, targetShardNum uint32,
	selector node.Selector, client queue.Client,
	md metadata.Repo, fileSystem fs.FileSystem,
	l *logger.Logger, counter *uint64, orphanCfg orphanConfig, srcStage string,
) (*measureRowReplayer, error) {
	measures, err := md.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, fmt.Errorf("list measures of group %s: %w", group, err)
	}
	rules, err := md.IndexRuleRegistry().ListIndexRule(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, fmt.Errorf("list index rules of group %s: %w", group, err)
	}
	bindings, err := md.IndexRuleBindingRegistry().ListIndexRuleBinding(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, fmt.Errorf("list index rule bindings of group %s: %w", group, err)
	}
	// Index the already-listed measures by name so loadSchema resolves each
	// subject from this snapshot instead of issuing a redundant GetMeasure
	// per measure. Using one consistent snapshot also avoids mid-migration
	// schema drift.
	measureSchemas := make(map[string]*databasev1.Measure, len(measures))
	for _, m := range measures {
		measureSchemas[m.GetMetadata().GetName()] = m
	}
	return &measureRowReplayer{
		group:           group,
		targetShardNum:  targetShardNum,
		selector:        selector,
		sender:          newBatchSender(client, data.TopicMeasureWrite, rowReplayMaxBatchRows, int(rowReplayMaxBatchBytes), measureReplayBatchTimeout),
		archiver:        newOrphanArchiver(orphanCfg, group, catalogMeasure, l),
		fs:              fileSystem,
		logger:          l,
		measureSchemas:  measureSchemas,
		schemaCache:     make(map[string]*cachedMeasureSchema),
		mergedRuleToTag: deriveMergedRuleToTag(measures, rules, bindings),
		rebuildIdx:      buildEntityRebuildIndex(measureSchemas),
		counter:         counter,
		srcStage:        srcStage,
	}, nil
}

// Close drains any outstanding batch confirmations, closes the publisher and
// releases cached IndexResolver handles.
func (r *measureRowReplayer) Close() (map[string]*common.Error, error) {
	tally := r.logger.Info().
		Str("group", r.group).
		Int("byte_limit_flushes", r.sender.byteLimitFlushes).
		Int("row_limit_flushes", r.sender.rowLimitFlushes).
		Int("tail_flushes", r.sender.tailFlushes).
		Int("skipped_rows", r.sender.skippedRows)
	if r.sender.skippedRows > 0 {
		tally = tally.Bool("incomplete_due_to_sidx_gap", true)
	}
	tally.Msg("row-replay flush-reason tally (byte_limit=hit maxBytes; row_limit=hit maxRows; tail=end-of-part; skipped_rows=unresolvable series dropped)")
	cee, closeErr := r.sender.close()
	closeIndexResolver(&r.irMu, &r.irResolver, &r.irPath)
	return cee, closeErr
}

// loadSchema resolves a measure schema by name from the snapshot listed at
// construction and lazily caches its derived routers.
func (r *measureRowReplayer) loadSchema(measureName string) (*cachedMeasureSchema, error) {
	r.schemaCacheMu.Lock()
	defer r.schemaCacheMu.Unlock()
	if c, ok := r.schemaCache[measureName]; ok {
		return c, nil
	}
	m, ok := r.measureSchemas[measureName]
	if !ok {
		return nil, fmt.Errorf("%w: measure %s/%s", errOrphanSchema, r.group, measureName)
	}
	c := &cachedMeasureSchema{
		schema:        m,
		entityLocator: partition.NewEntityLocator(m.TagFamilies, m.Entity, m.GetMetadata().GetModRevision()),
	}
	if sk := m.GetShardingKey(); sk != nil && len(sk.GetTagNames()) > 0 {
		c.shardingKey = partition.NewShardingKeyLocator(m.TagFamilies, sk)
	}
	r.schemaCache[measureName] = c
	return c, nil
}

// loadIndexResolver lazily opens (and keeps resident) the IndexResolver for the
// current source segment via the shared helper, decoding indexed values with the
// group's merged rule-to-tag map.
func (r *measureRowReplayer) loadIndexResolver(segmentPath string) (*dump.IndexResolver, error) {
	return reloadIndexResolver(&r.irMu, &r.irResolver, &r.irPath, segmentPath, r.mergedRuleToTag)
}

// replayPart opens a source part and replays its rows through the sender, which
// sends and confirms batches through the bounded pipeline. The returned error is
// non-nil when any row build/route, iteration, or batch confirmation failed. The
// result's skipped count and detail are this part's delta from the (replayer-wide)
// sender tallies.
func (r *measureRowReplayer) replayPart(ctx context.Context, partPath string) (partReplayResult, error) {
	partID, shardPath, segmentPath, parseErr := parseReplayPartPath(partPath)
	if parseErr != nil {
		return partReplayResult{}, parseErr
	}

	reader, err := dumpmeasure.OpenPart(partID, shardPath, r.fs)
	if err != nil {
		return partReplayResult{}, fmt.Errorf("open part %s: %w", partPath, err)
	}
	defer reader.Close()

	ir, err := r.loadIndexResolver(segmentPath)
	if err != nil {
		return partReplayResult{}, fmt.Errorf("load index resolver for segment %s: %w", segmentPath, err)
	}
	reader.SetIndexResolver(ir)

	loc := sourceLoc{
		Stage:   r.srcStage,
		Segment: segmentSuffixFromPath(segmentPath),
		Shard:   shardFromPath(shardPath),
		Part:    fmt.Sprintf("%016x", partID),
	}

	// Reuse the dump iterator's file-read/decompress scratch across blocks
	// (transient buffers, decoder owns the decompressed output) to cut churn.
	reader.SetReuseBuffers(true)
	// A3 columnar replay: pull one row at a time but build each proto straight
	// from the shared column view, with per-block constants (entity decode,
	// indexed tags, route) resolved once per block (A2) instead of per row.
	cur := reader.ColumnarIterator()
	defer cur.Close()
	// bc caches per-block constants (A2); pb is a single reusable proto tree the
	// sender marshals to bytes immediately, so one tree serves every row instead
	// of allocating a fresh graph. Both are part-local: parts replay sequentially.
	var bc measureBlockCtx
	bc.partPath = partPath
	var pb measureProtoBuilder
	// runPart opens the part's archive writer and commits/aborts its manifest; the
	// orphan rows are appended to that writer (reached via bc.pw) inside replay.
	var res partReplayResult
	runErr := r.archiver.runPart(loc, func(pw *orphanPartWriter) error {
		bc.pw = pw
		var replayErr error
		res, replayErr = r.sender.replay(ctx, r.logger, r.group, partPath, r.counter, cur,
			func() error { return r.publishRowColumnar(ctx, &bc, &pb, cur.Block(), cur.Index()) })
		return replayErr
	})
	return res, runErr
}

// measureBlockCtx caches the constants that are identical for every row of one
// block (one series): the decoded subject/entity, the schema, the indexed-tag
// and entity TagValue maps, and — when routing does not depend on per-row tags
// (no sharding key) — the resolved shardID, encoded entity values and node.
type measureBlockCtx struct {
	cached         *cachedMeasureSchema
	pw             *orphanPartWriter
	entityIdx      map[string]*modelv1.TagValue
	indexedTyped   map[string]*modelv1.TagValue
	subject        string
	nodeID         string
	partPath       string
	entityEnc      []*modelv1.TagValue
	seriesID       uint64
	orphanSeriesID uint64
	sizeHint       int
	shardID        uint32
	valid          bool
	routeValid     bool
	orphan         bool
}

// ensureBlock recomputes the block-constant context when the series changes.
func (r *measureRowReplayer) ensureBlock(ir *dump.IndexResolver, bc *measureBlockCtx, cb *dumpmeasure.ColumnarBlock) error {
	// Same orphan series as the previous row: the block was already archived once;
	// keep skipping its rows without re-archiving. This consecutive-row guard is
	// sufficient (no cross-block dedup set is needed) because a measure part emits
	// exactly one contiguous block per series, in ascending seriesID order — see
	// banyand/internal/dump/measure/iterator.go, whose blocks are per-series and
	// series-sorted. A given series therefore never reappears in a later block of
	// the same part, so it can never be archived twice. A per-series dedup set would
	// be not only unnecessary but unsafe: a series legitimately never spans two
	// blocks, so any such set could only ever wrongly drop rows.
	if bc.orphan && bc.orphanSeriesID == uint64(cb.SeriesID) {
		return newOrphanSkip(bc.partPath, uint64(cb.SeriesID), bc.subject)
	}
	if bc.valid && bc.seriesID == uint64(cb.SeriesID) {
		return nil
	}
	// Resolve the entity from sidx, falling back to a column rebuild on a sidx gap
	// so row-replay no longer depends on sidx completeness (S4 root fix).
	subject, evList, reason, ok := r.decodeOrRebuildEntity(cb.SeriesID, cb.EntityValues, columnarBlockLookup(cb))
	if !ok {
		return &skipError{partPath: bc.partPath, seriesID: uint64(cb.SeriesID), reason: reason}
	}
	cached, err := r.loadSchema(subject)
	if err != nil {
		if errors.Is(err, errOrphanSchema) {
			// Deleted schema: archive this block's rows once, then skip every row of
			// the series (the source segment is still deleted by the visitor). A
			// failed archive write under the archive policy is FATAL — returning it
			// as-is aborts the part so the source is retained and resume retries,
			// never silently dropping orphan rows.
			if archiveErr := r.archiveOrphanBlock(bc.pw, cb, subject, evList); archiveErr != nil {
				return archiveErr
			}
			bc.valid = false
			bc.orphan = true
			bc.orphanSeriesID = uint64(cb.SeriesID)
			bc.subject = subject
			return newOrphanSkip(bc.partPath, uint64(cb.SeriesID), subject)
		}
		return err
	}
	bc.orphan = false
	bc.valid = true
	bc.seriesID = uint64(cb.SeriesID)
	bc.subject = subject
	bc.cached = cached
	bc.indexedTyped = ir.DecodeTagValues(cb.IndexedTags)
	bc.entityIdx = buildEntityTagIndex(cached.schema.Entity, evList)
	bc.routeValid = false
	bc.sizeHint = 0
	return nil
}

// publishRowColumnar fills the reusable proto tree for row i of cb and enqueues
// it, reusing the block-constant context. Output is byte-identical to publishRow.
func (r *measureRowReplayer) publishRowColumnar(ctx context.Context, bc *measureBlockCtx, pb *measureProtoBuilder, cb *dumpmeasure.ColumnarBlock, i int) error {
	ir, err := r.currentIndexResolver()
	if err != nil {
		return err
	}
	iwr, nodeID, err := r.fillWriteRequestColumnar(ir, bc, pb, cb, i)
	if err != nil {
		return err
	}
	// Rows of a block share one series/schema, so their marshaled size lands in
	// the same size class. Size the first row once and reuse it as the borrow
	// hint for the rest, replacing a per-row proto.Size walk (~10% of replay CPU).
	if bc.sizeHint == 0 {
		bc.sizeHint = proto.Size(iwr)
	}
	return r.sender.enqueueMarshaled(ctx, nodeID, r.group, iwr, bc.sizeHint)
}

// routeColumnar resolves the shardID, encoded entity values and target node for
// the current block. When the measure has no sharding key the entity fully
// determines routing, so it is resolved once per block (cached on bc) and reused
// for every row; otherwise it is recomputed per row from tagFamilies.
func (r *measureRowReplayer) routeColumnar(
	bc *measureBlockCtx, tagFamilies []*modelv1.TagFamilyForWrite,
) (uint32, []*modelv1.TagValue, string, error) {
	if bc.cached.shardingKey == nil {
		if !bc.routeValid {
			tagValues, sid, rErr := partition.ApplyLocators(bc.subject, tagFamilies,
				bc.cached.entityLocator, nil, r.targetShardNum)
			if rErr != nil {
				return 0, nil, "", fmt.Errorf("route %s: %w", bc.subject, rErr)
			}
			pickedNode, pErr := r.selector.Pick(r.group, bc.subject, uint32(sid), 0)
			if pErr != nil {
				return 0, nil, "", fmt.Errorf("pick target node for %s: %w", bc.subject, pErr)
			}
			bc.shardID = uint32(sid)
			bc.entityEnc = tagValues[1:].Encode()
			bc.nodeID = pickedNode
			bc.routeValid = true
		}
		return bc.shardID, bc.entityEnc, bc.nodeID, nil
	}
	tagValues, sid, rErr := partition.ApplyLocators(bc.subject, tagFamilies,
		bc.cached.entityLocator, bc.cached.shardingKey, r.targetShardNum)
	if rErr != nil {
		return 0, nil, "", fmt.Errorf("route %s: %w", bc.subject, rErr)
	}
	nodeID, pErr := r.selector.Pick(r.group, bc.subject, uint32(sid), 0)
	if pErr != nil {
		return 0, nil, "", fmt.Errorf("pick target node for %s: %w", bc.subject, pErr)
	}
	return uint32(sid), tagValues[1:].Encode(), nodeID, nil
}

// currentIndexResolver returns the resolver opened for the part being replayed.
func (r *measureRowReplayer) currentIndexResolver() (*dump.IndexResolver, error) {
	r.irMu.Lock()
	defer r.irMu.Unlock()
	if r.irResolver == nil {
		return nil, fmt.Errorf("index resolver not initialized")
	}
	return r.irResolver, nil
}

// buildWriteRequest reconstructs the WriteRequest + InternalWriteRequest pair
// from a raw measure Row. Separated from publishRow for testability.
func (r *measureRowReplayer) buildWriteRequest(
	ir *dump.IndexResolver, row dumpmeasure.Row,
) (*measurev1.WriteRequest, *measurev1.InternalWriteRequest, error) {
	subject, evList, reason, ok := r.decodeOrRebuildEntity(row.SeriesID, row.EntityValues, rowLookup(row))
	if !ok {
		return nil, nil, &skipError{seriesID: uint64(row.SeriesID), reason: reason}
	}
	cached, err := r.loadSchema(subject)
	if err != nil {
		return nil, nil, err
	}
	indexedTyped := ir.DecodeTagValues(row.IndexedTags)
	tagFamilies := buildMeasureTagFamilies(cached.schema.TagFamilies, cached.schema.Entity, row, evList, indexedTyped)
	fields := buildMeasureFields(cached.schema.Fields, row)

	wr := &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: r.group, Name: subject},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(time.Unix(0, row.Timestamp)),
			Version:     row.Version,
			TagFamilies: tagFamilies,
			Fields:      fields,
		},
		MessageId: uint64(time.Now().UnixNano()),
	}
	tagValues, shardID, err := partition.ApplyLocators(subject, tagFamilies,
		cached.entityLocator, cached.shardingKey, r.targetShardNum)
	if err != nil {
		return nil, nil, fmt.Errorf("route %s: %w", subject, err)
	}
	iwr := &measurev1.InternalWriteRequest{
		ShardId:      uint32(shardID),
		EntityValues: tagValues[1:].Encode(),
		Request:      wr,
	}
	return wr, iwr, nil
}

// segmentSuffixFromPath extracts "20260601" from ".../seg-20260601".
func segmentSuffixFromPath(segmentPath string) string {
	return strings.TrimPrefix(filepath.Base(segmentPath), "seg-")
}

// shardFromPath extracts the shard id from ".../shard-0".
func shardFromPath(shardPath string) uint32 {
	id, _ := strconv.ParseUint(strings.TrimPrefix(filepath.Base(shardPath), "shard-"), 10, 32)
	return uint32(id)
}

// archiveOrphanBlock writes all rows of an orphan series to the part archive
// (a no-op write when policy is discard, which still tallies via the writer).
// It returns a non-nil error when an archive write fails so the caller can treat
// it as fatal (under the archive policy) and retain the source segment.
func (r *measureRowReplayer) archiveOrphanBlock(pw *orphanPartWriter, cb *dumpmeasure.ColumnarBlock, subject string, evList pbv1.EntityValues) error {
	entity := orphanEntityStrings(evList, cb.EntityValues)
	// Indexed tags are block-constant (one series per block), so render them once
	// here instead of per row inside the loop.
	indexedTags := renderIndexedTags(cb.IndexedTags)
	for i := 0; i < cb.Count; i++ {
		rec := &archiveRecord{
			Group: r.group, Catalog: catalogMeasure, Measure: subject,
			Source:    pw.loc,
			SeriesID:  uint64(cb.SeriesID),
			Entity:    entity,
			Timestamp: time.Unix(0, cb.Timestamp(i)).UTC().Format(time.RFC3339Nano),
			TimeNanos: cb.Timestamp(i),
			Version:   cb.Version(i),
		}
		rec.Tags = columnsToTyped(cb.TagCols, cb.TagTypes, i)
		rec.Fields = columnsToTyped(cb.FieldCols, cb.FieldTypes, i)
		rec.IndexedTags = indexedTags
		if err := pw.appendRow(rec); err != nil {
			return fmt.Errorf("archive orphan measure %s row: %w", subject, err)
		}
	}
	return nil
}

// orphanEntityStrings renders an orphan series' entity faithfully from the
// decoded entity values; when evList is nil (decode path produced nothing) it
// falls back to unmarshaling the raw EntityValues bytes.
func orphanEntityStrings(evList pbv1.EntityValues, raw []byte) []string {
	entity := make([]string, 0)
	if evList != nil {
		for _, ev := range evList {
			entity = append(entity, entityValueString(ev))
		}
		return entity
	}
	var s pbv1.Series
	if s.Unmarshal(raw) == nil {
		for _, ev := range s.EntityValues {
			entity = append(entity, entityValueString(ev))
		}
	}
	return entity
}

// columnsToTyped renders row i of named columns to schema-free typed values.
func columnsToTyped(cols map[string][][]byte, types map[string]pbv1.ValueType, i int) map[string]typedValue {
	if len(cols) == 0 {
		return nil
	}
	out := make(map[string]typedValue, len(cols))
	for name, vals := range cols {
		if i < len(vals) {
			out[name] = valueWithType(dump.DecodeTagValue(types[name], vals[i], nil))
		}
	}
	return out
}

// renderIndexedTags renders the block's indexed tags. Indexed-tag raw bytes carry
// no stored value type, so a printable value is emitted as text and a
// non-printable one as "base64:"+encoding to avoid corrupting binary content.
func renderIndexedTags(m map[uint32][][]byte) map[string][]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]string, len(m))
	for ruleID, vals := range m {
		ss := make([]string, 0, len(vals))
		for _, v := range vals {
			if isPrintableBytes(v) {
				ss = append(ss, string(v))
			} else {
				ss = append(ss, "base64:"+base64.StdEncoding.EncodeToString(v))
			}
		}
		out[strconv.FormatUint(uint64(ruleID), 10)] = ss
	}
	return out
}

// isPrintableBytes reports whether b contains only printable ASCII (plus common
// whitespace), mirroring banyand/cmd/dump's isPrintable.
func isPrintableBytes(b []byte) bool {
	for _, c := range b {
		if c < 32 && c != '\n' && c != '\r' && c != '\t' || c > 126 {
			return false
		}
	}
	return true
}
