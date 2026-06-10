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
	"fmt"
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
	l *logger.Logger, counter *uint64,
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
		fs:              fileSystem,
		logger:          l,
		measureSchemas:  measureSchemas,
		schemaCache:     make(map[string]*cachedMeasureSchema),
		mergedRuleToTag: deriveMergedRuleToTag(measures, rules, bindings),
		rebuildIdx:      buildEntityRebuildIndex(measureSchemas),
		counter:         counter,
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
		return nil, fmt.Errorf("measure schema %s/%s not found in group snapshot", r.group, measureName)
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

// partReplayResult reports what one part's replay delivered: the rows published,
// the rows skipped because their series could not be resolved or rebuilt
// (errSkipSeries), and a bounded sample of those skips' locations for the report.
// A skipped > 0 result means the source part still holds data the migration could
// not republish, so its source segment must be retained rather than deleted.
type partReplayResult struct {
	detail  []skipError
	rows    int
	skipped int
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
	// The sender's skip tallies are replayer-wide (one sender serves every part),
	// so snapshot them around this part to recover its own skipped delta.
	beforeRows := r.sender.skippedRows
	beforeDetail := len(r.sender.skippedDetail)
	rowCount, replayErr := r.sender.replay(ctx, r.logger, r.group, partPath, r.counter, cur,
		func() error { return r.publishRowColumnar(ctx, &bc, &pb, cur.Block(), cur.Index()) })
	res := partReplayResult{rows: rowCount, skipped: r.sender.skippedRows - beforeRows}
	if d := r.sender.skippedDetail[beforeDetail:]; len(d) > 0 {
		res.detail = append([]skipError(nil), d...)
	}
	return res, replayErr
}

// measureBlockCtx caches the constants that are identical for every row of one
// block (one series): the decoded subject/entity, the schema, the indexed-tag
// and entity TagValue maps, and — when routing does not depend on per-row tags
// (no sharding key) — the resolved shardID, encoded entity values and node.
type measureBlockCtx struct {
	cached       *cachedMeasureSchema
	entityIdx    map[string]*modelv1.TagValue
	indexedTyped map[string]*modelv1.TagValue
	subject      string
	nodeID       string
	partPath     string
	entityEnc    []*modelv1.TagValue
	seriesID     uint64
	sizeHint     int
	shardID      uint32
	valid        bool
	routeValid   bool
}

// ensureBlock recomputes the block-constant context when the series changes.
func (r *measureRowReplayer) ensureBlock(ir *dump.IndexResolver, bc *measureBlockCtx, cb *dumpmeasure.ColumnarBlock) error {
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
		return err
	}
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
