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
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
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
	counter         *uint64
	group           string
	irPath          string
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
		sender:          newBatchSender(client, data.TopicMeasureWrite, measureReplayBatchSize, measureReplayBatchTimeout),
		fs:              fileSystem,
		logger:          l,
		measureSchemas:  measureSchemas,
		schemaCache:     make(map[string]*cachedMeasureSchema),
		mergedRuleToTag: deriveMergedRuleToTag(measures, rules, bindings),
		counter:         counter,
	}, nil
}

// Close drains any outstanding batch confirmations, closes the publisher and
// releases cached IndexResolver handles.
func (r *measureRowReplayer) Close() (map[string]*common.Error, error) {
	cee, closeErr := r.sender.close()
	r.irMu.Lock()
	if r.irResolver != nil {
		_ = r.irResolver.Close()
		r.irResolver = nil
		r.irPath = ""
	}
	r.irMu.Unlock()
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

// loadIndexResolver lazily opens an IndexResolver for the current source
// segment. Only one resolver is kept resident: same segmentPath reuses it
// (so all shards/parts under a segment share the bluge reader and avoid
// reopening the same exclusive-locked dir), and a different segmentPath closes
// the prior one before opening a new one. The replayer's Close releases the
// last resolver.
func (r *measureRowReplayer) loadIndexResolver(segmentPath string) (*dump.IndexResolver, error) {
	r.irMu.Lock()
	defer r.irMu.Unlock()
	if r.irResolver != nil && r.irPath == segmentPath {
		return r.irResolver, nil
	}
	if r.irResolver != nil {
		_ = r.irResolver.Close()
		r.irResolver = nil
		r.irPath = ""
	}
	ir, err := dump.NewIndexResolver(segmentPath, dump.DefaultIndexCacheSize, r.mergedRuleToTag)
	if err != nil {
		return nil, fmt.Errorf("open index resolver for %s: %w", segmentPath, err)
	}
	r.irResolver = ir
	r.irPath = segmentPath
	return ir, nil
}

// replayPart opens a source part and replays its rows through the sender, which
// sends and confirms batches through the bounded pipeline. The returned error is
// non-nil when any row build/route, iteration, or batch confirmation failed.
func (r *measureRowReplayer) replayPart(ctx context.Context, partPath string) (int, error) {
	partID, parseErr := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("invalid part path %s: %w", partPath, parseErr)
	}
	shardPath := filepath.Dir(partPath)
	segmentPath := filepath.Dir(shardPath)

	reader, err := dumpmeasure.OpenPart(partID, shardPath, r.fs)
	if err != nil {
		return 0, fmt.Errorf("open part %s: %w", partPath, err)
	}
	defer reader.Close()

	ir, err := r.loadIndexResolver(segmentPath)
	if err != nil {
		return 0, fmt.Errorf("load index resolver for segment %s: %w", segmentPath, err)
	}
	reader.SetIndexResolver(ir)

	it := reader.Iterator()
	defer it.Close()
	return r.sender.replay(ctx, r.logger, r.group, partPath, r.counter, it,
		func() error { return r.publishRow(ctx, ir, it.Row()) })
}

// buildWriteRequest reconstructs the WriteRequest + InternalWriteRequest pair
// from a raw measure Row. Separated from publishRow for testability.
func (r *measureRowReplayer) buildWriteRequest(
	ir *dump.IndexResolver, row dumpmeasure.Row,
) (*measurev1.WriteRequest, *measurev1.InternalWriteRequest, error) {
	subject, evList, err := decodeSeriesEntityValues(row.EntityValues)
	if err != nil {
		return nil, nil, fmt.Errorf("decode entity values (seriesID=%d): %w", row.SeriesID, err)
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

// publishRow rebuilds a WriteRequest from a measure Row and hands it to the
// sender. The returned error is non-nil only on a build/route error or when a
// full batch's asynchronous confirmation surfaced an earlier per-node failure.
func (r *measureRowReplayer) publishRow(ctx context.Context, ir *dump.IndexResolver, row dumpmeasure.Row) error {
	wr, iwr, err := r.buildWriteRequest(ir, row)
	if err != nil {
		return err
	}
	nodeID, err := r.selector.Pick(r.group, wr.Metadata.Name, iwr.ShardId, 0)
	if err != nil {
		return fmt.Errorf("pick target node for %s: %w", wr.Metadata.Name, err)
	}
	msg := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	return r.sender.enqueue(ctx, msg)
}
