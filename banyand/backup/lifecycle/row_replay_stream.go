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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

const (
	streamReplayBatchSize    = 2000
	streamReplayBatchTimeout = 30 * time.Second
)

type cachedStreamSchema struct {
	schema        *databasev1.Stream
	entityLocator partition.Locator
}

// streamRowReplayer replays a group's stream parts row-by-row.
type streamRowReplayer struct {
	selector       node.Selector
	sender         *batchSender
	metadata       metadata.Repo
	fs             fs.FileSystem
	logger         *logger.Logger
	schemaCache    map[string]*cachedStreamSchema
	irResolver     *dump.IndexResolver
	counter        *uint64
	group          string
	irPath         string
	schemaCacheMu  sync.Mutex
	irMu           sync.Mutex
	targetShardNum uint32
}

func newStreamRowReplayer(
	group string, targetShardNum uint32,
	selector node.Selector, client queue.Client,
	md metadata.Repo, fileSystem fs.FileSystem,
	l *logger.Logger, counter *uint64,
) *streamRowReplayer {
	return &streamRowReplayer{
		group:          group,
		targetShardNum: targetShardNum,
		selector:       selector,
		sender:         newBatchSender(client, data.TopicStreamWrite, streamReplayBatchSize, streamReplayBatchTimeout),
		metadata:       md,
		fs:             fileSystem,
		logger:         l,
		schemaCache:    make(map[string]*cachedStreamSchema),
		counter:        counter,
	}
}

// Close drains any outstanding batch confirmations, closes the publisher and
// releases cached IndexResolver handles.
func (r *streamRowReplayer) Close() (map[string]*common.Error, error) {
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

// loadIndexResolver lazily opens an IndexResolver for the current source
// segment. Only one resolver is kept resident: same segmentPath reuses it
// (so all shards/parts under a segment share the bluge reader and avoid
// reopening the same exclusive-locked dir), and a different segmentPath closes
// the prior one before opening a new one. The replayer's Close releases the
// last resolver.
func (r *streamRowReplayer) loadIndexResolver(segmentPath string) (*dump.IndexResolver, error) {
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
	ir, err := dump.NewIndexResolver(segmentPath, dump.DefaultIndexCacheSize, nil)
	if err != nil {
		return nil, fmt.Errorf("open stream index resolver for %s: %w", segmentPath, err)
	}
	r.irResolver = ir
	r.irPath = segmentPath
	return ir, nil
}

func (r *streamRowReplayer) loadSchema(ctx context.Context, streamName string) (*cachedStreamSchema, error) {
	r.schemaCacheMu.Lock()
	defer r.schemaCacheMu.Unlock()
	if c, ok := r.schemaCache[streamName]; ok {
		return c, nil
	}
	s, err := r.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{Group: r.group, Name: streamName})
	if err != nil {
		return nil, fmt.Errorf("load stream schema %s/%s: %w", r.group, streamName, err)
	}
	c := &cachedStreamSchema{
		schema:        s,
		entityLocator: partition.NewEntityLocator(s.TagFamilies, s.Entity, s.GetMetadata().GetModRevision()),
	}
	r.schemaCache[streamName] = c
	return c, nil
}

func (r *streamRowReplayer) replayPart(ctx context.Context, partPath string) (int, error) {
	partID, parseErr := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("invalid part path %s: %w", partPath, parseErr)
	}
	shardPath := filepath.Dir(partPath)
	segmentPath := filepath.Dir(shardPath)

	reader, err := dumpstream.OpenPart(partID, shardPath, r.fs)
	if err != nil {
		return 0, fmt.Errorf("open stream part %s: %w", partPath, err)
	}
	defer reader.Close()

	ir, irErr := r.loadIndexResolver(segmentPath)
	if irErr != nil {
		return 0, fmt.Errorf("load stream index resolver for segment %s: %w", segmentPath, irErr)
	}
	reader.SetIndexResolver(ir)

	it := reader.Iterator()
	defer it.Close()
	return r.sender.replay(ctx, r.logger, r.group, partPath, r.counter, it,
		func() error { return r.publishRow(ctx, it.Row()) })
}

// buildWriteRequest reconstructs the WriteRequest + InternalWriteRequest pair
// from a raw stream Row. Separated from publishRow for testability.
func (r *streamRowReplayer) buildWriteRequest(ctx context.Context, row dumpstream.Row) (*streamv1.WriteRequest, *streamv1.InternalWriteRequest, error) {
	subject, evList, err := decodeSeriesEntityValues(row.EntityValues)
	if err != nil {
		return nil, nil, fmt.Errorf("decode entity values (seriesID=%d): %w", row.SeriesID, err)
	}
	cached, err := r.loadSchema(ctx, subject)
	if err != nil {
		return nil, nil, err
	}
	tagFamilies := buildStreamTagFamilies(cached.schema.TagFamilies, cached.schema.Entity, row, evList)
	wr := &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: r.group, Name: subject},
		Element: &streamv1.ElementValue{
			ElementId:   base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(row.ElementID)),
			Timestamp:   timestamppb.New(time.Unix(0, row.Timestamp)),
			TagFamilies: tagFamilies,
		},
		MessageId: uint64(time.Now().UnixNano()),
	}
	tagValues, shardID, err := partition.ApplyLocators(subject, tagFamilies,
		cached.entityLocator, nil, r.targetShardNum)
	if err != nil {
		return nil, nil, fmt.Errorf("route %s: %w", subject, err)
	}
	iwr := &streamv1.InternalWriteRequest{
		ShardId:      uint32(shardID),
		EntityValues: tagValues[1:].Encode(),
		Request:      wr,
		RawElementId: row.ElementID,
	}
	return wr, iwr, nil
}

func (r *streamRowReplayer) publishRow(ctx context.Context, row dumpstream.Row) error {
	wr, iwr, err := r.buildWriteRequest(ctx, row)
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
