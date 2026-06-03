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
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
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
	traceReplayBatchSize    = 2000
	traceReplayBatchTimeout = 30 * time.Second
	traceReplayVersion      = 1 // trace Row has no version; receiver requires version > 0.
)

// traceRowReplayer replays a group's trace parts. Trace schemas are
// one-per-group; constructing the replayer for a group with multiple traces
// returns an error.
type traceRowReplayer struct {
	selector       node.Selector
	client         queue.Client
	publisher      queue.BatchPublisher
	fs             fs.FileSystem
	logger         *logger.Logger
	schema         *databasev1.Trace
	traceName      string
	counter        *uint64
	group          string
	batch          []bus.Message
	batchMu        sync.Mutex
	targetShardNum uint32
}

func newTraceRowReplayer(
	ctx context.Context,
	group string, targetShardNum uint32,
	selector node.Selector, client queue.Client,
	md metadata.Repo, fileSystem fs.FileSystem,
	l *logger.Logger, counter *uint64,
) (*traceRowReplayer, error) {
	traces, err := md.TraceRegistry().ListTrace(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return nil, fmt.Errorf("list traces of group %s: %w", group, err)
	}
	if len(traces) == 0 {
		return nil, fmt.Errorf("no traces in group %s", group)
	}
	if len(traces) > 1 {
		return nil, fmt.Errorf("group %s has %d traces; row-replay assumes a single trace per group", group, len(traces))
	}
	t := traces[0]
	return &traceRowReplayer{
		group:          group,
		targetShardNum: targetShardNum,
		selector:       selector,
		client:         client,
		publisher:      client.NewBatchPublisher(traceReplayBatchTimeout),
		fs:             fileSystem,
		logger:         l,
		schema:         t,
		traceName:      t.Metadata.Name,
		counter:        counter,
		batch:          make([]bus.Message, 0, traceReplayBatchSize),
	}, nil
}

// Close flushes pending messages and closes the publisher.
func (r *traceRowReplayer) Close() (map[string]*common.Error, error) {
	flushCtx, cancel := context.WithTimeout(context.Background(), traceReplayBatchTimeout)
	defer cancel()
	flushErr := r.flushBatch(flushCtx)
	cee, closeErr := r.publisher.Close()
	if flushErr != nil {
		return cee, flushErr
	}
	return cee, closeErr
}

// flushAndConfirm flushes the buffered batch, closes the publisher to obtain the
// per-node delivery result for everything sent since the last call, then opens a
// fresh publisher for subsequent shards. The batch publisher is client-streaming,
// so per-node errors are only observable once its stream closes; this lets a
// row-replayed shard be confirmed durable before it is marked completed.
func (r *traceRowReplayer) flushAndConfirm(ctx context.Context) (map[string]*common.Error, error) {
	flushErr := r.flushBatch(ctx)
	cee, closeErr := r.publisher.Close()
	r.publisher = r.client.NewBatchPublisher(traceReplayBatchTimeout)
	if flushErr != nil {
		return cee, flushErr
	}
	return cee, closeErr
}

func (r *traceRowReplayer) replayPart(ctx context.Context, partPath string) (int, error) {
	partID, parseErr := strconv.ParseUint(filepath.Base(partPath), 16, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("invalid part path %s: %w", partPath, parseErr)
	}
	shardPath := filepath.Dir(partPath)

	reader, err := dumptrace.OpenPart(partID, shardPath, r.fs)
	if err != nil {
		return 0, fmt.Errorf("open trace part %s: %w", partPath, err)
	}
	defer reader.Close()

	it := reader.Iterator()
	defer it.Close()

	rowCount := 0
	for it.Next() {
		row := it.Row()
		if rowErr := r.publishRow(ctx, row); rowErr != nil {
			pos := it.Position()
			r.logger.Warn().Err(rowErr).
				Str("group", r.group).
				Str("trace", r.traceName).
				Str("part", partPath).
				Int("block_idx", pos.BlockIdx).
				Int("row_idx", pos.RowIdx).
				Int("rows_published", rowCount).
				Msg("trace row-replay aborted mid-part on publish error; will retry on resume")
			return rowCount, rowErr
		}
		rowCount++
	}
	if iterErr := it.Err(); iterErr != nil {
		pos := it.Position()
		r.logger.Warn().Err(iterErr).
			Str("group", r.group).
			Str("trace", r.traceName).
			Str("part", partPath).
			Int("block_idx", pos.BlockIdx).
			Int("row_idx", pos.RowIdx).
			Int("rows_published", rowCount).
			Msg("trace row-replay aborted mid-part; will retry on resume")
		return rowCount, iterErr
	}
	if r.counter != nil {
		atomic.AddUint64(r.counter, 1)
	}
	return rowCount, nil
}

// buildWriteRequest reconstructs the WriteRequest + InternalWriteRequest pair
// from a raw trace Row. Separated from publishRow for testability.
func (r *traceRowReplayer) buildWriteRequest(row dumptrace.Row) (*tracev1.WriteRequest, *tracev1.InternalWriteRequest) {
	tags := buildTraceTags(r.schema.Tags, row, r.schema.TraceIdTagName, r.schema.SpanIdTagName)
	// Copy span bytes; the iterator buffer is reused across advances.
	spanCopy := append([]byte(nil), row.Span...)
	wr := &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: r.group, Name: r.traceName},
		Tags:     tags,
		Span:     spanCopy,
		Version:  traceReplayVersion,
	}
	shardID := partition.TraceShardID(row.TraceID, r.targetShardNum)
	iwr := &tracev1.InternalWriteRequest{
		ShardId: uint32(shardID),
		Request: wr,
	}
	return wr, iwr
}

func (r *traceRowReplayer) publishRow(ctx context.Context, row dumptrace.Row) error {
	_, iwr := r.buildWriteRequest(row)
	nodeID, err := r.selector.Pick(r.group, r.traceName, iwr.ShardId, 0)
	if err != nil {
		return fmt.Errorf("pick target node for trace %s: %w", r.traceName, err)
	}
	msg := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	return r.enqueue(ctx, msg)
}

// enqueue appends a message to the batch and flushes when full.
func (r *traceRowReplayer) enqueue(ctx context.Context, msg bus.Message) error {
	r.batchMu.Lock()
	r.batch = append(r.batch, msg)
	shouldFlush := len(r.batch) >= traceReplayBatchSize
	r.batchMu.Unlock()
	if shouldFlush {
		return r.flushBatch(ctx)
	}
	return nil
}

func (r *traceRowReplayer) flushBatch(ctx context.Context) error {
	r.batchMu.Lock()
	if len(r.batch) == 0 {
		r.batchMu.Unlock()
		return nil
	}
	pending := r.batch
	r.batch = make([]bus.Message, 0, traceReplayBatchSize)
	r.batchMu.Unlock()
	_, err := r.publisher.Publish(ctx, data.TopicTraceWrite, pending...)
	return err
}
