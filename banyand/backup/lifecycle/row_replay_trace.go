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
	sender         *batchSender
	fs             fs.FileSystem
	logger         *logger.Logger
	schema         *databasev1.Trace
	traceName      string
	counter        *uint64
	group          string
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
		sender:         newBatchSender(client, data.TopicTraceWrite, traceReplayBatchSize, traceReplayBatchTimeout),
		fs:             fileSystem,
		logger:         l,
		schema:         t,
		traceName:      t.Metadata.Name,
		counter:        counter,
	}, nil
}

// Close drains any outstanding batch confirmations and closes the publisher.
func (r *traceRowReplayer) Close() (map[string]*common.Error, error) {
	return r.sender.close()
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
	// Prefix the part label with the trace name so the shared driver's timing and
	// abort logs keep the trace identity the old per-type logs carried.
	return r.sender.replay(ctx, r.logger, r.group, r.traceName+"/"+partPath, r.counter, it,
		func() error { return r.publishRow(ctx, it.Row()) })
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
	return r.sender.enqueue(ctx, msg)
}
