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

package trace

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type writeQueueCallback struct {
	l                   *logger.Logger
	schemaRepo          *schemaRepo
	tire2Client         queue.Client
	maxDiskUsagePercent int
}

func setUpWriteQueueCallback(l *logger.Logger, schemaRepo *schemaRepo, maxDiskUsagePercent int, tire2Client queue.Client) bus.MessageListener {
	if maxDiskUsagePercent > 100 {
		maxDiskUsagePercent = 100
	}
	return &writeQueueCallback{
		l:                   l,
		schemaRepo:          schemaRepo,
		maxDiskUsagePercent: maxDiskUsagePercent,
		tire2Client:         tire2Client,
	}
}

func (w *writeQueueCallback) CheckHealth() *common.Error {
	if w.maxDiskUsagePercent < 1 {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "trace is readonly because \"trace-max-disk-usage-percent\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
}

func (w *writeQueueCallback) handle(dst map[string]*tracesInQueue, writeEvent *tracev1.InternalWriteRequest) (map[string]*tracesInQueue, error) {
	stm, ok := w.schemaRepo.loadTrace(writeEvent.GetRequest().GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find trace definition: %s", writeEvent.GetRequest().GetMetadata())
	}
	idx, err := getTagIndex(stm, stm.schema.TimestampTagName)
	if err != nil {
		return nil, err
	}
	t := writeEvent.Request.Tags[idx].GetTimestamp().AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()
	eq, err := w.prepareElementsInQueue(dst, writeEvent)
	if err != nil {
		return nil, err
	}
	et, err := w.prepareElementsInTable(eq, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	err = processTraces(w.schemaRepo, et.traces, writeEvent)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (w *writeQueueCallback) prepareElementsInQueue(dst map[string]*tracesInQueue, writeEvent *tracev1.InternalWriteRequest) (*tracesInQueue, error) {
	gn := writeEvent.Request.Metadata.Group
	queue, err := w.schemaRepo.loadQueue(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load queue for group %s: %w", gn, err)
	}

	eq, ok := dst[gn]
	if !ok {
		eq = &tracesInQueue{
			name:   gn,
			queue:  queue,
			tables: make([]*tracesInTable, 0),
		}
		dst[gn] = eq
	}
	return eq, nil
}

func (w *writeQueueCallback) prepareElementsInTable(eq *tracesInQueue, writeEvent *tracev1.InternalWriteRequest, ts int64) (*tracesInTable, error) {
	var et *tracesInTable
	for i := range eq.tables {
		if eq.tables[i].timeRange.Contains(ts) {
			et = eq.tables[i]
			break
		}
	}

	if et == nil {
		shardID := common.ShardID(writeEvent.ShardId)
		shard, err := eq.queue.GetOrCreateShard(shardID)
		if err != nil {
			return nil, fmt.Errorf("cannot create shard: %w", err)
		}

		tstb := shard.SubQueue()
		timeRange := eq.queue.GetTimeRange(time.Unix(0, ts))

		et = &tracesInTable{
			shardID:   shardID,
			timeRange: timeRange,
			tsTable:   tstb,
			traces:    generateTraces(),
		}
		et.traces.reset()
		eq.tables = append(eq.tables, et)
	}
	return et, nil
}

func (w *writeQueueCallback) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	events, ok := message.Data().([]any)
	if !ok {
		w.l.Warn().Msg("invalid event data type")
		return
	}
	if len(events) < 1 {
		w.l.Warn().Msg("empty event")
		return
	}
	groups := make(map[string]*tracesInQueue)
	for i := range events {
		var writeEvent *tracev1.InternalWriteRequest
		switch e := events[i].(type) {
		case *tracev1.InternalWriteRequest:
			writeEvent = e
		case []byte:
			writeEvent = &tracev1.InternalWriteRequest{}
			if err := proto.Unmarshal(e, writeEvent); err != nil {
				w.l.Error().Err(err).RawJSON("written", e).Msg("fail to unmarshal event")
				continue
			}
		default:
			w.l.Warn().Msg("invalid event data type")
			continue
		}
		var err error
		if groups, err = w.handle(groups, writeEvent); err != nil {
			w.l.Error().Err(err).Msg("cannot handle write event")
			groups = make(map[string]*tracesInQueue)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			es.tsTable.mustAddTraces(es.traces)
			releaseTraces(es.traces)
		}
	}
	return
}
