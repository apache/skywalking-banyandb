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
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
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
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "trace is readonly because \"trace-retention-high-watermark\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
}

func (w *writeQueueCallback) handle(dst map[string]*tracesInQueue, writeEvent *tracev1.InternalWriteRequest,
	metadata *commonv1.Metadata, spec *tracev1.TagSpec,
) (map[string]*tracesInQueue, error) {
	stm, ok := w.schemaRepo.loadTrace(metadata)
	if !ok {
		return nil, fmt.Errorf("cannot find trace definition: %s", metadata)
	}
	specMap := buildSpecMap(spec)
	idx, err := getTagIndex(stm, stm.schema.TimestampTagName, specMap)
	if err != nil {
		return nil, err
	}
	t := writeEvent.Request.Tags[idx].GetTimestamp().AsTime().Local()
	if err = timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()
	eq, err := w.prepareElementsInQueue(dst, metadata)
	if err != nil {
		return nil, err
	}
	et, err := w.prepareElementsInTable(eq, writeEvent, ts)
	if err != nil {
		return nil, err
	}
	err = processTraces(w.schemaRepo, et, writeEvent, metadata, spec)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (w *writeQueueCallback) prepareElementsInQueue(dst map[string]*tracesInQueue, metadata *commonv1.Metadata) (*tracesInQueue, error) {
	gn := metadata.Group
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
	shardID := common.ShardID(writeEvent.ShardId)
	for i := range eq.tables {
		if eq.tables[i].timeRange.Contains(ts) && eq.tables[i].shardID == shardID {
			et = eq.tables[i]
			break
		}
	}

	if et == nil {
		shard, err := eq.queue.GetOrCreateShard(shardID)
		if err != nil {
			return nil, fmt.Errorf("cannot create shard: %w", err)
		}

		tstb := shard.SubQueue()
		timeRange := eq.queue.GetTimeRange(time.Unix(0, ts))

		et = &tracesInTable{
			shardID:     shardID,
			timeRange:   timeRange,
			tsTable:     tstb,
			traces:      generateTraces(),
			sidxReqsMap: make(map[string][]sidx.WriteRequest),
			seriesDocs: seriesDoc{
				docs:        make(index.Documents, 0),
				docIDsAdded: make(map[uint64]struct{}),
			},
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
	var metadata *commonv1.Metadata
	var spec *tracev1.TagSpec
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
		req := writeEvent.Request
		if req != nil && req.GetMetadata() != nil {
			metadata = req.GetMetadata()
		}
		if req != nil && req.GetTagSpec() != nil {
			spec = req.GetTagSpec()
		}
		var err error
		if groups, err = w.handle(groups, writeEvent, metadata, spec); err != nil {
			w.l.Error().Err(err).Msg("cannot handle write event")
			groups = make(map[string]*tracesInQueue)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			// Marshal series metadata for persistence in part folder
			var seriesMetadataBytes []byte
			if len(es.seriesDocs.docs) > 0 {
				var marshalErr error
				seriesMetadataBytes, marshalErr = es.seriesDocs.docs.Marshal()
				if marshalErr != nil {
					w.l.Error().Err(marshalErr).Uint32("shardID", uint32(es.shardID)).Msg("failed to marshal series metadata for persistence")
					// Continue without series metadata, but log the error
				}
			}
			var sidxMemPartMap map[string]*sidx.MemPart
			for sidxName, sidxReqs := range es.sidxReqsMap {
				if len(sidxReqs) > 0 {
					sidxInstance, err := es.tsTable.getOrCreateSidx(sidxName)
					if err != nil {
						w.l.Error().Err(err).Str("sidx", sidxName).Msg("cannot get or create sidx instance")
						continue
					}
					var siMemPart *sidx.MemPart
					if siMemPart, err = sidxInstance.ConvertToMemPart(sidxReqs, es.timeRange.Start.UnixNano()); err != nil {
						w.l.Error().Err(err).Str("sidx", sidxName).Msg("cannot write to secondary index")
						continue
					}
					if sidxMemPartMap == nil {
						sidxMemPartMap = make(map[string]*sidx.MemPart)
					}
					sidxMemPartMap[sidxName] = siMemPart
				}
			}
			if es.tsTable != nil && es.traces != nil {
				es.tsTable.mustAddTracesWithSegmentID(es.traces, es.timeRange.Start.UnixNano(), sidxMemPartMap, seriesMetadataBytes)
				releaseTraces(es.traces)
			}

			nodes := g.queue.GetNodes(es.shardID)
			if len(nodes) == 0 {
				w.l.Warn().Uint32("shardID", uint32(es.shardID)).Msg("no nodes found for shard")
				continue
			}

			// Handle series index writing
			if len(seriesMetadataBytes) > 0 {
				// Encode group name, start timestamp from timeRange, and prepend to docData
				combinedData := make([]byte, 0, len(seriesMetadataBytes)+len(g.name)+8)
				combinedData = encoding.EncodeBytes(combinedData, convert.StringToBytes(g.name))
				combinedData = encoding.Int64ToBytes(combinedData, es.timeRange.Start.UnixNano())
				combinedData = append(combinedData, seriesMetadataBytes...)

				// Send to all nodes for this shard
				for _, node := range nodes {
					message := bus.NewMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, combinedData)
					_, publishErr := w.tire2Client.Publish(ctx, data.TopicTraceSidxSeriesWrite, message)
					if publishErr != nil {
						w.l.Error().Err(publishErr).Str("node", node).Uint32("shardID", uint32(es.shardID)).Msg("failed to publish series index to node")
					}
				}
			}
		}
	}
	return
}
