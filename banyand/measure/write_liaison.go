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

package measure

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

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

type writeQueueCallback struct {
	tire2Client         queue.Client
	l                   *logger.Logger
	schemaRepo          *schemaRepo
	maxDiskUsagePercent int
}

func (w *writeQueueCallback) CheckHealth() *common.Error {
	if w.maxDiskUsagePercent < 1 {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "measure is readonly because \"measure-retention-high-watermark\" is 0")
	}
	diskPercent := observability.GetPathUsedPercent(w.schemaRepo.path)
	if diskPercent < w.maxDiskUsagePercent {
		return nil
	}
	w.l.Warn().Int("maxPercent", w.maxDiskUsagePercent).Int("diskPercent", diskPercent).Msg("disk usage is too high, stop writing")
	return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
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
	groups := make(map[string]*dataPointsInQueue)
	for i := range events {
		var writeEvent *measurev1.InternalWriteRequest
		switch e := events[i].(type) {
		case *measurev1.InternalWriteRequest:
			writeEvent = e
		case []byte:
			writeEvent = &measurev1.InternalWriteRequest{}
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
			groups = make(map[string]*dataPointsInQueue)
			continue
		}
	}
	for i := range groups {
		g := groups[i]
		for j := range g.tables {
			es := g.tables[j]
			if es.tsTable != nil && es.dataPoints != nil {
				es.tsTable.mustAddDataPoints(es.dataPoints)
				releaseDataPoints(es.dataPoints)
			}
			nodes := g.queue.GetNodes(es.shardID)
			if len(nodes) == 0 {
				w.l.Warn().Uint32("shardID", uint32(es.shardID)).Msg("no nodes found for shard")
				continue
			}
			sendDocuments := func(topic bus.Topic, docs index.Documents) {
				seriesDocData, marshalErr := docs.Marshal()
				if marshalErr != nil {
					w.l.Error().Err(marshalErr).Uint32("shardID", uint32(es.shardID)).Msg("failed to marshal series documents")
					return
				}
				// Encode group name, start timestamp from timeRange, and prepend to docData
				combinedData := make([]byte, 0, len(seriesDocData)+len(g.name)+8)
				combinedData = encoding.EncodeBytes(combinedData, convert.StringToBytes(g.name))
				combinedData = encoding.Int64ToBytes(combinedData, es.timeRange.Start.UnixNano())
				combinedData = append(combinedData, seriesDocData...)

				// Send to all nodes for this shard
				for _, node := range nodes {
					message := bus.NewMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, combinedData)
					_, publishErr := w.tire2Client.Publish(ctx, topic, message)
					if publishErr != nil {
						w.l.Error().Err(publishErr).Str("node", node).Uint32("shardID", uint32(es.shardID)).Msg("failed to publish series index to node")
					}
				}
			}
			if len(es.metadataDocs) > 0 {
				sendDocuments(data.TopicMeasureSeriesIndexInsert, es.metadataDocs)
			}
			if len(es.indexModeDocs) > 0 {
				sendDocuments(data.TopicMeasureSeriesIndexUpdate, es.indexModeDocs)
			}
		}
	}
	return
}

func (w *writeQueueCallback) handle(dst map[string]*dataPointsInQueue, writeEvent *measurev1.InternalWriteRequest) (map[string]*dataPointsInQueue, error) {
	req := writeEvent.Request
	t := req.DataPoint.Timestamp.AsTime().Local()
	if err := timestamp.Check(t); err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	ts := t.UnixNano()

	gn := req.Metadata.Group
	queue, err := w.schemaRepo.loadQueue(gn)
	if err != nil {
		return nil, fmt.Errorf("cannot load tsdb for group %s: %w", gn, err)
	}
	dpg, ok := dst[gn]
	if !ok {
		dpg = &dataPointsInQueue{
			name:   gn,
			queue:  queue,
			tables: make([]*dataPointsInTable, 0),
		}
		dst[gn] = dpg
	}

	var dpt *dataPointsInTable
	shardID := common.ShardID(writeEvent.ShardId)
	for i := range dpg.tables {
		if dpg.tables[i].shardID == shardID && dpg.tables[i].timeRange.Contains(ts) {
			dpt = dpg.tables[i]
			break
		}
	}
	stm, ok := w.schemaRepo.loadMeasure(req.GetMetadata())
	if !ok {
		return nil, fmt.Errorf("cannot find measure definition: %s", req.GetMetadata())
	}
	fLen := len(req.DataPoint.GetTagFamilies())
	if fLen < 1 {
		return nil, fmt.Errorf("%s has no tag family", req.Metadata)
	}
	if fLen > len(stm.schema.GetTagFamilies()) {
		return nil, fmt.Errorf("%s has more tag families than %s", req.Metadata, stm.schema)
	}
	is := stm.indexSchema.Load().(indexSchema)
	if len(is.indexRuleLocators.TagFamilyTRule) != len(stm.GetSchema().GetTagFamilies()) {
		logger.Panicf("metadata crashed, tag family rule length %d, tag family length %d",
			len(is.indexRuleLocators.TagFamilyTRule), len(stm.GetSchema().GetTagFamilies()))
	}

	if dpt == nil {
		var shard *wqueue.Shard[*tsTable]
		shard, err = queue.GetOrCreateShard(shardID)
		if err != nil {
			return nil, fmt.Errorf("cannot get or create shard: %w", err)
		}
		dpt = newDpt(nil, queue.GetTimeRange(t), stm.schema.IndexMode, shard.SubQueue())
		dpt.shardID = shardID
		dpg.tables = append(dpg.tables, dpt)
	}

	sid, err := processDataPoint(dpt, req, writeEvent, stm, is, ts)
	if err != nil {
		return nil, err
	}
	w.schemaRepo.inFlow(stm.GetSchema(), sid, writeEvent.ShardId, writeEvent.EntityValues, req.DataPoint)
	return dst, nil
}
