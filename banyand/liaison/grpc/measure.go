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

package grpc

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type measureService struct {
	measurev1.UnimplementedMeasureServiceServer
	ingestionAccessLog accesslog.Log
	pipeline           queue.Client
	broadcaster        queue.Client
	*discoveryService
	sampled      *logger.Logger
	metrics      *metrics
	writeTimeout time.Duration
}

func (ms *measureService) setLogger(log *logger.Logger) {
	ms.sampled = log.Sampled(10)
}

func (ms *measureService) activeIngestionAccessLog(root string) (err error) {
	if ms.ingestionAccessLog, err = accesslog.
		NewFileLog(root, "measure-ingest-%s", 10*time.Minute, ms.log); err != nil {
		return err
	}
	return nil
}

func (ms *measureService) Write(measure measurev1.MeasureService_WriteServer) error {
	reply := func(metadata *commonv1.Metadata, status modelv1.Status, messageId uint64, measure measurev1.MeasureService_WriteServer, logger *logger.Logger) {
		if status != modelv1.Status_STATUS_SUCCEED {
			ms.metrics.totalStreamMsgReceivedErr.Inc(1, metadata.Group, "measure", "write")
		}
		ms.metrics.totalStreamMsgSent.Inc(1, metadata.Group, "measure", "write")
		if errResp := measure.Send(&measurev1.WriteResponse{Metadata: metadata, Status: status, MessageId: messageId}); errResp != nil {
			logger.Debug().Err(errResp).Msg("failed to send measure write response")
			ms.metrics.totalStreamMsgSentErr.Inc(1, metadata.Group, "measure", "write")
		}
	}
	ctx := measure.Context()
	publisher := ms.pipeline.NewBatchPublisher(ms.writeTimeout)
	ms.metrics.totalStreamStarted.Inc(1, "measure", "write")
	start := time.Now()
	defer func() {
		publisher.Close()
		ms.metrics.totalStreamFinished.Inc(1, "measure", "write")
		ms.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "measure", "write")
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		writeRequest, err := measure.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				ms.sampled.Error().Err(err).Stringer("written", writeRequest).Msg("failed to receive message")
			}
			return err
		}
		ms.metrics.totalStreamMsgReceived.Inc(1, writeRequest.Metadata.Group, "measure", "write")
		if errTime := timestamp.CheckPb(writeRequest.DataPoint.Timestamp); errTime != nil {
			ms.sampled.Error().Err(errTime).Stringer("written", writeRequest).Msg("the data point time is invalid")
			reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_INVALID_TIMESTAMP, writeRequest.GetMessageId(), measure, ms.sampled)
			continue
		}
		if writeRequest.Metadata.ModRevision > 0 {
			measureCache, existed := ms.entityRepo.getLocator(getID(writeRequest.GetMetadata()))
			if !existed {
				ms.sampled.Error().Err(err).Stringer("written", writeRequest).Msg("failed to measure schema not found")
				reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_NOT_FOUND, writeRequest.GetMessageId(), measure, ms.sampled)
				continue
			}
			if writeRequest.Metadata.ModRevision != measureCache.ModRevision {
				ms.sampled.Error().Stringer("written", writeRequest).Msg("the measure schema is expired")
				reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_EXPIRED_SCHEMA, writeRequest.GetMessageId(), measure, ms.sampled)
				continue
			}
		}
		entity, tagValues, shardID, err := ms.navigate(writeRequest.GetMetadata(), writeRequest.GetDataPoint().GetTagFamilies())
		if err != nil {
			ms.sampled.Error().Err(err).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to navigate to the write target")
			reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure, ms.sampled)
			continue
		}
		if writeRequest.DataPoint.Version == 0 {
			if writeRequest.MessageId == 0 {
				writeRequest.MessageId = uint64(time.Now().UnixNano())
			}
			writeRequest.DataPoint.Version = int64(writeRequest.MessageId)
		}
		if ms.ingestionAccessLog != nil {
			if errAccessLog := ms.ingestionAccessLog.Write(writeRequest); errAccessLog != nil {
				ms.sampled.Error().Err(errAccessLog).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to write access log")
			}
		}
		iwr := &measurev1.InternalWriteRequest{
			Request:      writeRequest,
			ShardId:      uint32(shardID),
			SeriesHash:   pbv1.HashEntity(entity),
			EntityValues: tagValues[1:].Encode(),
		}
		nodeID, errPickNode := ms.nodeRegistry.Locate(writeRequest.GetMetadata().GetGroup(), writeRequest.GetMetadata().GetName(), uint32(shardID))
		if errPickNode != nil {
			ms.sampled.Error().Err(errPickNode).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to pick an available node")
			reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure, ms.sampled)
			continue
		}
		message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
		_, errWritePub := publisher.Publish(ctx, data.TopicMeasureWrite, message)
		if errWritePub != nil {
			ms.sampled.Error().Err(errWritePub).RawJSON("written", logger.Proto(writeRequest)).Str("nodeID", nodeID).Msg("failed to send a message")
			reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure, ms.sampled)
			continue
		}
		reply(writeRequest.GetMetadata(), modelv1.Status_STATUS_SUCCEED, writeRequest.GetMessageId(), measure, ms.sampled)
	}
}

var emptyMeasureQueryResponse = &measurev1.QueryResponse{DataPoints: make([]*measurev1.DataPoint, 0)}

func (ms *measureService) Query(ctx context.Context, req *measurev1.QueryRequest) (resp *measurev1.QueryResponse, err error) {
	for _, g := range req.Groups {
		ms.metrics.totalStarted.Inc(1, g, "measure", "query")
	}
	start := time.Now()
	defer func() {
		for _, g := range req.Groups {
			ms.metrics.totalFinished.Inc(1, g, "measure", "query")
			if err != nil {
				ms.metrics.totalErr.Inc(1, g, "measure", "query")
			}
			ms.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "measure", "query")
		}
	}()
	if err = timestamp.CheckTimeRange(req.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", req.GetTimeRange(), err)
	}
	now := time.Now()
	if req.Trace {
		tracer, _ := query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ := tracer.StartSpan(ctx, "measure-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.AddSubTrace(resp.Trace)
				resp.Trace = tracer.ToProto()
			}
			span.Stop()
		}()
	}
	feat, err := ms.broadcaster.Publish(ctx, data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(now.UnixNano()), req))
	if err != nil {
		return nil, err
	}
	msg, err := feat.Get()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return emptyMeasureQueryResponse, nil
		}
		return nil, err
	}
	data := msg.Data()
	switch d := data.(type) {
	case *measurev1.QueryResponse:
		return d, nil
	case common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Msg())
	}
	return nil, nil
}

func (ms *measureService) TopN(ctx context.Context, topNRequest *measurev1.TopNRequest) (resp *measurev1.TopNResponse, err error) {
	if err = timestamp.CheckTimeRange(topNRequest.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", topNRequest.GetTimeRange(), err)
	}
	now := time.Now()
	if topNRequest.Trace {
		tracer, _ := query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ := tracer.StartSpan(ctx, "topn-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(topNRequest)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.AddSubTrace(resp.Trace)
				resp.Trace = tracer.ToProto()
			}
			span.Stop()
		}()
	}
	message := bus.NewMessage(bus.MessageID(now.UnixNano()), topNRequest)
	feat, errQuery := ms.broadcaster.Publish(ctx, data.TopicTopNQuery, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case *measurev1.TopNResponse:
		return d, nil
	case common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Msg())
	}
	return nil, nil
}

func (ms *measureService) Close() error {
	return ms.ingestionAccessLog.Close()
}
