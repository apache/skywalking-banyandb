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
	"hash/fnv"
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type traceService struct {
	tracev1.UnimplementedTraceServiceServer
	ingestionAccessLog accesslog.Log
	pipeline           queue.Client
	broadcaster        queue.Client
	*discoveryService
	l               *logger.Logger
	metrics         *metrics
	writeTimeout    time.Duration
	maxWaitDuration time.Duration
}

func (s *traceService) setLogger(log *logger.Logger) {
	s.l = log
}

func (s *traceService) activeIngestionAccessLog(root string, sampled bool) (err error) {
	if s.ingestionAccessLog, err = accesslog.
		NewFileLog(root, "trace-ingest-%s", 10*time.Minute, s.log, sampled); err != nil {
		return err
	}
	return nil
}

func (s *traceService) validateTimestamp(writeEntity *tracev1.WriteRequest) error {
	// Get trace schema from entityRepo
	id := getID(writeEntity.GetMetadata())
	traceEntity, existed := s.entityRepo.getTrace(id)
	if !existed {
		return errors.New("trace schema not found")
	}

	timestampTagName := traceEntity.GetTimestampTagName()
	for _, tag := range writeEntity.GetTags() {
		if tag.GetTimestamp() != nil {
			if err := timestamp.CheckPb(tag.GetTimestamp()); err != nil {
				s.l.Error().Stringer("written", writeEntity).Err(err).Msg("the timestamp is invalid")
				return err
			}
			return nil
		}
	}

	return errors.New("timestamp tag not found: " + timestampTagName)
}

func (s *traceService) validateMetadata(writeEntity *tracev1.WriteRequest) error {
	if writeEntity.Metadata.ModRevision > 0 {
		traceCache, existed := s.entityRepo.getTrace(getID(writeEntity.GetMetadata()))
		if !existed {
			return errors.New("trace schema not found")
		}
		if writeEntity.Metadata.ModRevision != traceCache.GetMetadata().GetModRevision() {
			return errors.New("expired trace schema")
		}
	}
	return nil
}

func (s *traceService) extractTraceID(tags []*modelv1.TagValue, traceIDIndex int) (string, error) {
	if len(tags) == 0 {
		return "", errors.New("no tags found")
	}

	if traceIDIndex < 0 || traceIDIndex >= len(tags) {
		return "", errors.New("trace ID tag index out of range")
	}

	tag := tags[traceIDIndex]
	switch v := tag.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return v.Str.GetValue(), nil
	case *modelv1.TagValue_BinaryData:
		return string(v.BinaryData), nil
	default:
		return "", errors.New("trace ID must be string or binary data")
	}
}

func (s *traceService) getTraceShardID(writeEntity *tracev1.WriteRequest) (common.ShardID, error) {
	// Get shard count from group configuration
	shardCount, existed := s.groupRepo.shardNum(writeEntity.GetMetadata().GetGroup())
	if !existed {
		return 0, errors.New("group not found or no shard configuration")
	}

	// Get cached trace ID index from entityRepo
	id := getID(writeEntity.GetMetadata())
	traceIDIndex, existed := s.entityRepo.getTraceIDIndex(id)
	if !existed {
		return 0, errors.New("trace schema not found")
	}

	if traceIDIndex == -1 {
		return 0, errors.New("trace ID tag not found in schema")
	}

	traceID, err := s.extractTraceID(writeEntity.GetTags(), traceIDIndex)
	if err != nil {
		return 0, err
	}

	// Calculate shard ID using hash of trace ID
	hasher := fnv.New32a()
	hasher.Write([]byte(traceID))
	hash := hasher.Sum32()

	return common.ShardID(hash % shardCount), nil
}

func (s *traceService) getTraceShardIDWithRetry(writeEntity *tracev1.WriteRequest) (common.ShardID, error) {
	if s.maxWaitDuration > 0 {
		retryInterval := 10 * time.Millisecond
		startTime := time.Now()
		for {
			shardID, err := s.getTraceShardID(writeEntity)
			if err == nil || !errors.Is(err, errNotExist) || time.Since(startTime) > s.maxWaitDuration {
				return shardID, err
			}
			time.Sleep(retryInterval)
			retryInterval = time.Duration(float64(retryInterval) * 1.5)
			if retryInterval > time.Second {
				retryInterval = time.Second
			}
		}
	}
	return s.getTraceShardID(writeEntity)
}

func (s *traceService) publishMessages(
	ctx context.Context,
	publisher queue.BatchPublisher,
	writeEntity *tracev1.WriteRequest,
	shardID common.ShardID,
) ([]string, error) {
	iwr := &tracev1.InternalWriteRequest{
		ShardId: uint32(shardID),
		Request: writeEntity,
	}
	nodeID, err := s.nodeRegistry.Locate(writeEntity.GetMetadata().GetGroup(), writeEntity.GetMetadata().GetName(), uint32(shardID), 0)
	if err != nil {
		return nil, err
	}

	message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	if _, err := publisher.Publish(ctx, data.TopicTraceWrite, message); err != nil {
		return nil, err
	}
	return []string{nodeID}, nil
}

func (s *traceService) Write(stream tracev1.TraceService_WriteServer) error {
	reply := func(metadata *commonv1.Metadata, status modelv1.Status, version uint64, stream tracev1.TraceService_WriteServer, logger *logger.Logger) {
		if status != modelv1.Status_STATUS_SUCCEED {
			s.metrics.totalStreamMsgReceivedErr.Inc(1, metadata.Group, "trace", "write")
		}
		s.metrics.totalStreamMsgSent.Inc(1, metadata.Group, "trace", "write")
		if errResp := stream.Send(&tracev1.WriteResponse{Metadata: metadata, Status: status.String(), Version: version}); errResp != nil {
			if dl := logger.Debug(); dl.Enabled() {
				dl.Err(errResp).Msg("failed to send trace write response")
			}
			s.metrics.totalStreamMsgSentErr.Inc(1, metadata.Group, "trace", "write")
		}
	}

	s.metrics.totalStreamStarted.Inc(1, "trace", "write")
	publisher := s.pipeline.NewBatchPublisher(s.writeTimeout)
	start := time.Now()
	var succeedSent []succeedSentMessage
	requestCount := 0
	defer func() {
		cee, err := publisher.Close()
		for _, ssm := range succeedSent {
			code := modelv1.Status_STATUS_SUCCEED
			if cee != nil {
				for _, node := range ssm.nodes {
					if ce, ok := cee[node]; ok {
						code = ce.Status()
						break
					}
				}
			}
			reply(ssm.metadata, code, ssm.messageID, stream, s.l)
		}
		if err != nil {
			s.l.Error().Err(err).Msg("failed to close the publisher")
		}
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Int("total_requests", requestCount).Msg("completed trace write batch")
		}
		s.metrics.totalStreamFinished.Inc(1, "trace", "write")
		s.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "trace", "write")
	}()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		writeEntity, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				s.l.Error().Stringer("written", writeEntity).Err(err).Msg("failed to receive message")
			}
			return err
		}

		requestCount++
		s.metrics.totalStreamMsgReceived.Inc(1, writeEntity.Metadata.Group, "trace", "write")

		if err = s.validateTimestamp(writeEntity); err != nil {
			reply(writeEntity.GetMetadata(), modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetVersion(), stream, s.l)
			continue
		}

		if err = s.validateMetadata(writeEntity); err != nil {
			status := modelv1.Status_STATUS_INTERNAL_ERROR
			if errors.Is(err, errors.New("trace schema not found")) {
				status = modelv1.Status_STATUS_NOT_FOUND
			} else if errors.Is(err, errors.New("expired trace schema")) {
				status = modelv1.Status_STATUS_EXPIRED_SCHEMA
			}
			s.l.Error().Err(err).Stringer("written", writeEntity).Msg("metadata validation failed")
			reply(writeEntity.GetMetadata(), status, writeEntity.GetVersion(), stream, s.l)
			continue
		}

		shardID, err := s.getTraceShardIDWithRetry(writeEntity)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("trace sharding failed")
			reply(writeEntity.GetMetadata(), modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), stream, s.l)
			continue
		}

		if s.ingestionAccessLog != nil {
			if errAL := s.ingestionAccessLog.Write(writeEntity); errAL != nil {
				s.l.Error().Err(errAL).Msg("failed to write ingestion access log")
			}
		}

		nodes, err := s.publishMessages(ctx, publisher, writeEntity, shardID)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("publishing failed")
			reply(writeEntity.GetMetadata(), modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), stream, s.l)
			continue
		}

		succeedSent = append(succeedSent, succeedSentMessage{
			metadata:  writeEntity.GetMetadata(),
			messageID: writeEntity.GetVersion(),
			nodes:     nodes,
		})
	}
}

var emptyTraceQueryResponse = &tracev1.QueryResponse{Spans: make([]*tracev1.Span, 0)}

func (s *traceService) Query(ctx context.Context, req *tracev1.QueryRequest) (resp *tracev1.QueryResponse, err error) {
	for _, g := range req.Groups {
		s.metrics.totalStarted.Inc(1, g, "trace", "query")
	}
	start := time.Now()
	defer func() {
		for _, g := range req.Groups {
			s.metrics.totalFinished.Inc(1, g, "trace", "query")
			if err != nil {
				s.metrics.totalErr.Inc(1, g, "trace", "query")
			}
			s.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "trace", "query")
		}
	}()
	timeRange := req.GetTimeRange()
	if timeRange == nil {
		req.TimeRange = timestamp.DefaultTimeRange
	}
	if err = timestamp.CheckTimeRange(req.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", req.GetTimeRange(), err)
	}
	now := time.Now()
	if req.Trace {
		tracer, _ := query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ := tracer.StartSpan(ctx, "trace-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.AddSubTrace(resp.TraceQueryResult)
				resp.TraceQueryResult = tracer.ToProto()
			}
			span.Stop()
		}()
	}
	message := bus.NewMessage(bus.MessageID(now.UnixNano()), req)
	feat, errQuery := s.broadcaster.Publish(ctx, data.TopicTraceQuery, message)
	if errQuery != nil {
		if errors.Is(errQuery, io.EOF) {
			return emptyTraceQueryResponse, nil
		}
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case *tracev1.QueryResponse:
		return d, nil
	case *common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Error())
	}
	return nil, nil
}

func (s *traceService) Close() error {
	if s.ingestionAccessLog != nil {
		return s.ingestionAccessLog.Close()
	}
	return nil
}
