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

type traceSpecLocator struct {
	traceIDIndex   int
	timestampIndex int
}

func newTraceSpecLocator(spec *tracev1.TagSpec, traceIDTagName, timestampTagName string) *traceSpecLocator {
	locator := &traceSpecLocator{
		traceIDIndex:   -1,
		timestampIndex: -1,
	}
	if spec == nil {
		return locator
	}
	for i, tagName := range spec.GetTagNames() {
		if tagName == traceIDTagName {
			locator.traceIDIndex = i
		}
		if tagName == timestampTagName {
			locator.timestampIndex = i
		}
	}
	return locator
}

type traceService struct {
	tracev1.UnimplementedTraceServiceServer
	ingestionAccessLog accesslog.Log
	queryAccessLog     accesslog.Log
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
		NewFileLog(root, "trace-ingest-%s", 10*time.Minute, s.l, sampled); err != nil {
		return err
	}
	return nil
}

func (s *traceService) activeQueryAccessLog(root string, sampled bool) (err error) {
	if s.queryAccessLog, err = accesslog.
		NewFileLog(root, "trace-query-%s", 10*time.Minute, s.l, sampled); err != nil {
		return err
	}
	return nil
}

func (s *traceService) validateWriteRequest(writeEntity *tracev1.WriteRequest,
	metadata *commonv1.Metadata, specLocator *traceSpecLocator, stream tracev1.TraceService_WriteServer,
) modelv1.Status {
	id := getID(metadata)
	traceEntity, existed := s.entityRepo.getTrace(id)
	if !existed {
		s.l.Error().Stringer("written", writeEntity).Msg("trace schema not found")
		s.sendReply(metadata, modelv1.Status_STATUS_NOT_FOUND, writeEntity.GetVersion(), stream)
		return modelv1.Status_STATUS_NOT_FOUND
	}

	var foundTimestamp bool
	if specLocator != nil && specLocator.timestampIndex >= 0 {
		tags := writeEntity.GetTags()
		if specLocator.timestampIndex < len(tags) {
			tagValue := tags[specLocator.timestampIndex]
			if tagValue != nil && tagValue.GetTimestamp() != nil {
				if errTime := timestamp.CheckPb(tagValue.GetTimestamp()); errTime != nil {
					s.l.Error().Err(errTime).Stringer("written", writeEntity).Msg("the timestamp is invalid")
					s.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetVersion(), stream)
					return modelv1.Status_STATUS_INVALID_TIMESTAMP
				}
				foundTimestamp = true
			}
		}
	}
	if !foundTimestamp {
		for _, tag := range writeEntity.GetTags() {
			if tag.GetTimestamp() != nil {
				if errTime := timestamp.CheckPb(tag.GetTimestamp()); errTime != nil {
					s.l.Error().Err(errTime).Stringer("written", writeEntity).Msg("the timestamp is invalid")
					s.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetVersion(), stream)
					return modelv1.Status_STATUS_INVALID_TIMESTAMP
				}
				foundTimestamp = true
				break
			}
		}
	}
	if !foundTimestamp {
		timestampTagName := traceEntity.GetTimestampTagName()
		s.l.Error().Stringer("written", writeEntity).Msg("timestamp tag not found: " + timestampTagName)
		s.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetVersion(), stream)
		return modelv1.Status_STATUS_INVALID_TIMESTAMP
	}

	if metadata.ModRevision > 0 {
		if metadata.ModRevision != traceEntity.GetMetadata().GetModRevision() {
			s.l.Error().Stringer("written", writeEntity).Msg("the trace schema is expired")
			s.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeEntity.GetVersion(), stream)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
	}

	return modelv1.Status_STATUS_SUCCEED
}

func (s *traceService) navigate(metadata *commonv1.Metadata,
	writeRequest *tracev1.WriteRequest, specLocator *traceSpecLocator,
) (common.ShardID, error) {
	shardCount, existed := s.groupRepo.shardNum(metadata.GetGroup())
	if !existed {
		return 0, errors.Wrapf(errNotExist, "finding the shard num by: %v", metadata)
	}
	id := getID(metadata)
	traceEntity, existed := s.entityRepo.getTrace(id)
	if !existed {
		return 0, errors.Wrapf(errNotExist, "finding trace schema by: %v", metadata)
	}

	var traceID string
	var err error
	if specLocator != nil && specLocator.traceIDIndex >= 0 {
		tags := writeRequest.GetTags()
		if specLocator.traceIDIndex < len(tags) {
			traceID, err = extractTraceIDFromTagValue(tags[specLocator.traceIDIndex])
			if err == nil {
				return s.shardID(traceID, shardCount), nil
			}
		}
	}

	traceIDIndex, existed := s.entityRepo.getTraceIDIndex(id)
	if !existed || traceIDIndex == -1 {
		return 0, errors.New("trace ID tag not found in schema: " + traceEntity.GetTraceIdTagName())
	}
	traceID, err = s.extractTraceID(writeRequest.GetTags(), traceIDIndex)
	if err != nil {
		return 0, err
	}
	return s.shardID(traceID, shardCount), nil
}

func (s *traceService) shardID(traceID string, shardCount uint32) common.ShardID {
	hash := convert.Hash([]byte(traceID))
	return common.ShardID(hash % uint64(shardCount))
}

func (s *traceService) extractTraceID(tags []*modelv1.TagValue, traceIDIndex int) (string, error) {
	if len(tags) == 0 {
		return "", errors.New("no tags found")
	}
	if traceIDIndex < 0 || traceIDIndex >= len(tags) {
		return "", errors.New("trace ID tag index out of range")
	}
	return extractTraceIDFromTagValue(tags[traceIDIndex])
}

func extractTraceIDFromTagValue(tag *modelv1.TagValue) (string, error) {
	switch v := tag.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return v.Str.GetValue(), nil
	case *modelv1.TagValue_BinaryData:
		return string(v.BinaryData), nil
	default:
		return "", errors.New("trace ID must be string or binary data")
	}
}

func (s *traceService) navigateWithRetry(writeEntity *tracev1.WriteRequest, metadata *commonv1.Metadata,
	specLocator *traceSpecLocator,
) (shardID common.ShardID, err error) {
	if s.maxWaitDuration > 0 {
		retryInterval := 10 * time.Millisecond
		startTime := time.Now()
		for {
			shardID, err = s.navigate(metadata, writeEntity, specLocator)
			if err == nil || !errors.Is(err, errNotExist) || time.Since(startTime) > s.maxWaitDuration {
				return
			}
			time.Sleep(retryInterval)
			retryInterval = time.Duration(float64(retryInterval) * 1.5)
			if retryInterval > time.Second {
				retryInterval = time.Second
			}
		}
	}
	return s.navigate(metadata, writeEntity, specLocator)
}

func (s *traceService) publishMessages(
	ctx context.Context,
	publisher queue.BatchPublisher,
	writeEntity *tracev1.WriteRequest,
	metadata *commonv1.Metadata,
	spec *tracev1.TagSpec,
	shardID common.ShardID,
	nodeMetadataSent map[string]bool,
	nodeSpecSent map[string]bool,
) ([]string, error) {
	nodeID, err := s.nodeRegistry.Locate(metadata.GetGroup(), metadata.GetName(), uint32(shardID), 0)
	if err != nil {
		return nil, err
	}

	requestToSend := &tracev1.WriteRequest{
		Version: writeEntity.GetVersion(),
		Tags:    writeEntity.GetTags(),
		Span:    writeEntity.GetSpan(),
	}
	if !nodeMetadataSent[nodeID] {
		requestToSend.Metadata = metadata
		nodeMetadataSent[nodeID] = true
	}
	if spec != nil && !nodeSpecSent[nodeID] {
		requestToSend.TagSpec = spec
		nodeSpecSent[nodeID] = true
	}
	iwr := &tracev1.InternalWriteRequest{
		ShardId: uint32(shardID),
		Request: requestToSend,
	}

	message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	if _, err := publisher.Publish(ctx, data.TopicTraceWrite, message); err != nil {
		return nil, err
	}
	return []string{nodeID}, nil
}

func (s *traceService) sendReply(metadata *commonv1.Metadata, status modelv1.Status, version uint64, stream tracev1.TraceService_WriteServer) {
	if metadata == nil {
		s.l.Error().Stringer("status", status).Msg("metadata is nil, cannot send reply")
		return
	}
	if status != modelv1.Status_STATUS_SUCCEED {
		s.metrics.totalStreamMsgReceivedErr.Inc(1, metadata.Group, "trace", "write")
	}
	s.metrics.totalStreamMsgSent.Inc(1, metadata.Group, "trace", "write")
	if errResp := stream.Send(&tracev1.WriteResponse{Metadata: metadata, Status: status.String(), Version: version}); errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to send trace write response")
		}
		s.metrics.totalStreamMsgSentErr.Inc(1, metadata.Group, "trace", "write")
	}
}

func (s *traceService) Write(stream tracev1.TraceService_WriteServer) error {
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
			s.sendReply(ssm.metadata, code, ssm.messageID, stream)
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

	var metadata *commonv1.Metadata
	var spec *tracev1.TagSpec
	var specLocator *traceSpecLocator
	isFirstRequest := true
	nodeMetadataSent := make(map[string]bool)
	nodeSpecSent := make(map[string]bool)

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
			if status.Code(err) != codes.Canceled && status.Code(err) != codes.DeadlineExceeded {
				s.l.Error().Stringer("written", writeEntity).Err(err).Msg("failed to receive message")
			}
			return err
		}

		if writeEntity.GetMetadata() != nil {
			metadata = writeEntity.GetMetadata()
			nodeMetadataSent = make(map[string]bool)
			specLocator = nil
		} else if isFirstRequest {
			s.l.Error().Msg("metadata is required for the first request of gRPC stream")
			s.sendReply(nil, modelv1.Status_STATUS_METADATA_REQUIRED, writeEntity.GetVersion(), stream)
			return errors.New("metadata is required for the first request of gRPC stream")
		}
		isFirstRequest = false
		if writeEntity.GetTagSpec() != nil {
			spec = writeEntity.GetTagSpec()
			nodeSpecSent = make(map[string]bool)
			id := getID(metadata)
			traceEntity, existed := s.entityRepo.getTrace(id)
			if existed {
				specLocator = newTraceSpecLocator(spec, traceEntity.GetTraceIdTagName(), traceEntity.GetTimestampTagName())
			}
		}

		requestCount++
		s.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "trace", "write")

		if s.validateWriteRequest(writeEntity, metadata, specLocator, stream) != modelv1.Status_STATUS_SUCCEED {
			continue
		}

		shardID, err := s.navigateWithRetry(writeEntity, metadata, specLocator)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("navigation failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), stream)
			continue
		}

		if s.ingestionAccessLog != nil {
			if errAL := s.ingestionAccessLog.Write(writeEntity); errAL != nil {
				s.l.Error().Err(errAL).Msg("failed to write ingestion access log")
			}
		}

		nodes, err := s.publishMessages(ctx, publisher, writeEntity, metadata, spec, shardID, nodeMetadataSent, nodeSpecSent)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("publishing failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), stream)
			continue
		}

		succeedSent = append(succeedSent, succeedSentMessage{
			metadata:  metadata,
			messageID: writeEntity.GetVersion(),
			nodes:     nodes,
		})
	}
}

var emptyTraceQueryResponse = &tracev1.QueryResponse{Traces: make([]*tracev1.Trace, 0)}

func (s *traceService) Query(ctx context.Context, req *tracev1.QueryRequest) (resp *tracev1.QueryResponse, err error) {
	for _, g := range req.Groups {
		s.metrics.totalStarted.Inc(1, g, "trace", "query")
	}
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		for _, g := range req.Groups {
			s.metrics.totalFinished.Inc(1, g, "trace", "query")
			if err != nil {
				s.metrics.totalErr.Inc(1, g, "trace", "query")
			}
			s.metrics.totalLatency.Inc(duration.Seconds(), g, "trace", "query")
		}
		// Log query with timing information at the end
		if s.queryAccessLog != nil {
			if errAccessLog := s.queryAccessLog.WriteQuery("trace", start, duration, req, err); errAccessLog != nil {
				s.l.Error().Err(errAccessLog).Msg("query access log error")
			}
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
	var tracer *query.Tracer
	var span *query.Span
	var responseTraceCount int
	if req.Trace {
		tracer, _ = query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "trace-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
				span.Stop()
			} else if resp != nil && resp != emptyTraceQueryResponse {
				span.Tagf("response_trace_count", "%d", responseTraceCount)
				span.AddSubTrace(resp.TraceQueryResult)
				span.Stop()
				resp.TraceQueryResult = tracer.ToProto()
			}
		}()
	}
	message := bus.NewMessage(bus.MessageID(now.UnixNano()), req)
	var future bus.Future
	future, err = s.broadcaster.Publish(ctx, data.TopicTraceQuery, message)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return emptyTraceQueryResponse, nil
		}
		return nil, err
	}
	var msg bus.Message
	msg, err = future.Get()
	if err != nil {
		return nil, err
	}
	switch d := msg.Data().(type) {
	case *tracev1.InternalQueryResponse:
		traces := make([]*tracev1.Trace, 0, len(d.InternalTraces))
		for _, internalTrace := range d.InternalTraces {
			trace := &tracev1.Trace{
				Spans:   internalTrace.Spans,
				TraceId: internalTrace.TraceId,
			}
			traces = append(traces, trace)
		}
		responseTraceCount = len(traces)
		return &tracev1.QueryResponse{
			Traces:           traces,
			TraceQueryResult: d.TraceQueryResult,
		}, nil
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
