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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type streamTagFamilySpec struct {
	*streamv1.TagFamilySpec
}

func (s streamTagFamilySpec) GetName() string {
	return s.TagFamilySpec.GetName()
}

func (s streamTagFamilySpec) GetTagNames() []string {
	return s.TagFamilySpec.GetTagNames()
}

type streamService struct {
	streamv1.UnimplementedStreamServiceServer
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

func (s *streamService) setLogger(log *logger.Logger) {
	s.l = log
}

func (s *streamService) activeIngestionAccessLog(root string, sampled bool) (err error) {
	if s.ingestionAccessLog, err = accesslog.
		NewFileLog(root, "stream-ingest-%s", 10*time.Minute, s.log, sampled); err != nil {
		return err
	}
	return nil
}

func (s *streamService) activeQueryAccessLog(root string, sampled bool) (err error) {
	if s.queryAccessLog, err = accesslog.
		NewFileLog(root, "stream-query-%s", 10*time.Minute, s.log, sampled); err != nil {
		return err
	}
	return nil
}

func (s *streamService) validateWriteRequest(writeEntity *streamv1.WriteRequest,
	metadata *commonv1.Metadata, stream streamv1.StreamService_WriteServer,
) modelv1.Status {
	if errTime := timestamp.CheckPb(writeEntity.GetElement().Timestamp); errTime != nil {
		s.l.Error().Err(errTime).Stringer("written", writeEntity).Msg("the element time is invalid")
		s.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetMessageId(), stream)
		return modelv1.Status_STATUS_INVALID_TIMESTAMP
	}

	if metadata.ModRevision > 0 {
		streamCache, existed := s.entityRepo.getLocator(getID(metadata))
		if !existed {
			s.l.Error().Stringer("written", writeEntity).Msg("stream schema not found")
			s.sendReply(metadata, modelv1.Status_STATUS_NOT_FOUND, writeEntity.GetMessageId(), stream)
			return modelv1.Status_STATUS_NOT_FOUND
		}
		if metadata.ModRevision != streamCache.ModRevision {
			s.l.Error().Stringer("written", writeEntity).Msg("the stream schema is expired")
			s.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeEntity.GetMessageId(), stream)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
	}

	return modelv1.Status_STATUS_SUCCEED
}

func (s *streamService) navigate(metadata *commonv1.Metadata, writeRequest *streamv1.WriteRequest,
	specLocator *specLocator,
) (pbv1.EntityValues, common.ShardID, error) {
	tagFamilies := writeRequest.GetElement().GetTagFamilies()
	return s.navigateByLocator(metadata, tagFamilies, specLocator, nil)
}

func (s *streamService) buildSpecLocator(metadata *commonv1.Metadata, spec []*streamv1.TagFamilySpec) *specLocator {
	if spec == nil {
		return nil
	}
	id := getID(metadata)
	stream, ok := s.entityRepo.getStream(id)
	if !ok {
		return nil
	}
	specFamilies := make([]tagFamilySpec, len(spec))
	for i, f := range spec {
		specFamilies[i] = streamTagFamilySpec{f}
	}
	return newSpecLocator(stream.GetTagFamilies(), stream.GetEntity().GetTagNames(), specFamilies)
}

func (s *streamService) navigateWithRetry(writeEntity *streamv1.WriteRequest, metadata *commonv1.Metadata,
	specLocator *specLocator,
) (tagValues pbv1.EntityValues, shardID common.ShardID, err error) {
	if s.maxWaitDuration > 0 {
		retryInterval := 10 * time.Millisecond
		startTime := time.Now()
		for {
			tagValues, shardID, err = s.navigate(metadata, writeEntity, specLocator)
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

func (s *streamService) publishMessages(
	ctx context.Context,
	publisher queue.BatchPublisher,
	writeEntity *streamv1.WriteRequest,
	metadata *commonv1.Metadata,
	spec []*streamv1.TagFamilySpec,
	shardID common.ShardID,
	tagValues pbv1.EntityValues,
	nodeMetadataSent map[string]bool,
	nodeSpecSent map[string]bool,
) ([]string, error) {
	iwr := &streamv1.InternalWriteRequest{
		Request:      writeEntity,
		ShardId:      uint32(shardID),
		EntityValues: tagValues[1:].Encode(),
	}
	nodeID, err := s.nodeRegistry.Locate(metadata.GetGroup(), metadata.GetName(), uint32(shardID), 0)
	if err != nil {
		return nil, err
	}

	if !nodeMetadataSent[nodeID] {
		iwr.Request.Metadata = metadata
		nodeMetadataSent[nodeID] = true
	}
	if spec != nil && !nodeSpecSent[nodeID] {
		iwr.Request.TagFamilySpec = spec
		nodeSpecSent[nodeID] = true
	}

	message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
	if _, err := publisher.Publish(ctx, data.TopicStreamWrite, message); err != nil {
		return nil, err
	}
	return []string{nodeID}, nil
}

func (s *streamService) sendReply(metadata *commonv1.Metadata, status modelv1.Status, messageID uint64, stream streamv1.StreamService_WriteServer) {
	if metadata == nil {
		s.l.Error().Stringer("status", status).Msg("metadata is nil, cannot send reply")
		return
	}
	if status != modelv1.Status_STATUS_SUCCEED {
		s.metrics.totalStreamMsgReceivedErr.Inc(1, metadata.Group, "stream", "write")
	}
	s.metrics.totalStreamMsgSent.Inc(1, metadata.Group, "stream", "write")
	if errResp := stream.Send(&streamv1.WriteResponse{Metadata: metadata, Status: status.String(), MessageId: messageID}); errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to send stream write response")
		}
		s.metrics.totalStreamMsgSentErr.Inc(1, metadata.Group, "stream", "write")
	}
}

func (s *streamService) Write(stream streamv1.StreamService_WriteServer) error {
	s.metrics.totalStreamStarted.Inc(1, "stream", "write")
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
			dl.Int("total_requests", requestCount).Msg("completed stream write batch")
		}
		s.metrics.totalStreamFinished.Inc(1, "stream", "write")
		s.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "stream", "write")
	}()

	ctx := stream.Context()

	var metadata *commonv1.Metadata
	var spec []*streamv1.TagFamilySpec
	var specLocator *specLocator
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
		} else if isFirstRequest {
			s.l.Error().Msg("metadata is required for the first request of gRPC stream")
			s.sendReply(nil, modelv1.Status_STATUS_METADATA_REQUIRED, writeEntity.GetMessageId(), stream)
			return errors.New("metadata is required for the first request of gRPC stream")
		}
		isFirstRequest = false
		if writeEntity.GetTagFamilySpec() != nil {
			spec = writeEntity.GetTagFamilySpec()
			nodeSpecSent = make(map[string]bool)
			specLocator = s.buildSpecLocator(metadata, spec)
		}

		requestCount++
		s.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "stream", "write")

		if acquireErr := s.groupRepo.acquireRequest(metadata.Group); acquireErr != nil {
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), stream)
			continue
		}

		if s.validateWriteRequest(writeEntity, metadata, stream) != modelv1.Status_STATUS_SUCCEED {
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		tagValues, shardID, err := s.navigateWithRetry(writeEntity, metadata, specLocator)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("navigation failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), stream)
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		if s.ingestionAccessLog != nil {
			if errAL := s.ingestionAccessLog.Write(writeEntity); errAL != nil {
				s.l.Error().Err(errAL).Msg("failed to write ingestion access log")
			}
		}

		nodes, err := s.publishMessages(ctx, publisher, writeEntity, metadata, spec, shardID, tagValues, nodeMetadataSent, nodeSpecSent)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("publishing failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), stream)
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}
		s.groupRepo.releaseRequest(metadata.Group)

		succeedSent = append(succeedSent, succeedSentMessage{
			metadata:  metadata,
			messageID: writeEntity.GetMessageId(),
			nodes:     nodes,
		})
	}
}

var emptyStreamQueryResponse = &streamv1.QueryResponse{Elements: make([]*streamv1.Element, 0)}

func (s *streamService) Query(ctx context.Context, req *streamv1.QueryRequest) (resp *streamv1.QueryResponse, err error) {
	for _, g := range req.Groups {
		if acquireErr := s.groupRepo.acquireRequest(g); acquireErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "group %s is pending deletion", g)
		}
	}
	defer func() {
		for _, g := range req.Groups {
			s.groupRepo.releaseRequest(g)
		}
	}()
	for _, g := range req.Groups {
		s.metrics.totalStarted.Inc(1, g, "stream", "query")
	}
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		for _, g := range req.Groups {
			s.metrics.totalFinished.Inc(1, g, "stream", "query")
			if err != nil {
				s.metrics.totalErr.Inc(1, g, "stream", "query")
			}
			s.metrics.totalLatency.Inc(duration.Seconds(), g, "stream", "query")
		}
		// Log query with timing information at the end
		if s.queryAccessLog != nil {
			if errAccessLog := s.queryAccessLog.WriteQuery("stream", start, duration, req, err); errAccessLog != nil {
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
	var responseElementCount int
	if req.Trace {
		tracer, _ = query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "stream-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
				span.Stop()
			} else {
				span.Tagf("response_element_count", "%d", responseElementCount)
				span.AddSubTrace(resp.Trace)
				span.Stop()
				resp.Trace = tracer.ToProto()
			}
		}()
	}
	message := bus.NewMessage(bus.MessageID(now.UnixNano()), req)
	feat, errQuery := s.broadcaster.Publish(ctx, data.TopicStreamQuery, message)
	if errQuery != nil {
		if errors.Is(errQuery, io.EOF) {
			return emptyStreamQueryResponse, nil
		}
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case *streamv1.QueryResponse:
		responseElementCount = len(d.Elements)
		return d, nil
	case *common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Error())
	}
	return nil, nil
}

func (s *streamService) Close() error {
	if s.ingestionAccessLog != nil {
		return s.ingestionAccessLog.Close()
	}
	return nil
}
