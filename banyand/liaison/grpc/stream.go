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
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/run"
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
	queryBudget     *protector.QueryBudget
	protector       protector.Memory
	writeConfig     *writeSessionConfig
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
	metadata *commonv1.Metadata, stream streamResponseSender,
) modelv1.Status {
	if errTime := timestamp.CheckPb(writeEntity.GetElement().Timestamp); errTime != nil {
		s.l.Error().Err(errTime).Stringer("written", writeEntity).Msg("the element time is invalid")
		s.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeEntity.GetMessageId(), stream)
		return modelv1.Status_STATUS_INVALID_TIMESTAMP
	}

	if metadata.ModRevision > 0 {
		id := getID(metadata)
		streamCache, existed := s.entityRepo.getLocator(id)
		if !existed {
			s.l.Error().Stringer("written", writeEntity).Msg("stream schema not found")
			s.sendReply(metadata, modelv1.Status_STATUS_NOT_FOUND, writeEntity.GetMessageId(), stream)
			return modelv1.Status_STATUS_NOT_FOUND
		}
		// See banyand/liaison/grpc/measure.go for the rationale: the gate
		// reads through the per-node NodeRepoRegistry so its verdict tracks
		// the same cache the AwaitXXX barrier and the data-node executor
		// consult; the locator's ModRevision serves as the legacy fallback.
		reg := schemaRevisionRegistry(s.metadataRepo)
		clientRev := metadata.ModRevision
		cacheRev := resolveSchemaRevision(reg, schema.KindStream, metadata.GetGroup(), metadata.GetName(), streamCache.ModRevision)
		if clientRev < cacheRev {
			s.l.Error().Stringer("written", writeEntity).Msg("the stream schema is expired")
			s.metrics.emitStatusExpired(rpcLabelStreamWrite, metadata.GetGroup())
			s.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeEntity.GetMessageId(), stream)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
		if clientRev > cacheRev {
			reached := awaitRevisionReached(func() int64 {
				fallback := int64(0)
				if loc, ok := s.entityRepo.getLocator(id); ok {
					fallback = loc.ModRevision
				}
				return resolveSchemaRevision(reg, schema.KindStream, metadata.GetGroup(), metadata.GetName(), fallback)
			}, clientRev, s.maxWaitDuration)
			if !reached {
				s.metrics.emitStatusNotAppliedTimeout(rpcLabelStreamWrite, metadata.GetGroup())
				s.sendReply(metadata, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, writeEntity.GetMessageId(), stream)
				return modelv1.Status_STATUS_SCHEMA_NOT_APPLIED
			}
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

// streamResponseSender receives the responses of a stream Write stream.
type streamResponseSender interface {
	Send(*streamv1.WriteResponse) error
}

// streamSessionSender queues responses on a write session instead of writing them to the stream.
type streamSessionSender struct {
	session *writeSession
}

func (t streamSessionSender) Send(resp *streamv1.WriteResponse) error {
	t.session.Reply(writeReply{metadata: resp.GetMetadata(), id: resp.GetMessageId(), status: resp.GetStatusCode()})
	return nil
}

func (s *streamService) sendReply(metadata *commonv1.Metadata, status modelv1.Status, messageID uint64, sender streamResponseSender) {
	if metadata == nil {
		s.l.Error().Stringer("status", status).Msg("metadata is nil, cannot send reply")
		return
	}
	if errResp := sender.Send(&streamv1.WriteResponse{Metadata: metadata, Status: status.String(), StatusCode: status, MessageId: messageID}); errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to queue stream write response")
		}
	}
}

// sendResponse writes one queued response to the client. It runs on the write session's sender goroutine only.
func (s *streamService) sendResponse(stream streamv1.StreamService_WriteServer, reply writeReply) error {
	group := reply.metadata.GetGroup()
	if reply.status != modelv1.Status_STATUS_SUCCEED {
		s.metrics.totalStreamMsgReceivedErr.Inc(1, group, "stream", "write")
	}
	s.metrics.totalStreamMsgSent.Inc(1, group, "stream", "write")
	errResp := stream.Send(&streamv1.WriteResponse{
		Metadata:     reply.metadata,
		MessageId:    reply.id,
		Status:       reply.status.String(),
		StatusCode:   reply.status,
		RetryAfterMs: reply.retryAfterMs,
	})
	if errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to send stream write response")
		}
		s.metrics.totalStreamMsgSentErr.Inc(1, group, "stream", "write")
	}
	return errResp
}

func (s *streamService) memoryHigh() bool {
	return s.protector != nil && s.protector.State() == protector.StateHigh
}

type streamRecv struct {
	req *streamv1.WriteRequest
	err error
}

func (s *streamService) Write(stream streamv1.StreamService_WriteServer) error {
	s.metrics.totalStreamStarted.Inc(1, "stream", "write")
	start := time.Now()
	var cfg writeSessionConfig
	if s.writeConfig != nil {
		cfg = *s.writeConfig
	}
	session := newWriteSession(stream.Context(), cfg, writeSessionDeps{
		newPublisher: func() queue.BatchPublisher { return s.pipeline.NewBatchPublisher(s.writeTimeout) },
		send:         func(reply writeReply) error { return s.sendResponse(stream, reply) },
		memoryHigh:   s.memoryHigh,
		release:      s.groupRepo.releaseRequest,
		log:          s.l,
		metrics:      s.metrics,
		service:      "stream",
		topic:        data.TopicStreamWrite,
	})
	requestCount := 0
	err := s.receiveWrites(stream, session, &requestCount)
	finishErr := session.Finish()
	if err == nil {
		err = finishErr
	}
	if dl := s.l.Debug(); dl.Enabled() {
		dl.Int("total_requests", requestCount).Msg("completed stream write stream")
	}
	s.metrics.totalStreamFinished.Inc(1, "stream", "write")
	s.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "stream", "write")
	return err
}

// receiveWrites reads the client's rows into the session until the client half-closes (nil), the stream breaks,
// or the session fails. Rows are validated and routed here; their acknowledgement is the session's business.
func (s *streamService) receiveWrites(stream streamv1.StreamService_WriteServer, session *writeSession, requestCount *int) error {
	ctx := stream.Context()
	recvCh := make(chan streamRecv)
	stop := make(chan struct{})
	defer close(stop)
	// Recv runs on its own goroutine so that a session failure ends the stream even when the client is idle.
	run.Go(ctx, "stream-write-recv", s.l, func(context.Context) {
		for {
			req, err := stream.Recv()
			select {
			case recvCh <- streamRecv{req: req, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	})
	sender := streamSessionSender{session: session}

	var metadata *commonv1.Metadata
	var spec []*streamv1.TagFamilySpec
	var specLocator *specLocator
	var epoch uint64
	isFirstRequest := true

	for {
		var in streamRecv
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.Fatal():
			return session.Err()
		case in = <-recvCh:
		}
		writeEntity, err := in.req, in.err
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
			// Clients may repeat the metadata on every message; only a different target starts a new batch.
			if metadata == nil || !proto.Equal(metadata, writeEntity.GetMetadata()) {
				epoch++
			}
			metadata = writeEntity.GetMetadata()
		} else if isFirstRequest {
			s.l.Error().Msg("metadata is required for the first request of gRPC stream")
			s.sendReply(nil, modelv1.Status_STATUS_METADATA_REQUIRED, writeEntity.GetMessageId(), sender)
			return errors.New("metadata is required for the first request of gRPC stream")
		}
		isFirstRequest = false
		if writeEntity.GetTagFamilySpec() != nil {
			if !sameStreamSpec(spec, writeEntity.GetTagFamilySpec()) {
				epoch++
			}
			spec = writeEntity.GetTagFamilySpec()
			specLocator = s.buildSpecLocator(metadata, spec)
		}

		*requestCount++
		s.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "stream", "write")

		if acquireErr := s.groupRepo.acquireRequest(metadata.Group); acquireErr != nil {
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), sender)
			continue
		}

		if s.validateWriteRequest(writeEntity, metadata, sender) != modelv1.Status_STATUS_SUCCEED {
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		tagValues, shardID, err := s.navigateWithRetry(writeEntity, metadata, specLocator)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("navigation failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), sender)
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		if s.ingestionAccessLog != nil {
			if errAL := s.ingestionAccessLog.Write(writeEntity); errAL != nil {
				s.l.Error().Err(errAL).Msg("failed to write ingestion access log")
			}
		}

		if err = s.appendRow(session, writeEntity, metadata, spec, shardID, tagValues, epoch); err != nil {
			s.groupRepo.releaseRequest(metadata.Group)
			if !errors.Is(err, errWriteRowRejected) {
				return err
			}
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("publishing failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetMessageId(), sender)
		}
	}
}

// appendRow locates the row's node and appends it to that node's open batch. A nil error means the session owns
// the row and will release its group request when the batch is acknowledged. A rejection that concerns only this
// row wraps errWriteRowRejected; any other error means the session has failed or the stream is gone.
func (s *streamService) appendRow(session *writeSession, writeEntity *streamv1.WriteRequest, metadata *commonv1.Metadata,
	spec []*streamv1.TagFamilySpec, shardID common.ShardID, tagValues pbv1.EntityValues, epoch uint64,
) error {
	nodeID, err := s.nodeRegistry.Locate(metadata.GetGroup(), metadata.GetName(), uint32(shardID), 0)
	if err != nil {
		return fmt.Errorf("%w: %w", errWriteRowRejected, err)
	}
	err = session.Append(nodeID, metadata.GetGroup(), epoch, writeAck{metadata: metadata, id: writeEntity.GetMessageId()},
		func(first bool) ([]byte, error) {
			request := &streamv1.WriteRequest{
				MessageId: writeEntity.GetMessageId(),
				Element:   writeEntity.GetElement(),
			}
			if first {
				request.Metadata = metadata
				request.TagFamilySpec = spec
			}
			return proto.Marshal(&streamv1.InternalWriteRequest{
				Request:      request,
				ShardId:      uint32(shardID),
				EntityValues: tagValues[1:].Encode(),
			})
		})
	if errors.Is(err, errWriteRowSerialize) {
		return fmt.Errorf("%w: %w", errWriteRowRejected, err)
	}
	return err
}

var emptyStreamQueryResponse = &streamv1.QueryResponse{Elements: make([]*streamv1.Element, 0)}

func (s *streamService) Query(ctx context.Context, req *streamv1.QueryRequest) (resp *streamv1.QueryResponse, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is nil")
	}
	ctx, release, admissionErr := admitQuery(ctx, s.queryBudget, req.GetLimit(), req.GetOffset(), 20)
	if admissionErr != nil {
		return nil, admissionErr
	}
	defer release()
	defer func() {
		if err == nil {
			err = query.ChargeResponse(ctx, resp)
			if err != nil {
				resp = nil
			}
		}
		err = queryStatus(err)
	}()
	return s.query(ctx, req)
}

func (s *streamService) query(ctx context.Context, req *streamv1.QueryRequest) (resp *streamv1.QueryResponse, err error) {
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
	streamReg := schemaRevisionRegistry(s.metadataRepo)
	gatedStatuses, shortCircuit := checkQueryGate(req.Groups, req.Name, req.GroupModRevisions,
		func(name, group string) (int64, bool) {
			loc, ok := s.entityRepo.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(streamReg, schema.KindStream, group, name, loc.ModRevision, ok)
		}, s.maxWaitDuration)
	recordQueryGateStatuses(s.metrics, rpcLabelStreamQuery, gatedStatuses)
	if shortCircuit {
		return &streamv1.QueryResponse{GroupStatuses: gatedStatuses}, nil
	}
	// See measure.go for rationale: gate the clamp on the same opt-in trigger as the
	// query gate so legacy clients that omit GroupModRevisions are not affected.
	if req.TimeRange != nil && len(req.GroupModRevisions) > 0 {
		createdAts := make([]time.Time, 0, len(req.Groups))
		for _, group := range req.Groups {
			streamEntity, streamOK := s.entityRepo.getStream(identity{name: req.Name, group: group})
			if streamOK && streamEntity.GetCreatedAt() != nil {
				createdAts = append(createdAts, streamEntity.GetCreatedAt().AsTime())
			}
		}
		endTime := time.Time{}
		if req.TimeRange.GetEnd() != nil {
			endTime = req.TimeRange.GetEnd().AsTime()
		}
		originalBegin := req.TimeRange.GetBegin().AsTime()
		newBegin, rangeEmpty := clampTimeRangeBegin(originalBegin, endTime, createdAts)
		if rangeEmpty {
			if len(gatedStatuses) > 0 {
				return &streamv1.QueryResponse{GroupStatuses: gatedStatuses}, nil
			}
			return emptyStreamQueryResponse, nil
		}
		if newBegin != originalBegin {
			req.TimeRange.Begin = timestamppb.New(newBegin)
		}
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
		if len(gatedStatuses) > 0 {
			d.GroupStatuses = gatedStatuses
		}
		return d, nil
	case *common.Error:
		return nil, dataNodeQueryError(d)
	}
	return nil, nil
}

func (s *streamService) Close() error {
	if s.ingestionAccessLog != nil {
		return s.ingestionAccessLog.Close()
	}
	return nil
}

// sameStreamSpec reports whether two tag family specs are identical, so a client
// repeating its spec on every message does not start a new batch each time.
func sameStreamSpec(a, b []*streamv1.TagFamilySpec) bool {
	if a == nil || len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
