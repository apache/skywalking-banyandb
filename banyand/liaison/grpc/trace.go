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
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/run"
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
	queryBudget     *protector.QueryBudget
	protector       protector.Memory
	writeConfig     *writeSessionConfig
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
	metadata *commonv1.Metadata, specLocator *traceSpecLocator, stream traceResponseSender,
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
		// See banyand/liaison/grpc/measure.go for the rationale: the gate
		// reads through the per-node NodeRepoRegistry so its verdict tracks
		// the same cache the AwaitXXX barrier and the data-node executor
		// consult; the entityRepo trace's ModRevision serves as the legacy
		// fallback when the metadata service is not a metadata.Service.
		reg := schemaRevisionRegistry(s.metadataRepo)
		clientRev := metadata.ModRevision
		cacheRev := resolveSchemaRevision(reg, schema.KindTrace, metadata.GetGroup(), metadata.GetName(), traceEntity.GetMetadata().GetModRevision())
		if clientRev < cacheRev {
			s.l.Error().Stringer("written", writeEntity).Msg("the trace schema is expired")
			s.metrics.emitStatusExpired(rpcLabelTraceWrite, metadata.GetGroup())
			s.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeEntity.GetVersion(), stream)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
		if clientRev > cacheRev {
			reached := awaitRevisionReached(func() int64 {
				fallback := int64(0)
				if e, ok := s.entityRepo.getTrace(id); ok {
					fallback = e.GetMetadata().GetModRevision()
				}
				return resolveSchemaRevision(reg, schema.KindTrace, metadata.GetGroup(), metadata.GetName(), fallback)
			}, clientRev, s.maxWaitDuration)
			if !reached {
				s.metrics.emitStatusNotAppliedTimeout(rpcLabelTraceWrite, metadata.GetGroup())
				s.sendReply(metadata, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, writeEntity.GetVersion(), stream)
				return modelv1.Status_STATUS_SCHEMA_NOT_APPLIED
			}
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
	return partition.TraceShardID(traceID, shardCount)
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

// traceResponseSender receives the responses of a trace Write stream.
type traceResponseSender interface {
	Send(*tracev1.WriteResponse) error
}

// traceSessionSender queues responses on a write session instead of writing them to the stream.
type traceSessionSender struct {
	session *writeSession
}

func (t traceSessionSender) Send(resp *tracev1.WriteResponse) error {
	t.session.Reply(writeReply{metadata: resp.GetMetadata(), id: resp.GetVersion(), status: resp.GetStatusCode()})
	return nil
}

func (s *traceService) sendReply(metadata *commonv1.Metadata, status modelv1.Status, version uint64, sender traceResponseSender) {
	if metadata == nil {
		s.l.Error().Stringer("status", status).Msg("metadata is nil, cannot send reply")
		return
	}
	if errResp := sender.Send(&tracev1.WriteResponse{Metadata: metadata, Status: status.String(), StatusCode: status, Version: version}); errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to queue trace write response")
		}
	}
}

// sendResponse writes one queued response to the client. It runs on the write session's sender goroutine only.
func (s *traceService) sendResponse(stream tracev1.TraceService_WriteServer, reply writeReply) error {
	group := reply.metadata.GetGroup()
	if reply.status != modelv1.Status_STATUS_SUCCEED {
		s.metrics.totalStreamMsgReceivedErr.Inc(1, group, "trace", "write")
	}
	s.metrics.totalStreamMsgSent.Inc(1, group, "trace", "write")
	errResp := stream.Send(&tracev1.WriteResponse{
		Metadata:     reply.metadata,
		Version:      reply.id,
		Status:       reply.status.String(),
		StatusCode:   reply.status,
		RetryAfterMs: reply.retryAfterMs,
	})
	if errResp != nil {
		if dl := s.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to send trace write response")
		}
		s.metrics.totalStreamMsgSentErr.Inc(1, group, "trace", "write")
	}
	return errResp
}

func (s *traceService) memoryHigh() bool {
	return s.protector != nil && s.protector.State() == protector.StateHigh
}

type traceRecv struct {
	req *tracev1.WriteRequest
	err error
}

func (s *traceService) Write(stream tracev1.TraceService_WriteServer) error {
	s.metrics.totalStreamStarted.Inc(1, "trace", "write")
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
		service:      "trace",
		topic:        data.TopicTraceWrite,
	})
	requestCount := 0
	err := s.receiveWrites(stream, session, &requestCount)
	finishErr := session.Finish()
	if err == nil {
		err = finishErr
	}
	if dl := s.l.Debug(); dl.Enabled() {
		dl.Int("total_requests", requestCount).Msg("completed trace write stream")
	}
	s.metrics.totalStreamFinished.Inc(1, "trace", "write")
	s.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "trace", "write")
	return err
}

// receiveWrites reads the client's rows into the session until the client half-closes (nil), the stream breaks,
// or the session fails. Rows are validated and routed here; their acknowledgement is the session's business.
func (s *traceService) receiveWrites(stream tracev1.TraceService_WriteServer, session *writeSession, requestCount *int) error {
	ctx := stream.Context()
	recvCh := make(chan traceRecv)
	stop := make(chan struct{})
	defer close(stop)
	// Recv runs on its own goroutine so that a session failure ends the stream even when the client is idle.
	run.Go(ctx, "trace-write-recv", s.l, func(context.Context) {
		for {
			req, err := stream.Recv()
			select {
			case recvCh <- traceRecv{req: req, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	})
	sender := traceSessionSender{session: session}

	var metadata *commonv1.Metadata
	var spec *tracev1.TagSpec
	var specLocator *traceSpecLocator
	var epoch uint64
	isFirstRequest := true

	for {
		var in traceRecv
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
			specLocator = nil
		} else if isFirstRequest {
			s.l.Error().Msg("metadata is required for the first request of gRPC stream")
			s.sendReply(nil, modelv1.Status_STATUS_METADATA_REQUIRED, writeEntity.GetVersion(), sender)
			return errors.New("metadata is required for the first request of gRPC stream")
		}
		isFirstRequest = false
		if writeEntity.GetTagSpec() != nil {
			if spec == nil || !proto.Equal(spec, writeEntity.GetTagSpec()) {
				epoch++
			}
			spec = writeEntity.GetTagSpec()
			id := getID(metadata)
			traceEntity, existed := s.entityRepo.getTrace(id)
			if existed {
				specLocator = newTraceSpecLocator(spec, traceEntity.GetTraceIdTagName(), traceEntity.GetTimestampTagName())
			}
		}

		*requestCount++
		s.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "trace", "write")

		if acquireErr := s.groupRepo.acquireRequest(metadata.Group); acquireErr != nil {
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), sender)
			continue
		}

		if s.validateWriteRequest(writeEntity, metadata, specLocator, sender) != modelv1.Status_STATUS_SUCCEED {
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		shardID, err := s.navigateWithRetry(writeEntity, metadata, specLocator)
		if err != nil {
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("navigation failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), sender)
			s.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		if s.ingestionAccessLog != nil {
			if errAL := s.ingestionAccessLog.Write(writeEntity); errAL != nil {
				s.l.Error().Err(errAL).Msg("failed to write ingestion access log")
			}
		}

		if err = s.appendRow(session, writeEntity, metadata, spec, shardID, epoch); err != nil {
			s.groupRepo.releaseRequest(metadata.Group)
			if !errors.Is(err, errWriteRowRejected) {
				return err
			}
			s.l.Error().Err(err).RawJSON("written", logger.Proto(writeEntity)).Msg("publishing failed")
			s.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeEntity.GetVersion(), sender)
		}
	}
}

// appendRow locates the row's node and appends it to that node's open batch. A nil error means the session owns
// the row and will release its group request when the batch is acknowledged. A rejection that concerns only this
// row wraps errWriteRowRejected; any other error means the session has failed or the stream is gone.
func (s *traceService) appendRow(session *writeSession, writeEntity *tracev1.WriteRequest,
	metadata *commonv1.Metadata, spec *tracev1.TagSpec, shardID common.ShardID, epoch uint64,
) error {
	nodeID, err := s.nodeRegistry.Locate(metadata.GetGroup(), metadata.GetName(), uint32(shardID), 0)
	if err != nil {
		return fmt.Errorf("%w: %w", errWriteRowRejected, err)
	}
	err = session.Append(nodeID, metadata.GetGroup(), epoch, writeAck{metadata: metadata, id: writeEntity.GetVersion()},
		func(first bool) ([]byte, error) {
			request := &tracev1.WriteRequest{
				Version: writeEntity.GetVersion(),
				Tags:    writeEntity.GetTags(),
				Span:    writeEntity.GetSpan(),
			}
			if first {
				request.Metadata = metadata
				request.TagSpec = spec
			}
			return proto.Marshal(&tracev1.InternalWriteRequest{ShardId: uint32(shardID), Request: request})
		})
	if errors.Is(err, errWriteRowSerialize) {
		return fmt.Errorf("%w: %w", errWriteRowRejected, err)
	}
	return err
}

var emptyTraceQueryResponse = &tracev1.QueryResponse{Traces: make([]*tracev1.Trace, 0)}

// applyClamp rewrites req.TimeRange.Begin to schema.CreatedAt for schema-aware
// queries (those carrying GroupModRevisions). Returns true when the clamped
// range is empty, in which case the caller should short-circuit with an empty
// response. See measure.go for the rationale on the GroupModRevisions opt-in.
func (s *traceService) applyClamp(req *tracev1.QueryRequest) bool {
	if req.TimeRange == nil || len(req.GroupModRevisions) == 0 {
		return false
	}
	createdAts := make([]time.Time, 0, len(req.Groups))
	for _, group := range req.Groups {
		traceEntity, traceOK := s.entityRepo.getTrace(identity{name: req.Name, group: group})
		if traceOK && traceEntity.GetCreatedAt() != nil {
			createdAts = append(createdAts, traceEntity.GetCreatedAt().AsTime())
		}
	}
	endTime := time.Time{}
	if req.TimeRange.GetEnd() != nil {
		endTime = req.TimeRange.GetEnd().AsTime()
	}
	originalBegin := req.TimeRange.GetBegin().AsTime()
	newBegin, rangeEmpty := clampTimeRangeBegin(originalBegin, endTime, createdAts)
	if rangeEmpty {
		return true
	}
	if newBegin != originalBegin {
		req.TimeRange.Begin = timestamppb.New(newBegin)
	}
	return false
}

func (s *traceService) Query(ctx context.Context, req *tracev1.QueryRequest) (resp *tracev1.QueryResponse, err error) {
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
	traceReg := schemaRevisionRegistry(s.metadataRepo)
	gatedStatuses, shortCircuit := checkQueryGate(req.Groups, req.Name, req.GroupModRevisions,
		func(name, group string) (int64, bool) {
			loc, ok := s.entityRepo.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(traceReg, schema.KindTrace, group, name, loc.ModRevision, ok)
		}, s.maxWaitDuration)
	recordQueryGateStatuses(s.metrics, rpcLabelTraceQuery, gatedStatuses)
	if shortCircuit {
		return &tracev1.QueryResponse{GroupStatuses: gatedStatuses}, nil
	}
	if rangeEmpty := s.applyClamp(req); rangeEmpty {
		if len(gatedStatuses) > 0 {
			return &tracev1.QueryResponse{GroupStatuses: gatedStatuses}, nil
		}
		return emptyTraceQueryResponse, nil
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
		queryResp := &tracev1.QueryResponse{
			Traces:           traces,
			TraceQueryResult: d.TraceQueryResult,
		}
		if len(gatedStatuses) > 0 {
			queryResp.GroupStatuses = gatedStatuses
		}
		return queryResp, nil
	case *common.Error:
		return nil, dataNodeQueryError(d)
	}
	return nil, nil
}

func (s *traceService) Close() error {
	if s.ingestionAccessLog != nil {
		return s.ingestionAccessLog.Close()
	}
	return nil
}
