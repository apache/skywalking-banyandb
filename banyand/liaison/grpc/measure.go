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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type measureTagFamilySpec struct {
	*measurev1.TagFamilySpec
}

func (m measureTagFamilySpec) GetName() string {
	return m.TagFamilySpec.GetName()
}

func (m measureTagFamilySpec) GetTagNames() []string {
	return m.TagFamilySpec.GetTagNames()
}

type measureService struct {
	measurev1.UnimplementedMeasureServiceServer
	ingestionAccessLog accesslog.Log
	queryAccessLog     accesslog.Log
	pipeline           queue.Client
	broadcaster        queue.Client
	*discoveryService
	l               *logger.Logger
	metrics         *metrics
	queryBudget     *protector.QueryBudget
	writeTimeout    time.Duration
	maxWaitDuration time.Duration
}

func (ms *measureService) setLogger(log *logger.Logger) {
	ms.l = log
}

func (ms *measureService) activeIngestionAccessLog(root string, sampled bool) (err error) {
	if ms.ingestionAccessLog, err = accesslog.
		NewFileLog(root, "measure-ingest-%s", 10*time.Minute, ms.log, sampled); err != nil {
		return err
	}
	return nil
}

func (ms *measureService) activeQueryAccessLog(root string, sampled bool) (err error) {
	if ms.queryAccessLog, err = accesslog.
		NewFileLog(root, "measure-query-%s", 10*time.Minute, ms.log, sampled); err != nil {
		return err
	}
	return nil
}

func (ms *measureService) Write(measure measurev1.MeasureService_WriteServer) error {
	ctx := measure.Context()
	publisher := ms.pipeline.NewBatchPublisher(ms.writeTimeout)
	ms.metrics.totalStreamStarted.Inc(1, "measure", "write")
	start := time.Now()
	var succeedSent []succeedSentMessage

	defer ms.handleWriteCleanup(publisher, &succeedSent, measure, start)

	var metadata *commonv1.Metadata
	var spec *measurev1.DataPointSpec
	var specEntityLocator *specLocator
	var specShardingKeyLocator *specLocator
	isFirstRequest := true
	nodeMetadataSent := make(map[string]bool)
	nodeSpecSent := make(map[string]bool)

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
			if status.Code(err) != codes.Canceled && status.Code(err) != codes.DeadlineExceeded {
				ms.l.Error().Err(err).Stringer("written", writeRequest).Msg("failed to receive message")
			}
			return err
		}

		if writeRequest.GetMetadata() != nil {
			metadata = writeRequest.GetMetadata()
			nodeMetadataSent = make(map[string]bool)
		} else if isFirstRequest {
			ms.l.Error().Msg("metadata is required for the first request of gRPC stream")
			ms.sendReply(nil, modelv1.Status_STATUS_METADATA_REQUIRED, writeRequest.GetMessageId(), measure)
			return errors.New("metadata is required for the first request of gRPC stream")
		}
		isFirstRequest = false
		if writeRequest.GetDataPointSpec() != nil {
			spec = writeRequest.GetDataPointSpec()
			nodeSpecSent = make(map[string]bool)
			specEntityLocator, specShardingKeyLocator = ms.buildSpecLocators(metadata, spec)
		}

		ms.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "measure", "write")

		if acquireErr := ms.groupRepo.acquireRequest(metadata.Group); acquireErr != nil {
			ms.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure)
			continue
		}

		if status := ms.validateWriteRequest(writeRequest, metadata, measure); status != modelv1.Status_STATUS_SUCCEED {
			ms.groupRepo.releaseRequest(metadata.Group)
			continue
		}

		if err := ms.processAndPublishRequest(ctx, writeRequest, metadata, spec,
			specEntityLocator, specShardingKeyLocator, publisher, &succeedSent, measure, nodeMetadataSent, nodeSpecSent); err != nil {
			ms.groupRepo.releaseRequest(metadata.Group)
			continue
		}
		ms.groupRepo.releaseRequest(metadata.Group)
	}
}

func (ms *measureService) buildSpecLocators(metadata *commonv1.Metadata, spec *measurev1.DataPointSpec) (*specLocator, *specLocator) {
	if spec == nil {
		return nil, nil
	}
	id := getID(metadata)
	measure, ok := ms.entityRepo.getMeasure(id)
	if !ok {
		return nil, nil
	}
	specFamilies := make([]tagFamilySpec, len(spec.GetTagFamilySpec()))
	for i, f := range spec.GetTagFamilySpec() {
		specFamilies[i] = measureTagFamilySpec{f}
	}
	entityLocator := newSpecLocator(measure.GetTagFamilies(), measure.GetEntity().GetTagNames(), specFamilies)
	var shardingKeyLocator *specLocator
	shardingKey := measure.GetShardingKey()
	if shardingKey != nil && len(shardingKey.GetTagNames()) > 0 {
		shardingKeyLocator = newSpecLocator(measure.GetTagFamilies(), shardingKey.GetTagNames(), specFamilies)
	}
	return entityLocator, shardingKeyLocator
}

func (ms *measureService) validateWriteRequest(writeRequest *measurev1.WriteRequest,
	metadata *commonv1.Metadata, measure measurev1.MeasureService_WriteServer,
) modelv1.Status {
	if errTime := timestamp.CheckPb(writeRequest.DataPoint.Timestamp); errTime != nil {
		ms.l.Error().Err(errTime).Stringer("written", writeRequest).Msg("the data point time is invalid")
		ms.sendReply(metadata, modelv1.Status_STATUS_INVALID_TIMESTAMP, writeRequest.GetMessageId(), measure)
		return modelv1.Status_STATUS_INVALID_TIMESTAMP
	}

	if metadata.ModRevision > 0 {
		id := getID(metadata)
		measureCache, existed := ms.entityRepo.getLocator(id)
		if !existed {
			ms.l.Error().Stringer("written", writeRequest).Msg("measure schema not found")
			ms.sendReply(metadata, modelv1.Status_STATUS_NOT_FOUND, writeRequest.GetMessageId(), measure)
			return modelv1.Status_STATUS_NOT_FOUND
		}
		// Read the cache revision from the per-node NodeRepoRegistry — the
		// same view AwaitRevisionApplied / AwaitSchemaApplied consult — so the
		// gate's verdict is in lockstep with the barrier on this node and the
		// data-node executor cannot miss a key the gate just certified. The
		// locator's ModRevision is the legacy fallback for fixtures where the
		// metadata service does not implement metadata.Service.
		reg := schemaRevisionRegistry(ms.metadataRepo)
		clientRev := metadata.ModRevision
		cacheRev := resolveSchemaRevision(reg, schema.KindMeasure, metadata.GetGroup(), metadata.GetName(), measureCache.ModRevision)
		if clientRev < cacheRev {
			ms.l.Error().Stringer("written", writeRequest).Msg("the measure schema is expired")
			ms.metrics.emitStatusExpired(rpcLabelMeasureWrite, metadata.GetGroup())
			ms.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeRequest.GetMessageId(), measure)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
		if clientRev > cacheRev {
			reached := awaitRevisionReached(func() int64 {
				fallback := int64(0)
				if loc, ok := ms.entityRepo.getLocator(id); ok {
					fallback = loc.ModRevision
				}
				return resolveSchemaRevision(reg, schema.KindMeasure, metadata.GetGroup(), metadata.GetName(), fallback)
			}, clientRev, ms.maxWaitDuration)
			if !reached {
				ms.metrics.emitStatusNotAppliedTimeout(rpcLabelMeasureWrite, metadata.GetGroup())
				ms.sendReply(metadata, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, writeRequest.GetMessageId(), measure)
				return modelv1.Status_STATUS_SCHEMA_NOT_APPLIED
			}
		}
	}

	return modelv1.Status_STATUS_SUCCEED
}

func (ms *measureService) processAndPublishRequest(ctx context.Context, writeRequest *measurev1.WriteRequest,
	metadata *commonv1.Metadata, spec *measurev1.DataPointSpec,
	specEntityLocator *specLocator, specShardingKeyLocator *specLocator,
	publisher queue.BatchPublisher, succeedSent *[]succeedSentMessage, measure measurev1.MeasureService_WriteServer,
	nodeMetadataSent map[string]bool, nodeSpecSent map[string]bool,
) error {
	// Retry with backoff when encountering errNotExist
	var tagValues pbv1.EntityValues
	var shardID common.ShardID
	var err error

	if ms.maxWaitDuration > 0 {
		retryInterval := 10 * time.Millisecond
		startTime := time.Now()
		for {
			tagValues, shardID, err = ms.navigate(metadata, writeRequest, specEntityLocator, specShardingKeyLocator)
			if err == nil || !errors.Is(err, errNotExist) || time.Since(startTime) > ms.maxWaitDuration {
				break
			}

			// Exponential backoff with jitter
			time.Sleep(retryInterval)
			retryInterval = time.Duration(float64(retryInterval) * 1.5)
			if retryInterval > time.Second {
				retryInterval = time.Second
			}
		}
	} else {
		tagValues, shardID, err = ms.navigate(metadata, writeRequest, specEntityLocator, specShardingKeyLocator)
	}

	if err != nil {
		ms.l.Error().Err(err).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to navigate to the write target")
		ms.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure)
		return err
	}

	if writeRequest.DataPoint.Version == 0 {
		if writeRequest.MessageId == 0 {
			writeRequest.MessageId = uint64(time.Now().UnixNano())
		}
		writeRequest.DataPoint.Version = int64(writeRequest.MessageId)
	}

	if ms.ingestionAccessLog != nil {
		if errAccessLog := ms.ingestionAccessLog.Write(writeRequest); errAccessLog != nil {
			ms.l.Error().Err(errAccessLog).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to write access log")
		}
	}

	iwr := &measurev1.InternalWriteRequest{
		Request:      writeRequest,
		ShardId:      uint32(shardID),
		EntityValues: tagValues[1:].Encode(),
	}

	nodes, err := ms.publishToNodes(ctx, writeRequest, metadata, spec, iwr, publisher, uint32(shardID), measure, nodeMetadataSent, nodeSpecSent)
	if err != nil {
		return err
	}

	*succeedSent = append(*succeedSent, succeedSentMessage{
		metadata:  metadata,
		messageID: writeRequest.GetMessageId(),
		nodes:     nodes,
	})
	return nil
}

func (ms *measureService) publishToNodes(ctx context.Context, writeRequest *measurev1.WriteRequest,
	metadata *commonv1.Metadata, spec *measurev1.DataPointSpec, iwr *measurev1.InternalWriteRequest,
	publisher queue.BatchPublisher, shardID uint32, measure measurev1.MeasureService_WriteServer,
	nodeMetadataSent map[string]bool, nodeSpecSent map[string]bool,
) ([]string, error) {
	nodeID, errPickNode := ms.nodeRegistry.Locate(metadata.GetGroup(), metadata.GetName(), shardID, 0)
	if errPickNode != nil {
		ms.l.Error().Err(errPickNode).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to pick an available node")
		ms.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure)
		return nil, errPickNode
	}

	if !nodeMetadataSent[nodeID] {
		iwr.Request.Metadata = metadata
		nodeMetadataSent[nodeID] = true
	}
	if spec != nil && !nodeSpecSent[nodeID] {
		iwr.Request.DataPointSpec = spec
		nodeSpecSent[nodeID] = true
	}

	message := bus.NewBatchMessageWithNodeAndGroup(bus.MessageID(time.Now().UnixNano()), nodeID, metadata.GetGroup(), iwr)
	_, errWritePub := publisher.Publish(ctx, data.TopicMeasureWrite, message)
	if errWritePub != nil {
		ms.l.Error().Err(errWritePub).RawJSON("written", logger.Proto(writeRequest)).Str("nodeID", nodeID).Msg("failed to send a message")
		var ce *common.Error
		if errors.As(errWritePub, &ce) {
			ms.sendReply(metadata, ce.Status(), writeRequest.GetMessageId(), measure)
			return nil, errWritePub
		}
		ms.sendReply(metadata, modelv1.Status_STATUS_INTERNAL_ERROR, writeRequest.GetMessageId(), measure)
		return nil, errWritePub
	}
	return []string{nodeID}, nil
}

func (ms *measureService) navigate(metadata *commonv1.Metadata, writeRequest *measurev1.WriteRequest,
	specEntityLocator *specLocator, specShardingKeyLocator *specLocator,
) (pbv1.EntityValues, common.ShardID, error) {
	tagFamilies := writeRequest.GetDataPoint().GetTagFamilies()
	return ms.navigateByLocator(metadata, tagFamilies, specEntityLocator, specShardingKeyLocator)
}

func (ms *measureService) sendReply(metadata *commonv1.Metadata, status modelv1.Status, messageID uint64, measure measurev1.MeasureService_WriteServer) {
	if metadata == nil {
		ms.l.Error().Stringer("status", status).Msg("metadata is nil, cannot send reply")
		return
	}
	if status != modelv1.Status_STATUS_SUCCEED {
		ms.metrics.totalStreamMsgReceivedErr.Inc(1, metadata.Group, "measure", "write")
	}
	ms.metrics.totalStreamMsgSent.Inc(1, metadata.Group, "measure", "write")
	if errResp := measure.Send(&measurev1.WriteResponse{Metadata: metadata, Status: status.String(), MessageId: messageID}); errResp != nil {
		if dl := ms.l.Debug(); dl.Enabled() {
			dl.Err(errResp).Msg("failed to send measure write response")
		}
		ms.metrics.totalStreamMsgSentErr.Inc(1, metadata.Group, "measure", "write")
	}
}

func (ms *measureService) handleWriteCleanup(publisher queue.BatchPublisher, succeedSent *[]succeedSentMessage,
	measure measurev1.MeasureService_WriteServer, start time.Time,
) {
	cee, err := publisher.Close()
	for _, s := range *succeedSent {
		code := modelv1.Status_STATUS_SUCCEED
		if cee != nil {
			for _, node := range s.nodes {
				if ce, ok := cee[node]; ok {
					code = ce.Status()
					if ce.Status() == modelv1.Status_STATUS_SUCCEED {
						code = modelv1.Status_STATUS_SUCCEED
						break
					}
				}
			}
		}
		ms.sendReply(s.metadata, code, s.messageID, measure)
	}
	if err != nil {
		ms.l.Error().Err(err).Msg("failed to close the publisher")
	}
	if dl := ms.l.Debug(); dl.Enabled() {
		dl.Int("total_requests", len(*succeedSent)).Msg("completed measure write batch")
	}
	ms.metrics.totalStreamFinished.Inc(1, "measure", "write")
	ms.metrics.totalStreamLatency.Inc(time.Since(start).Seconds(), "measure", "write")
}

var emptyMeasureQueryResponse = &measurev1.QueryResponse{DataPoints: make([]*measurev1.DataPoint, 0)}

func (ms *measureService) Query(ctx context.Context, req *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
	return ms.query(ctx, req, false)
}

// query shares admission, lifecycle gates and response accounting across both callers.
func (ms *measureService) query(ctx context.Context, req *measurev1.QueryRequest, batches bool) (resp *measurev1.QueryResponse, err error) {
	return ms.queryWithRawPlan(ctx, req, batches, nil, false)
}

func (ms *measureService) queryWithRawPlan(ctx context.Context, req *measurev1.QueryRequest,
	batches bool, rawPlan *ql.RawPlan, fromQL bool,
) (resp *measurev1.QueryResponse, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is nil")
	}
	budget := ms.queryBudget
	if budget == nil {
		budget = protector.QueryBudgetFor(nil)
	}
	// Admit a scan to preserve historical measure limit and offset semantics.
	ctx, release, admissionErr := budget.AdmitMeasureContext(ctx)
	if admissionErr != nil {
		return nil, queryStatus(admissionErr)
	}
	defer release()
	ctx = vectorized.WithQueryMemoryScope(ctx)
	defer func() { err = queryStatus(err) }()
	return ms.executeQueryWithRawPlan(ctx, req, batches, rawPlan, fromQL)
}

// finalizeMeasureResponse runs after trace assembly and before query metrics.
func finalizeMeasureResponse(ctx context.Context, resp **measurev1.QueryResponse, queryErr *error) {
	if *queryErr != nil {
		return
	}
	release, err := vectorized.ReserveResponseEncoding(ctx, *resp)
	defer release()
	if err != nil {
		*resp = nil
		*queryErr = err
	}
}

func (ms *measureService) executeQueryWithRawPlan(ctx context.Context, req *measurev1.QueryRequest,
	batches bool, rawPlan *ql.RawPlan, fromQL bool,
) (resp *measurev1.QueryResponse, err error) {
	acquired := make([]string, 0, len(req.Groups))
	defer func() {
		for _, group := range acquired {
			ms.groupRepo.releaseRequest(group)
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for _, g := range req.Groups {
		if acquireErr := ms.groupRepo.acquireRequest(g); acquireErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "group %s is pending deletion", g)
		}
		acquired = append(acquired, g)
	}
	for _, g := range req.Groups {
		ms.metrics.totalStarted.Inc(1, g, "measure", "query")
	}
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		for _, g := range req.Groups {
			ms.metrics.totalFinished.Inc(1, g, "measure", "query")
			if err != nil {
				ms.metrics.totalErr.Inc(1, g, "measure", "query")
			}
			ms.metrics.totalLatency.Inc(duration.Seconds(), g, "measure", "query")
		}
		// Log query with timing information at the end
		if ms.queryAccessLog != nil {
			if errAccessLog := ms.queryAccessLog.WriteQuery("measure", start, duration, req, err); errAccessLog != nil {
				ms.l.Error().Err(errAccessLog).Msg("query access log error")
			}
		}
	}()
	defer finalizeMeasureResponse(ctx, &resp, &err)
	if err = timestamp.CheckTimeRange(req.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", req.GetTimeRange(), err)
	}
	now := time.Now()
	var tracer *query.Tracer
	var span *query.Span
	if req.Trace {
		tracer, _ = query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "measure-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
				span.Stop()
			} else {
				span.Tagf("response_data_point_count", "%d", len(resp.GetDataPoints()))
				span.AddSubTrace(resp.Trace)
				span.Stop()
				resp.Trace = tracer.ToProto()
			}
		}()
	}
	defer finalizeEmptyQLCountDistinct(ctx, req, &resp, &err, fromQL, rawPlan)
	measureReg := schemaRevisionRegistry(ms.metadataRepo)
	gatedStatuses, shortCircuit := checkQueryGate(req.Groups, req.Name, req.GroupModRevisions,
		func(name, group string) (int64, bool) {
			loc, ok := ms.entityRepo.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(measureReg, schema.KindMeasure, group, name, loc.ModRevision, ok)
		}, ms.maxWaitDuration)
	recordQueryGateStatuses(ms.metrics, rpcLabelMeasureQuery, gatedStatuses)
	if shortCircuit {
		return &measurev1.QueryResponse{GroupStatuses: gatedStatuses}, nil
	}
	// Gate the clamp on the same opt-in trigger as the query gate (GroupModRevisions
	// non-empty). Schema-aware clients that pass per-group revisions also accept
	// pre-creation filtering; legacy clients that omit GroupModRevisions retain the
	// pre-Step-1.6 behavior where data with timestamps preceding CreatedAt is still
	// returned. This keeps backward compatibility for fixtures that write historical
	// data into a freshly-created measure.
	if req.TimeRange != nil && len(req.GroupModRevisions) > 0 {
		createdAts := make([]time.Time, 0, len(req.Groups))
		for _, group := range req.Groups {
			measureEntity, measureOK := ms.entityRepo.getMeasure(identity{name: req.Name, group: group})
			if measureOK && measureEntity.GetCreatedAt() != nil {
				createdAts = append(createdAts, measureEntity.GetCreatedAt().AsTime())
			}
		}
		endTime := time.Time{}
		if req.TimeRange.GetEnd() != nil {
			endTime = req.TimeRange.GetEnd().AsTime()
		}
		originalBegin := req.TimeRange.GetBegin().AsTime()
		newBegin, rangeEmpty := clampTimeRangeBegin(originalBegin, endTime, createdAts)
		if rangeEmpty {
			return emptyPlannedMeasure(ctx, gatedStatuses, rawPlan)
		}
		if newBegin != originalBegin {
			req.TimeRange.Begin = timestamppb.New(newBegin)
		}
	}
	if isEmptyRawQuery(rawPlan) {
		return emptyPlannedMeasure(ctx, gatedStatuses, rawPlan)
	}
	if ms.canUseNativeAggregate(req, rawPlan) {
		return ms.executeNativeAggregate(ctx, req, gatedStatuses, rawPlan)
	}
	var payload any = req
	if batches {
		payload = measureBatchRequest(req, rawPlan)
	}
	feat, err := ms.broadcaster.Publish(ctx, data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(now.UnixNano()), payload))
	if err != nil {
		return nil, err
	}
	msg, err := feat.Get()
	if err != nil {
		if errors.Is(err, io.EOF) {
			if rawPlan != nil && rawPlan.Aggregate != nil {
				return emptyPlannedMeasure(ctx, gatedStatuses, rawPlan)
			}
			return emptyMeasureQueryResponse, nil
		}
		return nil, err
	}
	resp, err = measureQueryResponseWithPlan(ctx, msg.Data(), gatedStatuses, rawPlan)
	return resp, err
}

func measureQueryResponse(ctx context.Context, data any, gatedStatuses map[string]modelv1.Status) (*measurev1.QueryResponse, error) {
	switch result := data.(type) {
	case vmeasure.BatchQueryResult:
		resp, err := collectMeasureBatches(ctx, result)
		if err != nil {
			return nil, queryStatus(err)
		}
		resp.GroupStatuses = gatedStatuses
		return resp, nil
	case *measurev1.QueryResponse:
		if len(gatedStatuses) > 0 {
			result.GroupStatuses = gatedStatuses
		}
		return result, nil
	case *common.Error:
		return nil, errors.WithMessage(errQueryMsg, result.Error())
	case error:
		return nil, queryStatus(result)
	}
	return nil, nil
}

// collectMeasureBatches serializes only final egress rows after NativeScan consumes batches.
func collectMeasureBatches(ctx context.Context, result vmeasure.BatchQueryResult) (_ *measurev1.QueryResponse, err error) {
	scan := ql.NewNativeScan(result)
	defer func() {
		if closeErr := scan.Close(); err == nil {
			err = closeErr
		}
	}()
	iter, err := result.NativeIteratorFrom(ctx, scan)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := iter.Close(); err == nil {
			err = closeErr
		}
	}()
	resp := &measurev1.QueryResponse{}
	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		current := iter.Current()
		if len(current) > 0 {
			resp.DataPoints = append(resp.DataPoints, current[0].GetDataPoint())
		}
	}
	if errIterator, ok := iter.(interface{ Err() error }); ok {
		if err = errIterator.Err(); err != nil {
			return nil, err
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = iter.Close(); err != nil {
		return nil, err
	}
	resp.Trace = result.QueryTrace()
	return resp, nil
}

func (ms *measureService) TopN(ctx context.Context, topNRequest *measurev1.TopNRequest) (resp *measurev1.TopNResponse, err error) {
	for _, g := range topNRequest.GetGroups() {
		if acquireErr := ms.groupRepo.acquireRequest(g); acquireErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "group %s is pending deletion", g)
		}
	}
	defer func() {
		for _, g := range topNRequest.GetGroups() {
			ms.groupRepo.releaseRequest(g)
		}
	}()
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		// Log query with timing information at the end
		if ms.queryAccessLog != nil {
			if errAccessLog := ms.queryAccessLog.WriteQuery("measure", start, duration, topNRequest, err); errAccessLog != nil {
				ms.l.Error().Err(errAccessLog).Msg("query access log error")
			}
		}
	}()
	if err = timestamp.CheckTimeRange(topNRequest.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", topNRequest.GetTimeRange(), err)
	}
	now := time.Now()
	var topNTracer *query.Tracer
	var topNSpan *query.Span
	var responseListCount int
	if topNRequest.Trace {
		topNTracer, _ = query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		topNSpan, _ = topNTracer.StartSpan(ctx, "topn-grpc")
		topNSpan.Tag("request", convert.BytesToString(logger.Proto(topNRequest)))
		defer func() {
			if err != nil {
				topNSpan.Error(err)
			} else {
				topNSpan.Tagf("response_list_count", "%d", responseListCount)
				topNSpan.AddSubTrace(resp.Trace)
				resp.Trace = topNTracer.ToProto()
			}
			topNSpan.Stop()
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
		responseListCount = len(d.Lists)
		return d, nil
	case *common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Error())
	}
	return nil, nil
}

func (ms *measureService) Close() error {
	return ms.ingestionAccessLog.Close()
}

type succeedSentMessage struct {
	metadata  *commonv1.Metadata
	nodes     []string
	messageID uint64
}
