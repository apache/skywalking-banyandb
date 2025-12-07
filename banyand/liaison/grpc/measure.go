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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type measureService struct {
	measurev1.UnimplementedMeasureServiceServer
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
		}

		ms.metrics.totalStreamMsgReceived.Inc(1, metadata.Group, "measure", "write")

		if status := ms.validateWriteRequest(writeRequest, metadata, measure); status != modelv1.Status_STATUS_SUCCEED {
			continue
		}

		if err := ms.processAndPublishRequest(ctx, writeRequest, metadata, spec, publisher, &succeedSent, measure, nodeMetadataSent, nodeSpecSent); err != nil {
			continue
		}
	}
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
		measureCache, existed := ms.entityRepo.getLocator(getID(metadata))
		if !existed {
			ms.l.Error().Stringer("written", writeRequest).Msg("measure schema not found")
			ms.sendReply(metadata, modelv1.Status_STATUS_NOT_FOUND, writeRequest.GetMessageId(), measure)
			return modelv1.Status_STATUS_NOT_FOUND
		}
		if metadata.ModRevision != measureCache.ModRevision {
			ms.l.Error().Stringer("written", writeRequest).Msg("the measure schema is expired")
			ms.sendReply(metadata, modelv1.Status_STATUS_EXPIRED_SCHEMA, writeRequest.GetMessageId(), measure)
			return modelv1.Status_STATUS_EXPIRED_SCHEMA
		}
	}

	return modelv1.Status_STATUS_SUCCEED
}

func (ms *measureService) processAndPublishRequest(ctx context.Context, writeRequest *measurev1.WriteRequest,
	metadata *commonv1.Metadata, spec *measurev1.DataPointSpec, publisher queue.BatchPublisher,
	succeedSent *[]succeedSentMessage, measure measurev1.MeasureService_WriteServer,
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
			tagValues, shardID, err = ms.navigate(metadata, writeRequest, spec)
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
		tagValues, shardID, err = ms.navigate(metadata, writeRequest, spec)
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

	message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), nodeID, iwr)
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

func (ms *measureService) navigate(metadata *commonv1.Metadata,
	writeRequest *measurev1.WriteRequest, spec *measurev1.DataPointSpec,
) (pbv1.EntityValues, common.ShardID, error) {
	tagFamilies := writeRequest.GetDataPoint().GetTagFamilies()
	if spec == nil {
		return ms.navigateByLocator(metadata, tagFamilies)
	}
	return ms.navigateByTagSpec(metadata, spec, tagFamilies)
}

func (ms *measureService) navigateByTagSpec(
	metadata *commonv1.Metadata, spec *measurev1.DataPointSpec, tagFamilies []*modelv1.TagFamilyForWrite,
) (pbv1.EntityValues, common.ShardID, error) {
	shardNum, existed := ms.groupRepo.shardNum(metadata.Group)
	if !existed {
		return nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding the shard num by: %v", metadata)
	}
	id := getID(metadata)
	measure, ok := ms.entityRepo.getMeasure(id)
	if !ok {
		return nil, common.ShardID(0), errors.Wrapf(errNotExist, "finding measure schema by: %v", metadata)
	}
	specFamilyMap, specTagMaps := ms.buildSpecMaps(spec)

	entityValues := ms.findTagValuesByNames(
		metadata.Name,
		measure.GetTagFamilies(),
		tagFamilies,
		measure.GetEntity().GetTagNames(),
		specFamilyMap,
		specTagMaps,
	)
	entity, err := entityValues.ToEntity()
	if err != nil {
		return nil, common.ShardID(0), err
	}

	shardingKey := measure.GetShardingKey()
	if shardingKey != nil && len(shardingKey.GetTagNames()) > 0 {
		shardingKeyValues := ms.findTagValuesByNames(
			metadata.Name,
			measure.GetTagFamilies(),
			tagFamilies,
			shardingKey.GetTagNames(),
			specFamilyMap,
			specTagMaps,
		)
		shardingEntity, shardingErr := shardingKeyValues.ToEntity()
		if shardingErr != nil {
			return nil, common.ShardID(0), shardingErr
		}
		shardID, shardingErr := partition.ShardID(shardingEntity.Marshal(), shardNum)
		if shardingErr != nil {
			return nil, common.ShardID(0), shardingErr
		}
		return entityValues, common.ShardID(shardID), nil
	}

	shardID, err := partition.ShardID(entity.Marshal(), shardNum)
	if err != nil {
		return nil, common.ShardID(0), err
	}
	return entityValues, common.ShardID(shardID), nil
}

func (ms *measureService) buildSpecMaps(spec *measurev1.DataPointSpec) (map[string]int, map[string]map[string]int) {
	specFamilyMap := make(map[string]int)
	specTagMaps := make(map[string]map[string]int)
	for i, specFamily := range spec.GetTagFamilySpec() {
		specFamilyMap[specFamily.GetName()] = i
		tagMap := make(map[string]int)
		for j, tagName := range specFamily.GetTagNames() {
			tagMap[tagName] = j
		}
		specTagMaps[specFamily.GetName()] = tagMap
	}
	return specFamilyMap, specTagMaps
}

func (ms *measureService) findTagValuesByNames(
	subject string,
	schemaFamilies []*databasev1.TagFamilySpec,
	srcTagFamilies []*modelv1.TagFamilyForWrite,
	tagNames []string,
	specFamilyMap map[string]int,
	specTagMaps map[string]map[string]int,
) pbv1.EntityValues {
	entityValues := make(pbv1.EntityValues, len(tagNames)+1)
	entityValues[0] = pbv1.EntityStrValue(subject)
	for i, tagName := range tagNames {
		tagValue := ms.findTagValueByName(schemaFamilies, srcTagFamilies, tagName, specFamilyMap, specTagMaps)
		if tagValue == nil {
			entityValues[i+1] = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
		} else {
			entityValues[i+1] = tagValue
		}
	}
	return entityValues
}

func (ms *measureService) findTagValueByName(
	schemaFamilies []*databasev1.TagFamilySpec,
	srcTagFamilies []*modelv1.TagFamilyForWrite,
	tagName string,
	specFamilyMap map[string]int,
	specTagMaps map[string]map[string]int,
) *modelv1.TagValue {
	for _, schemaFamily := range schemaFamilies {
		for _, schemaTag := range schemaFamily.GetTags() {
			if schemaTag.GetName() != tagName {
				continue
			}
			familyIdx, ok := specFamilyMap[schemaFamily.GetName()]
			if !ok || familyIdx >= len(srcTagFamilies) {
				return nil
			}
			tagMap := specTagMaps[schemaFamily.GetName()]
			if tagMap == nil {
				return nil
			}
			tagIdx, ok := tagMap[tagName]
			if !ok || tagIdx >= len(srcTagFamilies[familyIdx].GetTags()) {
				return nil
			}
			return srcTagFamilies[familyIdx].GetTags()[tagIdx]
		}
	}
	return nil
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

func (ms *measureService) Query(ctx context.Context, req *measurev1.QueryRequest) (resp *measurev1.QueryResponse, err error) {
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
	if err = timestamp.CheckTimeRange(req.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", req.GetTimeRange(), err)
	}
	now := time.Now()
	var tracer *query.Tracer
	var span *query.Span
	var responseDataPointCount int
	if req.Trace {
		tracer, _ = query.NewTracer(ctx, now.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "measure-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
				span.Stop()
			} else {
				span.Tagf("response_data_point_count", "%d", responseDataPointCount)
				span.AddSubTrace(resp.Trace)
				span.Stop()
				resp.Trace = tracer.ToProto()
			}
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
		responseDataPointCount = len(d.DataPoints)
		return d, nil
	case *common.Error:
		return nil, errors.WithMessage(errQueryMsg, d.Error())
	}
	return nil, nil
}

func (ms *measureService) TopN(ctx context.Context, topNRequest *measurev1.TopNRequest) (resp *measurev1.TopNResponse, err error) {
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
