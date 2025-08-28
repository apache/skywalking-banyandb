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

package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var (
	internalTraceGroupName  = "_property_gossip"
	internalTraceStreamName = "_property_gossip_trace_stream"

	traceSavingTimeout     = time.Second * 10
	traceStreamShardNum    = 1
	traceStreamCopiesCount = 1
)

// TraceTag constants are used for tagging spans in the tracing system.
const (
	TraceTagTargetNode  = "target_node"
	TraceTagGroupName   = "group_name"
	TraceTagShardID     = "shard_id"
	TraceTagOperateType = "operate_type"
	TraceTagPropertyID  = "property_id"

	TraceTagOperateReceive         = "receive_gossip"
	TraceTagOperateHandle          = "handle_gossip"
	TraceTagOperateSelectNode      = "select_node"
	TraceTagOperateListenerReceive = "listener_receive"
	TraceTagOperateSendToNext      = "send_to_next_node"
	TraceTagOperateStartSync       = "start_sync"
	TraceTagOperateRepairProperty  = "repair property"
)

// Span interface defines the methods for a tracing span.
type Span interface {
	ID() string
	TraceID() string
	Tag(key string, value string)
	End()
	Error(reason string)
}

// Trace interface defines the methods for creating and activating spans in the tracing system.
type Trace interface {
	// CreateSpan creates a new span with the given parent span and span name.
	CreateSpan(parent Span, message string) Span
	// ActivateSpan activates the current span, returning it for further operations.
	ActivateSpan() Span
}

func (s *service) initTracing(ctx context.Context) error {
	if !s.traceLogEnabled {
		return nil
	}
	err := s.initInternalTraceGroup(ctx)
	if err != nil && !errors.Is(err, schema.ErrGRPCAlreadyExists) {
		s.traceLogEnabled = false
		return err
	}

	stream := &databasev1.Stream{
		Metadata: &commonv1.Metadata{
			Name:  internalTraceStreamName,
			Group: internalTraceGroupName,
		},
		Entity: &databasev1.Entity{
			TagNames: []string{
				"trace_id", "span_id",
			},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "searchable",
				Tags: []*databasev1.TagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "parent_span_id", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "current_node_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "start_time", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "end_time", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "is_error", Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: "sequence_number", Type: databasev1.TagType_TAG_TYPE_INT},
				},
			},
			{
				Name: "storage-only",
				Tags: []*databasev1.TagSpec{
					{Name: "tags_json", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "message", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "error_reason", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
	_, err = s.metadata.StreamRegistry().CreateStream(ctx, stream)
	if err != nil && !errors.Is(err, schema.ErrGRPCAlreadyExists) {
		s.traceLogEnabled = false
		return err
	}

	dbStream, err := s.metadata.StreamRegistry().GetStream(ctx, stream.Metadata)
	if err != nil {
		s.traceLogEnabled = false
		return err
	}
	s.traceEntityLocator = partition.Locator{
		TagLocators: partition.NewEntityLocator(dbStream.TagFamilies, stream.Entity, dbStream.GetMetadata().GetModRevision()).TagLocators,
		ModRevision: dbStream.GetMetadata().GetModRevision(),
	}
	s.traceStreamSelector = node.NewRoundRobinSelector(data.TopicStreamWrite.String(), s.metadata)
	s.pipeline.Register(data.TopicStreamWrite, s)
	return nil
}

func (s *service) initInternalTraceGroup(ctx context.Context) error {
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: internalTraceGroupName,
		},
		Catalog: commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: uint32(traceStreamShardNum),
			Replicas: uint32(traceStreamCopiesCount),
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  3,
			},
		},
	}
	return s.metadata.GroupRegistry().CreateGroup(ctx, g)
}

func (s *service) savingTracingSpans() (err error) {
	spans := s.readAllReadySendTraceSpan()
	s.log.Debug().Int("spans", len(spans)).Msg("ready to save trace spans to storage")
	if len(spans) == 0 {
		return nil
	}
	ctx := context.Background()
	publisher := s.pipeline.NewBatchPublisher(traceSavingTimeout)
	defer func() {
		result, closeErr := publisher.Close()
		if err != nil {
			return
		}
		if closeErr != nil {
			err = fmt.Errorf("failed to close publisher for trace spans: %w", closeErr)
			return
		}
		var resultErr error
		for nodeID, publishErr := range result {
			if publishErr != nil {
				resultErr = multierr.Append(resultErr, fmt.Errorf("failed to publish trace span to node %s: %w", nodeID, publishErr))
			}
		}
		err = resultErr
	}()

	for _, span := range spans {
		stream, err := span.toStream()
		if err != nil {
			return err
		}
		entityValues, shardID, err := s.traceEntityLocator.Locate(stream.Metadata.Name, stream.GetElement().GetTagFamilies(), uint32(traceStreamShardNum))
		if err != nil {
			return fmt.Errorf("failed to locate entity for trace span: %w", err)
		}

		iwr := &streamv1.InternalWriteRequest{
			Request:      stream,
			ShardId:      uint32(shardID),
			EntityValues: entityValues[1:].Encode(),
		}

		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), iwr)
		if _, err := publisher.Publish(ctx, data.TopicStreamWrite, message); err != nil {
			return fmt.Errorf("failed to publish trace span: %w", err)
		}
	}
	return nil
}

func (s *service) readAllReadySendTraceSpan() []*recordTraceSpan {
	s.traceSpanLock.Lock()
	defer s.traceSpanLock.Unlock()
	spans := make([]*recordTraceSpan, 0, len(s.traceSpanSending))
	spans = append(spans, s.traceSpanSending...)
	s.traceSpanSending = s.traceSpanSending[:0]
	return spans
}

func (s *service) appendReadySendTraceSpan(span *recordTraceSpan) {
	s.traceSpanLock.Lock()
	defer s.traceSpanLock.Unlock()
	if s.traceSpanNotified == nil {
		s.traceSpanNotified = new(int32)
	}
	s.traceSpanSending = append(s.traceSpanSending, span)
	if !atomic.CompareAndSwapInt32(s.traceSpanNotified, 0, 1) {
		return
	}
	go func() {
		select {
		case <-s.closer.CloseNotify():
			return
		case <-time.After(time.Second * 3):
			atomic.StoreInt32(s.traceSpanNotified, 0)
			if err := s.savingTracingSpans(); err != nil {
				s.log.Warn().Err(err).Msg("failed to save tracing spans")
			}
		}
	}()
}

func (s *service) createTraceForRequest(request *propertyv1.PropagationRequest) Trace {
	if !s.traceLogEnabled {
		return &emptyTrace{}
	}

	var parent Span
	var id string
	if request.TraceContext != nil {
		parent = &remoteSpan{id: request.TraceContext.ParentSpanId}
		id = request.TraceContext.TraceId
	} else {
		parent = nil
		id = fmt.Sprintf("%s_%d_%s_%d", request.Group, request.ShardId, request.Context.OriginNode, time.Now().UnixNano())
		request.TraceContext = &propertyv1.PropagationTraceContext{
			TraceId: id,
		}
	}
	return &recordTrace{
		s:           s,
		request:     request,
		currentSpan: parent,
		id:          id,
		roundNum:    int(request.Context.CurrentPropagationCount),
	}
}

type recordTrace struct {
	currentSpan Span
	s           *service
	request     *propertyv1.PropagationRequest
	id          string
	allSpans    []*recordTraceSpan
	roundNum    int
	lock        sync.Mutex
}

func (r *recordTrace) CreateSpan(parent Span, message string) Span {
	spanID := fmt.Sprintf("%s_%d", r.s.nodeID, time.Now().UnixNano())
	r.changeParentID(spanID)
	span := &recordTraceSpan{
		trace:     r,
		id:        spanID,
		parent:    parent,
		message:   message,
		tags:      make(map[string]string),
		startTime: time.Now(),
	}
	r.allSpans = append(r.allSpans, span)
	return span
}

func (r *recordTrace) changeParentID(id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.request.TraceContext.ParentSpanId = id
}

func (r *recordTrace) ActivateSpan() Span {
	return r.currentSpan
}

// remoteSpan is a placeholder for a span that is created from downstream node(only works for the parent span id).
type remoteSpan struct {
	emptyTraceSpan
	id      string
	traceID string
}

func (r *remoteSpan) ID() string {
	return r.id
}

func (r *remoteSpan) TraceID() string {
	return r.traceID
}

type recordTraceSpan struct {
	trace     *recordTrace
	id        string
	parent    Span
	message   string
	tags      map[string]string
	startTime time.Time
	endTime   time.Time
	errorMsg  string
}

func (r *recordTraceSpan) ID() string {
	return r.id
}

func (r *recordTraceSpan) TraceID() string {
	return r.trace.id
}

func (r *recordTraceSpan) Tag(key string, value string) {
	r.tags[key] = value
}

func (r *recordTraceSpan) End() {
	r.endTime = time.Now()
	r.trace.s.appendReadySendTraceSpan(r)
	// if still have parent span, then this is not the root span
	// change the context to parent span
	if r.parent != nil {
		r.trace.changeParentID(r.parent.ID())
	}
}

func (r *recordTraceSpan) Error(reason string) {
	r.errorMsg = reason
}

func (r *recordTraceSpan) toStream() (*streamv1.WriteRequest, error) {
	request := &streamv1.WriteRequest{}
	request.Metadata = &commonv1.Metadata{
		Name:           internalTraceStreamName,
		Group:          internalTraceGroupName,
		CreateRevision: r.startTime.UnixNano(),
	}
	var isError int64
	if r.errorMsg != "" {
		isError = 1
	}
	tags := make(map[string]string)
	for k, v := range r.tags {
		tags[k] = v
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return nil, err
	}
	var parentSpanID string
	if r.parent != nil {
		parentSpanID = r.parent.ID()
	}
	startTime := r.startTime.Truncate(time.Millisecond)
	request.Element = &streamv1.ElementValue{
		ElementId: fmt.Sprintf("%s_%s", r.trace.id, r.id),
		Timestamp: timestamppb.New(startTime),
		TagFamilies: []*modelv1.TagFamilyForWrite{
			{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.trace.id}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.id}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: parentSpanID}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.trace.s.nodeID}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: r.startTime.UnixNano()}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: r.endTime.UnixNano()}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: r.endTime.Sub(r.startTime).Nanoseconds()}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: isError}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(r.trace.roundNum)}}},
				},
			},
			{
				Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: string(tagsJSON)}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.message}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.errorMsg}}},
				},
			},
		},
	}
	return request, nil
}

type emptyTrace struct{}

func (t *emptyTrace) CreateSpan(Span, string) Span {
	return &emptyTraceSpan{}
}

func (t *emptyTrace) ActivateSpan() Span {
	return nil
}

type emptyTraceSpan struct{}

func (e *emptyTraceSpan) ID() string {
	return ""
}

func (e *emptyTraceSpan) TraceID() string {
	return ""
}

func (e *emptyTraceSpan) Tag(string, string) {
}

func (e *emptyTraceSpan) End() {
}

func (e *emptyTraceSpan) Error(_ string) {
}
