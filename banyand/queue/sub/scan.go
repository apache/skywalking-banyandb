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

package sub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	// defaultScanHeartbeatInterval is how long a scan may produce no chunk before a heartbeat is sent when the
	// liaison did not announce its idle timeout.
	defaultScanHeartbeatInterval = 2 * time.Second
	// minScanHeartbeatInterval keeps a very short idle timeout from turning into a busy heartbeat loop.
	minScanHeartbeatInterval = 50 * time.Millisecond
	// scanHeartbeatsPerIdleTimeout is how many heartbeats fit in the liaison's idle timeout.
	scanHeartbeatsPerIdleTimeout = 5
)

// scanHeartbeatInterval derives the heartbeat period from the idle timeout the liaison announced in its request.
func scanHeartbeatInterval(idleTimeoutMs uint32) time.Duration {
	if idleTimeoutMs == 0 {
		return defaultScanHeartbeatInterval
	}
	return max(time.Duration(idleTimeoutMs)*time.Millisecond/scanHeartbeatsPerIdleTimeout, minScanHeartbeatInterval)
}

// SubscribeStream implements queue.Server. At most one stream listener serves a topic.
func (s *server) SubscribeStream(topic bus.Topic, listener bus.StreamListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	if _, ok := s.streamListeners[topic]; ok {
		return fmt.Errorf("stream listener already registered for topic %s", topic)
	}
	s.streamListeners[topic] = listener
	s.topicMap[topic.String()] = topic
	return nil
}

func (s *server) getStreamListener(topic bus.Topic) bus.StreamListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	return s.streamListeners[topic]
}

// Scan implements clusterv1.ServiceServer. It runs the topic's stream listener and
// sends every chunk it emits; a request-level failure is reported as an error chunk
// that ends the stream, so the publisher never confuses it with a clean end.
func (s *server) Scan(req *clusterv1.ScanRequest, stream clusterv1.Service_ScanServer) error {
	identity := &streamIdentity{
		senderNode: req.GetSenderNode(),
		senderRole: req.GetSenderRole(),
		senderTier: req.GetSenderTier(),
		group:      req.GetGroup(),
		pinned:     true,
	}
	reject := func(errType, message string, cause error) error {
		s.log.Error().Err(cause).Str("topic", req.GetTopic()).Msg(message)
		if s.metrics != nil {
			s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, errType)
		}
		if errSend := stream.Send(&clusterv1.ScanResponse{
			MessageId: req.GetMessageId(),
			Error:     message,
			Status:    modelv1.Status_STATUS_INTERNAL_ERROR,
		}); errSend != nil {
			s.log.Error().Err(errSend).Msg("failed to send the scan error response")
		}
		return nil
	}
	if req.GetVersionInfo() != nil {
		vc, versionStatus := checkVersionCompatibility(req.GetVersionInfo())
		if versionStatus != modelv1.Status_STATUS_SUCCEED {
			s.log.Warn().Str("reason", vc.Reason).Msg("version compatibility check failed")
			if errSend := stream.Send(&clusterv1.ScanResponse{
				MessageId:            req.GetMessageId(),
				Status:               versionStatus,
				Error:                vc.Reason,
				VersionCompatibility: vc,
			}); errSend != nil {
				s.log.Error().Err(errSend).Msg("failed to send the version incompatibility response")
			}
			return nil
		}
	}
	if req.GetTopic() == "" {
		return reject("empty_topic", "topic is empty", nil)
	}
	topic, ok := data.TopicMap[req.GetTopic()]
	if !ok {
		return reject("invalid_topic", "invalid topic", nil)
	}
	identity.operation = data.OperationOf(topic)
	listener := s.getStreamListener(topic)
	if listener == nil {
		return reject("no_listener", "no listener found", nil)
	}
	reqSupplier, ok := data.TopicRequestMap[topic]
	if !ok {
		return reject("unknown_topic", "unknown topic", nil)
	}
	var message bus.Message
	if body := reqSupplier(); body == nil {
		message = bus.NewMessageWithNodeAndGroup(bus.MessageID(req.GetMessageId()), "", req.GetGroup(), req.GetBody())
	} else {
		if errUnmarshal := proto.Unmarshal(req.GetBody(), body); errUnmarshal != nil {
			return reject("unmarshal_error", "failed to unmarshal message", errUnmarshal)
		}
		message = bus.NewMessageWithNodeAndGroup(bus.MessageID(req.GetMessageId()), "", req.GetGroup(), body)
	}
	if le := listener.CheckHealth(); le != nil {
		return reject("handler_error", le.Error(), le)
	}

	start := time.Now()
	queryCtx, queryScope := query.NewTransportScope(stream.Context())
	defer queryScope.Close()
	queryCtx = bus.WithChunkLimits(queryCtx, bus.ChunkLimits{MaxBytes: req.GetMaxChunkBytes(), MaxRows: req.GetMaxChunkRows()})
	data.IncrScanStarted()
	if s.metrics != nil {
		s.metrics.totalStarted.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
	}
	sink := &scanSink{
		stream:    stream,
		topic:     topic,
		messageID: req.GetMessageId(),
		interval:  scanHeartbeatInterval(req.GetIdleTimeoutMs()),
		lastSent:  start,
	}
	stopHeartbeat := sink.startHeartbeat(queryCtx)
	runErr := listener.RevStream(queryCtx, message, sink)
	stopHeartbeat()

	finalResp := &clusterv1.ScanResponse{MessageId: req.GetMessageId(), Last: true}
	if runErr != nil {
		finalResp = &clusterv1.ScanResponse{MessageId: req.GetMessageId(), Error: runErr.Error(), Status: scanErrorStatus(runErr)}
	}
	if errSend := sink.sendFinal(finalResp); errSend != nil && stream.Context().Err() == nil {
		s.log.Error().Err(errSend).Msg("failed to send the final scan response")
	}
	if s.metrics != nil {
		if runErr != nil {
			s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, "handler_error")
		} else {
			s.metrics.totalFinished.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
		}
		s.metrics.totalLatency.Observe(time.Since(start).Seconds(),
			identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
	}
	return nil
}

func scanErrorStatus(err error) modelv1.Status {
	var ce *common.Error
	if errors.As(err, &ce) {
		return ce.Status()
	}
	return modelv1.Status_STATUS_INTERNAL_ERROR
}

// scanSink marshals the chunks of one scan and sends them on the gRPC stream.
// The mutex serializes the listener's sends with the heartbeats and the final response.
type scanSink struct {
	stream    clusterv1.Service_ScanServer
	lastSent  time.Time
	topic     bus.Topic
	mu        sync.Mutex
	interval  time.Duration
	messageID uint64
	seq       uint32
}

// Send implements bus.Sink. A nil chunk payload is skipped: an empty body is a heartbeat on the wire.
func (k *scanSink) Send(ctx context.Context, chunk bus.Message) error {
	body, err := encodeChunk(k.topic, chunk.Data())
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	k.seq++
	if sendErr := k.stream.Send(&clusterv1.ScanResponse{MessageId: k.messageID, Seq: k.seq, Body: body}); sendErr != nil {
		return sendErr
	}
	k.lastSent = time.Now()
	data.IncrScanChunk()
	return nil
}

func (k *scanSink) sendFinal(resp *clusterv1.ScanResponse) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.seq++
	resp.Seq = k.seq
	return k.stream.Send(resp)
}

// startHeartbeat sends a heartbeat whenever no chunk was sent for the interval.
// The returned function stops it and waits for it to exit.
func (k *scanSink) startHeartbeat(ctx context.Context) func() {
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(k.interval)
		defer timer.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-timer.C:
			}
			k.mu.Lock()
			idleFor := time.Since(k.lastSent)
			if idleFor >= k.interval {
				k.seq++
				if err := k.stream.Send(&clusterv1.ScanResponse{MessageId: k.messageID, Seq: k.seq}); err != nil {
					k.mu.Unlock()
					return
				}
				k.lastSent = time.Now()
				idleFor = 0
			}
			k.mu.Unlock()
			timer.Reset(k.interval - idleFor)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// encodeChunk marshals a chunk payload like marshalResponse does for unary replies:
// protobuf messages as they are, and raw frames only on the topics that allow them.
func encodeChunk(topic bus.Topic, payload any) ([]byte, error) {
	switch d := payload.(type) {
	case nil:
		return nil, nil
	case proto.Message:
		body, err := proto.Marshal(d)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal chunk")
		}
		return body, nil
	case []byte:
		if !rawBodyAllowed(topic) {
			return nil, fmt.Errorf("invalid chunk: unexpected raw body on topic %s", topic)
		}
		data.IncrFrameEncoded(topic)
		return d, nil
	default:
		return nil, fmt.Errorf("invalid chunk type %T", d)
	}
}
