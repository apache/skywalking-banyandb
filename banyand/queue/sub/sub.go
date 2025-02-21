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

// Package sub implements the queue server.
package sub

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func (s *server) Send(stream clusterv1.Service_SendServer) error {
	ctx := stream.Context()
	var topic *bus.Topic
	var m bus.Message
	var dataCollection []any
	start := time.Now()
	defer func() {
		if topic != nil {
			s.metrics.totalFinished.Inc(1, topic.String())
			s.metrics.totalLatency.Inc(time.Since(start).Seconds(), topic.String())
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		writeEntity, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.handleEOF(stream, topic, dataCollection, writeEntity)
			return nil
		}
		if err != nil {
			return s.handleRecvError(err)
		}
		s.metrics.totalMsgReceived.Inc(1, writeEntity.Topic)
		if writeEntity.Topic != "" && topic == nil {
			t, ok := data.TopicMap[writeEntity.Topic]
			if !ok {
				s.reply(stream, writeEntity, err, "invalid topic")
				continue
			}
			topic = &t
		}
		if topic == nil {
			s.reply(stream, writeEntity, err, "topic is empty")
			continue
		}

		if reqSupplier, ok := data.TopicRequestMap[*topic]; ok {
			req := reqSupplier()
			if errUnmarshal := writeEntity.Body.UnmarshalTo(req); errUnmarshal != nil {
				s.reply(stream, writeEntity, errUnmarshal, "failed to unmarshal message")
				continue
			}
			m = bus.NewMessage(bus.MessageID(writeEntity.MessageId), req)
		} else {
			s.reply(stream, writeEntity, err, "unknown topic")
			continue
		}
		if writeEntity.BatchMod {
			s.handleBatch(&dataCollection, writeEntity, &start)
			continue
		}
		s.metrics.totalStarted.Inc(1, writeEntity.Topic)
		listeners := s.getListeners(*topic)
		if len(listeners) == 0 {
			s.reply(stream, writeEntity, err, "no listener found")
			continue
		}
		if len(listeners) > 1 {
			logger.Panicf("multiple listeners found for topic %s", *topic)
		}
		listener := listeners[0]

		m = listener.Rev(ctx, m)
		if m.Data() == nil {
			if errSend := stream.Send(&clusterv1.SendResponse{
				MessageId: writeEntity.MessageId,
			}); errSend != nil {
				s.log.Error().Stringer("request", writeEntity).Err(errSend).Msg("failed to send empty response")
				s.metrics.totalMsgSentErr.Inc(1, writeEntity.Topic)
				continue
			}
			s.metrics.totalMsgSent.Inc(1, writeEntity.Topic)
			continue
		}
		var message proto.Message
		switch d := m.Data().(type) {
		case proto.Message:
			message = d
		case *common.Error:
			select {
			case <-ctx.Done():
				s.metrics.totalMsgReceivedErr.Inc(1, writeEntity.Topic)
				return ctx.Err()
			default:
			}
			s.reply(stream, writeEntity, nil, d.Error())
			continue
		default:
			s.reply(stream, writeEntity, nil, fmt.Sprintf("invalid response: %T", d))
			continue
		}
		anyMessage, err := anypb.New(message)
		if err != nil {
			s.reply(stream, writeEntity, err, "failed to marshal message")
			continue
		}
		if err := stream.Send(&clusterv1.SendResponse{
			MessageId: writeEntity.MessageId,
			Body:      anyMessage,
		}); err != nil {
			s.log.Error().Stringer("request", writeEntity).Dur("latency", time.Since(start)).Err(err).Msg("failed to send query response")
			s.metrics.totalMsgSentErr.Inc(1, writeEntity.Topic)
			continue
		}
		s.metrics.totalMsgSent.Inc(1, writeEntity.Topic)
	}
}

func (s *server) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	listeners, ok := s.listeners[topic]
	if ok {
		listeners = append(listeners, listener)
		s.listeners[topic] = listeners
		return nil
	}
	listeners = make([]bus.MessageListener, 0)
	listeners = append(listeners, listener)
	s.listeners[topic] = listeners
	s.topicMap[topic.String()] = topic
	return nil
}

func (s *server) getListeners(topic bus.Topic) []bus.MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	return s.listeners[topic]
}

func (s *server) reply(stream clusterv1.Service_SendServer, writeEntity *clusterv1.SendRequest, err error, message string) {
	s.log.Error().Stringer("request", writeEntity).Err(err).Msg(message)
	s.metrics.totalMsgReceivedErr.Inc(1, writeEntity.Topic)
	resp := &clusterv1.SendResponse{
		MessageId: writeEntity.MessageId,
	}

	var ce *common.Error
	if errors.As(err, &ce) {
		resp.Error = ce.Error()
		resp.Status = ce.Status()
	} else {
		resp.Error = message
	}
	if errResp := stream.Send(&clusterv1.SendResponse{
		MessageId: writeEntity.MessageId,
		Error:     message,
	}); errResp != nil {
		s.log.Error().Err(errResp).AnErr("original", err).Stringer("request", writeEntity).Msg("failed to send error response")
		s.metrics.totalMsgSentErr.Inc(1, writeEntity.Topic)
	}
}
