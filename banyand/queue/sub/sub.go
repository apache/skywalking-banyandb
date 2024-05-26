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

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

func (s *server) Send(stream clusterv1.Service_SendServer) error {
	reply := func(writeEntity *clusterv1.SendRequest, err error, message string) {
		s.log.Error().Stringer("written", writeEntity).Err(err).Msg(message)
		if errResp := stream.Send(&clusterv1.SendResponse{
			MessageId: writeEntity.MessageId,
			Error:     message,
		}); errResp != nil {
			s.log.Err(errResp).Msg("failed to send response")
		}
	}
	ctx := stream.Context()
	var topic *bus.Topic
	var m bus.Message
	var dataCollection []any
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		writeEntity, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if len(dataCollection) < 1 {
				return nil
			}
			listener := s.getListeners(*topic)
			if listener == nil {
				reply(writeEntity, err, "no listener found")
				return nil
			}
			_ = listener.Rev(bus.NewMessage(bus.MessageID(0), dataCollection))
			return nil
		}
		if err != nil {
			if status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("failed to receive message")
			return err
		}
		if writeEntity.Topic != "" && topic == nil {
			t, ok := data.TopicMap[writeEntity.Topic]
			if !ok {
				reply(writeEntity, err, "invalid topic")
				continue
			}
			topic = &t
		}
		if topic == nil {
			reply(writeEntity, err, "topic is empty")
			continue
		}

		if reqSupplier, ok := data.TopicRequestMap[*topic]; ok {
			req := reqSupplier()
			if errUnmarshal := writeEntity.Body.UnmarshalTo(req); errUnmarshal != nil {
				reply(writeEntity, errUnmarshal, "failed to unmarshal message")
				continue
			}
			m = bus.NewMessage(bus.MessageID(writeEntity.MessageId), req)
		} else {
			reply(writeEntity, err, "unknown topic")
			continue
		}
		if writeEntity.BatchMod {
			dataCollection = append(dataCollection, writeEntity.Body)
			if errSend := stream.Send(&clusterv1.SendResponse{
				MessageId: writeEntity.MessageId,
			}); errSend != nil {
				s.log.Error().Stringer("written", writeEntity).Err(errSend).Msg("failed to send response")
			}
			continue
		}
		listener := s.getListeners(*topic)
		if listener == nil {
			reply(writeEntity, err, "no listener found")
			continue
		}

		m = listener.Rev(m)
		if m.Data() == nil {
			if errSend := stream.Send(&clusterv1.SendResponse{
				MessageId: writeEntity.MessageId,
			}); errSend != nil {
				s.log.Error().Stringer("written", writeEntity).Err(errSend).Msg("failed to send response")
			}
			continue
		}
		var message proto.Message
		switch d := m.Data().(type) {
		case proto.Message:
			message = d
		case common.Error:
			reply(writeEntity, nil, d.Msg())
			continue
		default:
			reply(writeEntity, nil, fmt.Sprintf("invalid response: %T", d))
			continue
		}
		anyMessage, err := anypb.New(message)
		if err != nil {
			reply(writeEntity, err, "failed to marshal message")
			continue
		}
		if err := stream.Send(&clusterv1.SendResponse{
			MessageId: writeEntity.MessageId,
			Body:      anyMessage,
		}); err != nil {
			s.log.Error().Stringer("written", writeEntity).Err(err).Msg("failed to send response")
		}
	}
}

func (s *server) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	if _, ok := s.listeners[topic]; !ok {
		s.listeners[topic] = listener
		return nil
	}
	return errors.New("topic already exists")
}

func (s *server) getListeners(topic bus.Topic) bus.MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	return s.listeners[topic]
}
