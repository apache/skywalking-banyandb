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
	"io"

	"github.com/pkg/errors"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// Write implements v1.ServiceServer.
func (s *server) Write(stream clusterv1.Service_WriteServer) error {
	reply := func(stream clusterv1.Service_WriteServer) {
		if errResp := stream.Send(&clusterv1.WriteResponse{}); errResp != nil {
			s.log.Err(errResp).Msg("failed to send response")
		}
	}
	ctx := stream.Context()
	var topic *bus.Topic
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
			s.log.Error().Stringer("written", writeEntity).Err(err).Msg("failed to receive message")
			reply(stream)
			continue
		}
		if writeEntity.Topic != "" && topic == nil {
			t := bus.UniTopic(writeEntity.Topic)
			topic = &t
		}
		if topic == nil {
			s.log.Error().Stringer("written", writeEntity).Msg("topic is empty")
			reply(stream)
			continue
		}
		for _, listener := range s.getListeners(*topic) {
			_ = listener.Rev(bus.NewMessage(bus.MessageID(writeEntity.MessageId), writeEntity.Request))
		}
		reply(stream)
	}
}

func (s *server) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	if _, ok := s.listeners[topic]; !ok {
		s.listeners[topic] = make([]bus.MessageListener, 0)
	}
	s.listeners[topic] = append(s.listeners[topic], listener)
	return nil
}

func (s *server) getListeners(topic bus.Topic) []bus.MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	return s.listeners[topic]
}
