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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func (s *server) handleEOF(stream clusterv1.Service_SendServer, topic *bus.Topic, dataCollection []any, writeEntity *clusterv1.SendRequest, identity *streamIdentity) {
	if len(dataCollection) < 1 {
		return
	}
	listeners := s.getListeners(*topic)
	if len(listeners) == 0 {
		s.replyWithErrType(stream, writeEntity, nil, "no listener found", identity, "no_listener")
		return
	}
	if len(listeners) > 1 {
		logger.Panicf("multiple listeners found for topic %s", *topic)
	}
	listener := listeners[0]
	if le := listener.CheckHealth(); le != nil {
		s.replyWithErrType(stream, writeEntity, le, "", identity, "handler_error")
		return
	}
	message := listener.Rev(stream.Context(), bus.NewMessage(bus.MessageID(0), dataCollection))
	// writeEntity is nil when the stream closed (Recv returned io.EOF), so guard the ID access.
	var msgID uint64
	if writeEntity != nil {
		msgID = writeEntity.MessageId
	}
	resp := &clusterv1.SendResponse{MessageId: msgID}
	if ce, ok := message.Data().(*common.Error); ok {
		resp.Error = ce.Error()
		resp.Status = ce.Status()
	}
	if errSend := stream.Send(resp); errSend != nil {
		s.log.Error().Stringer("written", writeEntity).Err(errSend).Msg("failed to send write response")
		if s.metrics != nil && writeEntity != nil {
			s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, "send_error")
		}
	}
}

func (s *server) handleRecvError(err error) error {
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return nil
	}
	s.log.Error().Err(err).Msg("failed to receive message")
	return err
}

func (s *server) handleBatch(dataCollection *[]any, writeEntity *clusterv1.SendRequest, start *time.Time, identity *streamIdentity) {
	if len(*dataCollection) == 0 {
		if s.metrics != nil {
			s.metrics.totalStarted.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
		}
		*start = time.Now()
	}
	*dataCollection = append(*dataCollection, writeEntity.Body)
}
