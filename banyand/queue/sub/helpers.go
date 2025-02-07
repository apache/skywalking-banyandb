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

func (s *server) handleEOF(stream clusterv1.Service_SendServer, topic *bus.Topic, dataCollection []any, writeEntity *clusterv1.SendRequest) {
	if len(dataCollection) < 1 {
		return
	}
	listeners := s.getListeners(*topic)
	if len(listeners) == 0 {
		s.reply(stream, writeEntity, nil, "no listener found")
		return
	}
	if len(listeners) > 1 {
		logger.Panicf("multiple listeners found for topic %s", *topic)
	}
	listener := listeners[0]
	if le := listener.CheckHealth(); le != nil {
		s.reply(stream, writeEntity, le, "")
		return
	}
	message := listener.Rev(stream.Context(), bus.NewMessage(bus.MessageID(0), dataCollection))
	var resp *clusterv1.SendResponse
	data := message.Data()
	if data != nil {
		switch d := data.(type) {
		case *common.Error:
			resp = &clusterv1.SendResponse{
				MessageId: writeEntity.MessageId,
				Error:     d.Error(),
				Status:    d.Status(),
			}
		default:
			resp = &clusterv1.SendResponse{
				MessageId: writeEntity.MessageId,
			}
		}
	}
	if errSend := stream.Send(resp); errSend != nil {
		s.log.Error().Stringer("written", writeEntity).Err(errSend).Msg("failed to send write response")
		s.metrics.totalMsgSentErr.Inc(1, writeEntity.Topic)
	}
}

func (s *server) handleRecvError(err error) error {
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return nil
	}
	s.log.Error().Err(err).Msg("failed to receive message")
	return err
}

func (s *server) handleBatch(dataCollection *[]any, writeEntity *clusterv1.SendRequest, start *time.Time) {
	if len(*dataCollection) == 0 {
		s.metrics.totalStarted.Inc(1, writeEntity.Topic)
		*start = time.Now()
	}
	*dataCollection = append(*dataCollection, writeEntity.Body)
	s.metrics.totalMsgSent.Inc(1, writeEntity.Topic)
}
