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

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type streamService struct {
	streamv1.UnimplementedStreamServiceServer
	ser *server
}

func (s *streamService) DeleteExpiredSegments(ctx context.Context, request *streamv1.DeleteExpiredSegmentsRequest) (*streamv1.DeleteExpiredSegmentsResponse, error) {
	s.ser.listenersLock.RLock()
	defer s.ser.listenersLock.RUnlock()
	ll := s.ser.getListeners(data.TopicDeleteExpiredStreamSegments)
	if len(ll) == 0 {
		logger.Panicf("no listener found for topic %s", data.TopicDeleteExpiredStreamSegments)
	}
	var deleted int64
	for _, l := range ll {
		message := l.Rev(ctx, bus.NewMessage(bus.MessageID(0), request))
		data := message.Data()
		if data != nil {
			d, ok := data.(int64)
			if !ok {
				logger.Panicf("invalid data type %T", data)
			}
			deleted += d
		}
	}
	return &streamv1.DeleteExpiredSegmentsResponse{Deleted: deleted}, nil
}

type measureService struct {
	measurev1.UnimplementedMeasureServiceServer
	ser *server
}

func (s *measureService) DeleteExpiredSegments(ctx context.Context, request *measurev1.DeleteExpiredSegmentsRequest) (*measurev1.DeleteExpiredSegmentsResponse, error) {
	s.ser.listenersLock.RLock()
	defer s.ser.listenersLock.RUnlock()
	ll := s.ser.getListeners(data.TopicMeasureDeleteExpiredSegments)
	if len(ll) == 0 {
		logger.Panicf("no listener found for topic %s", data.TopicMeasureDeleteExpiredSegments)
	}
	var deleted int64
	for _, l := range ll {
		message := l.Rev(ctx, bus.NewMessage(bus.MessageID(0), request))
		data := message.Data()
		if data != nil {
			d, ok := data.(int64)
			if !ok {
				logger.Panicf("invalid data type %T", data)
			}
			deleted += d
		}
	}
	return &measurev1.DeleteExpiredSegmentsResponse{Deleted: deleted}, nil
}
