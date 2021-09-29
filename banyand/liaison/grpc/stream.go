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

	"github.com/apache/skywalking-banyandb/api/data"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

func (s *Server) Write(stream streamv2.StreamService_WriteServer) error {
	for {
		writeEntity, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		id := getID(writeEntity.GetMetadata())
		shardNum, existed := s.shardRepo.shardNum(id)
		if !existed {
			continue
		}
		locator, existed := s.entityRepo.getLocator(id)
		if !existed {
			continue
		}
		entity, shardID, err := locator.Locate(writeEntity.GetElement().TagFamilies, shardNum)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to locate write target")
			continue
		}
		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &streamv2.InternalWriteRequest{
			Request:    writeEntity,
			ShardId:    uint32(shardID),
			SeriesHash: tsdb.HashEntity(entity),
		})
		_, errWritePub := s.pipeline.Publish(data.TopicStreamWrite, message)
		if errWritePub != nil {
			return errWritePub
		}
		if errSend := stream.Send(&streamv2.WriteResponse{}); errSend != nil {
			return errSend
		}
	}
}

func (s *Server) Query(_ context.Context, entityCriteria *streamv2.QueryRequest) (*streamv2.QueryResponse, error) {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), entityCriteria)
	feat, errQuery := s.pipeline.Publish(data.TopicStreamQuery, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	queryMsg, ok := msg.Data().([]*streamv2.Element)
	if !ok {
		return nil, ErrQueryMsg
	}
	return &streamv2.QueryResponse{Elements: queryMsg}, nil
}
