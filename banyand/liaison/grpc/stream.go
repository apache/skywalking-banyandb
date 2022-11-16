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
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type streamService struct {
	*discoveryService
	streamv1.UnimplementedStreamServiceServer
}

func (s *streamService) Write(stream streamv1.StreamService_WriteServer) error {
	reply := func() error {
		if err := stream.Send(&streamv1.WriteResponse{}); err != nil {
			return err
		}
		return nil
	}
	sampled := s.log.Sample(&zerolog.BasicSampler{N: 10})
	for {
		writeEntity, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if errTime := timestamp.CheckPb(writeEntity.GetElement().Timestamp); errTime != nil {
			sampled.Error().Stringer("written", writeEntity).Err(errTime).Msg("the element time is invalid")
			if errResp := reply(); errResp != nil {
				return errResp
			}
			continue
		}
		entity, shardID, err := s.navigate(writeEntity.GetMetadata(), writeEntity.GetElement().GetTagFamilies())
		if err != nil {
			sampled.Error().Err(err).Msg("failed to navigate to the write target")
			if errResp := reply(); errResp != nil {
				return errResp
			}
			continue
		}
		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), &streamv1.InternalWriteRequest{
			Request:    writeEntity,
			ShardId:    uint32(shardID),
			SeriesHash: tsdb.HashEntity(entity),
		})
		_, errWritePub := s.pipeline.Publish(data.TopicStreamWrite, message)
		if errWritePub != nil {
			sampled.Error().Err(errWritePub).Msg("failed to send a message")
			if errResp := reply(); errResp != nil {
				return errResp
			}
			continue
		}
		if errSend := reply(); errSend != nil {
			return errSend
		}
	}
}

func (s *streamService) Query(_ context.Context, entityCriteria *streamv1.QueryRequest) (*streamv1.QueryResponse, error) {
	timeRange := entityCriteria.GetTimeRange()
	if timeRange == nil {
		entityCriteria.TimeRange = timestamp.DefaultTimeRange
	}
	if err := timestamp.CheckTimeRange(entityCriteria.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", entityCriteria.GetTimeRange(), err)
	}
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), entityCriteria)
	feat, errQuery := s.pipeline.Publish(data.TopicStreamQuery, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case []*streamv1.Element:
		return &streamv1.QueryResponse{Elements: d}, nil
	case common.Error:
		return nil, errors.WithMessage(ErrQueryMsg, d.Msg())
	}
	return nil, ErrQueryMsg
}
