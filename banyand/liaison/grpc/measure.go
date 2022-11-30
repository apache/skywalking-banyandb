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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type measureService struct {
	*discoveryService
	measurev1.UnimplementedMeasureServiceServer
}

func (ms *measureService) Write(measure measurev1.MeasureService_WriteServer) error {
	reply := func() error {
		if err := measure.Send(&measurev1.WriteResponse{}); err != nil {
			return err
		}
		return nil
	}
	sampled := ms.log.Sample(&zerolog.BasicSampler{N: 10})
	for {
		writeRequest, err := measure.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if errTime := timestamp.CheckPb(writeRequest.DataPoint.Timestamp); errTime != nil {
			sampled.Error().Err(errTime).Stringer("written", writeRequest).Msg("the data point time is invalid")
			if errResp := reply(); errResp != nil {
				return errResp
			}
			continue
		}
		entity, tagValues, shardID, err := ms.navigate(writeRequest.GetMetadata(), writeRequest.GetDataPoint().GetTagFamilies())
		if err != nil {
			sampled.Error().Err(err).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to navigate to the write target")
			if errResp := reply(); errResp != nil {
				return errResp
			}
			continue
		}
		iwr := &measurev1.InternalWriteRequest{
			Request:    writeRequest,
			ShardId:    uint32(shardID),
			SeriesHash: tsdb.HashEntity(entity),
		}
		if ms.log.Debug().Enabled() {
			iwr.EntityValues = tagValues.Encode()
		}
		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), iwr)
		_, errWritePub := ms.pipeline.Publish(data.TopicMeasureWrite, message)
		if errWritePub != nil {
			sampled.Error().Err(errWritePub).RawJSON("written", logger.Proto(writeRequest)).Msg("failed to send a message")
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

var emptyMeasureQueryResponse = &measurev1.QueryResponse{DataPoints: make([]*measurev1.DataPoint, 0)}

func (ms *measureService) Query(_ context.Context, req *measurev1.QueryRequest) (*measurev1.QueryResponse, error) {
	if err := timestamp.CheckTimeRange(req.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", req.GetTimeRange(), err)
	}
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req)
	feat, errQuery := ms.pipeline.Publish(data.TopicMeasureQuery, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		if errFeat == io.EOF {
			return emptyMeasureQueryResponse, nil
		}
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case []*measurev1.DataPoint:
		return &measurev1.QueryResponse{DataPoints: d}, nil
	case common.Error:
		return nil, errors.WithMessage(ErrQueryMsg, d.Msg())
	}
	return nil, nil
}

func (ms *measureService) TopN(_ context.Context, topNRequest *measurev1.TopNRequest) (*measurev1.TopNResponse, error) {
	if err := timestamp.CheckTimeRange(topNRequest.GetTimeRange()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v is invalid :%s", topNRequest.GetTimeRange(), err)
	}

	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), topNRequest)
	feat, errQuery := ms.pipeline.Publish(data.TopicTopNQuery, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	data := msg.Data()
	switch d := data.(type) {
	case []*measurev1.TopNList:
		return &measurev1.TopNResponse{Lists: d}, nil
	case common.Error:
		return nil, errors.WithMessage(ErrQueryMsg, d.Msg())
	}
	return nil, nil
}
