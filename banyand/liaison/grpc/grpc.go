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
	"net"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	logical "github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/run"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"io"
	"log"
	"net"
	"strings"
)

type Server struct {
	addr       string
	log        *logger.Logger
	ser        *grpc.Server
	pipeline   queue.Queue
	repo       discovery.ServiceRepo
	shardInfo  *shardInfo
	seriesInfo *seriesInfo
}

type shardInfo struct {
	log *logger.Logger
}
var shardEventData *v1.ShardEvent
func (s *shardInfo) Rev(message bus.Message) (resp bus.Message) {
	shardEvent, ok := message.Data().(*v1.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.log.Info().
		Str("action", v1.Action_name[int32(shardEvent.Action)]).
		Uint64("shardID", shardEvent.Shard.Id).
		Msg("received a shard event")
	return
}

type seriesInfo struct {
	log *logger.Logger
}

var seriesEventData *v1.SeriesEvent
func (s *seriesInfo) Rev(message bus.Message) (resp bus.Message) {
	seriesEvent, ok := message.Data().(*v1.SeriesEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.log.Info().
		Str("action", v1.Action_name[int32(seriesEvent.Action)]).
		Str("name", seriesEvent.Series.Name).
		Str("group", seriesEvent.Series.Group).
		Msg("received a shard event")
	return
}

func (s *Server) PreRun() error {
	s.log = logger.GetLogger("liaison-grpc")
	s.shardInfo.log = s.log
	s.seriesInfo.log = s.log
	err := s.repo.Subscribe(event.TopicShardEvent, s.shardInfo)
	if err != nil {
		return err
	}
	return s.repo.Subscribe(event.TopicSeriesEvent, s.seriesInfo)
}

func NewServer(ctx context.Context, pipeline queue.Queue, repo discovery.ServiceRepo) *Server {
	return &Server{
		pipeline:   pipeline,
		repo:       repo,
		shardInfo:  &shardInfo{},
		seriesInfo: &seriesInfo{},
	}
}

func (s *Server) Name() string {
	return "grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "the address of banyand listens")
	return fs
}

func (s *Server) Validate() error {
	return nil
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatal().Err(err).Msg("Failed to listen")
	}

	s.ser = grpc.NewServer()
	// TODO: add server implementation here
	v1.RegisterTraceServer(s.ser, v1.UnimplementedTraceServer{})

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info().Msg("stopping")
	s.ser.GracefulStop()
}

type TraceServer struct {
	v1.UnimplementedTraceServer
}

func (t *TraceServer) Write(TraceWriteServer v1.Trace_WriteServer) error {
	for {
		writeEntity, err := TraceWriteServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		//log.Println("writeEntity:", writeEntity)
		ana := logical.DefaultAnalyzer()
		metadata := common.Metadata{
			KindVersion: apischema.SeriesKindVersion,
			Spec:        writeEntity.MetaData(nil),
		}
		schema, ruleError := ana.BuildTraceSchema(context.TODO(), metadata)
		if ruleError != nil {
			return  ruleError
		}
		seriesIdLen := seriesEventData.FieldNamesCompositeSeriesIdLength()
		var str string
		var arr []string
		for i := 0; i < seriesIdLen; i++ {
			id := seriesEventData.FieldNamesCompositeSeriesId(i)
			if defined, sub := schema.FieldSubscript(string(id)); defined {
				var field v1.Field
				if ok := writeEntity.Entity(nil).Fields(&field, sub); !ok {
					return nil
				}
				unionValueTable := new(flatbuffers.Table)
				if ok := field.Value(unionValueTable); !ok {
					return nil
				}
				if field.ValueType() == v1.ValueTypeStringArray {
					unionStrArr := new(v1.StringArray)
					unionStrArr.Init(unionValueTable.Bytes, unionValueTable.Pos)
					for j := 0; j < unionStrArr.ValueLength(); j++ {
						arr = append(arr, string(unionStrArr.Value(j)))
					}
				} else if field.ValueType() == v1.ValueTypeIntArray {
					unionIntArr := new(v1.IntArray)
					unionIntArr.Init(unionValueTable.Bytes, unionValueTable.Pos)
					for t := 0; t < unionIntArr.ValueLength(); t++ {
						arr = append(arr, fmt.Sprint(unionIntArr.Value(t)))
					}
				} else if field.ValueType() == v1.ValueTypeInt {
					unionInt := new(v1.Int)
					unionInt.Init(unionValueTable.Bytes, unionValueTable.Pos)
					arr = append(arr, fmt.Sprint(unionInt.Value()))
				} else if field.ValueType() == v1.ValueTypeString {
					unionStr := new(v1.String)
					unionStr.Init(unionValueTable.Bytes, unionValueTable.Pos)
					arr = append(arr, string(unionStr.Value()))
				}
			}
		}
		str = strings.Join(arr, "")
		if str == "" {
			return errors.New("invalid seriesID")
		}
		seriesID := []byte(str)
		shardNum := shardEventData.Shard(nil).Id()
		if shardNum < 1 {
			shardNum = 1
		}
		shardID, shardIdError := partition.ShardID(seriesID, uint(shardNum))
		if shardIdError != nil {
			return shardIdError
		}
		log.Println("shardID:", shardID)
		builder := flatbuffers.NewBuilder(0)
		v1.WriteResponseStart(builder)
		builder.Finish(v1.WriteResponseEnd(builder))
		if errSend := TraceWriteServer.Send(builder); errSend != nil {
			return errSend
		}
		//queue
	}
}

func (t *TraceServer) Query(ctx context.Context, entityCriteria *v1.EntityCriteria) (*flatbuffers.Builder, error) {
	log.Println("entityCriteria:", entityCriteria)

	// receive entity, then serialize entity
	b := flatbuffers.NewBuilder(0)
	v1.EntityStart(b)
	b.Finish(v1.EntityEnd(b))

	return b, nil
}
