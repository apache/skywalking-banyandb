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
	"github.com/apache/skywalking-banyandb/pkg/run"
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
