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
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"net"

	flatbuffers "github.com/google/flatbuffers/go"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/encoding"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var _ apiv1.QueryServer = (*Server)(nil)

type Server struct {
	apiv1.UnimplementedQueryServer
	addr string
	log  *logger.Logger
	ser  *grpclib.Server
}

func (s *Server) QueryTraces(ctx context.Context, criteria *apiv1.EntityCriteria) (*flatbuffers.Builder, error) {
	panic("implement me")
}

func NewServer(ctx context.Context) *Server {
	return &Server{}
}

func (s *Server) Name() string {
	return "query-grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.StringVarP(&s.addr, "addr", "", ":17922", "the address of banyandb-query endpoints")
	return fs
}

func (s *Server) Validate() error {
	return nil
}

func (s *Server) Serve() error {
	s.log = logger.GetLogger("query-grpc")
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatal("Failed to listen", logger.Error(err))
	}

	encoding.RegisterCodec(flatbuffers.FlatbuffersCodec{})
	s.ser = grpclib.NewServer()

	apiv1.RegisterQueryServer(s.ser, nil)

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info("stopping")
	s.ser.GracefulStop()
}
