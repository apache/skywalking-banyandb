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
	"fmt"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	flatbuffers "github.com/google/flatbuffers/go"
	grpclib "google.golang.org/grpc"
	"io"
	"net"
	"sync"
)

type Server struct {
	addr     string
	log      *logger.Logger
	ser      *grpclib.Server
	pipeline queue.Queue
	writeEntity *v1.WriteEntity
}

func NewServer(ctx context.Context, pipeline queue.Queue) *Server {
	return &Server{pipeline: pipeline}
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
func init(){
	//encoding.RegisterCodec(flatbuffers.FlatbuffersCodec{})
}
func (s *Server) Serve() error {
	s.log = logger.GetLogger("grpc")
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatal("Failed to listen", logger.Error(err))
	}


	s.ser = grpclib.NewServer(grpclib.CustomCodec(flatbuffers.FlatbuffersCodec{}))
	//s.ser = grpclib.NewServer()

	v1.RegisterTraceServer(s.ser, &TraceServer{})

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info("stopping")
	s.ser.GracefulStop()
}

//var _ gomock.TestHelper = (*TraceServer)(nil)

type TraceServer struct {
	v1.UnimplementedTraceServer
	writeData []*v1.WriteEntity
	mu         sync.Mutex
}

func (t *TraceServer) Write(TraceWriteServer v1.Trace_WriteServer) error {
	for {
		writeEntity, err := TraceWriteServer.Recv()
		fmt.Println(writeEntity)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		t.writeData = append(t.writeData, writeEntity)
		builder := flatbuffers.NewBuilder(0)
		v1.WriteResponseStart(builder)
		builder.Finish(v1.WriteResponseEnd(builder))
		if err := TraceWriteServer.Send(builder); err != nil {
			return err
		}
		//writeEntity.Entity().Fields()
		//writeEntity.MetaData(nil).Group()
		//serviceID+instanceID
		//seriesID := hash(fieds, f1, f2)
		//shardID := shardingFunc(seriesID, shardNum)
		//queue
		//for _, l := range t.writeData {
		//	if err := TraceWriteServer.Send(l); err != nil {
		//		return err
		//	}
		//}
	}
}

func (t *TraceServer) Query(ctx context.Context, entityCriteria *v1.EntityCriteria) (*flatbuffers.Builder, error) {
	b := flatbuffers.NewBuilder(0)
	v1.EntityCriteriaStart(b)
	b.Finish(v1.EntityCriteriaEnd(b))

	return b, nil
}
