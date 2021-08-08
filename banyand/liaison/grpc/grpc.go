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
	"embed"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "", "The TLS cert file")
	keyFile  = flag.String("key_file", "", "The TLS key file")
)

type Server struct {
	addr       string
	log        *logger.Logger
	ser        *grpclib.Server
	pipeline   queue.Queue
	repo       discovery.ServiceRepo
	shardInfo  *shardInfo
	seriesInfo *seriesInfo
	v1.UnimplementedTraceServiceServer
}

type shardInfo struct {
	log        *logger.Logger
	shardEvent *v1.ShardEvent
}

func (s *shardInfo) Rev(message bus.Message) (resp bus.Message) {
	shardEvent, ok := message.Data().(*v1.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.shardEvent = shardEvent
	s.log.Info().
		Str("action", v1.Action_name[int32(shardEvent.Action)]).
		Uint64("shardID", shardEvent.Shard.Id).
		Msg("received a shard event")
	return
}

type seriesInfo struct {
	log         *logger.Logger
	seriesEvent *v1.SeriesEvent
}

func (s *seriesInfo) Rev(message bus.Message) (resp bus.Message) {
	seriesEvent, ok := message.Data().(*v1.SeriesEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.seriesEvent = seriesEvent
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
	var opts []grpclib.ServerOption
	var f embed.FS
	if *tls {
		if *certFile == "" {
			serverCert, _ := f.ReadFile("data/x509/server_cert.pem")
			*certFile = string(serverCert)
		}
		if *keyFile == "" {
			serverKey, _ := f.ReadFile("data/x509/server_key.pem")
			*keyFile = string(serverKey)
		}
		cred, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpclib.ServerOption{grpclib.Creds(cred)}
	}
	s.ser = grpclib.NewServer(opts...)
	v1.RegisterTraceServiceServer(s.ser, s)

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info().Msg("stopping")
	s.ser.GracefulStop()
}

func assemblyWriteData(shardID uint, writeEntity *v1.WriteRequest, seriesID uint64) data.TraceWriteDate {
	return data.TraceWriteDate{ShardID: shardID, SeriesID: seriesID, WriteRequest: writeEntity}
}

func (s *Server) Write(TraceWriteServer v1.TraceService_WriteServer) error {
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
			Spec:        writeEntity.GetMetadata(),
		}
		schema, ruleError := ana.BuildTraceSchema(context.TODO(), metadata)
		if ruleError != nil {
			return ruleError
		}
		if s.seriesInfo.seriesEvent == nil {
			return errors.New("No seriesEvents")
		}
		var str string
		var arr []string
		fieldRefs, errField := schema.CreateRef(s.seriesInfo.seriesEvent.FieldNamesCompositeSeriesId...)
		if errField != nil {
			return errField
		}
		for _, ref := range fieldRefs {
			field := writeEntity.GetEntity().GetFields()[ref.Spec.Idx]
			switch v := field.GetValueType().(type) {
			case *v1.Field_StrArray:
				for j := 0; j < len(v.StrArray.Value); j++ {
					arr = append(arr, v.StrArray.Value[j])
				}
			case *v1.Field_IntArray:
				for t := 0; t < len(v.IntArray.Value); t++ {
					arr = append(arr, fmt.Sprint(v.IntArray.Value[t]))
				}
			case *v1.Field_Int:
				arr = append(arr, fmt.Sprint(v.Int.Value))
			case *v1.Field_Str:
				arr = append(arr, fmt.Sprint(v.Str.Value))
			}
		}
		str = strings.Join(arr, "")
		if str == "" {
			return errors.New("invalid seriesID")
		}
		seriesID := []byte(str)
		shardNum := s.shardInfo.shardEvent.GetShard().GetId()
		if shardNum < 1 {
			shardNum = 1
		}
		shardID, shardIDError := partition.ShardID(seriesID, uint32(shardNum))
		if shardIDError != nil {
			return shardIDError
		}
		mergeData := assemblyWriteData(shardID, writeEntity, convert.BytesToUint64(seriesID))
		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), mergeData)
		_, errWritePub := s.pipeline.Publish(data.TopicWriteEvent, message)
		if errWritePub != nil {
			return errWritePub
		}
		if errSend := TraceWriteServer.Send(&v1.WriteResponse{}); errSend != nil {
			return errSend
		}
	}
}

func (s *Server) Query(ctx context.Context, entityCriteria *v1.QueryRequest) (*v1.QueryResponse, error) {
	log.Println("entityCriteria:", entityCriteria)

	return &v1.QueryResponse{}, nil
}
