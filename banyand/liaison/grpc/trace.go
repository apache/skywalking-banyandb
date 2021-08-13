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
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
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
	ErrSeriesEvents    = errors.New("no seriesEvent")
	ErrShardEvents     = errors.New("no shardEvent")
	ErrInvalidSeriesID = errors.New("invalid seriesID")
	ErrServerCert      = errors.New("invalid server cert file")
	ErrServerKey       = errors.New("invalid server key file")
	ErrNoAddr          = errors.New("no address")
	ErrQueryMsg        = errors.New("invalid query message")

	defaultRecvSize = 1024 * 1024 * 10
)

type Server struct {
	addr           string
	maxRecvMsgSize int
	tls            bool
	certFile       string
	keyFile        string
	log            *logger.Logger
	ser            *grpclib.Server
	pipeline       queue.Queue
	repo           discovery.ServiceRepo
	shardInfo      *shardInfo
	seriesInfo     *seriesInfo
	tracev1.UnimplementedTraceServiceServer
	creds credentials.TransportCredentials
}

type shardInfo struct {
	log            *logger.Logger
	shardEventsMap map[string]uint32
	sync.RWMutex
}

func (s *shardInfo) Rev(message bus.Message) (resp bus.Message) {
	e, ok := message.Data().(*databasev1.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	s.setShardNum(e)
	s.log.Info().
		Str("action", databasev1.Action_name[int32(e.Action)]).
		Uint64("shardID", e.Shard.Id).
		Msg("received a shard e")
	return
}

func (s *shardInfo) setShardNum(eventVal *databasev1.ShardEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	idx := eventVal.Shard.Series.GetName() + "-" + eventVal.Shard.Series.GetGroup()
	if eventVal.Action == databasev1.Action_ACTION_PUT {
		s.shardEventsMap[idx] = eventVal.Shard.Total
	} else if eventVal.Action == databasev1.Action_ACTION_DELETE {
		delete(s.shardEventsMap, idx)
	}
}

func (s *shardInfo) shardNum(idx string) uint32 {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.shardEventsMap[idx]
}

type seriesInfo struct {
	log             *logger.Logger
	seriesEventsMap map[string][]int
	sync.RWMutex
}

func (s *seriesInfo) Rev(message bus.Message) (resp bus.Message) {
	e, ok := message.Data().(*databasev1.SeriesEvent)
	if !ok {
		s.log.Warn().Msg("invalid e data type")
		return
	}
	s.updateFieldIndexCompositeSeriesID(e)
	s.log.Info().
		Str("action", databasev1.Action_name[int32(e.Action)]).
		Str("name", e.Series.Name).
		Str("group", e.Series.Group).
		Msg("received a shard e")
	return
}

func (s *seriesInfo) updateFieldIndexCompositeSeriesID(seriesEventVal *databasev1.SeriesEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	str := seriesEventVal.Series.GetName() + "-" + seriesEventVal.Series.GetGroup()
	if seriesEventVal.Action == databasev1.Action_ACTION_PUT {
		ana := logical.DefaultAnalyzer()
		metadata := common.Metadata{
			KindVersion: apischema.SeriesKindVersion,
			Spec:        seriesEventVal.Series,
		}
		schema, err := ana.BuildTraceSchema(context.TODO(), metadata)
		if err != nil {
			s.log.Err(err).Msg("build trace schema")
			return
		}
		fieldRefs, errField := schema.CreateRef(seriesEventVal.FieldNamesCompositeSeriesId...)
		if errField != nil {
			s.log.Err(errField).Msg("create series ref")
			return
		}
		refIdx := make([]int, len(fieldRefs))
		for i, ref := range fieldRefs {
			refIdx[i] = ref.Spec.Idx
		}
		s.seriesEventsMap[str] = refIdx
	} else if seriesEventVal.Action == databasev1.Action_ACTION_DELETE {
		delete(s.seriesEventsMap, str)
	}
}

func (s *seriesInfo) FieldIndexCompositeSeriesID(seriesMeta string) []int {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.seriesEventsMap[seriesMeta]
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

func NewServer(_ context.Context, pipeline queue.Queue, repo discovery.ServiceRepo) *Server {
	return &Server{
		pipeline:   pipeline,
		repo:       repo,
		shardInfo:  &shardInfo{shardEventsMap: make(map[string]uint32)},
		seriesInfo: &seriesInfo{seriesEventsMap: make(map[string][]int)},
	}
}

func (s *Server) Name() string {
	return "grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.IntVarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", defaultRecvSize, "The size of max receiving message")
	fs.BoolVarP(&s.tls, "tls", "", true, "Connection uses TLS if true, else plain TCP")
	fs.StringVarP(&s.certFile, "cert-file", "", "server_cert.pem", "The TLS cert file")
	fs.StringVarP(&s.keyFile, "key-file", "", "server_key.pem", "The TLS key file")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "The address of banyand listens")
	return fs
}

func (s *Server) Validate() error {
	if s.addr == "" {
		return ErrNoAddr
	}
	if !s.tls {
		return nil
	}
	if s.certFile == "" {
		return ErrServerCert
	}
	if s.keyFile == "" {
		return ErrServerKey
	}
	creds, errTLS := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
	if errTLS != nil {
		return errTLS
	}
	s.creds = creds
	return nil
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatal().Err(err).Msg("Failed to listen")
	}
	if errValidate := s.Validate(); errValidate != nil {
		s.log.Fatal().Err(errValidate).Msg("Failed to validate data")
	}
	var opts []grpclib.ServerOption
	if s.tls {
		opts = []grpclib.ServerOption{grpclib.Creds(s.creds)}
	}
	opts = append(opts, grpclib.MaxRecvMsgSize(s.maxRecvMsgSize))
	s.ser = grpclib.NewServer(opts...)
	tracev1.RegisterTraceServiceServer(s.ser, s)

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info().Msg("stopping")
	s.ser.GracefulStop()
}

func (s *Server) computeSeriesID(writeEntity *tracev1.WriteRequest, mapIndexName string) ([]byte, error) {
	fieldNames := s.seriesInfo.FieldIndexCompositeSeriesID(mapIndexName)
	if fieldNames == nil {
		return nil, ErrSeriesEvents
	}
	var str string
	for _, ref := range fieldNames {
		field := writeEntity.GetEntity().GetFields()[ref]
		switch v := field.GetValueType().(type) {
		case *modelv1.Field_StrArray:
			for j := 0; j < len(v.StrArray.Value); j++ {
				str = str + v.StrArray.Value[j]
			}
		case *modelv1.Field_IntArray:
			for t := 0; t < len(v.IntArray.Value); t++ {
				str = str + strconv.FormatInt(v.IntArray.Value[t], 10)
			}
		case *modelv1.Field_Int:
			str = str + strconv.FormatInt(v.Int.Value, 10)
		case *modelv1.Field_Str:
			str = str + v.Str.Value
		}
		str = str + ":"
	}
	if str == "" {
		return nil, ErrInvalidSeriesID
	}

	return []byte(str), nil
}

func (s *Server) computeShardID(seriesID []byte, mapIndexName string) (uint, error) {
	shardNum := s.shardInfo.shardNum(mapIndexName)
	if shardNum < 1 {
		return 0, ErrShardEvents
	}
	shardID, shardIDError := partition.ShardID(seriesID, shardNum)
	if shardIDError != nil {
		return 0, shardIDError
	}
	return shardID, nil
}

func (s *Server) Write(stream tracev1.TraceService_WriteServer) error {
	for {
		writeEntity, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		mapIndexName := writeEntity.GetMetadata().GetName() + "-" + writeEntity.GetMetadata().GetGroup()
		seriesID, err := s.computeSeriesID(writeEntity, mapIndexName)
		if err != nil {
			return err
		}
		shardID, err := s.computeShardID(seriesID, mapIndexName)
		if err != nil {
			return err
		}
		mergeData := assemblyWriteData(shardID, writeEntity, convert.BytesToUint64(seriesID))
		message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), mergeData)
		_, errWritePub := s.pipeline.Publish(data.TopicWriteEvent, message)
		if errWritePub != nil {
			return errWritePub
		}
		if errSend := stream.Send(&tracev1.WriteResponse{}); errSend != nil {
			return errSend
		}
	}
}

func (s *Server) Query(_ context.Context, entityCriteria *tracev1.QueryRequest) (*tracev1.QueryResponse, error) {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), entityCriteria)
	feat, errQuery := s.pipeline.Publish(data.TopicQueryEvent, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	queryMsg, ok := msg.Data().([]data.Entity)
	if !ok {
		return nil, ErrQueryMsg
	}
	var arr []*tracev1.Entity
	for i := 0; i < len(queryMsg); i++ {
		arr = append(arr, queryMsg[i].Entity)
	}

	return &tracev1.QueryResponse{Entities: arr}, nil
}

func assemblyWriteData(shardID uint, writeEntity *tracev1.WriteRequest, seriesID uint64) data.TraceWriteDate {
	return data.TraceWriteDate{ShardID: shardID, SeriesID: seriesID, WriteRequest: writeEntity}
}
