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
	"io"
	"log"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	ErrSeriesEvents    = errors.New("no seriesEvent")
	ErrShardEvents     = errors.New("no shardEvent")
	ErrInvalidSeriesID = errors.New("invalid seriesID")
	ErrServerCert      = errors.New("invalid server cert file")
	ErrServerKey       = errors.New("invalid server key file")
	ErrNoAddr          = errors.New("no address")
)

type Server struct {
	addr           string
	maxRecvMsgSize int
	tlsVal         bool
	certFile       string
	keyFile        string
	log            *logger.Logger
	ser            *grpclib.Server
	pipeline       queue.Queue
	repo           discovery.ServiceRepo
	shardInfo      *shardInfo
	seriesInfo     *seriesInfo
	v1.UnimplementedTraceServiceServer
}

type shardInfo struct {
	log        *logger.Logger
	shardEvent *shardEvent
}

func (s *shardInfo) Rev(message bus.Message) (resp bus.Message) {
	event, ok := message.Data().(*v1.ShardEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.shardEvent.setShardEvents(event)
	s.log.Info().
		Str("action", v1.Action_name[int32(event.Action)]).
		Uint64("shardID", event.Shard.Id).
		Msg("received a shard event")
	return
}

type shardEvent struct {
	shardEventsMap map[string]*v1.ShardEvent
	sync.RWMutex
}

func (s *shardEvent) setShardEvents(eventVal *v1.ShardEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	idx := eventVal.Shard.Series.GetName() + "-" + eventVal.Shard.Series.GetGroup()
	if eventVal.Action == v1.Action_ACTION_PUT {
		s.shardEventsMap[idx] = eventVal
	} else if eventVal.Action == v1.Action_ACTION_DELETE {
		delete(s.shardEventsMap, idx)
	}
}

func (s *shardEvent) getShardEvent(idx string) *v1.ShardEvent {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.shardEventsMap[idx]
}

type seriesInfo struct {
	log         *logger.Logger
	seriesEvent *seriesEvent
}

func (s *seriesInfo) Rev(message bus.Message) (resp bus.Message) {
	event, ok := message.Data().(*v1.SeriesEvent)
	if !ok {
		s.log.Warn().Msg("invalid event data type")
		return
	}
	s.seriesEvent.setSeriesEvents(event)
	s.log.Info().
		Str("action", v1.Action_name[int32(event.Action)]).
		Str("name", event.Series.Name).
		Str("group", event.Series.Group).
		Msg("received a shard event")
	return
}

type seriesEvent struct {
	seriesEventsMap map[string]*v1.SeriesEvent
	sync.RWMutex
}

func (s *seriesEvent) setSeriesEvents(seriesEventVal *v1.SeriesEvent) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	str := seriesEventVal.Series.GetName() + "-" + seriesEventVal.Series.GetGroup()
	if seriesEventVal.Action == v1.Action_ACTION_PUT {
		s.seriesEventsMap[str] = seriesEventVal
	} else if seriesEventVal.Action == v1.Action_ACTION_DELETE {
		delete(s.seriesEventsMap, str)
	}
}

func (s *seriesEvent) getSeriesEvent(idx string) *v1.SeriesEvent {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.seriesEventsMap[idx]
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
		shardInfo:  &shardInfo{shardEvent: &shardEvent{shardEventsMap: map[string]*v1.ShardEvent{}}},
		seriesInfo: &seriesInfo{seriesEvent: &seriesEvent{seriesEventsMap: map[string]*v1.SeriesEvent{}}},
	}
}

func (s *Server) Name() string {
	return "grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	size := 1024 * 1024 * 10
	_, currentFile, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(currentFile)
	serverCert := filepath.Join(basePath, "data/server_cert.pem")
	serverKey := filepath.Join(basePath, "data/server_key.pem")

	fs := run.NewFlagSet("grpc")
	fs.IntVarP(&s.maxRecvMsgSize, "maxRecvMsgSize", "", size, "The size of max receiving message")
	fs.BoolVarP(&s.tlsVal, "tlsVal", "", true, "Connection uses TLS if true, else plain TCP")
	fs.StringVarP(&s.certFile, "certFile", "", serverCert, "The TLS cert file")
	fs.StringVarP(&s.keyFile, "keyFile", "", serverKey, "The TLS key file")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "The address of banyand listens")

	return fs
}

func (s *Server) Validate() error {
	if s.addr == "" {
		return ErrNoAddr
	}
	if s.tlsVal {
		if s.certFile == "" {
			return ErrServerCert
		}
		if s.keyFile == "" {
			return ErrServerKey
		}
		_, errTLS := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
		if errTLS != nil {
			return errTLS
		}
	}
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
	if s.tlsVal {
		creds, _ := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
		opts = []grpclib.ServerOption{grpclib.Creds(creds)}
	}
	opts = append(opts, grpclib.MaxRecvMsgSize(s.maxRecvMsgSize))
	s.ser = grpclib.NewServer(opts...)
	v1.RegisterTraceServiceServer(s.ser, s)

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info().Msg("stopping")
	s.ser.GracefulStop()
}

func (s *Server) computeSeriesID(writeEntity *v1.WriteRequest, mapIndexName string) (SeriesID []byte, err error) {
	ana := logical.DefaultAnalyzer()
	metadata := common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        writeEntity.GetMetadata(),
	}
	schema, ruleError := ana.BuildTraceSchema(context.TODO(), metadata)
	if ruleError != nil {
		return nil, ruleError
	}
	seriesEventVal := s.seriesInfo.seriesEvent.getSeriesEvent(mapIndexName)
	if seriesEventVal == nil {
		return nil, ErrSeriesEvents
	}
	var str string
	var arr []string
	fieldRefs, errField := schema.CreateRef(seriesEventVal.FieldNamesCompositeSeriesId...)
	if errField != nil {
		return nil, errField
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
		return nil, ErrInvalidSeriesID
	}
	seriesID := []byte(str)

	return seriesID, nil
}

func (s *Server) computeShardID(seriesID []byte, mapIndexName string) (shardID uint, err error) {
	shardEventVal := s.shardInfo.shardEvent.getShardEvent(mapIndexName)
	if shardEventVal == nil {
		return 0, ErrShardEvents
	}
	shardNum := shardEventVal.GetShard().GetId()
	if shardNum < 1 {
		shardNum = 1
	}
	shardID, shardIDError := partition.ShardID(seriesID, uint32(shardNum))
	if shardIDError != nil {
		return 0, shardIDError
	}

	return shardID, nil
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
		if errSend := TraceWriteServer.Send(&v1.WriteResponse{}); errSend != nil {
			return errSend
		}
	}
}

func (s *Server) Query(ctx context.Context, entityCriteria *v1.QueryRequest) (*v1.QueryResponse, error) {
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), entityCriteria)
	feat, errQuery := s.pipeline.Publish(data.TopicQueryEvent, message)
	if errQuery != nil {
		return nil, errQuery
	}
	msg, errFeat := feat.Get()
	if errFeat != nil {
		return nil, errFeat
	}
	queryMsg := msg.Data()
	log.Println(queryMsg)
	return &v1.QueryResponse{}, nil // Entities
}

func assemblyWriteData(shardID uint, writeEntity *v1.WriteRequest, seriesID uint64) data.TraceWriteDate {
	return data.TraceWriteDate{ShardID: shardID, SeriesID: seriesID, WriteRequest: writeEntity}
}
