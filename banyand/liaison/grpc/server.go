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

	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const defaultRecvSize = 1024 * 1024 * 10

var (
	ErrServerCert = errors.New("invalid server cert file")
	ErrServerKey  = errors.New("invalid server key file")
	ErrNoAddr     = errors.New("no address")
	ErrQueryMsg   = errors.New("invalid query message")
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
	creds          credentials.TransportCredentials

	stopCh chan struct{}

	streamSVC  *streamService
	measureSVC *measureService
	*streamRegistryServer
	*indexRuleBindingRegistryServer
	*indexRuleRegistryServer
	*measureRegistryServer
	*groupRegistryServer
	*topNAggregationRegistryServer
}

func NewServer(_ context.Context, pipeline queue.Queue, repo discovery.ServiceRepo, schemaRegistry metadata.Service) *Server {
	return &Server{
		pipeline: pipeline,
		repo:     repo,
		streamSVC: &streamService{
			discoveryService: newDiscoveryService(pipeline),
		},
		measureSVC: &measureService{
			discoveryService: newDiscoveryService(pipeline),
		},
		streamRegistryServer: &streamRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		indexRuleBindingRegistryServer: &indexRuleBindingRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		indexRuleRegistryServer: &indexRuleRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		measureRegistryServer: &measureRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		groupRegistryServer: &groupRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		topNAggregationRegistryServer: &topNAggregationRegistryServer{
			schemaRegistry: schemaRegistry,
		},
	}
}

func (s *Server) PreRun() error {
	s.log = logger.GetLogger("liaison-grpc")
	components := []struct {
		shardEvent   bus.Topic
		entityEvent  bus.Topic
		discoverySVC *discoveryService
	}{
		{
			shardEvent:   event.StreamTopicShardEvent,
			entityEvent:  event.StreamTopicEntityEvent,
			discoverySVC: s.streamSVC.discoveryService,
		},
		{
			shardEvent:   event.MeasureTopicShardEvent,
			entityEvent:  event.MeasureTopicEntityEvent,
			discoverySVC: s.measureSVC.discoveryService,
		},
	}
	for _, c := range components {
		c.discoverySVC.SetLogger(s.log)
		err := s.repo.Subscribe(c.shardEvent, c.discoverySVC.shardRepo)
		if err != nil {
			return err
		}
		err = s.repo.Subscribe(c.entityEvent, c.discoverySVC.entityRepo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Name() string {
	return "grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.IntVarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", defaultRecvSize, "The size of max receiving message")
	fs.BoolVarP(&s.tls, "tls", "", false, "Connection uses TLS if true, else plain TCP")
	fs.StringVarP(&s.certFile, "cert-file", "", "", "The TLS cert file")
	fs.StringVarP(&s.keyFile, "key-file", "", "", "The TLS key file")
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
		return errors.Wrap(errTLS, "failed to load cert and key")
	}
	s.creds = creds
	return nil
}

func (s *Server) Serve() run.StopNotify {
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

	streamv1.RegisterStreamServiceServer(s.ser, s.streamSVC)
	// measurev1.RegisterMeasureServiceServer(s.ser, s.measureSVC)
	// register *Registry
	databasev1.RegisterGroupRegistryServiceServer(s.ser, s.groupRegistryServer)
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(s.ser, s.indexRuleBindingRegistryServer)
	databasev1.RegisterIndexRuleRegistryServiceServer(s.ser, s.indexRuleRegistryServer)
	databasev1.RegisterStreamRegistryServiceServer(s.ser, s.streamRegistryServer)
	databasev1.RegisterMeasureRegistryServiceServer(s.ser, s.measureRegistryServer)

	s.stopCh = make(chan struct{})
	go func() {
		s.log.Info().Str("addr", s.addr).Msg("Listening to")
		_ = s.ser.Serve(lis)
		close(s.stopCh)
	}()
	return s.stopCh
}

func (s *Server) GracefulStop() {
	s.log.Info().Msg("stopping")
	s.ser.GracefulStop()
}
