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

// Package grpc implements the gRPC services defined by APIs.
package grpc

import (
	"context"
	"net"
	"time"

	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
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
	errServerCert = errors.New("invalid server cert file")
	errServerKey  = errors.New("invalid server key file")
	errNoAddr     = errors.New("no address")
	errQueryMsg   = errors.New("invalid query message")
)

type server struct {
	pipeline queue.Queue
	creds    credentials.TransportCredentials
	repo     discovery.ServiceRepo
	stopCh   chan struct{}
	*measureRegistryServer
	log *logger.Logger
	ser *grpclib.Server
	*propertyServer
	*topNAggregationRegistryServer
	*groupRegistryServer
	*indexRuleRegistryServer
	streamSVC  *streamService
	measureSVC *measureService
	*streamRegistryServer
	*indexRuleBindingRegistryServer
	addr           string
	keyFile        string
	certFile       string
	maxRecvMsgSize int
	tls            bool
}

// NewServer returns a new gRPC server.
func NewServer(_ context.Context, pipeline queue.Queue, repo discovery.ServiceRepo, schemaRegistry metadata.Service) run.Unit {
	return &server{
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
		propertyServer: &propertyServer{
			schemaRegistry: schemaRegistry,
		},
	}
}

func (s *server) PreRun() error {
	s.log = logger.GetLogger("liaison-grpc")
	s.streamSVC.setLogger(s.log)
	s.measureSVC.setLogger(s.log)
	components := []struct {
		discoverySVC *discoveryService
		shardEvent   bus.Topic
		entityEvent  bus.Topic
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

func (s *server) Name() string {
	return "grpc"
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.IntVarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", defaultRecvSize, "the size of max receiving message")
	fs.BoolVarP(&s.tls, "tls", "", false, "connection uses TLS if true, else plain TCP")
	fs.StringVarP(&s.certFile, "cert-file", "", "", "the TLS cert file")
	fs.StringVarP(&s.keyFile, "key-file", "", "", "the TLS key file")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "the address of banyand listens")
	return fs
}

func (s *server) Validate() error {
	if s.addr == "" {
		return errNoAddr
	}
	if !s.tls {
		return nil
	}
	if s.certFile == "" {
		return errServerCert
	}
	if s.keyFile == "" {
		return errServerKey
	}
	creds, errTLS := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
	if errTLS != nil {
		return errors.Wrap(errTLS, "failed to load cert and key")
	}
	s.creds = creds
	return nil
}

func (s *server) Serve() run.StopNotify {
	var opts []grpclib.ServerOption
	if s.tls {
		opts = []grpclib.ServerOption{grpclib.Creds(s.creds)}
	}
	opts = append(opts, grpclib.MaxRecvMsgSize(s.maxRecvMsgSize),
		grpclib.UnaryInterceptor(grpc_validator.UnaryServerInterceptor()),
		grpclib.StreamInterceptor(grpc_validator.StreamServerInterceptor()),
	)
	s.ser = grpclib.NewServer(opts...)

	streamv1.RegisterStreamServiceServer(s.ser, s.streamSVC)
	measurev1.RegisterMeasureServiceServer(s.ser, s.measureSVC)
	// register *Registry
	databasev1.RegisterGroupRegistryServiceServer(s.ser, s.groupRegistryServer)
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(s.ser, s.indexRuleBindingRegistryServer)
	databasev1.RegisterIndexRuleRegistryServiceServer(s.ser, s.indexRuleRegistryServer)
	databasev1.RegisterStreamRegistryServiceServer(s.ser, s.streamRegistryServer)
	databasev1.RegisterMeasureRegistryServiceServer(s.ser, s.measureRegistryServer)
	propertyv1.RegisterPropertyServiceServer(s.ser, s.propertyServer)
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())

	s.stopCh = make(chan struct{})
	go func() {
		lis, err := net.Listen("tcp", s.addr)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to listen")
			close(s.stopCh)
			return
		}
		s.log.Info().Str("addr", s.addr).Msg("Listening to")
		err = s.ser.Serve(lis)
		if err != nil {
			s.log.Error().Err(err).Msg("server is interrupted")
		}
		close(s.stopCh)
	}()
	return s.stopCh
}

func (s *server) GracefulStop() {
	s.log.Info().Msg("stopping")
	stopped := make(chan struct{})
	go func() {
		s.ser.GracefulStop()
		close(stopped)
	}()

	t := time.NewTimer(10 * time.Second)
	select {
	case <-t.C:
		s.ser.Stop()
		s.log.Info().Msg("force stopped")
	case <-stopped:
		t.Stop()
		s.log.Info().Msg("stopped gracefully")
	}
}
