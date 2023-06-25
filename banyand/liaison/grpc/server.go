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
	"runtime/debug"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/event"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const defaultRecvSize = 10 << 20

var (
	errServerCert        = errors.New("invalid server cert file")
	errServerKey         = errors.New("invalid server key file")
	errNoAddr            = errors.New("no address")
	errQueryMsg          = errors.New("invalid query message")
	errAccessLogRootPath = errors.New("access log root path is required")
)

type server struct {
	pipeline queue.Queue
	creds    credentials.TransportCredentials
	repo     discovery.ServiceRepo
	*indexRuleRegistryServer
	measureSVC *measureService
	log        *logger.Logger
	ser        *grpclib.Server
	*propertyServer
	*topNAggregationRegistryServer
	*groupRegistryServer
	stopCh    chan struct{}
	streamSVC *streamService
	*measureRegistryServer
	*streamRegistryServer
	*indexRuleBindingRegistryServer
	addr                     string
	keyFile                  string
	certFile                 string
	accessLogRootPath        string
	accessLogRecorders       []accessLogRecorder
	maxRecvMsgSize           run.Bytes
	tls                      bool
	enableIngestionAccessLog bool
}

// NewServer returns a new gRPC server.
func NewServer(_ context.Context, pipeline queue.Queue, repo discovery.ServiceRepo, schemaRegistry metadata.Service) run.Unit {
	streamSVC := &streamService{
		discoveryService: newDiscoveryService(pipeline),
	}
	measureSVC := &measureService{
		discoveryService: newDiscoveryService(pipeline),
	}
	s := &server{
		pipeline:   pipeline,
		repo:       repo,
		streamSVC:  streamSVC,
		measureSVC: measureSVC,
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
	s.accessLogRecorders = []accessLogRecorder{streamSVC, measureSVC}
	return s
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

	if s.enableIngestionAccessLog {
		for _, alr := range s.accessLogRecorders {
			if err := alr.activeIngestionAccessLog(s.accessLogRootPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *server) Name() string {
	return "grpc"
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	s.maxRecvMsgSize = defaultRecvSize
	fs.VarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", "the size of max receiving message")
	fs.BoolVarP(&s.tls, "tls", "", false, "connection uses TLS if true, else plain TCP")
	fs.StringVarP(&s.certFile, "cert-file", "", "", "the TLS cert file")
	fs.StringVarP(&s.keyFile, "key-file", "", "", "the TLS key file")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "the address of banyand listens")
	fs.BoolVarP(&s.enableIngestionAccessLog, "enable-ingestion-access-log", "", false, "enable ingestion access log")
	fs.StringVarP(&s.accessLogRootPath, "access-log-root-path", "", "", "access log root path")
	return fs
}

func (s *server) Validate() error {
	if s.addr == "" {
		return errNoAddr
	}
	if s.enableIngestionAccessLog && s.accessLogRootPath == "" {
		return errAccessLogRootPath
	}
	observability.UpdateAddress("grpc", s.addr)
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
	grpcPanicRecoveryHandler := func(p any) (err error) {
		s.log.Error().Interface("panic", p).Str("stack", string(debug.Stack())).Msg("recovered from panic")

		return status.Errorf(codes.Internal, "%s", p)
	}

	unaryMetrics, streamMetrics := observability.MetricsServerInterceptor()
	streamChain := []grpclib.StreamServerInterceptor{
		grpc_validator.StreamServerInterceptor(),
		recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	if streamMetrics != nil {
		streamChain = append(streamChain, streamMetrics)
	}
	unaryChain := []grpclib.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	if unaryMetrics != nil {
		unaryChain = append(unaryChain, unaryMetrics)
	}

	opts = append(opts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(unaryChain...),
		grpclib.ChainStreamInterceptor(streamChain...),
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
	databasev1.RegisterTopNAggregationRegistryServiceServer(s.ser, s.topNAggregationRegistryServer)
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
		if s.enableIngestionAccessLog {
			for _, alr := range s.accessLogRecorders {
				_ = alr.Close()
			}
		}
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

type accessLogRecorder interface {
	activeIngestionAccessLog(root string) error
	Close() error
}
