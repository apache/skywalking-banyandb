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
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const defaultRecvSize = 10 << 20

var (
	errServerCert        = errors.New("invalid server cert file")
	errServerKey         = errors.New("invalid server key file")
	errNoAddr            = errors.New("no address")
	errNoHealthAddr      = errors.New("no address for health check")
	errQueryMsg          = errors.New("invalid query message")
	errAccessLogRootPath = errors.New("access log root path is required")

	liaisonGrpcScope = observability.RootScope.SubScope("liaison_grpc")
)

// Server defines the gRPC server.
type Server interface {
	run.Unit
	GetPort() *uint32
}

// NodeRegistries contains the node registries.
type NodeRegistries struct {
	StreamNodeRegistry   NodeRegistry
	MeasureNodeRegistry  NodeRegistry
	PropertyNodeRegistry NodeRegistry
}

type server struct {
	databasev1.UnimplementedSnapshotServiceServer
	creds      credentials.TransportCredentials
	omr        observability.MetricsRegistry
	measureSVC *measureService
	ser        *grpclib.Server
	healthSer  *grpclib.Server
	log        *logger.Logger
	*propertyServer
	*topNAggregationRegistryServer
	*groupRegistryServer
	stopCh chan struct{}
	*indexRuleRegistryServer
	*measureRegistryServer
	streamSVC *streamService
	*streamRegistryServer
	*indexRuleBindingRegistryServer
	metrics                  *metrics
	keyFile                  string
	certFile                 string
	authConfigFile           string
	accessLogRootPath        string
	addr                     string
	healthAddr               string
	host                     string
	healthHost               string
	accessLogRecorders       []accessLogRecorder
	maxRecvMsgSize           run.Bytes
	port                     uint32
	healthPort               uint32
	closed                   uint32
	enableIngestionAccessLog bool
	tls                      bool
}

// NewServer returns a new gRPC server.
func NewServer(_ context.Context, pipeline, broadcaster queue.Client, schemaRegistry metadata.Repo, nr NodeRegistries, omr observability.MetricsRegistry) Server {
	streamSVC := &streamService{
		discoveryService: newDiscoveryService(schema.KindStream, schemaRegistry, nr.StreamNodeRegistry),
		pipeline:         pipeline,
		broadcaster:      broadcaster,
	}
	measureSVC := &measureService{
		discoveryService: newDiscoveryService(schema.KindMeasure, schemaRegistry, nr.MeasureNodeRegistry),
		pipeline:         pipeline,
		broadcaster:      broadcaster,
	}
	s := &server{
		omr:        omr,
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
			pipeline:       pipeline,
			nodeRegistry:   nr.PropertyNodeRegistry,
		},
	}
	s.accessLogRecorders = []accessLogRecorder{streamSVC, measureSVC}
	return s
}

func (s *server) PreRun(_ context.Context) error {
	s.log = logger.GetLogger("liaison-grpc")
	s.streamSVC.setLogger(s.log)
	s.measureSVC.setLogger(s.log)
	components := []*discoveryService{
		s.streamSVC.discoveryService,
		s.measureSVC.discoveryService,
	}
	for _, c := range components {
		c.SetLogger(s.log)
		if err := c.initialize(); err != nil {
			return err
		}
	}
	if s.authConfigFile != "" {
		if err := auth.LoadConfig(s.authConfigFile); err != nil {
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
	metrics := newMetrics(s.omr.With(liaisonGrpcScope))
	s.metrics = metrics
	s.streamSVC.metrics = metrics
	s.measureSVC.metrics = metrics
	s.propertyServer.metrics = metrics
	s.streamRegistryServer.metrics = metrics
	s.indexRuleBindingRegistryServer.metrics = metrics
	s.indexRuleRegistryServer.metrics = metrics
	s.measureRegistryServer.metrics = metrics
	s.groupRegistryServer.metrics = metrics
	s.topNAggregationRegistryServer.metrics = metrics
	return nil
}

func (s *server) Name() string {
	return "grpc"
}

func (s *server) Role() databasev1.Role {
	return databasev1.Role_ROLE_LIAISON
}

func (s *server) GetPort() *uint32 {
	return &s.port
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	s.maxRecvMsgSize = defaultRecvSize
	fs.VarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", "the size of max receiving message")
	fs.BoolVar(&s.tls, "tls", false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, "cert-file", "", "the TLS cert file")
	fs.StringVar(&s.keyFile, "key-file", "", "the TLS key file")
	fs.StringVar(&s.authConfigFile, "auth-config-file", "", "Path to the authentication config file (YAML format)")
	fs.StringVar(&s.host, "grpc-host", "", "the host of banyand listens")
	fs.StringVar(&s.healthHost, "grpc-health-host", "", "the host of health check service")
	fs.Uint32Var(&s.port, "grpc-port", 17912, "the port of banyand listens")
	fs.Uint32Var(&s.healthPort, "grpc-health-port", 0, "the port of health check service")
	fs.BoolVar(&auth.Cfg.HealthAuthEnabled, "enable-health-auth", false, "enable authentication for health check")
	fs.BoolVar(&s.enableIngestionAccessLog, "enable-ingestion-access-log", false, "enable ingestion access log")
	fs.StringVar(&s.accessLogRootPath, "access-log-root-path", "", "access log root path")
	fs.DurationVar(&s.streamSVC.writeTimeout, "stream-write-timeout", 15*time.Second, "stream write timeout")
	fs.DurationVar(&s.measureSVC.writeTimeout, "measure-write-timeout", 15*time.Second, "measure write timeout")
	return fs
}

func (s *server) Validate() error {
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
		return errNoAddr
	}
	s.healthAddr = net.JoinHostPort(s.healthHost, strconv.FormatUint(uint64(s.healthPort), 10))
	if s.healthAddr == ":" {
		return errNoHealthAddr
	}
	if s.enableIngestionAccessLog && s.accessLogRootPath == "" {
		return errAccessLogRootPath
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
	grpcPanicRecoveryHandler := func(p any) (err error) {
		s.log.Error().Interface("panic", p).Str("stack", string(debug.Stack())).Msg("recovered from panic")
		s.metrics.totalPanic.Inc(1)
		return status.Errorf(codes.Internal, "%s", p)
	}

	streamChain := []grpclib.StreamServerInterceptor{
		authStreamInterceptor,
		grpc_validator.StreamServerInterceptor(),
		recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	unaryChain := []grpclib.UnaryServerInterceptor{
		authInterceptor,
		grpc_validator.UnaryServerInterceptor(),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	healthStreamChain := streamChain
	healthUnaryChain := unaryChain
	if !auth.Cfg.HealthAuthEnabled {
		healthStreamChain = []grpclib.StreamServerInterceptor{
			grpc_validator.StreamServerInterceptor(),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		}
		healthUnaryChain = []grpclib.UnaryServerInterceptor{
			grpc_validator.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		}
	}
	if s.authConfigFile == "" {
		streamChain = []grpclib.StreamServerInterceptor{
			grpc_validator.StreamServerInterceptor(),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		}
		unaryChain = []grpclib.UnaryServerInterceptor{
			grpc_validator.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		}
	}
	commonOpts := opts
	healthOpts := opts

	commonOpts = append(commonOpts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(unaryChain...),
		grpclib.ChainStreamInterceptor(streamChain...),
	)
	healthOpts = append(healthOpts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(healthUnaryChain...),
		grpclib.ChainStreamInterceptor(healthStreamChain...),
	)
	s.ser = grpclib.NewServer(commonOpts...)
	s.healthSer = grpclib.NewServer(healthOpts...)

	commonv1.RegisterServiceServer(s.ser, &apiVersionService{})
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
	databasev1.RegisterSnapshotServiceServer(s.ser, s)
	if s.healthPort != 0 {
		grpc_health_v1.RegisterHealthServer(s.healthSer, health.NewServer())
	} else {
		grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())
	}

	s.stopCh = make(chan struct{})
	s.closed = 0
	go func() {
		lis, err := net.Listen("tcp", s.addr)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to listen")
			s.tryClose()
			return
		}
		s.log.Info().Str("addr", s.addr).Msg("Listening to")
		err = s.ser.Serve(lis)
		if err != nil {
			s.log.Error().Err(err).Msg("server is interrupted")
		}
		s.tryClose()
	}()
	if s.healthPort != 0 {
		go func() {
			listen, err := net.Listen("tcp", s.healthAddr)
			if err != nil {
				s.log.Error().Err(err).Msg("Failed to listen")
				s.tryClose()
				return
			}
			s.log.Info().Str("health check addr", s.healthAddr).Msg("Listening to")
			err = s.healthSer.Serve(listen)
			if err != nil {
				s.log.Error().Err(err).Msg("server is interrupted")
			}
			s.tryClose()
		}()
	}
	return s.stopCh
}

func (s *server) tryClose() {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		close(s.stopCh)
	}
}

func (s *server) GracefulStop() {
	s.log.Info().Msg("stopping")
	stopped := make(chan struct{})
	go func() {
		s.ser.GracefulStop()
		s.healthSer.GracefulStop()
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
		s.healthSer.Stop()
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
