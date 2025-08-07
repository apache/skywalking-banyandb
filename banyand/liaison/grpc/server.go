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
	"strconv"
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
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

const defaultRecvSize = 10 << 20

var (
	errServerCert        = errors.New("invalid server cert file")
	errServerKey         = errors.New("invalid server key file")
	errNoAddr            = errors.New("no address")
	errQueryMsg          = errors.New("invalid query message")
	errAccessLogRootPath = errors.New("access log root path is required")

	liaisonGrpcScope = observability.RootScope.SubScope("liaison_grpc")
)

// Server defines the gRPC server.
type Server interface {
	run.Unit
	GetAuthCfg() *auth.Config
	GetPort() *uint32
}

// NodeRegistries contains the node registries.
type NodeRegistries struct {
	StreamLiaisonNodeRegistry  NodeRegistry
	MeasureLiaisonNodeRegistry NodeRegistry
	PropertyNodeRegistry       NodeRegistry
}

type server struct {
	databasev1.UnimplementedSnapshotServiceServer
	omr        observability.MetricsRegistry
	schemaRepo metadata.Repo
	*topNAggregationRegistryServer
	*groupRegistryServer
	stopCh chan struct{}
	*indexRuleRegistryServer
	*measureRegistryServer
	streamSVC *streamService
	*streamRegistryServer
	measureSVC *measureService
	log        *logger.Logger
	*propertyRegistryServer
	ser         *grpclib.Server
	tlsReloader *pkgtls.Reloader
	*propertyServer
	*indexRuleBindingRegistryServer
	*traceRegistryServer
	groupRepo                *groupRepo
	metrics                  *metrics
	certFile                 string
	keyFile                  string
	authConfigFile           string
	cfg                      *auth.Config
	host                     string
	addr                     string
	accessLogRootPath        string
	accessLogRecorders       []accessLogRecorder
	maxRecvMsgSize           run.Bytes
	port                     uint32
	enableIngestionAccessLog bool
	tls                      bool
}

// NewServer returns a new gRPC server.
func NewServer(_ context.Context, tir1Client, tir2Client, broadcaster queue.Client,
	schemaRegistry metadata.Repo, nr NodeRegistries, omr observability.MetricsRegistry,
) Server {
	gr := &groupRepo{resourceOpts: make(map[string]*commonv1.ResourceOpts)}
	er := &entityRepo{entitiesMap: make(map[identity]partition.Locator), measureMap: make(map[identity]*databasev1.Measure)}
	streamSVC := &streamService{
		discoveryService: newDiscoveryService(schema.KindStream, schemaRegistry, nr.StreamLiaisonNodeRegistry, gr),
		pipeline:         tir1Client,
		broadcaster:      broadcaster,
	}
	measureSVC := &measureService{
		discoveryService: newDiscoveryServiceWithEntityRepo(schema.KindMeasure, schemaRegistry, nr.MeasureLiaisonNodeRegistry, gr, er),
		pipeline:         tir1Client,
		broadcaster:      broadcaster,
	}

	s := &server{
		omr:        omr,
		streamSVC:  streamSVC,
		measureSVC: measureSVC,
		groupRepo:  gr,
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
			schemaRegistry:   schemaRegistry,
			pipeline:         tir2Client,
			nodeRegistry:     nr.PropertyNodeRegistry,
			discoveryService: newDiscoveryService(schema.KindProperty, schemaRegistry, nr.PropertyNodeRegistry, gr),
		},
		propertyRegistryServer: &propertyRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		traceRegistryServer: &traceRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		schemaRepo: schemaRegistry,
		cfg:        auth.InitCfg(),
	}
	s.accessLogRecorders = []accessLogRecorder{streamSVC, measureSVC}

	return s
}

func (s *server) PreRun(_ context.Context) error {
	s.log = logger.GetLogger("liaison-grpc")
	s.streamSVC.setLogger(s.log.Named("stream-t1"))
	s.measureSVC.setLogger(s.log)
	s.propertyServer.SetLogger(s.log)
	components := []*discoveryService{
		s.streamSVC.discoveryService,
		s.measureSVC.discoveryService,
		s.propertyServer.discoveryService,
	}
	s.schemaRepo.RegisterHandler("liaison", schema.KindGroup, s.groupRepo)
	for _, c := range components {
		c.SetLogger(s.log)
		if err := c.initialize(); err != nil {
			return err
		}
	}
	if s.authConfigFile != "" {
		if err := auth.LoadConfig(s.cfg, s.authConfigFile); err != nil {
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
	s.propertyRegistryServer.metrics = metrics
	s.traceRegistryServer.metrics = metrics

	if s.tls {
		var err error
		s.tlsReloader, err = pkgtls.NewReloader(s.certFile, s.keyFile, s.log)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to initialize TLSReloader for gRPC")
			return err
		}
	}
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

// GetAuthCfg returns auth cfg (for httpserver).
func (s *server) GetAuthCfg() *auth.Config {
	return s.cfg
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	s.maxRecvMsgSize = defaultRecvSize
	fs.VarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", "the size of max receiving message")
	fs.BoolVar(&s.tls, "tls", false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, "cert-file", "", "the TLS cert file")
	fs.StringVar(&s.keyFile, "key-file", "", "the TLS key file")
	fs.StringVar(&s.authConfigFile, "auth-config-file", "", "Path to the authentication config file (YAML format)")
	fs.BoolVar(&s.cfg.HealthAuthEnabled, "enable-health-auth", false, "enable authentication for health check")
	fs.StringVar(&s.host, "grpc-host", "", "the host of banyand listens")
	fs.Uint32Var(&s.port, "grpc-port", 17912, "the port of banyand listens")
	fs.BoolVar(&s.enableIngestionAccessLog, "enable-ingestion-access-log", false, "enable ingestion access log")
	fs.StringVar(&s.accessLogRootPath, "access-log-root-path", "", "access log root path")
	fs.DurationVar(&s.streamSVC.writeTimeout, "stream-write-timeout", 15*time.Second, "timeout for writing stream among liaison nodes")
	fs.DurationVar(&s.measureSVC.writeTimeout, "measure-write-timeout", 15*time.Second, "timeout for writing measure among liaison nodes")
	fs.DurationVar(&s.measureSVC.maxWaitDuration, "measure-metadata-cache-wait-duration", 0,
		"the maximum duration to wait for metadata cache to load (for testing purposes)")
	fs.DurationVar(&s.streamSVC.maxWaitDuration, "stream-metadata-cache-wait-duration", 0,
		"the maximum duration to wait for metadata cache to load (for testing purposes)")
	fs.IntVar(&s.propertyServer.repairQueueCount, "property-repair-queue-count", 128, "the number of queues for property repair")
	return fs
}

func (s *server) Validate() error {
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
		return errNoAddr
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
	return nil
}

func (s *server) Serve() run.StopNotify {
	var opts []grpclib.ServerOption
	if s.tls {
		if err := s.tlsReloader.Start(); err != nil {
			s.log.Error().Err(err).Msg("Failed to start TLSReloader for gRPC")
			close(s.stopCh)
			return s.stopCh
		}
		s.log.Info().Str("certFile", s.certFile).Str("keyFile", s.keyFile).Msg("Starting TLS file monitoring")
		tlsConfig := s.tlsReloader.GetTLSConfig()
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpclib.Creds(creds))
	}
	grpcPanicRecoveryHandler := func(p any) (err error) {
		s.log.Error().Interface("panic", p).Str("stack", string(debug.Stack())).Msg("recovered from panic")
		s.metrics.totalPanic.Inc(1)
		return status.Errorf(codes.Internal, "%s", p)
	}

	streamChain := []grpclib.StreamServerInterceptor{
		grpc_validator.StreamServerInterceptor(),
		recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	unaryChain := []grpclib.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	if s.authConfigFile != "" {
		streamChain = append(streamChain, authStreamInterceptor(s.cfg))
		unaryChain = append(unaryChain, authInterceptor(s.cfg))
	}

	opts = append(opts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(unaryChain...),
		grpclib.ChainStreamInterceptor(streamChain...),
	)
	s.ser = grpclib.NewServer(opts...)

	commonv1.RegisterServiceServer(s.ser, &apiVersionService{})
	streamv1.RegisterStreamServiceServer(s.ser, s.streamSVC)
	measurev1.RegisterMeasureServiceServer(s.ser, s.measureSVC)
	databasev1.RegisterGroupRegistryServiceServer(s.ser, s.groupRegistryServer)
	databasev1.RegisterIndexRuleBindingRegistryServiceServer(s.ser, s.indexRuleBindingRegistryServer)
	databasev1.RegisterIndexRuleRegistryServiceServer(s.ser, s.indexRuleRegistryServer)
	databasev1.RegisterStreamRegistryServiceServer(s.ser, s.streamRegistryServer)
	databasev1.RegisterMeasureRegistryServiceServer(s.ser, s.measureRegistryServer)
	propertyv1.RegisterPropertyServiceServer(s.ser, s.propertyServer)
	databasev1.RegisterTopNAggregationRegistryServiceServer(s.ser, s.topNAggregationRegistryServer)
	databasev1.RegisterSnapshotServiceServer(s.ser, s)
	databasev1.RegisterPropertyRegistryServiceServer(s.ser, s.propertyRegistryServer)
	databasev1.RegisterTraceRegistryServiceServer(s.ser, s.traceRegistryServer)
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())

	s.stopCh = make(chan struct{})
	s.propertyServer.startRepairQueue(s.stopCh)
	s.log.Info().Str("addr", s.addr).Msg("Starting gRPC server")
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
	if s.tls && s.tlsReloader != nil {
		s.tlsReloader.Stop()
	}
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
