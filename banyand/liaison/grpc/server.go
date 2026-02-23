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
	"strings"
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

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

const (
	defaultRecvSize         = 16 << 20
	maxReasonableBufferSize = 1 << 30 // 1GB
)

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
	GetAuthReloader() *auth.Reloader
	GetPort() *uint32
}

// NodeRegistries contains the node registries.
type NodeRegistries struct {
	StreamLiaisonNodeRegistry  NodeRegistry
	MeasureLiaisonNodeRegistry NodeRegistry
	PropertyNodeRegistry       NodeRegistry
	TraceLiaisonNodeRegistry   NodeRegistry
}

type server struct {
	databasev1.UnimplementedSnapshotServiceServer
	databasev1.UnimplementedClusterStateServiceServer
	omr        observability.MetricsRegistry
	schemaRepo metadata.Repo
	protector  protector.Memory
	traceSVC   *traceService
	stopCh     chan struct{}
	*indexRuleRegistryServer
	*measureRegistryServer
	streamSVC *streamService
	*streamRegistryServer
	measureSVC *measureService
	bydbQLSVC  *bydbQLService
	log        *logger.Logger
	*propertyRegistryServer
	ser         *grpclib.Server
	tlsReloader *pkgtls.Reloader
	*propertyServer
	*topNAggregationRegistryServer
	*groupRegistryServer
	*traceRegistryServer
	authReloader *auth.Reloader
	groupRepo    *groupRepo
	*indexRuleBindingRegistryServer
	metrics                  *metrics
	routeTableProviders      map[string]route.TableProvider
	keyFile                  string
	authConfigFile           string
	addr                     string
	accessLogRootPath        string
	certFile                 string
	host                     string
	accessLogRecorders       []accessLogRecorder
	queryAccessLogRecorders  []queryAccessLogRecorder
	maxRecvMsgSize           run.Bytes
	grpcBufferMemoryRatio    float64
	port                     uint32
	tls                      bool
	enableIngestionAccessLog bool
	enableQueryAccessLog     bool
	accessLogSampled         bool
	healthAuthEnabled        bool
}

// NewServer returns a new gRPC server.
func NewServer(_ context.Context, tir1Client, tir2Client, broadcaster queue.Client,
	schemaRegistry metadata.Repo, nr NodeRegistries, omr observability.MetricsRegistry,
	protectorService protector.Memory, routeProviders map[string]route.TableProvider,
) Server {
	gr := &groupRepo{
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}
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
	traceSVC := &traceService{
		discoveryService: newDiscoveryService(schema.KindTrace, schemaRegistry, nr.TraceLiaisonNodeRegistry, gr),
		pipeline:         tir1Client,
		broadcaster:      broadcaster,
	}
	propertyService := &propertyServer{
		schemaRegistry:   schemaRegistry,
		pipeline:         tir2Client,
		nodeRegistry:     nr.PropertyNodeRegistry,
		discoveryService: newDiscoveryService(schema.KindProperty, schemaRegistry, nr.PropertyNodeRegistry, gr),
	}
	bydbQLSVC := &bydbQLService{
		repo:           schemaRegistry,
		transformer:    bydbql.NewTransformer(schemaRegistry),
		streamSvc:      streamSVC,
		measureSvc:     measureSVC,
		traceSvc:       traceSVC,
		propertyServer: propertyService,
	}

	s := &server{
		omr:        omr,
		streamSVC:  streamSVC,
		measureSVC: measureSVC,
		traceSVC:   traceSVC,
		bydbQLSVC:  bydbQLSVC,
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
		propertyServer: propertyService,
		propertyRegistryServer: &propertyRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		traceRegistryServer: &traceRegistryServer{
			schemaRegistry: schemaRegistry,
		},
		schemaRepo:          schemaRegistry,
		authReloader:        auth.InitAuthReloader(),
		protector:           protectorService,
		routeTableProviders: routeProviders,
	}
	s.accessLogRecorders = []accessLogRecorder{streamSVC, measureSVC, traceSVC, s.propertyServer}
	s.queryAccessLogRecorders = []queryAccessLogRecorder{streamSVC, measureSVC, traceSVC, s.propertyServer}

	return s
}

func (s *server) PreRun(ctx context.Context) error {
	s.log = logger.GetLogger("liaison-grpc")
	s.streamSVC.setLogger(s.log.Named("stream-t1"))
	s.measureSVC.setLogger(s.log)
	s.traceSVC.setLogger(s.log.Named("trace"))
	s.propertyServer.SetLogger(s.log)
	s.bydbQLSVC.setLogger(s.log.Named("bydbql"))
	s.groupRegistryServer.deletionTaskManager = newGroupDeletionTaskManager(
		s.groupRegistryServer.schemaRegistry, s.propertyServer, s.groupRepo, s.log.Named("group-deletion"),
	)
	if initErr := s.groupRegistryServer.deletionTaskManager.initPropertyStorage(ctx); initErr != nil {
		return initErr
	}
	components := []*discoveryService{
		s.streamSVC.discoveryService,
		s.measureSVC.discoveryService,
		s.traceSVC.discoveryService,
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
		if err := s.authReloader.ConfigAuthReloader(s.authConfigFile, s.healthAuthEnabled, s.log); err != nil {
			return err
		}
	}

	if s.enableIngestionAccessLog {
		for _, alr := range s.accessLogRecorders {
			if err := alr.activeIngestionAccessLog(s.accessLogRootPath, s.accessLogSampled); err != nil {
				return err
			}
		}
	}

	if s.enableQueryAccessLog {
		for _, qalr := range s.queryAccessLogRecorders {
			if err := qalr.activeQueryAccessLog(s.accessLogRootPath, s.accessLogSampled); err != nil {
				return err
			}
		}
	}
	metrics := newMetrics(s.omr.With(liaisonGrpcScope))
	s.metrics = metrics
	s.streamSVC.metrics = metrics
	s.measureSVC.metrics = metrics
	s.traceSVC.metrics = metrics
	s.bydbQLSVC.metrics = metrics
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

// GetAuthReloader returns auth reloader (for httpserver).
func (s *server) GetAuthReloader() *auth.Reloader {
	return s.authReloader
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	s.maxRecvMsgSize = defaultRecvSize
	fs.VarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", "the size of max receiving message")
	fs.BoolVar(&s.tls, "tls", false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, "cert-file", "", "the TLS cert file")
	fs.StringVar(&s.keyFile, "key-file", "", "the TLS key file")
	fs.StringVar(&s.authConfigFile, "auth-config-file", "", "Path to the authentication config file (YAML format)")
	fs.BoolVar(&s.healthAuthEnabled, "enable-health-auth", false, "enable authentication for health check")
	fs.StringVar(&s.host, "grpc-host", "", "the host of banyand listens")
	fs.Uint32Var(&s.port, "grpc-port", 17912, "the port of banyand listens")
	fs.BoolVar(&s.enableIngestionAccessLog, "enable-ingestion-access-log", false, "enable ingestion access log")
	fs.BoolVar(&s.enableQueryAccessLog, "enable-query-access-log", false, "enable query access log")
	fs.StringVar(&s.accessLogRootPath, "access-log-root-path", "", "access log root path")
	fs.BoolVar(&s.accessLogSampled, "access-log-sampled", false, "if true, requests may be dropped when the channel is full; if false, requests are never dropped")
	fs.DurationVar(&s.streamSVC.writeTimeout, "stream-write-timeout", time.Minute, "timeout for writing stream among liaison nodes")
	fs.DurationVar(&s.measureSVC.writeTimeout, "measure-write-timeout", time.Minute, "timeout for writing measure among liaison nodes")
	fs.DurationVar(&s.traceSVC.writeTimeout, "trace-write-timeout", time.Minute, "timeout for writing trace among liaison nodes")
	fs.DurationVar(&s.measureSVC.maxWaitDuration, "measure-metadata-cache-wait-duration", 0,
		"the maximum duration to wait for metadata cache to load (for testing purposes)")
	fs.DurationVar(&s.streamSVC.maxWaitDuration, "stream-metadata-cache-wait-duration", 0,
		"the maximum duration to wait for metadata cache to load (for testing purposes)")
	fs.DurationVar(&s.traceSVC.maxWaitDuration, "trace-metadata-cache-wait-duration", 0,
		"the maximum duration to wait for metadata cache to load (for testing purposes)")
	fs.IntVar(&s.propertyServer.repairQueueCount, "property-repair-queue-count", 128, "the number of queues for property repair")
	s.grpcBufferMemoryRatio = 0.1
	fs.Float64Var(&s.grpcBufferMemoryRatio, "grpc-buffer-memory-ratio", 0.1,
		"ratio of memory limit to use for gRPC buffer size calculation (0.0 < ratio <= 1.0)")
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
	if s.grpcBufferMemoryRatio <= 0.0 || s.grpcBufferMemoryRatio > 1.0 {
		return errors.Errorf("grpc-buffer-memory-ratio must be in range (0.0, 1.0], got %f", s.grpcBufferMemoryRatio)
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
	if s.authConfigFile != "" {
		if err := s.authReloader.Start(); err != nil {
			s.log.Error().Err(err).Msg("Failed to start authReloader for gRPC")
			close(s.stopCh)
			return s.stopCh
		}
		s.log.Info().Str("authConfigFile", s.authConfigFile).Msg("Starting auth config file monitoring")
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
		streamChain = append(streamChain, authStreamInterceptor(s.authReloader))
		unaryChain = append(unaryChain, authInterceptor(s.authReloader))
	}
	if s.protector != nil {
		streamChain = append(streamChain, s.protectorLoadSheddingInterceptor)
	}

	// Calculate dynamic buffer sizes based on available memory
	connWindowSize, streamWindowSize := s.calculateGrpcBufferSizes()
	if connWindowSize > 0 && streamWindowSize > 0 {
		opts = append(opts,
			grpclib.InitialConnWindowSize(connWindowSize),
			grpclib.InitialWindowSize(streamWindowSize),
		)
	} else if s.log != nil {
		s.log.Warn().Msg("using gRPC default buffer sizes")
	}

	opts = append(opts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(unaryChain...),
		grpclib.ChainStreamInterceptor(streamChain...),
	)
	s.ser = grpclib.NewServer(opts...)

	commonv1.RegisterServiceServer(s.ser, &apiVersionService{})
	streamv1.RegisterStreamServiceServer(s.ser, s.streamSVC)
	measurev1.RegisterMeasureServiceServer(s.ser, s.measureSVC)
	tracev1.RegisterTraceServiceServer(s.ser, s.traceSVC)
	bydbqlv1.RegisterBydbQLServiceServer(s.ser, s.bydbQLSVC)
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
	databasev1.RegisterClusterStateServiceServer(s.ser, s)
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

// protectorLoadSheddingInterceptor rejects streams when memory pressure is high.
func (s *server) protectorLoadSheddingInterceptor(srv interface{}, ss grpclib.ServerStream, info *grpclib.StreamServerInfo, handler grpclib.StreamHandler) error {
	// Fail open if protector is not available
	if s.protector == nil {
		return handler(srv, ss)
	}

	// Get current memory state and update metric
	state := s.protector.State()
	if s.metrics != nil {
		stateValue := 0 // StateLow
		if state == protector.StateHigh {
			stateValue = 1 // StateHigh
		}
		s.metrics.updateMemoryState(stateValue)
	}

	if state == protector.StateHigh {
		// Extract service name from FullMethod (e.g., "/banyandb.stream.v1.StreamService/Write")
		serviceName := "unknown"
		if info != nil && info.FullMethod != "" {
			// Extract service name from FullMethod
			parts := strings.Split(info.FullMethod, "/")
			if len(parts) >= 2 {
				serviceName = parts[1]
			}
		}

		// Log rejection with metrics
		if s.log != nil {
			s.log.Warn().
				Str("service", info.FullMethod).
				Msg("rejecting new stream due to high memory pressure")
		}
		if s.metrics != nil {
			s.metrics.memoryLoadSheddingRejections.Inc(1, serviceName)
		}

		return status.Errorf(codes.ResourceExhausted, "server is under memory pressure, please retry later")
	}

	return handler(srv, ss)
}

// calculateGrpcBufferSizes calculates the gRPC buffer sizes based on available system memory.
// Returns (connWindowSize, streamWindowSize) in bytes.
// Returns (0, 0) if protector is unavailable, which will cause gRPC to use defaults.
func (s *server) calculateGrpcBufferSizes() (int32, int32) {
	// Fail open if protector is not available
	if s.protector == nil {
		if s.log != nil {
			s.log.Warn().Msg("protector unavailable, using gRPC default buffer sizes")
		}
		return 0, 0
	}

	// Get memory limit from protector
	memoryLimit := s.protector.GetLimit()
	if memoryLimit == 0 {
		if s.log != nil {
			s.log.Warn().Msg("memory limit not set, using gRPC default buffer sizes")
		}
		return 0, 0
	}

	// Calculate total buffer size: min(availableMemory * ratio, maxReasonableBufferSize)
	// Note: We use the memory limit (not available bytes) as the basis for calculation
	// to ensure consistent buffer sizing regardless of current usage
	totalBufferSize := uint64(float64(memoryLimit) * s.grpcBufferMemoryRatio)
	if totalBufferSize > maxReasonableBufferSize {
		totalBufferSize = maxReasonableBufferSize
	}

	// Split buffer size: 2/3 for connection-level, 1/3 for stream-level
	connWindowSize := int32(totalBufferSize * 2 / 3)
	streamWindowSize := int32(totalBufferSize * 1 / 3)

	if s.log != nil {
		s.log.Info().
			Uint64("memory_limit", memoryLimit).
			Float64("ratio", s.grpcBufferMemoryRatio).
			Int32("conn_window_size", connWindowSize).
			Int32("stream_window_size", streamWindowSize).
			Msg("calculated gRPC buffer sizes")
	}

	// Update metrics
	if s.metrics != nil {
		s.metrics.updateBufferSizeMetrics(connWindowSize, streamWindowSize)
	}

	return connWindowSize, streamWindowSize
}

// GetClusterState returns the current state of all nodes in the cluster organized by route tables.
func (s *server) GetClusterState(_ context.Context, _ *databasev1.GetClusterStateRequest) (*databasev1.GetClusterStateResponse, error) {
	if s.routeTableProviders == nil {
		return &databasev1.GetClusterStateResponse{RouteTables: map[string]*databasev1.RouteTable{}}, nil
	}

	routeTables := make(map[string]*databasev1.RouteTable, len(s.routeTableProviders))
	for routeKey, provider := range s.routeTableProviders {
		if provider == nil {
			s.log.Warn().Str("routeKey", routeKey).Msg("route table provider is nil")
			continue
		}

		routeTable := provider.GetRouteTable()
		if routeTable != nil {
			routeTables[routeKey] = routeTable
		}
	}

	return &databasev1.GetClusterStateResponse{RouteTables: routeTables}, nil
}

func (s *server) GracefulStop() {
	s.log.Info().Msg("stopping")
	if s.tls && s.tlsReloader != nil {
		s.tlsReloader.Stop()
	}
	if s.authConfigFile != "" && s.authReloader != nil {
		s.authReloader.Stop()
	}
	stopped := make(chan struct{})
	go func() {
		s.ser.GracefulStop()
		if s.enableIngestionAccessLog {
			for _, alr := range s.accessLogRecorders {
				_ = alr.Close()
			}
		}
		if s.enableQueryAccessLog {
			for _, qalr := range s.queryAccessLogRecorders {
				_ = qalr.Close()
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
	activeIngestionAccessLog(root string, sampled bool) error
	Close() error
}

type queryAccessLogRecorder interface {
	activeQueryAccessLog(root string, sampled bool) error
	Close() error
}
