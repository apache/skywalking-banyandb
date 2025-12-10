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

package sub

import (
	"context"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/healthcheck"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

const defaultRecvSize = 10 << 20

var (
	errServerCert = errors.New("invalid server cert file")
	errServerKey  = errors.New("invalid server key file")
	errNoAddr     = errors.New("no address")

	_ run.PreRunner = (*server)(nil)
	_ run.Service   = (*server)(nil)

	queueSubScope = observability.RootScope.SubScope("queue_sub")
)

type server struct {
	clusterv1.UnimplementedServiceServer
	clusterv1.UnimplementedChunkedSyncServiceServer
	streamv1.UnimplementedStreamServiceServer
	databasev1.UnimplementedSnapshotServiceServer
	creds               credentials.TransportCredentials
	tlsReloader         *pkgtls.Reloader
	omr                 observability.MetricsRegistry
	metrics             *metrics
	ser                 *grpclib.Server
	listeners           map[bus.Topic][]bus.MessageListener
	topicMap            map[string]bus.Topic
	chunkedSyncHandlers map[bus.Topic]queue.ChunkedSyncHandler
	log                 *logger.Logger
	httpSrv             *http.Server
	clientCloser        context.CancelFunc
	httpAddr            string
	addr                string
	host                string
	certFile            string
	keyFile             string
	flagNamePrefix      string
	maxRecvMsgSize      run.Bytes
	listenersLock       sync.RWMutex
	port                uint32
	httpPort            uint32
	tls                 bool
	// Chunk ordering configuration
	enableChunkReordering bool
	maxChunkBufferSize    uint32
	chunkBufferTimeout    time.Duration
	maxChunkGapSize       uint32
}

// NewServer returns a new gRPC server.
func NewServer(omr observability.MetricsRegistry) queue.Server {
	return NewServerWithPorts(omr, "", 17912, 17913)
}

// NewServerWithPorts returns a new gRPC server with specified ports.
func NewServerWithPorts(omr observability.MetricsRegistry, flagNamePrefix string, port, httpPort uint32) queue.Server {
	return &server{
		listeners:           make(map[bus.Topic][]bus.MessageListener),
		topicMap:            make(map[string]bus.Topic),
		chunkedSyncHandlers: make(map[bus.Topic]queue.ChunkedSyncHandler),
		omr:                 omr,
		maxRecvMsgSize:      defaultRecvSize,
		flagNamePrefix:      flagNamePrefix,
		port:                port,
		httpPort:            httpPort,
		// Default chunk ordering configuration
		enableChunkReordering: true,
		maxChunkBufferSize:    10,
		chunkBufferTimeout:    5 * time.Second,
		maxChunkGapSize:       5,
	}
}

func (s *server) PreRun(_ context.Context) error {
	s.log = logger.GetLogger("server-queue-sub")
	s.metrics = newMetrics(s.omr.With(queueSubScope))

	// Initialize TLS reloader if TLS is enabled
	if s.tls {
		var err error
		s.tlsReloader, err = pkgtls.NewReloader(s.certFile, s.keyFile, s.log)
		if err != nil {
			return errors.Wrap(err, "failed to initialize TLS reloader for queue server")
		}
		s.log.Info().Str("certFile", s.certFile).Str("keyFile", s.keyFile).Msg("Initialized TLS reloader for queue server")
	}

	return nil
}

func (s *server) Name() string {
	return "server-queue"
}

func (s *server) Role() databasev1.Role {
	return databasev1.Role_ROLE_UNSPECIFIED
}

func (s *server) GetPort() *uint32 {
	return &s.port
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	prefixFlag := func(name string) string {
		if s.flagNamePrefix == "" {
			return name
		}
		return s.flagNamePrefix + "-" + name
	}
	fs.VarP(&s.maxRecvMsgSize, prefixFlag("max-recv-msg-size"), "", "the size of max receiving message")
	fs.BoolVar(&s.tls, prefixFlag("tls"), false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, prefixFlag("cert-file"), "", "the TLS cert file")
	fs.StringVar(&s.keyFile, prefixFlag("key-file"), "", "the TLS key file")
	fs.StringVar(&s.host, prefixFlag("grpc-host"), "", "the host of banyand listens")
	fs.Uint32Var(&s.port, prefixFlag("grpc-port"), s.port, "the port of banyand listens")
	fs.Uint32Var(&s.httpPort, prefixFlag("http-port"), s.httpPort, "the port of banyand http api listens")
	// Chunk ordering configuration flags
	fs.BoolVar(&s.enableChunkReordering, prefixFlag("enable-chunk-reordering"), s.enableChunkReordering, "enable out-of-order chunk handling")
	fs.Uint32Var(&s.maxChunkBufferSize, prefixFlag("max-chunk-buffer-size"), s.maxChunkBufferSize, "maximum chunks to buffer for reordering")
	fs.DurationVar(&s.chunkBufferTimeout, prefixFlag("chunk-buffer-timeout"), s.chunkBufferTimeout, "maximum time to wait for missing chunks")
	fs.Uint32Var(&s.maxChunkGapSize, prefixFlag("max-chunk-gap-size"), s.maxChunkGapSize, "maximum gap in chunk sequence")
	return fs
}

func (s *server) Validate() error {
	if s.port == 0 {
		s.port = 17912
	}
	if s.httpPort == 0 {
		s.httpPort = 17913
	}
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
		return errNoAddr
	}
	s.httpAddr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.httpPort), 10))
	if s.httpAddr == ":" {
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
	// TLS reloader will be initialized in PreRun, so we don't need to load credentials here
	// The credentials will be loaded dynamically when the server starts
	return nil
}

func (s *server) Serve() run.StopNotify {
	var opts []grpclib.ServerOption
	if s.tls {
		// Start TLS reloader if enabled
		if s.tlsReloader != nil {
			if err := s.tlsReloader.Start(); err != nil {
				s.log.Error().Err(err).Msg("Failed to start TLS reloader for queue server")
				stopCh := make(chan struct{})
				close(stopCh)
				return stopCh
			}
			s.log.Info().Str("certFile", s.certFile).Str("keyFile", s.keyFile).Msg("Started TLS file monitoring for queue server")
			tlsConfig := s.tlsReloader.GetTLSConfig()
			creds := credentials.NewTLS(tlsConfig)
			opts = []grpclib.ServerOption{grpclib.Creds(creds)}
		} else {
			// Fallback to static loading if reloader is not available
			creds, errTLS := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
			if errTLS != nil {
				s.log.Error().Err(errTLS).Msg("Failed to load TLS credentials")
				stopCh := make(chan struct{})
				close(stopCh)
				return stopCh
			}
			opts = []grpclib.ServerOption{grpclib.Creds(creds)}
		}
	}
	grpcPanicRecoveryHandler := func(p any) (err error) {
		s.log.Error().Interface("panic", p).Str("stack", string(debug.Stack())).Msg("recovered from panic")

		return status.Errorf(codes.Internal, "%s", p)
	}

	streamChain := []grpclib.StreamServerInterceptor{
		recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}
	unaryChain := []grpclib.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
	}

	opts = append(opts, grpclib.MaxRecvMsgSize(int(s.maxRecvMsgSize)),
		grpclib.ChainUnaryInterceptor(unaryChain...),
		grpclib.ChainStreamInterceptor(streamChain...),
	)
	s.ser = grpclib.NewServer(opts...)
	clusterv1.RegisterServiceServer(s.ser, s)
	clusterv1.RegisterChunkedSyncServiceServer(s.ser, s)
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())
	databasev1.RegisterSnapshotServiceServer(s.ser, s)
	streamv1.RegisterStreamServiceServer(s.ser, &streamService{ser: s})
	measurev1.RegisterMeasureServiceServer(s.ser, &measureService{ser: s})
	tracev1.RegisterTraceServiceServer(s.ser, &traceService{ser: s})

	var ctx context.Context
	ctx, s.clientCloser = context.WithCancel(context.Background())
	clientOpts := make([]grpclib.DialOption, 0, 1)
	if s.creds == nil {
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(s.creds))
	}
	stopCh := make(chan struct{})
	client, err := healthcheck.NewClient(ctx, s.log, s.addr, clientOpts)
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to health check client")
		close(stopCh)
		return stopCh
	}
	gwMux := runtime.NewServeMux(runtime.WithHealthzEndpoint(client))
	if err := databasev1.RegisterSnapshotServiceHandlerFromEndpoint(ctx, gwMux, s.addr, clientOpts); err != nil {
		s.log.Error().Err(err).Msg("Failed to register snapshot service")
		close(stopCh)
		return stopCh
	}
	mux := chi.NewRouter()
	mux.Mount("/api", http.StripPrefix("/api", gwMux))
	s.httpSrv = &http.Server{
		Addr:              s.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		lis, err := net.Listen("tcp", s.addr)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to listen")
			close(stopCh)
			return
		}
		s.log.Info().Str("addr", s.addr).Msg("Listening to")
		err = s.ser.Serve(lis)
		if err != nil {
			s.log.Error().Err(err).Msg("server is interrupted")
		}
		wg.Done()
	}()
	go func() {
		s.log.Info().Str("listenAddr", s.httpAddr).Msg("Start healthz http server")
		err := s.httpSrv.ListenAndServe()
		if err != http.ErrServerClosed {
			s.log.Error().Err(err)
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		s.log.Info().Msg("All servers are stopped")
		close(stopCh)
	}()
	return stopCh
}

func (s *server) GracefulStop() {
	s.log.Info().Msg("stopping")

	// Stop TLS reloader if enabled
	if s.tlsReloader != nil {
		s.tlsReloader.Stop()
	}

	stopped := make(chan struct{})
	s.clientCloser()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.httpSrv.Shutdown(ctx)
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

// RegisterChunkedSyncHandler implements queue.Server.
func (s *server) RegisterChunkedSyncHandler(topic bus.Topic, handler queue.ChunkedSyncHandler) {
	s.chunkedSyncHandlers[topic] = handler
}

type metrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter

	totalMsgReceived    meter.Counter
	totalMsgReceivedErr meter.Counter
	totalMsgSent        meter.Counter
	totalMsgSentErr     meter.Counter

	// Chunk ordering metrics
	outOfOrderChunksReceived meter.Counter
	chunksBuffered           meter.Counter
	bufferTimeouts           meter.Counter
	largeGapsRejected        meter.Counter
	bufferCapacityExceeded   meter.Counter
}

func newMetrics(factory *observability.Factory) *metrics {
	return &metrics{
		totalStarted:        factory.NewCounter("total_started", "topic"),
		totalFinished:       factory.NewCounter("total_finished", "topic"),
		totalErr:            factory.NewCounter("total_err", "topic"),
		totalLatency:        factory.NewCounter("total_latency", "topic"),
		totalMsgReceived:    factory.NewCounter("total_msg_received", "topic"),
		totalMsgReceivedErr: factory.NewCounter("total_msg_received_err", "topic"),
		totalMsgSent:        factory.NewCounter("total_msg_sent", "topic"),
		totalMsgSentErr:     factory.NewCounter("total_msg_sent_err", "topic"),

		// Chunk ordering metrics
		outOfOrderChunksReceived: factory.NewCounter("out_of_order_chunks_received", "session_id"),
		chunksBuffered:           factory.NewCounter("chunks_buffered", "session_id"),
		bufferTimeouts:           factory.NewCounter("buffer_timeouts", "session_id"),
		largeGapsRejected:        factory.NewCounter("large_gaps_rejected", "session_id"),
		bufferCapacityExceeded:   factory.NewCounter("buffer_capacity_exceeded", "session_id"),
	}
}

// updateChunkOrderMetrics updates chunk ordering metrics.
func (s *server) updateChunkOrderMetrics(event string, sessionID string) {
	if s.metrics == nil {
		return // Skip metrics if not initialized (e.g., during tests)
	}
	switch event {
	case "out_of_order_received":
		s.metrics.outOfOrderChunksReceived.Inc(1, sessionID)
	case "chunk_buffered":
		s.metrics.chunksBuffered.Inc(1, sessionID)
	case "buffer_timeout":
		s.metrics.bufferTimeouts.Inc(1, sessionID)
	case "gap_too_large":
		s.metrics.largeGapsRejected.Inc(1, sessionID)
	case "buffer_full":
		s.metrics.bufferCapacityExceeded.Inc(1, sessionID)
	}
}
