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
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/healthcheck"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
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
	omr       observability.MetricsRegistry
	creds     credentials.TransportCredentials
	log       *logger.Logger
	ser       *grpclib.Server
	listeners map[bus.Topic]bus.MessageListener
	*clusterv1.UnimplementedServiceServer
	metrics        *metrics
	clientCloser   context.CancelFunc
	host           string
	keyFile        string
	addr           string
	healthAddr     string
	certFile       string
	maxRecvMsgSize run.Bytes
	listenersLock  sync.RWMutex
	port           uint32
	healthPort     uint32
	tls            bool
}

// NewServer returns a new gRPC server.
func NewServer(omr observability.MetricsRegistry) queue.Server {
	return &server{
		listeners: make(map[bus.Topic]bus.MessageListener),
		omr:       omr,
	}
}

func (s *server) PreRun(_ context.Context) error {
	s.log = logger.GetLogger("server-queue")
	s.metrics = newMetrics(s.omr.With(queueSubScope))
	return nil
}

func (s *server) Name() string {
	return "server-queue"
}

func (s *server) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
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
	fs.StringVar(&s.host, "grpc-host", "", "the host of banyand listens")
	fs.Uint32Var(&s.port, "grpc-port", 17912, "the port of banyand listens")
	fs.Uint32Var(&s.healthPort, "health-port", 17913, "the port of banyand health check listens")
	return fs
}

func (s *server) Validate() error {
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
		return errNoAddr
	}
	s.healthAddr = net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(s.healthPort), 10))
	if s.healthAddr == ":" {
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
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())

	stopCh := make(chan struct{})
	go func(stopCh chan struct{}) {
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
		close(stopCh)
	}(stopCh)

	var ctx context.Context
	ctx, s.clientCloser = context.WithCancel(context.Background())
	clientOpts := make([]grpclib.DialOption, 0, 1)
	if s.creds == nil {
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		clientOpts = append(clientOpts, grpclib.WithTransportCredentials(s.creds))
	}
	client, err := healthcheck.NewClient(ctx, s.log, s.addr, clientOpts)
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to health check client")
		close(stopCh)
		return stopCh
	}
	gwMux := runtime.NewServeMux(runtime.WithHealthzEndpoint(client))
	mux := chi.NewRouter()
	mux.Mount("/api", gwMux)
	healthSrv := &http.Server{
		Addr:              s.healthAddr,
		Handler:           chi.NewRouter(),
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func(stopCh chan struct{}) {
		s.log.Info().Str("listenAddr", s.healthAddr).Msg("Start healthz http server")
		err := healthSrv.ListenAndServe()
		if err != http.ErrServerClosed {
			s.log.Error().Err(err)
		}

		close(stopCh)
	}(stopCh)
	return stopCh
}

func (s *server) GracefulStop() {
	s.log.Info().Msg("stopping")
	stopped := make(chan struct{})
	s.clientCloser()
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

type metrics struct {
	totalStarted  meter.Counter
	totalFinished meter.Counter
	totalErr      meter.Counter
	totalLatency  meter.Counter

	totalMsgReceived    meter.Counter
	totalMsgReceivedErr meter.Counter
	totalMsgSent        meter.Counter
	totalMsgSentErr     meter.Counter
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
	}
}
