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

package gossip

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/validator"
	"github.com/pkg/errors"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	errServerCert = errors.New("invalid server cert file")
	errServerKey  = errors.New("invalid server key file")
	errNoAddr     = errors.New("no address")

	serverScope = observability.RootScope.SubScope("property_repair_gossip_server")
)

const (
	defaultRecvSize = 10 << 20

	defaultTotalTimeout = 20 * time.Minute
)

type service struct {
	schema.UnimplementedOnInitHandler
	pipeline            queue.Client
	metadata            metadata.Repo
	creds               credentials.TransportCredentials
	omr                 observability.MetricsRegistry
	sel                 node.Selector
	traceStreamSelector node.Selector
	ser                 *grpclib.Server
	log                 *logger.Logger
	closer              *run.Closer
	serverMetrics       *serverMetrics
	protocolHandler     *protocolHandler
	registered          map[string]*databasev1.Node
	traceSpanNotified   *int32
	caCertPath          string
	host                string
	addr                string
	nodeID              string
	certFile            string
	keyFile             string
	traceSpanSending    []*recordTraceSpan
	listeners           []MessageListener
	serviceRegister     []func(s *grpclib.Server)
	traceEntityLocator  partition.Locator
	maxRecvMsgSize      run.Bytes
	totalTimeout        time.Duration
	scheduleInterval    time.Duration
	serviceRegisterLock sync.RWMutex
	traceSpanLock       sync.RWMutex
	listenersLock       sync.RWMutex
	mu                  sync.RWMutex
	port                uint32
	traceLogEnabled     bool
	tls                 bool
}

// NewMessenger creates a new instance of Messenger for gossip propagation communication between nodes.
func NewMessenger(omr observability.MetricsRegistry, metadata metadata.Repo, pipeline queue.Client) Messenger {
	return &service{
		metadata:         metadata,
		omr:              omr,
		closer:           run.NewCloser(1),
		pipeline:         pipeline,
		serverMetrics:    newServerMetrics(omr.With(serverScope)),
		maxRecvMsgSize:   defaultRecvSize,
		totalTimeout:     defaultTotalTimeout,
		listeners:        make([]MessageListener, 0),
		registered:       make(map[string]*databasev1.Node),
		scheduleInterval: time.Hour * 2,
		sel:              node.NewRoundRobinSelector("", metadata),
		port:             17932,
	}
}

// NewMessengerWithoutMetadata creates a new instance of Messenger without metadata.
func NewMessengerWithoutMetadata(omr observability.MetricsRegistry, port int) Messenger {
	messenger := NewMessenger(omr, nil, nil)
	messenger.(*service).port = uint32(port)
	return messenger
}

func (s *service) PreRun(ctx context.Context) error {
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	s.nodeID = node.NodeID
	s.log = logger.GetLogger("gossip-messenger")
	s.listeners = make([]MessageListener, 0)
	s.serverMetrics = newServerMetrics(s.omr.With(serverScope))
	if s.metadata != nil {
		s.sel.OnInit([]schema.Kind{schema.KindGroup})
		s.metadata.RegisterHandler("property-repair-nodes", schema.KindNode, s)
		s.metadata.RegisterHandler("property-repair-groups", schema.KindGroup, s)
		if err := s.initTracing(ctx); err != nil {
			s.log.Warn().Err(err).Msg("failed to init internal trace stream")
		}
	}
	s.protocolHandler = newProtocolHandler(s)
	go s.protocolHandler.processPropagation()
	return nil
}

func (s *service) GetServerPort() *uint32 {
	return &s.port
}

func (s *service) Name() string {
	return "gossip-messenger"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_UNSPECIFIED
}

func (s *service) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("gossip-messenger")

	fs.VarP(&s.maxRecvMsgSize, "property-repair-gossip-grpc-max-recv-msg-size", "", "the size of max receiving message")
	fs.StringVar(&s.host, "property-repair-gossip-grpc-host", "", "the host of banyand listens")
	fs.Uint32Var(&s.port, "property-repair-gossip-grpc-port", s.port, "the port of banyand listens")

	fs.BoolVar(&s.tls, "property-repair-gossip-grpc-tls", false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, "property-repair-gossip-grpc-server-cert-file", "", "the TLS cert file")
	fs.StringVar(&s.keyFile, "property-repair-gossip-grpc-server-key-file", "", "the TLS key file")
	fs.StringVar(&s.caCertPath, "property-repair-gossip-client-ca-cert", "", "Path to the CA certificate for gossip client TLS communication.")
	fs.DurationVar(&s.totalTimeout, "property-repair-gossip-total-timeout", defaultTotalTimeout, "the total timeout for gossip propagation")
	fs.BoolVar(&s.traceLogEnabled, "property-repair-gossip-trace-log", true, "enable trace log")
	return fs
}

func (s *service) Validate() error {
	// client side tls validation
	if s.tls && s.caCertPath == "" {
		return fmt.Errorf("CA certificate path must be provided when TLS is enabled")
	}

	// server side validation
	if s.port == 0 {
		s.port = 17932
	}
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
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

func (s *service) Serve(stopCh chan struct{}) {
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

	propertyv1.RegisterGossipServiceServer(s.ser, s.protocolHandler)
	for _, register := range s.getServiceRegisters() {
		register(s.ser)
	}
	var wg sync.WaitGroup
	wg.Add(1)
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
		wg.Wait()
		s.log.Info().Msg("GRPC server is stopped")
	}()
}

func (s *service) GracefulStop() {
	if s.ser == nil {
		return
	}
	s.closer.Done()
	s.closer.CloseThenWait()

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
