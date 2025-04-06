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
	"crypto/tls"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
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
	creds       credentials.TransportCredentials
	tlsReloader *TLSReloader
	omr         observability.MetricsRegistry
	measureSVC  *measureService
	ser         *grpclib.Server
	log         *logger.Logger
	*propertyServer
	*topNAggregationRegistryServer
	*groupRegistryServer
	stopCh chan struct{}
	*indexRuleRegistryServer
	*measureRegistryServer
	streamSVC *streamService
	*streamRegistryServer
	*indexRuleBindingRegistryServer
	*propertyRegistryServer
	metrics                  *metrics
	keyFile                  string
	certFile                 string
	accessLogRootPath        string
	addr                     string
	host                     string
	accessLogRecorders       []accessLogRecorder
	maxRecvMsgSize           run.Bytes
	port                     uint32
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
		propertyRegistryServer: &propertyRegistryServer{
			schemaRegistry: schemaRegistry,
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

	if s.tls {
		var err error
		s.tlsReloader, err = NewTLSReloader(s.certFile, s.keyFile, s.log)
		if err != nil {
			s.log.Error().Err(err).Msg("Failed to initialize TLSReloader for gRPC")
			return err
		}
		s.creds = NewDynamicTLSCredentials(s.tlsReloader)
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

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	s.maxRecvMsgSize = defaultRecvSize
	fs.VarP(&s.maxRecvMsgSize, "max-recv-msg-size", "", "the size of max receiving message")
	fs.BoolVar(&s.tls, "tls", false, "connection uses TLS if true, else plain TCP")
	fs.StringVar(&s.certFile, "cert-file", "", "the TLS cert file")
	fs.StringVar(&s.keyFile, "key-file", "", "the TLS key file")
	fs.StringVar(&s.host, "grpc-host", "", "the host of banyand listens")
	fs.Uint32Var(&s.port, "grpc-port", 17912, "the port of banyand listens")
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
		opts = append(opts, grpclib.Creds(s.creds))
		if err := s.tlsReloader.Start(); err != nil {
			s.log.Error().Err(err).Msg("Failed to start TLSReloader for gRPC")
			close(s.stopCh)
			return s.stopCh
		}
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
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())

	s.stopCh = make(chan struct{})
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

// TLSReloader manages dynamic reloading of TLS certificates and keys for the gRPC server.
type TLSReloader struct {
	watcher  *fsnotify.Watcher // 8 bytes
	cert     *tls.Certificate  // 8 bytes
	log      *logger.Logger    // 8 bytes
	stopCh   chan struct{}     // 8 bytes
	certFile string            // 16 bytes
	keyFile  string            // 16 bytes
	mu       sync.RWMutex      // 24 bytes
}

// NewTLSReloader creates a new TLSReloader instance for the gRPC server.
func NewTLSReloader(certFile, keyFile string, log *logger.Logger) (*TLSReloader, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("certFile and keyFile must be provided")
	}
	if log == nil {
		return nil, errors.New("logger must not be nil")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create fsnotify watcher")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		watcher.Close()
		return nil, errors.Wrap(err, "failed to load initial TLS certificate")
	}

	log.Info().Str("certFile", certFile).Str("keyFile", keyFile).Msg("Successfully loaded initial TLS certificates for gRPC")

	tr := &TLSReloader{
		certFile: certFile,
		keyFile:  keyFile,
		cert:     &cert,
		log:      log,
		watcher:  watcher,
		stopCh:   make(chan struct{}),
	}

	return tr, nil
}

// Start begins monitoring the TLS certificate and key files for changes.
func (tr *TLSReloader) Start() error {
	tr.log.Info().Str("certFile", tr.certFile).Str("keyFile", tr.keyFile).Msg("Starting TLS file monitoring for gRPC")
	err := tr.watcher.Add(tr.certFile)
	if err != nil {
		return errors.Wrapf(err, "failed to watch cert file: %s", tr.certFile)
	}
	err = tr.watcher.Add(tr.keyFile)
	if err != nil {
		return errors.Wrapf(err, "failed to watch key file: %s", tr.keyFile)
	}
	go tr.watchFiles()
	return nil
}

// watchFiles monitors the certificate and key files for changes and reloads credentials.
func (tr *TLSReloader) watchFiles() {
	tr.log.Info().Msg("TLS file watcher loop started for gRPC")
	for {
		select {
		case event, ok := <-tr.watcher.Events:
			if !ok {
				tr.log.Warn().Msg("Watcher events channel closed unexpectedly")
				return
			}
			if event.Op&fsnotify.Remove == fsnotify.Remove {
				if event.Name == tr.certFile {
					if err := tr.watcher.Add(tr.certFile); err != nil {
						tr.log.Error().Err(err).Str("file", tr.certFile).Msg("Failed to re-add cert file to watcher")
					}
				} else if event.Name == tr.keyFile {
					if err := tr.watcher.Add(tr.keyFile); err != nil {
						tr.log.Error().Err(err).Str("file", tr.keyFile).Msg("Failed to re-add key file to watcher")
					}
				}
			}
			if event.Op&fsnotify.Write == fsnotify.Write ||
				event.Op&fsnotify.Rename == fsnotify.Rename ||
				event.Op&fsnotify.Create == fsnotify.Create {
				tr.log.Info().Str("file", event.Name).Msg("Detected uploaded/changed certificate")
				if err := tr.reloadCertificate(); err != nil {
					tr.log.Error().Err(err).Str("file", event.Name).Msg("Failed to reload TLS certificate")
				} else {
					tr.log.Info().Str("file", event.Name).Msg("Successfully updated TLS certificate")
				}
			}
		case err, ok := <-tr.watcher.Errors:
			if !ok {
				tr.log.Warn().Msg("Watcher errors channel closed unexpectedly")
				return
			}
			tr.log.Error().Err(err).Msg("Error in file watcher")
		case <-tr.stopCh:
			tr.log.Info().Msg("Stopping TLS file watcher for gRPC")
			return
		}
	}
}

// reloadCertificate reloads the TLS certificate from the certificate and key files.
func (tr *TLSReloader) reloadCertificate() error {
	newCert, err := tls.LoadX509KeyPair(tr.certFile, tr.keyFile)
	if err != nil {
		return errors.Wrap(err, "failed to reload TLS certificate")
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.cert = &newCert
	return nil
}

// GetCertificate returns the current TLS certificate.
func (tr *TLSReloader) GetCertificate() *tls.Certificate {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.cert
}

// Stop gracefully stops the TLS reloader.
func (tr *TLSReloader) Stop() {
	close(tr.stopCh)
	if err := tr.watcher.Close(); err != nil {
		tr.log.Error().Err(err).Msg("Failed to close fsnotify watcher")
	}
}

// DynamicTLSCredentials implements credentials.TransportCredentials for dynamic reloading.
type DynamicTLSCredentials struct {
	reloader *TLSReloader
}

// NewDynamicTLSCredentials creates a new DynamicTLSCredentials instance.
func NewDynamicTLSCredentials(reloader *TLSReloader) credentials.TransportCredentials {
	return &DynamicTLSCredentials{reloader: reloader}
}

// ServerHandshake implements the server-side handshake for gRPC TLS.
func (d *DynamicTLSCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tlsConfig := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return d.reloader.GetCertificate(), nil
		},
		MinVersion: tls.VersionTLS12,
	}
	tlsConn := tls.Server(conn, tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return nil, nil, err
	}
	return tlsConn, credentials.TLSInfo{State: tlsConn.ConnectionState()}, nil
}

// ClientHandshake is not implemented as this is server-side only.
func (d *DynamicTLSCredentials) ClientHandshake(_ context.Context, _ string, _ net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("ClientHandshake not supported")
}

// Info provides protocol information.
func (d *DynamicTLSCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
	}
}

// Clone creates a copy of the credentials.
func (d *DynamicTLSCredentials) Clone() credentials.TransportCredentials {
	return &DynamicTLSCredentials{reloader: d.reloader}
}

// OverrideServerName is not needed for this implementation.
func (d *DynamicTLSCredentials) OverrideServerName(string) error {
	return nil
}

type accessLogRecorder interface {
	activeIngestionAccessLog(root string) error
	Close() error
}
