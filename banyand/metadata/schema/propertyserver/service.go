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

// Package propertyserver implements a standalone gRPC server for schema property management.
package propertyserver

import (
	"context"
	"fmt"
	"net"
	"path"
	"path/filepath"
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
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

const (
	schemaGroup     = "_schema"
	defaultShardID  = common.ShardID(0)
	defaultRecvSize = 10 << 20
)

var (
	_ run.PreRunner = (*server)(nil)
	_ run.Config    = (*server)(nil)
	_ run.Service   = (*server)(nil)
)

// Server is the interface for the standalone property server.
type Server interface {
	run.PreRunner
	run.Config
	run.Service
	GetPort() *uint32
}

type server struct {
	db                       db.Database
	lfs                      fs.FileSystem
	omr                      observability.MetricsRegistry
	closer                   *run.Closer
	l                        *logger.Logger
	ser                      *grpclib.Server
	tlsReloader              *pkgtls.Reloader
	schemaService            *schemaManagementServer
	updateService            *schemaUpdateServer
	host                     string
	certFile                 string
	root                     string
	keyFile                  string
	addr                     string
	repairBuildTreeCron      string
	minFileSnapshotAge       time.Duration
	flushTimeout             time.Duration
	snapshotSeq              uint64
	expireTimeout            time.Duration
	repairQuickBuildTreeTime time.Duration
	maxRecvMsgSize           run.Bytes
	maxFileSnapshotNum       int
	repairTreeSlotCount      int
	snapshotMu               sync.Mutex
	port                     uint32
	tls                      bool
}

// NewServer returns a new standalone property server.
func NewServer(omr observability.MetricsRegistry) Server {
	return &server{
		omr:    omr,
		closer: run.NewCloser(0),
	}
}

// GetPort returns the gRPC server port.
func (s *server) GetPort() *uint32 {
	return &s.port
}

func (s *server) Name() string {
	return "property-server"
}

func (s *server) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("schema-property-server")
	s.maxRecvMsgSize = defaultRecvSize
	flagS.StringVar(&s.root, "schema-property-server-root-path", "/tmp", "root storage path")
	flagS.StringVar(&s.host, "schema-property-server-grpc-host", "", "the host of schema property server")
	flagS.Uint32Var(&s.port, "schema-property-server-grpc-port", 17916, "the port of schema property server")
	flagS.DurationVar(&s.flushTimeout, "schema-property-server-flush-timeout", 5*time.Second, "memory flush interval")
	flagS.DurationVar(&s.expireTimeout, "schema-property-server-expire-delete-timeout", time.Hour*24*7, "soft-delete expiration")
	flagS.BoolVar(&s.tls, "schema-property-server-tls", false, "connection uses TLS if true")
	flagS.StringVar(&s.certFile, "schema-property-server-cert-file", "", "the TLS cert file")
	flagS.StringVar(&s.keyFile, "schema-property-server-key-file", "", "the TLS key file")
	flagS.VarP(&s.maxRecvMsgSize, "schema-property-server-max-recv-msg-size", "", "max gRPC receive message size")
	flagS.IntVar(&s.repairTreeSlotCount, "schema-property-server-repair-tree-slot-count", 32, "repair tree slot count")
	flagS.StringVar(&s.repairBuildTreeCron, "schema-property-server-repair-build-tree-cron", "@every 1h",
		"cron for repair tree building")
	flagS.DurationVar(&s.repairQuickBuildTreeTime, "schema-property-server-repair-quick-build-tree-time",
		s.repairQuickBuildTreeTime, "schema-quick build tree duration")
	flagS.IntVar(&s.maxFileSnapshotNum, "schema-property-server-max-file-snapshot-num", 10, "the maximum number of file snapshots allowed")
	flagS.DurationVar(&s.minFileSnapshotAge, "schema-property-server-min-file-snapshot-age", time.Hour, "the minimum age for file snapshots to be eligible for deletion")
	return flagS
}

func (s *server) Validate() error {
	if s.root == "" {
		return errors.New("root path must not be empty")
	}
	if s.port == 0 {
		s.port = 17920
	}
	s.addr = net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
	if s.addr == ":" {
		return errors.New("no address")
	}
	if !s.tls {
		return nil
	}
	if s.certFile == "" {
		return errors.New("invalid server cert file")
	}
	if s.keyFile == "" {
		return errors.New("invalid server key file")
	}
	return nil
}

func (s *server) PreRun(_ context.Context) error {
	s.l = logger.GetLogger("schema-property-server")
	s.lfs = fs.NewLocalFileSystem()

	grpcFactory := s.omr.With(propertyServerScope.SubScope("grpc"))
	sm := newServerMetrics(grpcFactory)
	s.schemaService = &schemaManagementServer{
		server:  s,
		l:       s.l,
		metrics: sm,
	}
	s.updateService = &schemaUpdateServer{
		server:  s,
		l:       s.l,
		metrics: sm,
	}

	if s.tls {
		var tlsErr error
		s.tlsReloader, tlsErr = pkgtls.NewReloader(s.certFile, s.keyFile, s.l)
		if tlsErr != nil {
			return errors.Wrap(tlsErr, "failed to initialize TLS reloader for property server")
		}
	}

	dataDir := filepath.Join(s.root, "schema-property", "data")
	snapshotDir := filepath.Join(s.root, "schema-property", "snapshots")
	repairDir := filepath.Join(s.root, "schema-property", "repair")

	cfg := db.Config{
		Location:               dataDir,
		FlushInterval:          s.flushTimeout,
		ExpireToDeleteDuration: s.expireTimeout,
		Repair: db.RepairConfig{
			Enabled:            true,
			Location:           repairDir,
			BuildTreeCron:      s.repairBuildTreeCron,
			QuickBuildTreeTime: s.repairQuickBuildTreeTime,
			TreeSlotCount:      s.repairTreeSlotCount,
		},
		Index: db.IndexConfig{
			BatchWaitSec:       5,
			WaitForPersistence: false,
		},
		Snapshot: db.SnapshotConfig{
			Location: snapshotDir,
			Func: func(ctx context.Context) (string, error) {
				s.snapshotMu.Lock()
				defer s.snapshotMu.Unlock()
				storage.DeleteStaleSnapshots(snapshotDir, s.maxFileSnapshotNum, s.minFileSnapshotAge, s.lfs)
				sn := s.snapshotName()
				snapshot := s.db.TakeSnapShot(ctx, sn)
				if snapshot.Error != "" {
					return "", fmt.Errorf("failed to find snapshot %s: %s", sn, snapshot.Error)
				}
				return path.Join(snapshotDir, sn, storage.DataDir), nil
			},
		},
	}

	var openErr error
	// nolint:contextcheck
	s.db, openErr = db.OpenDB(s.closer.Ctx(), cfg, s.omr, s.lfs)
	if openErr != nil {
		return errors.Wrap(openErr, "failed to open property database")
	}
	s.l.Info().Str("root", s.root).Msg("property database initialized")
	return nil
}

func (s *server) snapshotName() string {
	s.snapshotSeq++
	return fmt.Sprintf("%s-%08X", time.Now().UTC().Format(storage.SnapshotTimeFormat), s.snapshotSeq)
}

func (s *server) Serve() run.StopNotify {
	var opts []grpclib.ServerOption
	if s.tls {
		if s.tlsReloader != nil {
			if startErr := s.tlsReloader.Start(); startErr != nil {
				s.l.Error().Err(startErr).Msg("Failed to start TLS reloader for property server")
				return s.closer.CloseNotify()
			}
			s.l.Info().Str("certFile", s.certFile).Str("keyFile", s.keyFile).
				Msg("Started TLS file monitoring for property server")
			tlsConfig := s.tlsReloader.GetTLSConfig()
			creds := credentials.NewTLS(tlsConfig)
			opts = []grpclib.ServerOption{grpclib.Creds(creds)}
		} else {
			creds, tlsErr := credentials.NewServerTLSFromFile(s.certFile, s.keyFile)
			if tlsErr != nil {
				s.l.Error().Err(tlsErr).Msg("Failed to load TLS credentials")
				return s.closer.CloseNotify()
			}
			opts = []grpclib.ServerOption{grpclib.Creds(creds)}
		}
	}
	grpcPanicRecoveryHandler := func(p any) (err error) {
		s.l.Error().Interface("panic", p).Str("stack", string(debug.Stack())).Msg("recovered from panic")
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
	schemav1.RegisterSchemaManagementServiceServer(s.ser, s.schemaService)
	schemav1.RegisterSchemaUpdateServiceServer(s.ser, s.updateService)
	grpc_health_v1.RegisterHealthServer(s.ser, health.NewServer())

	s.closer.AddRunning()
	go func() {
		defer s.closer.Done()
		lis, lisErr := net.Listen("tcp", s.addr)
		if lisErr != nil {
			s.l.Error().Err(lisErr).Msg("Failed to listen")
			return
		}
		s.l.Info().Str("addr", s.addr).Msg("Listening to")
		serveErr := s.ser.Serve(lis)
		if serveErr != nil {
			s.l.Error().Err(serveErr).Msg("server is interrupted")
		}
	}()
	return s.closer.CloseNotify()
}

func (s *server) GracefulStop() {
	if s.tlsReloader != nil {
		s.tlsReloader.Stop()
	}
	stopped := make(chan struct{})
	go func() {
		s.ser.GracefulStop()
		close(stopped)
	}()
	t := time.NewTimer(10 * time.Second)
	select {
	case <-t.C:
		s.ser.Stop()
		s.l.Info().Msg("force stopped")
	case <-stopped:
		t.Stop()
		s.l.Info().Msg("stopped gracefully")
	}
	if s.db != nil {
		if closeErr := s.db.Close(); closeErr != nil {
			s.l.Error().Err(closeErr).Msg("failed to close database")
		}
	}
	s.closer.CloseThenWait()
}
