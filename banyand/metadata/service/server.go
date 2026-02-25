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

// Package service implements the metadata server wrapper supporting both
// embedded etcd (standalone) and external etcd (data node) modes.
package service

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/schemaserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	schemaTypeEtcd     = metadata.RegistryModeEtcd
	schemaTypeProperty = metadata.RegistryModeProperty
)

// Service extends metadata.Service with schema server port access.
type Service interface {
	metadata.Service
	GetSchemaServerPort() *uint32
}

type server struct {
	metadata.Service
	etcdServer              embeddedetcd.Server
	propServer              schemaserver.Server
	omr                     observability.MetricsRegistry
	serviceFlags            *run.FlagSet
	scheduler               *timestamp.Scheduler
	ecli                    *clientv3.Client
	closer                  *run.Closer
	rootDir                 string
	defragCron              string
	autoCompactionMode      string
	autoCompactionRetention string
	schemaRegistryMode      string
	nodeDiscoveryMode       string
	listenClientURL         []string
	listenPeerURL           []string
	quotaBackendBytes       run.Bytes
	embedded                bool
}

func (s *server) Name() string {
	return "metadata"
}

func (s *server) Role() databasev1.Role {
	if s.schemaRegistryMode == schemaTypeProperty {
		return databasev1.Role_ROLE_META
	}
	return databasev1.Role_ROLE_UNSPECIFIED
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVar(&s.schemaRegistryMode, "schema-registry-mode", schemaTypeEtcd,
		"Schema registry mode: 'etcd' for etcd-based storage, 'property' for property-based storage")
	fs.StringVar(&s.nodeDiscoveryMode, "node-discovery-mode", metadata.NodeDiscoveryModeEtcd,
		"Node discovery mode: 'etcd' for etcd-based, 'dns' for DNS-based, 'file' for file-based")
	if s.embedded {
		fs.StringVar(&s.rootDir, "metadata-root-path", "/tmp", "the root path of metadata")
		fs.StringVar(&s.autoCompactionMode, "etcd-auto-compaction-mode", "periodic", "auto compaction mode: 'periodic' or 'revision'")
		fs.StringVar(&s.autoCompactionRetention, "etcd-auto-compaction-retention", "1h",
			"auto compaction retention: e.g. '1h', '30m', '24h' for periodic; '1000' for revision")
		fs.StringVar(&s.defragCron, "etcd-defrag-cron", "@daily",
			"defragmentation cron: e.g. '@daily', '@hourly', '0 0 * * 0', '0 */6 * * *'")
		fs.StringSliceVar(&s.listenClientURL, "etcd-listen-client-url", []string{"http://localhost:2379"}, "A URL to listen on for client traffic")
		fs.StringSliceVar(&s.listenPeerURL, "etcd-listen-peer-url", []string{"http://localhost:2380"}, "A URL to listen on for peer traffic")
		fs.VarP(&s.quotaBackendBytes, "etcd-quota-backend-bytes", "", "Quota for backend storage")
	}
	if s.propServer == nil {
		omr := s.omr
		if omr == nil {
			omr = observability.BypassRegistry
		}
		s.propServer = schemaserver.NewServer(omr)
	}
	if s.serviceFlags == nil {
		s.serviceFlags = s.Service.FlagSet()
	}
	fs.AddFlagSet(s.propServer.FlagSet().FlagSet)
	fs.AddFlagSet(s.serviceFlags.FlagSet)
	return fs
}

func (s *server) Validate() error {
	if s.serviceFlags == nil {
		return errors.New("service flags are not initialized")
	}
	s.serviceFlags.Set("schema-registry-mode", s.schemaRegistryMode)
	s.serviceFlags.Set("node-discovery-mode", s.nodeDiscoveryMode)
	needEtcd := s.schemaRegistryMode == schemaTypeEtcd || s.nodeDiscoveryMode == metadata.NodeDiscoveryModeEtcd
	if s.embedded && needEtcd {
		if s.rootDir == "" {
			return errors.New("rootDir is empty")
		}
		if s.listenClientURL == nil {
			return errors.New("listenClientURL is empty")
		}
		if s.listenPeerURL == nil {
			return errors.New("listenPeerURL is empty")
		}
		if s.autoCompactionMode == "" {
			return errors.New("autoCompactionMode is empty")
		}
		if s.autoCompactionMode != "periodic" && s.autoCompactionMode != "revision" {
			return errors.New("autoCompactionMode is invalid")
		}
		if s.autoCompactionRetention == "" {
			return errors.New("autoCompactionRetention is empty")
		}
		if setErr := s.serviceFlags.Set(metadata.FlagEtcdEndpointsName,
			strings.Join(s.listenClientURL, ",")); setErr != nil {
			return setErr
		}
	}
	if validateErr := s.Service.Validate(); validateErr != nil {
		return validateErr
	}
	if s.schemaRegistryMode == schemaTypeProperty && s.propServer != nil {
		return s.propServer.Validate()
	}
	return nil
}

func (s *server) PreRun(ctx context.Context) error {
	needEtcd := s.schemaRegistryMode == schemaTypeEtcd || s.nodeDiscoveryMode == metadata.NodeDiscoveryModeEtcd
	if s.embedded && needEtcd {
		etcdServer, startErr := embeddedetcd.NewServer(embeddedetcd.RootDir(s.rootDir), embeddedetcd.ConfigureListener(s.listenClientURL, s.listenPeerURL),
			embeddedetcd.AutoCompactionMode(s.autoCompactionMode), embeddedetcd.AutoCompactionRetention(s.autoCompactionRetention),
			embeddedetcd.QuotaBackendBytes(int64(s.quotaBackendBytes)))
		if startErr != nil {
			return startErr
		}
		s.etcdServer = etcdServer
		<-s.etcdServer.ReadyNotify()
	}
	switch s.schemaRegistryMode {
	case schemaTypeEtcd:
		s.propServer = nil
	case schemaTypeProperty:
		ctx = s.enrichContextWithSchemaAddress(ctx)
		if propPreRunErr := s.propServer.PreRun(ctx); propPreRunErr != nil {
			return propPreRunErr
		}
	default:
		return errors.New("unknown schema storage type")
	}
	return s.Service.PreRun(ctx)
}

func (s *server) Serve() run.StopNotify {
	if s.propServer != nil {
		s.closer.AddRunning()
		go func() {
			defer s.closer.Done()
			<-s.propServer.Serve()
		}()
	}
	if s.etcdServer != nil {
		s.registerDefrag()
		s.closer.AddRunning()
		go func() {
			defer s.closer.Done()
			<-s.etcdServer.StoppingNotify()
		}()
	}
	_ = s.Service.Serve()
	return s.closer.CloseNotify()
}

func (s *server) GracefulStop() {
	if s.propServer != nil {
		s.propServer.GracefulStop()
	}
	if s.scheduler != nil {
		s.scheduler.Close()
	}
	if s.ecli != nil {
		_ = s.ecli.Close()
	}
	s.Service.GracefulStop()
	if s.etcdServer != nil {
		s.etcdServer.Close()
		<-s.etcdServer.StopNotify()
	}
	s.closer.CloseThenWait()
}

// NewService returns a new metadata repository Service.
// When embedded is true (standalone mode), an embedded etcd server is started.
// When embedded is false (data node mode), the service connects to external etcd.
func NewService(_ context.Context, embedded bool) (Service, error) {
	s := &server{
		closer:   run.NewCloser(0),
		embedded: embedded,
	}
	var clientErr error
	if embedded {
		s.Service, clientErr = metadata.NewClient(true, true)
	} else {
		s.Service, clientErr = metadata.NewClient(true, false)
	}
	if clientErr != nil {
		return nil, clientErr
	}
	return s, nil
}

// SetMetricsRegistry stores the metrics registry for lazy propServer creation.
func (s *server) SetMetricsRegistry(omr observability.MetricsRegistry) {
	s.omr = omr
	s.Service.SetMetricsRegistry(omr)
}

// GetSchemaServerPort returns the schema server gRPC port.
func (s *server) GetSchemaServerPort() *uint32 {
	if s.propServer != nil && s.schemaRegistryMode == schemaTypeProperty {
		return s.propServer.GetPort()
	}
	return nil
}

func (s *server) enrichContextWithSchemaAddress(ctx context.Context) context.Context {
	port := s.propServer.GetPort()
	if port == nil {
		return ctx
	}
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return ctx
	}
	node, ok := val.(common.Node)
	if !ok {
		return ctx
	}
	if node.PropertySchemaGrpcAddress != "" {
		return ctx
	}
	nodeHost := node.GrpcAddress
	if idx := strings.LastIndex(nodeHost, ":"); idx >= 0 {
		nodeHost = nodeHost[:idx]
	}
	if nodeHost == "" {
		nodeHost = "localhost"
	}
	node.PropertySchemaGrpcAddress = net.JoinHostPort(nodeHost, strconv.FormatUint(uint64(*port), 10))
	return context.WithValue(ctx, common.ContextNodeKey, node)
}

func performDefrag(listenURLs []string, ecli *clientv3.Client) error {
	for _, listenURL := range listenURLs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := ecli.Defragment(ctx, listenURL)
		return err
	}
	return nil
}

func (s *server) registerDefrag() {
	var (
		err        error
		etcdLogger = logger.GetLogger().Named("etcd-server")
		parser     = cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
	)

	s.ecli, err = clientv3.New(clientv3.Config{
		Endpoints: s.listenClientURL,
	})
	if err != nil {
		etcdLogger.Error().Err(err).Msg("failed to create client")
		return
	}
	s.scheduler = timestamp.NewScheduler(etcdLogger, timestamp.NewClock())

	err = s.scheduler.Register("defrag", parser, s.defragCron, func(_ time.Time, l *logger.Logger) bool {
		if errInner := performDefrag(s.listenClientURL, s.ecli); errInner != nil {
			l.Error().Err(errInner).Msg("failed to execute defragmentation")
			return false
		}
		return true
	})
	if err != nil {
		etcdLogger.Error().Err(err).Msg("failed to register defragmentation")
	}
}
