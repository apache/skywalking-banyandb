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

// Package embeddedserver implements an embedded meta server.
package embeddedserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type server struct {
	metadata.Service
	metaServer              embeddedetcd.Server
	scheduler               *timestamp.Scheduler
	ecli                    *clientv3.Client
	rootDir                 string
	defragCron              string
	autoCompactionMode      string
	autoCompactionRetention string
	listenClientURL         []string
	listenPeerURL           []string
	quotaBackendBytes       run.Bytes
}

func (s *server) Name() string {
	return "metadata"
}

func (s *server) Role() databasev1.Role {
	return databasev1.Role_ROLE_META
}

func (s *server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("metadata")
	fs.StringVar(&s.rootDir, "metadata-root-path", "/tmp", "the root path of metadata")
	fs.StringVar(&s.autoCompactionMode, "etcd-auto-compaction-mode", "periodic", "auto compaction mode: 'periodic' or 'revision'")
	fs.StringVar(&s.autoCompactionRetention, "etcd-auto-compaction-retention", "1h", "auto compaction retention: e.g. '1h', '30m', '24h' for periodic; '1000' for revision")
	fs.StringVar(&s.defragCron, "etcd-defrag-cron", "@daily", "defragmentation cron: e.g. '@daily', '@hourly', '0 0 * * 0', '0 */6 * * *'")
	fs.StringSliceVar(&s.listenClientURL, "etcd-listen-client-url", []string{"http://localhost:2379"}, "A URL to listen on for client traffic")
	fs.StringSliceVar(&s.listenPeerURL, "etcd-listen-peer-url", []string{"http://localhost:2380"}, "A URL to listen on for peer traffic")
	fs.VarP(&s.quotaBackendBytes, "etcd-quota-backend-bytes", "", "Quota for backend storage")
	return fs
}

func (s *server) Validate() error {
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
	if err := s.Service.FlagSet().Set(metadata.FlagEtcdEndpointsName,
		strings.Join(s.listenClientURL, ",")); err != nil {
		return err
	}
	return s.Service.Validate()
}

func (s *server) PreRun(ctx context.Context) error {
	var err error
	s.metaServer, err = embeddedetcd.NewServer(embeddedetcd.RootDir(s.rootDir), embeddedetcd.ConfigureListener(s.listenClientURL, s.listenPeerURL),
		embeddedetcd.AutoCompactionMode(s.autoCompactionMode), embeddedetcd.AutoCompactionRetention(s.autoCompactionRetention),
		embeddedetcd.QuotaBackendBytes(int64(s.quotaBackendBytes)))
	if err != nil {
		return err
	}
	<-s.metaServer.ReadyNotify()
	return s.Service.PreRun(ctx)
}

func (s *server) Serve() run.StopNotify {
	_ = s.Service.Serve()
	s.registerDefrag()
	return s.metaServer.StoppingNotify()
}

func (s *server) GracefulStop() {
	if s.scheduler != nil {
		s.scheduler.Close()
	}
	if s.ecli != nil {
		_ = s.ecli.Close()
	}
	s.Service.GracefulStop()
	if s.metaServer != nil {
		s.metaServer.Close()
		<-s.metaServer.StopNotify()
	}
}

// NewService returns a new metadata repository Service.
func NewService(_ context.Context) (metadata.Service, error) {
	s := &server{}
	var err error
	s.Service, err = metadata.NewClient(true, true)
	if err != nil {
		return nil, err
	}
	return s, nil
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
		defrag     = func(_ time.Time, _ *logger.Logger) bool {
			if err = performDefrag(s.listenClientURL, s.ecli); err != nil {
				etcdLogger.Error().Err(err).Msg("failed to execute defragmentation")
				return false
			}
			return true
		}
		parser = cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
	)

	s.ecli, err = clientv3.New(clientv3.Config{
		Endpoints: s.listenClientURL,
	})
	if err != nil {
		etcdLogger.Error().Err(err).Msg("failed to create client")
		return
	}
	s.scheduler = timestamp.NewScheduler(etcdLogger, timestamp.NewClock())

	err = s.scheduler.Register("defrag", parser, s.defragCron, defrag)
	if err != nil {
		etcdLogger.Error().Err(err).Msg("failed to register defragmentation")
	}
}
