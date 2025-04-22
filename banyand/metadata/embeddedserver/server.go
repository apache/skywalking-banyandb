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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type server struct {
	metadata.Service
	metaServer              embeddedetcd.Server
	rootDir                 string
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
	fs.StringVar(&s.autoCompactionMode, "etcd-auto-compaction-mode", "periodic", "auto compaction mode")
	fs.StringVar(&s.autoCompactionRetention, "etcd-auto-compaction-retention", "1h", "auto compaction retention")
	fs.StringSliceVar(&s.listenClientURL, "etcd-listen-client-url", []string{"http://localhost:2379"}, "A URL to listen on for client traffic")
	fs.StringSliceVar(&s.listenPeerURL, "etcd-listen-peer-url", []string{"http://localhost:2380"}, "A URL to listen on for peer traffic")
	fs.VarP(&s.quotaBackendBytes, "quota-backend-bytes", "", "Quota for backend storage")
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
	return s.metaServer.StoppingNotify()
}

func (s *server) GracefulStop() {
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
