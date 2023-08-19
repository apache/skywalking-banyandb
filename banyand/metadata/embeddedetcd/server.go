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

// Package embeddedetcd implements an embedded etcd server.
package embeddedetcd

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Server is the interface of etcd server.
type Server interface {
	io.Closer
	ReadyNotify() <-chan struct{}
	StopNotify() <-chan struct{}
	StoppingNotify() <-chan struct{}
}

type server struct {
	server *embed.Etcd
}

// Option is the option for etcd server.
type Option func(*config)

// RootDir sets the root directory of Registry.
func RootDir(rootDir string) Option {
	return func(config *config) {
		config.rootDir = rootDir
	}
}

// ConfigureListener sets peer urls of listeners.
func ConfigureListener(lcs, lps []string) Option {
	return func(config *config) {
		config.listenerClientURLs = lcs
		config.listenerPeerURLs = lps
	}
}

type config struct {
	// rootDir is the root directory for etcd storage
	rootDir string
	// listenerClientURLs is the listener for client
	listenerClientURLs []string
	// listenerPeerURLs is the listener for peer
	listenerPeerURLs []string
}

func (e *server) ReadyNotify() <-chan struct{} {
	return e.server.Server.ReadyNotify()
}

func (e *server) StopNotify() <-chan struct{} {
	return e.server.Server.StopNotify()
}

func (e *server) StoppingNotify() <-chan struct{} {
	return e.server.Server.StoppingNotify()
}

func (e *server) Close() error {
	e.server.Close()
	<-e.server.Server.StopNotify()
	return nil
}

// NewServer returns a new etcd server.
func NewServer(options ...Option) (Server, error) {
	conf := &config{
		rootDir:            os.TempDir(),
		listenerClientURLs: []string{embed.DefaultListenClientURLs},
		listenerPeerURLs:   []string{embed.DefaultListenPeerURLs},
	}
	for _, opt := range options {
		opt(conf)
	}
	zapCfg := logger.GetLogger("etcd").ToZapConfig()

	var l *zap.Logger
	var err error
	if l, err = zapCfg.Build(); err != nil {
		return nil, err
	}
	// TODO: allow use cluster setting
	embedConfig, err := newEmbedEtcdConfig(conf, l)
	if err != nil {
		return nil, err
	}
	e, err := embed.StartEtcd(embedConfig)
	if err != nil {
		return nil, err
	}
	if e != nil {
		<-e.Server.ReadyNotify() // wait for e.Server to join the cluster
	}
	reg := &server{
		server: e,
	}
	return reg, nil
}

func newEmbedEtcdConfig(config *config, logger *zap.Logger) (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger)
	cfg.Dir = filepath.Join(config.rootDir, "metadata")
	observability.UpdatePath(cfg.Dir)
	parseURLs := func(urls []string) ([]url.URL, error) {
		uu := make([]url.URL, 0, len(urls))
		for _, u := range urls {
			cURL, err := url.Parse(u)
			if err != nil {
				return nil, err
			}
			uu = append(uu, *cURL)
		}
		return uu, nil
	}
	cURLs, err := parseURLs(config.listenerClientURLs)
	if err != nil {
		return nil, err
	}
	pURLs, err := parseURLs(config.listenerPeerURLs)
	if err != nil {
		return nil, err
	}
	cfg.Name = "meta"
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = cURLs, cURLs
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = pURLs, pURLs
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	cfg.BackendBatchInterval = 500 * time.Millisecond
	cfg.BackendBatchLimit = 10000
	cfg.MaxRequestBytes = 10 * 1024 * 1024
	return cfg, nil
}
