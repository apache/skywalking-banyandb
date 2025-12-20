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

// Package server provides HTTP server functionality for exposing Prometheus metrics.
package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/apache/skywalking-banyandb/fodc/internal/exporter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	defaultReadHeaderTimeout = 3 * time.Second
	defaultShutdownTimeout   = 5 * time.Second
)

// Config holds configuration for the Prometheus metrics server.
type Config struct {
	ListenAddr        string
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
}

// Server represents a Prometheus metrics HTTP server.
type Server struct {
	server  *http.Server
	logger  *logger.Logger
	config  Config
	started bool
}

// NewServer creates a new Prometheus metrics server with the given configuration.
func NewServer(config Config) (*Server, error) {
	if config.ListenAddr == "" {
		return nil, fmt.Errorf("listen address cannot be empty")
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}

	return &Server{
		config: config,
		logger: logger.GetLogger("server"),
	}, nil
}

// Start starts the Prometheus metrics server with the provided registry.
// It registers the DatasourceCollector and starts serving metrics on the configured address.
func (s *Server) Start(registry *prometheus.Registry, datasourceCollector *exporter.DatasourceCollector) (<-chan error, error) {
	if s.started {
		return nil, fmt.Errorf("server is already started")
	}

	// Register the datasource collector with the registry
	if registerErr := registry.Register(datasourceCollector); registerErr != nil {
		return nil, fmt.Errorf("failed to register DatasourceCollector: %w", registerErr)
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{},
	))

	s.server = &http.Server{
		Addr:              s.config.ListenAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
	}

	// Start server in goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		s.logger.Info().
			Str("listen-addr", s.config.ListenAddr).
			Msg("Starting Prometheus metrics server")
		if serveErr := s.server.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			serverErrCh <- fmt.Errorf("metrics server error: %w", serveErr)
		}
	}()

	s.started = true
	return serverErrCh, nil
}

// Stop gracefully stops the Prometheus metrics server.
// It waits for ongoing requests to complete within the configured shutdown timeout.
func (s *Server) Stop() error {
	if !s.started {
		return fmt.Errorf("server is not started")
	}

	s.logger.Info().Msg("Stopping Prometheus metrics server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer shutdownCancel()

	if shutdownErr := s.server.Shutdown(shutdownCtx); shutdownErr != nil {
		s.logger.Warn().Err(shutdownErr).Msg("Error shutting down metrics server")
		return shutdownErr
	}

	s.started = false
	s.logger.Info().Msg("Prometheus metrics server stopped")
	return nil
}

// IsStarted returns true if the server has been started.
func (s *Server) IsStarted() bool {
	return s.started
}

// GetListenAddr returns the configured listen address.
func (s *Server) GetListenAddr() string {
	return s.config.ListenAddr
}
