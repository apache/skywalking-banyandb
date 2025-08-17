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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/collector"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/config"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/export"
)

// Server manages gRPC and HTTP servers
type Server struct {
	config     config.ServerConfig
	collector  *collector.Collector
	logger     *zap.Logger
	grpcServer *grpc.Server
	httpServer *http.Server
}

// New creates a new server instance
func New(cfg config.ServerConfig, coll *collector.Collector, logger *zap.Logger) (*Server, error) {
	s := &Server{
		config:    cfg,
		collector: coll,
		logger:    logger,
	}

	// Initialize gRPC server
	if err := s.initGRPC(); err != nil {
		return nil, fmt.Errorf("failed to initialize gRPC server: %w", err)
	}

	// Initialize HTTP server
	if err := s.initHTTP(); err != nil {
		return nil, fmt.Errorf("failed to initialize HTTP server: %w", err)
	}

	return s, nil
}

// initGRPC initializes the gRPC server
func (s *Server) initGRPC() error {
	opts := []grpc.ServerOption{}

	// Add TLS if configured
	if s.config.GRPC.TLS.Enabled {
		// TODO: Implement TLS configuration
		s.logger.Warn("TLS configuration not yet implemented")
	}

	s.grpcServer = grpc.NewServer(opts...)
	
	// TODO: Register gRPC services
	// pb.RegisterEBPFMetricsServer(s.grpcServer, s)

	return nil
}

// initHTTP initializes the HTTP server
func (s *Server) initHTTP() error {
	mux := http.NewServeMux()

	// Register metrics endpoint
	mux.HandleFunc(s.config.HTTP.MetricsPath, s.handleMetrics)
	
	// Register health endpoint
	mux.HandleFunc("/health", s.handleHealth)
	
	// Register API endpoints
	mux.HandleFunc("/api/v1/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.HTTP.Port),
		Handler: mux,
	}

	return nil
}

// Start starts both gRPC and HTTP servers
func (s *Server) Start() error {
	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.GRPC.Port))
		if err != nil {
			s.logger.Error("Failed to listen for gRPC", zap.Error(err))
			return
		}
		
		s.logger.Info("Starting gRPC server", zap.Int("port", s.config.GRPC.Port))
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Start HTTP server
	s.logger.Info("Starting HTTP server", zap.Int("port", s.config.HTTP.Port))
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}

	return nil
}

// Stop gracefully stops both servers
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping servers")

	// Stop gRPC server
	s.grpcServer.GracefulStop()

	// Stop HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	return nil
}

// handleMetrics handles the metrics endpoint
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.collector.GetMetrics()
	
	// TODO: Implement proper Prometheus format export
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	
	// Write metrics in Prometheus format
	for module, metricSet := range metrics.GetAll() {
		for _, metric := range metricSet.GetMetrics() {
			fmt.Fprintf(w, "# HELP %s %s\n", metric.Name, metric.Help)
			fmt.Fprintf(w, "# TYPE %s %s\n", metric.Name, metric.Type)
			fmt.Fprintf(w, "%s{module=\"%s\"} %v\n", metric.Name, module, metric.Value)
		}
	}
}

// handleHealth handles the health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"healthy"}`)
}

// handleStats handles the stats API endpoint
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	metrics := s.collector.GetMetrics()
	
	w.Header().Set("Content-Type", "application/json")
	
	// TODO: Implement proper JSON serialization
	fmt.Fprintf(w, `{"modules":%d,"status":"ok"}`, len(metrics.GetAll()))
}