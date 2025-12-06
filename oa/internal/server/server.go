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
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	ebpfv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/ebpf/v1"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/collector"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/config"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/export"
)

// Server manages gRPC and HTTP servers.
type Server struct {
	collector        *collector.Collector
	logger           *zap.Logger
	grpcServer       *grpc.Server
	httpServer       *http.Server
	promExporter     *export.PrometheusExporter
	banyandbExporter *export.BanyanDBExporter
	exportConfig     config.ExportConfig
	config           config.ServerConfig
}

// New creates a new server instance with export configuration.
func New(cfg config.ServerConfig, exportCfg config.ExportConfig, coll *collector.Collector, logger *zap.Logger) (*Server, error) {
	s := &Server{
		config:       cfg,
		exportConfig: exportCfg,
		collector:    coll,
		logger:       logger,
		promExporter: export.NewPrometheusExporter("ebpf"),
	}

	// Initialize BanyanDB exporter if configured
	if exportCfg.Type == "banyandb" {
		banyandbCfg := export.BanyanDBConfig{
			Endpoint:      exportCfg.BanyanDB.Endpoint,
			Group:         exportCfg.BanyanDB.Group,
			MeasureName:   "ebpf-metrics",
			Timeout:       exportCfg.BanyanDB.Timeout,
			BatchSize:     1000,
			FlushInterval: 10 * time.Second,
		}
		exporter, err := export.NewBanyanDBExporter(banyandbCfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create BanyanDB exporter: %w", err)
		}
		s.banyandbExporter = exporter
	}

	// Initialize gRPC server
	s.initGRPC()

	// Initialize HTTP server
	s.initHTTP()

	return s, nil
}

// initGRPC initializes the gRPC server.
func (s *Server) initGRPC() {
	opts := []grpc.ServerOption{}

	// Add TLS if configured
	if s.config.GRPC.TLS.Enabled {
		// TODO: Implement TLS configuration
		s.logger.Warn("TLS configuration not yet implemented")
	}

	s.grpcServer = grpc.NewServer(opts...)

	// Register gRPC services
	grpcService := NewGRPCServer(s.collector, s.logger)
	ebpfv1.RegisterEBPFMetricsServiceServer(s.grpcServer, grpcService)
}

// initHTTP initializes the HTTP server.
func (s *Server) initHTTP() {
	mux := http.NewServeMux()

	// Register metrics endpoint
	mux.HandleFunc(s.config.HTTP.MetricsPath, s.handleMetrics)

	// Register health endpoint
	mux.HandleFunc("/health", s.handleHealth)

	// Register API endpoints
	mux.HandleFunc("/api/v1/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.config.HTTP.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

// Start starts both gRPC and HTTP servers.
func (s *Server) Start(ctx context.Context) error {
	// Connect to BanyanDB if configured
	if s.banyandbExporter != nil {
		if err := s.banyandbExporter.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to BanyanDB: %w", err)
		}

		// Start periodic export to BanyanDB
		go s.banyandbExporter.StartPeriodicExport(ctx, s.collector.GetMetrics())
		s.logger.Info("Started BanyanDB export", zap.String("endpoint", s.exportConfig.BanyanDB.Endpoint))
	}

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

// Stop gracefully stops both servers.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping servers")

	// Stop BanyanDB exporter if configured
	if s.banyandbExporter != nil {
		//nolint:contextcheck // Close method doesn't accept context parameter
		if err := s.banyandbExporter.Close(); err != nil {
			s.logger.Error("Failed to close BanyanDB exporter", zap.Error(err))
		}
	}

	// Stop gRPC server
	s.grpcServer.GracefulStop()

	// Stop HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	return nil
}

// handleMetrics handles the metrics endpoint.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// TODO: Use r for query parameters like ?format=openmetrics or ?module=iomonitor
	_ = r
	metrics := s.collector.GetMetrics()

	// Set proper Prometheus content type
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// Use proper Prometheus exporter
	if err := s.promExporter.Export(w, metrics); err != nil {
		s.logger.Error("Failed to export metrics", zap.Error(err))
		http.Error(w, "Failed to export metrics", http.StatusInternalServerError)
		return
	}
}

// handleHealth handles the health check endpoint.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// TODO: Use r.Method to ensure GET request
	// TODO: Could add detailed health check with query params
	_ = r
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"healthy"}`)
}

// handleStats handles the stats API endpoint.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	// TODO: Use r for query parameters like ?module=iomonitor or ?format=detailed
	// TODO: Could support POST with JSON body for complex queries
	_ = r
	metrics := s.collector.GetMetrics()

	w.Header().Set("Content-Type", "application/json")

	// Build comprehensive stats
	allMetrics := metrics.GetAll()
	totalMetrics := 0
	moduleStats := make(map[string]int)

	for module, metricSet := range allMetrics {
		count := metricSet.Count()
		moduleStats[module] = count
		totalMetrics += count
	}

	stats := map[string]interface{}{
		"status":        "ok",
		"total_modules": len(allMetrics),
		"total_metrics": totalMetrics,
		"modules":       moduleStats,
	}

	if err := json.NewEncoder(w).Encode(stats); err != nil {
		s.logger.Error("Failed to encode stats", zap.Error(err))
		http.Error(w, "Failed to encode stats", http.StatusInternalServerError)
	}
}
