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

// Package main is the main package for the FODC Proxy.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/api"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/grpc"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	defaultGRPCListenAddr    = ":17907"
	defaultHTTPListenAddr    = ":17908"
	defaultHeartbeatTimeout  = 30 * time.Second
	defaultCleanupTimeout    = 5 * time.Minute
	defaultMaxAgents         = 1000
	defaultGRPCMaxMsgSize    = 4194304 // 4MB
	defaultHTTPReadTimeout   = 10 * time.Second
	defaultHTTPWriteTimeout  = 10 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
)

var (
	grpcListenAddr    string
	httpListenAddr    string
	heartbeatTimeout  time.Duration
	cleanupTimeout    time.Duration
	maxAgents         int
	grpcMaxMsgSize    int
	httpReadTimeout   time.Duration
	httpWriteTimeout  time.Duration
	heartbeatInterval time.Duration

	rootCmd = &cobra.Command{
		Use:     "fodc-proxy",
		Short:   "FODC Proxy - Central control plane and data aggregator for FODC infrastructure",
		Version: version.Build(),
		Long: `FODC Proxy is the central control plane and data aggregator for the First Occurrence Data Collection (FODC) infrastructure.
It acts as a unified gateway that aggregates observability data from multiple FODC Agents and exposes ecosystem-friendly interfaces.`,
		RunE: runProxy,
	}
)

func init() {
	rootCmd.Flags().StringVar(&grpcListenAddr, "grpc-listen-addr", defaultGRPCListenAddr,
		"gRPC server address where the Proxy listens for agent connections")
	rootCmd.Flags().StringVar(&httpListenAddr, "http-listen-addr", defaultHTTPListenAddr,
		"HTTP server listen address")
	rootCmd.Flags().DurationVar(&heartbeatTimeout, "agent-heartbeat-timeout", defaultHeartbeatTimeout,
		"Timeout for considering an agent offline if no heartbeat received")
	rootCmd.Flags().DurationVar(&cleanupTimeout, "agent-cleanup-timeout", defaultCleanupTimeout,
		"Timeout for automatically unregistering agents that have been offline")
	rootCmd.Flags().IntVar(&maxAgents, "max-agents", defaultMaxAgents,
		"Maximum number of agents that can be registered")
	rootCmd.Flags().IntVar(&grpcMaxMsgSize, "grpc-max-msg-size", defaultGRPCMaxMsgSize,
		"Maximum message size for gRPC messages")
	rootCmd.Flags().DurationVar(&httpReadTimeout, "http-read-timeout", defaultHTTPReadTimeout,
		"HTTP read timeout")
	rootCmd.Flags().DurationVar(&httpWriteTimeout, "http-write-timeout", defaultHTTPWriteTimeout,
		"HTTP write timeout")
	rootCmd.Flags().DurationVar(&heartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval,
		"Default heartbeat interval for agents")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runProxy(_ *cobra.Command, _ []string) error {
	if initErr := logger.Init(logger.Logging{
		Env:   "prod",
		Level: "info",
	}); initErr != nil {
		return fmt.Errorf("failed to initialize logger: %w", initErr)
	}

	log := logger.GetLogger("fodc-proxy")

	if cleanupTimeout <= heartbeatTimeout {
		return fmt.Errorf("agent-cleanup-timeout (%v) must be greater than agent-heartbeat-timeout (%v)", cleanupTimeout, heartbeatTimeout)
	}

	log.Info().
		Str("grpc-listen-addr", grpcListenAddr).
		Str("http-listen-addr", httpListenAddr).
		Dur("heartbeat-timeout", heartbeatTimeout).
		Dur("cleanup-timeout", cleanupTimeout).
		Int("max-agents", maxAgents).
		Msg("Starting FODC Proxy")

	agentRegistry := registry.NewAgentRegistry(log, heartbeatTimeout, cleanupTimeout, maxAgents)
	defer agentRegistry.Stop()

	metricsAggregator := metrics.NewAggregator(agentRegistry, nil, log)

	grpcService := grpc.NewFODCService(agentRegistry, metricsAggregator, log, heartbeatInterval)
	metricsAggregator.SetGRPCService(grpcService)

	grpcServer := grpc.NewServer(grpcService, grpcListenAddr, grpcMaxMsgSize, log)
	if startErr := grpcServer.Start(); startErr != nil {
		return fmt.Errorf("failed to start gRPC server: %w", startErr)
	}
	defer grpcServer.Stop()

	apiServer := api.NewServer(metricsAggregator, agentRegistry, log)
	if startErr := apiServer.Start(httpListenAddr, httpReadTimeout, httpWriteTimeout); startErr != nil {
		return fmt.Errorf("failed to start HTTP API server: %w", startErr)
	}
	defer func() {
		if stopErr := apiServer.Stop(); stopErr != nil {
			log.Error().Err(stopErr).Msg("Error stopping HTTP API server")
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	log.Info().Msg("Received shutdown signal")

	return nil
}
