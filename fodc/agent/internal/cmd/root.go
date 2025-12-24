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

// Package cmd is an internal package defining cli commands for fodc.
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	flightrecorder "github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/proxy"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/server"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/watchdog"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	defaultPollInterval                 = 10 * time.Second
	defaultMetricsEndpoint              = "http://localhost:2121/metrics"
	defaultMaxMetricsMemoryUsagePercent = 10
	defaultPrometheusListenAddr         = ":9090"
	defaultProxyAddr                    = "localhost:17900"
	defaultHeartbeatInterval            = 10 * time.Second
	defaultReconnectInterval            = 5 * time.Second
)

var (
	pollInterval                 time.Duration
	metricsEndpoint              string
	maxMetricsMemoryUsagePercent int
	prometheusListenAddr         string
	proxyAddr                    string
	nodeIP                       string
	nodePort                     int
	nodeRole                     string
	nodeLabels                   string
	heartbeatInterval            time.Duration
	reconnectInterval            time.Duration
	rootCmd                      = &cobra.Command{
		Use:     "fodc",
		Short:   "First Occurrence Data Collection (FODC) agent",
		Version: version.Build(),
		Long: `FODC (First Occurrence Data Collection) is an observability and diagnostics subsystem for BanyanDB.
It continuously collects runtime parameters, performance indicators, node states, and configuration data.`,
		RunE: runFODC,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().DurationVar(&pollInterval, "poll-metrics-interval", defaultPollInterval,
		"Interval at which the Watchdog polls metrics from the BanyanDB container")
	rootCmd.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", defaultMetricsEndpoint,
		"URL of the BanyanDB metrics endpoint to poll from")
	rootCmd.Flags().IntVar(&maxMetricsMemoryUsagePercent, "max-metrics-memory-usage-percentage",
		defaultMaxMetricsMemoryUsagePercent,
		"Maximum percentage of available memory (based on cgroup memory limit) that can be used for storing metrics in the Flight Recorder. Valid range: 0-100.")
	rootCmd.Flags().StringVar(&prometheusListenAddr, "prometheus-listen-addr", defaultPrometheusListenAddr,
		"Address on which to expose Prometheus metrics endpoint (e.g., :9090)")
	rootCmd.Flags().StringVar(&proxyAddr, "proxy-addr", defaultProxyAddr,
		"FODC Proxy gRPC address")
	rootCmd.Flags().StringVar(&nodeIP, "node-ip", "",
		"IP address for this BanyanDB node's primary gRPC address. Used as part of AgentIdentity for agent identification.")
	rootCmd.Flags().IntVar(&nodePort, "node-port", 0,
		"Port number for this BanyanDB node's primary gRPC address. Used as part of AgentIdentity for agent identification.")
	rootCmd.Flags().StringVar(&nodeRole, "node-role", "",
		"Role of this BanyanDB node. Valid values: liaison, datanode-hot, datanode-warm, datanode-cold, etc. Must match the node's actual role in the cluster.")
	rootCmd.Flags().StringVar(&nodeLabels, "node-labels", "",
		"Labels/metadata for this node. Format: key1=value1,key2=value2. Examples: zone=us-west-1,env=production. Used for filtering and grouping nodes in the Proxy.")
	rootCmd.Flags().DurationVar(&heartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval,
		"Interval for sending heartbeats to Proxy. Note: The Proxy may override this value in RegisterAgentResponse.")
	rootCmd.Flags().DurationVar(&reconnectInterval, "reconnect-interval", defaultReconnectInterval,
		"Interval for reconnection attempts when connection to Proxy is lost")
}

// runFODC is the main function for the FODC agent.
func runFODC(_ *cobra.Command, _ []string) error {
	if initErr := logger.Init(logger.Logging{
		Env:   "prod",
		Level: "info",
	}); initErr != nil {
		return fmt.Errorf("failed to initialize logger: %w", initErr)
	}

	log := logger.GetLogger("fodc")
	log.Info().
		Str("endpoint", metricsEndpoint).
		Dur("interval", pollInterval).
		Str("prometheus-listen-addr", prometheusListenAddr).
		Msg("Starting FODC agent")

	if pollInterval <= 0 {
		return fmt.Errorf("poll-metrics-interval must be greater than 0")
	}
	if metricsEndpoint == "" {
		return fmt.Errorf("metrics-endpoint cannot be empty")
	}
	if maxMetricsMemoryUsagePercent < 0 || maxMetricsMemoryUsagePercent > 100 {
		return fmt.Errorf("max-metrics-memory-usage-percentage must be between 0 and 100")
	}

	memoryLimit, memLimitErr := cgroups.MemoryLimit()
	if memLimitErr != nil {
		// On non-Linux systems (e.g., macOS), cgroups are not available
		// This is expected and not an error condition
		if runtime.GOOS != "linux" || strings.Contains(memLimitErr.Error(), "mountinfo") {
			log.Debug().Err(memLimitErr).Msg("Cgroup memory limit not available (expected on non-Linux systems), using default capacity")
		} else {
			log.Warn().Err(memLimitErr).Msg("Failed to get cgroup memory limit, using default capacity")
		}
		memoryLimit = 1024 * 1024 * 1024 // Default to 1GB if cgroup limit cannot be determined
	}

	var capacitySize int64
	if memoryLimit > 0 {
		capacitySize = (memoryLimit * int64(maxMetricsMemoryUsagePercent)) / 100
	} else {
		// If memory limit is unlimited (-1) or invalid, use a reasonable default
		// Default to 100MB for metrics storage
		capacitySize = 100 * 1024 * 1024
		log.Info().Msg("Memory limit is unlimited or invalid, using default capacity of 100MB")
	}

	log.Info().
		Int64("memory-limit-bytes", memoryLimit).
		Int("memory-usage-percent", maxMetricsMemoryUsagePercent).
		Int64("capacity-size-bytes", capacitySize).
		Msg("Flight Recorder capacity configured")

	fr := flightrecorder.NewFlightRecorder(capacitySize)

	// Create Prometheus registry
	promReg := prometheus.NewRegistry()
	datasourceCollector := exporter.NewDatasourceCollector(fr)

	// Create and start Prometheus metrics server
	metricsServer, serverCreateErr := server.NewServer(server.Config{
		ListenAddr:        prometheusListenAddr,
		ReadHeaderTimeout: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})
	if serverCreateErr != nil {
		return fmt.Errorf("failed to create metrics server: %w", serverCreateErr)
	}

	serverErrCh, serverStartErr := metricsServer.Start(promReg, datasourceCollector)
	if serverStartErr != nil {
		return fmt.Errorf("failed to start metrics server: %w", serverStartErr)
	}

	wd := watchdog.NewWatchdogWithConfig(fr, metricsEndpoint, pollInterval)

	ctx := context.Background()
	if preRunErr := wd.PreRun(ctx); preRunErr != nil {
		_ = metricsServer.Stop()
		return fmt.Errorf("failed to initialize watchdog: %w", preRunErr)
	}

	stopCh := wd.Serve()

	var proxyClient *proxy.ProxyClient
	if proxyAddr != "" && nodeIP != "" && nodePort > 0 && nodeRole != "" {
		labelsMap := proxy.ParseLabels(nodeLabels)
		proxyClient = proxy.NewProxyClient(
			proxyAddr,
			nodeIP,
			nodePort,
			nodeRole,
			labelsMap,
			heartbeatInterval,
			reconnectInterval,
			fr,
			log,
		)

		proxyCtx, proxyCancel := context.WithCancel(ctx)
		defer proxyCancel()

		go func() {
			if startErr := proxyClient.Start(proxyCtx); startErr != nil {
				log.Error().Err(startErr).Msg("Proxy client error")
			}
		}()

		log.Info().
			Str("proxy_addr", proxyAddr).
			Str("node_ip", nodeIP).
			Int("node_port", nodePort).
			Str("node_role", nodeRole).
			Msg("Proxy client started")
	} else {
		log.Info().Msg("Proxy client not started (missing required flags: --proxy-addr, --node-ip, --node-port, --node-role)")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Info().Msg("Received shutdown signal")
	case <-stopCh:
		log.Info().Msg("Watchdog stopped")
	case err := <-serverErrCh:
		log.Error().Err(err).Msg("Metrics server error")
	}

	// Graceful shutdown
	wd.GracefulStop()

	if proxyClient != nil {
		if disconnectErr := proxyClient.Disconnect(); disconnectErr != nil {
			log.Warn().Err(disconnectErr).Msg("Error disconnecting proxy client")
		}
	}

	if shutdownErr := metricsServer.Stop(); shutdownErr != nil {
		log.Warn().Err(shutdownErr).Msg("Error shutting down metrics server")
	}

	return nil
}
