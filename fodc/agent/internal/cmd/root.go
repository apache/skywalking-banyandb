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

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/proxy"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/server"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/watchdog"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	defaultMetricsPollInterval          = 10 * time.Second
	defaultPollMetricsPorts             = "2121"
	defaultMaxMetricsMemoryUsagePercent = 10
	defaultPrometheusListenAddr         = ":9090"
	defaultProxyAddr                    = "localhost:17900"
	defaultHeartbeatInterval            = 10 * time.Second
	defaultReconnectInterval            = 5 * time.Second
	defaultClusterStatePollInterval     = 30 * time.Second
)

var (
	metricsPollInterval          time.Duration
	pollMetricsPorts             []string
	maxMetricsMemoryUsagePercent int
	prometheusListenAddr         string
	proxyAddr                    string
	podName                      string
	containerNames               []string
	heartbeatInterval            time.Duration
	reconnectInterval            time.Duration
	clusterStatePorts            []string
	clusterStatePollInterval     time.Duration
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
	rootCmd.Flags().DurationVar(&metricsPollInterval, "poll-metrics-interval", defaultMetricsPollInterval,
		"Interval at which the Watchdog polls metrics from the BanyanDB container")
	rootCmd.Flags().StringSliceVar(&pollMetricsPorts, "poll-metrics-ports", []string{defaultPollMetricsPorts},
		"Ports of the BanyanDB metrics endpoints to poll from (can be specified multiple times or comma-separated)")
	rootCmd.Flags().IntVar(&maxMetricsMemoryUsagePercent, "max-metrics-memory-usage-percentage",
		defaultMaxMetricsMemoryUsagePercent,
		"Maximum percentage of available memory (based on cgroup memory limit) that can be used for storing metrics in the Flight Recorder. Valid range: 0-100.")
	rootCmd.Flags().StringVar(&prometheusListenAddr, "prometheus-listen-addr", defaultPrometheusListenAddr,
		"Address on which to expose Prometheus metrics endpoint (e.g., :9090)")
	rootCmd.Flags().StringVar(&proxyAddr, "proxy-addr", defaultProxyAddr,
		"FODC Proxy gRPC address")
	rootCmd.Flags().StringVar(&podName, "pod-name", "",
		"Name of the Kubernetes pod. Used as part of AgentIdentity for agent identification.")
	rootCmd.Flags().StringSliceVar(&containerNames, "container-names", []string{},
		"Names of the containers corresponding to each poll-metrics-port. Must have one-to-one correspondence with poll-metrics-ports.")
	rootCmd.Flags().DurationVar(&heartbeatInterval, "heartbeat-interval", defaultHeartbeatInterval,
		"Interval for sending heartbeats to Proxy. Note: The Proxy may override this value in RegisterAgentResponse.")
	rootCmd.Flags().DurationVar(&reconnectInterval, "reconnect-interval", defaultReconnectInterval,
		"Interval for reconnection attempts when connection to Proxy is lost")
	rootCmd.Flags().StringSliceVar(&clusterStatePorts, "cluster-state-ports", []string{},
		"Ports of the BanyanDB node's gRPC endpoints to poll cluster state from. If empty, cluster state polling is disabled.")
	rootCmd.Flags().DurationVar(&clusterStatePollInterval, "cluster-state-poll-interval", defaultClusterStatePollInterval,
		"Interval at which to poll cluster state from the BanyanDB nodes")
}

// runFODC is the main function for the FODC agent.
func runFODC(_ *cobra.Command, _ []string) error {
	if initErr := logger.Init(logger.Logging{Env: "prod", Level: "info"}); initErr != nil {
		return fmt.Errorf("failed to initialize logger: %w", initErr)
	}
	log := logger.GetLogger("fodc")

	if validateErr := validateFlags(); validateErr != nil {
		return validateErr
	}
	metricsEndpoints := generateMetricsEndpoints(pollMetricsPorts)
	log.Info().Strs("endpoints", metricsEndpoints).Dur("interval", metricsPollInterval).Msg("Starting FODC agent")

	capacitySize := calculateCapacity(log)
	fr := flightrecorder.NewFlightRecorder(capacitySize)

	metricsServer, serverErr := server.NewServer(server.Config{
		ListenAddr:        prometheusListenAddr,
		ReadHeaderTimeout: 3 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})
	if serverErr != nil {
		return fmt.Errorf("failed to create metrics server: %w", serverErr)
	}
	serverErrCh, startErr := metricsServer.Start(prometheus.NewRegistry(), exporter.NewDatasourceCollector(fr))
	if startErr != nil {
		return fmt.Errorf("failed to start metrics server: %w", startErr)
	}

	ctx := context.Background()
	clusterCollector, clusterErr := cluster.StartCollector(ctx, log, clusterStatePorts, clusterStatePollInterval, podName)
	if clusterErr != nil {
		_ = metricsServer.Stop()
		return clusterErr
	}

	var nodeRole string
	var nodeLabels map[string]string
	if clusterCollector != nil {
		nodeRole, nodeLabels = clusterCollector.GetNodeInfo()
		log.Info().Str("node_role", nodeRole).Int("labels_count", len(nodeLabels)).Msg("Node info fetched")
	}

	wd := watchdog.NewWatchdogWithConfig(fr, metricsEndpoints, metricsPollInterval, nodeRole, podName, containerNames)
	if preRunErr := wd.PreRun(ctx); preRunErr != nil {
		cluster.StopCollector(clusterCollector)
		_ = metricsServer.Stop()
		return fmt.Errorf("failed to initialize watchdog: %w", preRunErr)
	}
	stopCh := wd.Serve()

	proxyClient := startProxyClient(ctx, log, fr, nodeRole, nodeLabels, clusterCollector)

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

	wd.GracefulStop()
	if proxyClient != nil {
		if disconnectErr := proxyClient.Disconnect(); disconnectErr != nil {
			log.Warn().Err(disconnectErr).Msg("Error disconnecting proxy client")
		}
	} else {
		cluster.StopCollector(clusterCollector)
	}
	if shutdownErr := metricsServer.Stop(); shutdownErr != nil {
		log.Warn().Err(shutdownErr).Msg("Error shutting down metrics server")
	}
	return nil
}

func validateFlags() error {
	if metricsPollInterval <= 0 {
		return fmt.Errorf("poll-metrics-interval must be greater than 0")
	}
	if len(pollMetricsPorts) == 0 {
		return fmt.Errorf("poll-metrics-ports cannot be empty")
	}
	if len(containerNames) > 0 && len(containerNames) != len(pollMetricsPorts) {
		return fmt.Errorf("container-names count (%d) must match poll-metrics-ports count (%d)", len(containerNames), len(pollMetricsPorts))
	}
	if maxMetricsMemoryUsagePercent < 0 || maxMetricsMemoryUsagePercent > 100 {
		return fmt.Errorf("max-metrics-memory-usage-percentage must be between 0 and 100")
	}
	return nil
}

func calculateCapacity(log *logger.Logger) int64 {
	memoryLimit, memErr := cgroups.MemoryLimit()
	if memErr != nil {
		if runtime.GOOS != "linux" || strings.Contains(memErr.Error(), "mountinfo") {
			log.Debug().Err(memErr).Msg("Cgroup memory limit not available, using default")
		} else {
			log.Warn().Err(memErr).Msg("Failed to get cgroup memory limit, using default")
		}
		memoryLimit = 1024 * 1024 * 1024
	}
	var capacitySize int64
	if memoryLimit > 0 {
		capacitySize = (memoryLimit * int64(maxMetricsMemoryUsagePercent)) / 100
	} else {
		capacitySize = 100 * 1024 * 1024
		log.Info().Msg("Memory limit is unlimited or invalid, using default capacity of 100MB")
	}
	log.Info().
		Int64("memory-limit-bytes", memoryLimit).
		Int("memory-usage-percent", maxMetricsMemoryUsagePercent).
		Int64("capacity-size-bytes", capacitySize).
		Msg("Flight Recorder capacity configured")
	return capacitySize
}

func startProxyClient(ctx context.Context, log *logger.Logger, fr *flightrecorder.FlightRecorder,
	nodeRole string, nodeLabels map[string]string, collector *cluster.Collector,
) *proxy.Client {
	if proxyAddr == "" || podName == "" || nodeRole == "" {
		log.Info().Msg("Proxy client not started (missing: --proxy-addr, --pod-name, and --node-role)")
		return nil
	}
	client := proxy.NewClient(proxyAddr, nodeRole, podName, containerNames, nodeLabels,
		heartbeatInterval, reconnectInterval, fr, collector, log)
	go func() {
		if startErr := client.Start(ctx); startErr != nil {
			log.Error().Err(startErr).Msg("Proxy client error")
		}
	}()
	log.Info().Str("proxy_addr", proxyAddr).Str("pod_name", podName).Str("node_role", nodeRole).Msg("Proxy client started")
	return client
}

func generateMetricsEndpoints(ports []string) []string {
	endpoints := make([]string, 0, len(ports))
	for _, port := range ports {
		endpoints = append(endpoints, fmt.Sprintf("http://localhost:%s/metrics", port))
	}
	return endpoints
}
