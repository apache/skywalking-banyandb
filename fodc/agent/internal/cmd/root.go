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
	"go.uber.org/zap"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/ktm/iomonitor"
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
)

var (
	pollInterval                 time.Duration
	metricsEndpoint              string
	maxMetricsMemoryUsagePercent int
	prometheusListenAddr         string
	ktmEnabled                   bool
	ktmInterval                  time.Duration
	ktmModules                   []string
	ktmEBPFPinPath               string
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
	rootCmd.Flags().BoolVar(&ktmEnabled, "ktm-enabled", false, "Enable Kernel Trace Module (eBPF)")
	rootCmd.Flags().DurationVar(&ktmInterval, "ktm-interval", 10*time.Second, "Interval for KTM metrics collection")
	rootCmd.Flags().StringSliceVar(&ktmModules, "ktm-modules", []string{"iomonitor"}, "KTM modules to enable")
	rootCmd.Flags().StringVar(&ktmEBPFPinPath, "ktm-ebpf-pin-path", "/sys/fs/bpf/ebpf-sidecar", "Path to pin eBPF maps")
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

	var ktmSvc *ktm.KTM
	var bridgeStopCh chan struct{}

	if ktmEnabled {
		ktmCfg := ktm.Config{
			Enabled:  true,
			Interval: ktmInterval,
			Modules:  ktmModules,
			EBPF: iomonitor.EBPFConfig{
				PinPath:      ktmEBPFPinPath,
				MapSizeLimit: 10240,
			},
		}

		zapLog, zapErr := zap.NewProduction()
		if zapErr != nil {
			return fmt.Errorf("failed to create zap logger: %w", zapErr)
		}

		var err error
		ktmSvc, err = ktm.NewKTM(ktmCfg, zapLog)
		if err != nil {
			return fmt.Errorf("failed to create KTM: %w", err)
		}

		if err := ktmSvc.Start(ctx); err != nil {
			return fmt.Errorf("failed to start KTM: %w", err)
		}

		bridgeStopCh = make(chan struct{})
		go func() {
			ticker := time.NewTicker(ktmInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-bridgeStopCh:
					return
				case <-ticker.C:
					store := ktmSvc.GetMetrics()
					if store != nil {
						rawMetrics := ktm.ToRawMetrics(store)
						if len(rawMetrics) > 0 {
							if err := fr.Update(rawMetrics); err != nil {
								log.Warn().Err(err).Msg("Failed to update FlightRecorder with KTM metrics")
							}
						}
					}
				}
			}
		}()
	}

	if preRunErr := wd.PreRun(ctx); preRunErr != nil {
		_ = metricsServer.Stop()
		return fmt.Errorf("failed to initialize watchdog: %w", preRunErr)
	}

	stopCh := wd.Serve()

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

	if bridgeStopCh != nil {
		close(bridgeStopCh)
	}
	if ktmSvc != nil {
		_ = ktmSvc.Stop()
	}

	if shutdownErr := metricsServer.Stop(); shutdownErr != nil {
		log.Warn().Err(shutdownErr).Msg("Error shutting down metrics server")
	}

	return nil
}
