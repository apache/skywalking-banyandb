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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/fodc/internal/watchdog"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	defaultPollInterval    = 10 * time.Second
	defaultMetricsEndpoint = "http://localhost:2121/metrics"
)

var (
	pollInterval    time.Duration
	metricsEndpoint string
)

func main() {
	rootCmd := &cobra.Command{
		Use:     "fodc",
		Short:   "First Occurrence Data Collection (FODC) agent",
		Version: version.Build(),
		Long: `FODC (First Occurrence Data Collection) is an observability and diagnostics subsystem for BanyanDB.
It continuously collects runtime parameters, performance indicators, node states, and configuration data.`,
		RunE: runFODC,
	}

	rootCmd.Flags().DurationVar(&pollInterval, "poll-metrics-interval", defaultPollInterval,
		"Interval at which the Watchdog polls metrics from the BanyanDB container")
	rootCmd.Flags().StringVar(&metricsEndpoint, "metrics-endpoint", defaultMetricsEndpoint,
		"URL of the BanyanDB metrics endpoint to poll from")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runFODC(cmd *cobra.Command, args []string) error {
	// Initialize logger
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
		Msg("Starting FODC agent")

	// Validate configuration
	if pollInterval <= 0 {
		return fmt.Errorf("poll-metrics-interval must be greater than 0")
	}
	if metricsEndpoint == "" {
		return fmt.Errorf("metrics-endpoint cannot be empty")
	}

	// Create watchdog with configuration
	// TODO: Create FlightRecorder and pass it here when implemented
	wd := watchdog.NewWatchdogWithConfig(nil, metricsEndpoint, pollInterval)

	// Initialize watchdog
	ctx := context.Background()
	if preRunErr := wd.PreRun(ctx); preRunErr != nil {
		return fmt.Errorf("failed to initialize watchdog: %w", preRunErr)
	}

	// Start watchdog
	stopCh := wd.Serve()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Info().Msg("Received shutdown signal")
	case <-stopCh:
		log.Info().Msg("Watchdog stopped")
	}

	// Gracefully stop watchdog
	wd.GracefulStop()

	return nil
}
