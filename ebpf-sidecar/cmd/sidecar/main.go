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

// Package main implements the eBPF sidecar command-line interface.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/collector"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/config"
	"github.com/apache/skywalking-banyandb/ebpf-sidecar/internal/server"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "ebpf-sidecar",
	Short: "eBPF Sidecar Agent for BanyanDB",
	Long: `eBPF Sidecar Agent provides kernel-level observability for BanyanDB operations,
offering insights into system calls, memory management, and I/O patterns.`,
	RunE: run,
}

var (
	configFile string
	logLevel   string
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "info", "log level (debug, info, warn, error)")

	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		// NOTE: cmd and args are standard Cobra parameters
		// We keep them for potential future use (subcommands, etc)
		_ = cmd
		_ = args
		fmt.Printf("eBPF Sidecar Agent\n")
		fmt.Printf("Version:    %s\n", version)
		fmt.Printf("Build Time: %s\n", buildTime)
		fmt.Printf("Git Commit: %s\n", gitCommit)
	},
}

func run(cmd *cobra.Command, args []string) error {
	// NOTE: cmd could be used for accessing command flags dynamically
	// args could be used for additional positional arguments
	_ = cmd
	_ = args
	// Setup logger
	logger, err := setupLogger(logLevel)
	if err != nil {
		return fmt.Errorf("failed to setup logger: %w", err)
	}
	defer func() {
		_ = logger.Sync() // Best effort flush
	}()

	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize collector
	logger.Info("Initializing eBPF collector...")
	coll, err := collector.New(cfg.Collector, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize collector: %w", err)
	}
	defer coll.Close()

	// Start collector
	if startErr := coll.Start(ctx); startErr != nil {
		return fmt.Errorf("failed to start collector: %w", startErr)
	}

	// Initialize and start servers
	logger.Info("Starting servers...")
	srv, err := server.New(cfg.Server, cfg.Export, coll, logger)
	if err != nil {
		return fmt.Errorf("failed to initialize server: %w", err)
	}

	// Start servers in background
	go func() {
		if err := srv.Start(ctx); err != nil {
			logger.Error("Server error", zap.Error(err))
			cancel()
		}
	}()

	// Wait for shutdown signal
	logger.Info("eBPF Sidecar Agent started successfully")
	select {
	case <-sigChan:
		logger.Info("Received shutdown signal")
	case <-ctx.Done():
		logger.Info("Context canceled")
	}

	// Graceful shutdown
	logger.Info("Shutting down...")
	if err := srv.Stop(context.Background()); err != nil {
		logger.Error("Failed to stop server", zap.Error(err))
	}

	return nil
}

func setupLogger(level string) (*zap.Logger, error) {
	// Parse log level
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}

	// Create logger configuration
	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapLevel),
		Development:      zapLevel == zapcore.DebugLevel,
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	// Build logger
	logger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %w", err)
	}

	return logger, nil
}
