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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
	"github.com/apache/skywalking-banyandb/fodc/internal/sidecar"
)

const (
	DefaultMetricsURL               = "http://localhost:2121/metrics"
	DefaultPollInterval             = 5 * time.Second
	DefaultHealthCheckURL           = "http://localhost:17913/api/healthz"
	DefaultFlightRecorderPath       = "/tmp/fodc-flight-recorder.bin"
	DefaultFlightRecorderBufferSize = 1000
	DefaultHealthPort               = 17914
	DefaultRotationInterval         = 0 // 0 means no rotation by default
	Version                         = "0.1.0"
)

func main() {
	var (
		sidecarMode          = flag.Bool("sidecar", false, "Run in sidecar mode with auto-discovery")
		metricsURL           = flag.String("metrics-url", "", "Prometheus metrics endpoint URL (auto-discovered in sidecar mode)")
		pollInterval         = flag.Duration("poll-interval", DefaultPollInterval, "Interval for polling metrics")
		healthCheckURL       = flag.String("health-url", "", "Health check endpoint URL (auto-discovered in sidecar mode)")
		flightRecorderPath   = flag.String("flight-recorder-path", DefaultFlightRecorderPath, "Path to flight recorder memory-mapped file")
		flightRecorderBuffer = flag.Uint("flight-recorder-buffer", DefaultFlightRecorderBufferSize, "Number of snapshots to buffer in flight recorder")
		healthPort           = flag.Int("health-port", DefaultHealthPort, "Port for sidecar health endpoint")
		rotationInterval     = flag.Duration("flight-recorder-rotation", DefaultRotationInterval, "Interval to clear/rotate flight recorder (0 = disabled, e.g., 24h, 1h30m)")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Sidecar mode: auto-discover BanyanDB endpoints
	var endpoint *sidecar.BanyanDBEndpoint
	var healthServer *sidecar.HealthServer

	if *sidecarMode {
		log.Println("Running in SIDECAR mode")

		// Discover BanyanDB endpoints
		var err error
		endpoint, err = sidecar.DiscoverBanyanDB()
		if err != nil {
			log.Fatalf("Failed to discover BanyanDB endpoint: %v", err)
		}

		log.Printf("Discovered BanyanDB endpoints:")
		log.Printf("  Metrics URL: %s", endpoint.MetricsURL)
		log.Printf("  Health URL: %s", endpoint.HealthURL)
		if endpoint.PodName != "" {
			log.Printf("  Pod: %s (IP: %s)", endpoint.PodName, endpoint.PodIP)
		}

		// Verify endpoint is accessible
		if err := sidecar.VerifyEndpoint(endpoint, 5*time.Second); err != nil {
			log.Printf("Warning: BanyanDB endpoint verification failed: %v", err)
			log.Println("Continuing anyway - will retry during operation")
		} else {
			log.Println("✓ BanyanDB endpoint verified")
		}

		// Override URLs with discovered endpoints
		*metricsURL = endpoint.MetricsURL
		*healthCheckURL = endpoint.HealthURL

		// Initialize health server for sidecar
		healthServer = sidecar.NewHealthServer(*healthPort, Version)
		healthServer.SetMetadata("mode", "sidecar")
		healthServer.SetMetadata("banyandb_host", endpoint.Host)
		if endpoint.PodName != "" {
			healthServer.SetMetadata("pod_name", endpoint.PodName)
			healthServer.SetMetadata("pod_ip", endpoint.PodIP)
		}

		// Start health server
		go func() {
			log.Printf("Starting sidecar health server on port %d", *healthPort)
			if err := healthServer.Start(); err != nil && err != http.ErrServerClosed {
				log.Printf("ERROR: Failed to start health server on port %d: %v", *healthPort, err)
				log.Printf("ERROR: Health endpoint http://localhost:%d/healthz will not be available", *healthPort)
			}
		}()

		// Update health status
		healthServer.UpdateBanyanDBHealth(true, nil)
	} else {
		// Non-sidecar mode: use defaults or provided values
		if *metricsURL == "" {
			*metricsURL = DefaultMetricsURL
		}
		if *healthCheckURL == "" {
			*healthCheckURL = DefaultHealthCheckURL
		}
		log.Println("Running in STANDALONE mode")
	}

	// Initialize components
	metricsPoller := poller.NewMetricsPoller(*metricsURL, *pollInterval)

	// Initialize Flight Recorder
	flightRecorder, err := flightrecorder.NewFlightRecorder(*flightRecorderPath, uint32(*flightRecorderBuffer))
	if err != nil {
		log.Fatalf("Failed to initialize flight recorder: %v", err)
	}
	defer func() {
		if err := flightRecorder.Close(); err != nil {
			log.Printf("Error closing flight recorder: %v", err)
		}
	}()

	// Attempt to recover any existing data from flight recorder
	recoveredSnapshots, err := flightRecorder.ReadAll()
	if err != nil {
		log.Printf("Warning: Failed to recover flight recorder data: %v", err)
	} else if len(recoveredSnapshots) > 0 {
		log.Printf("Recovered %d snapshots from flight recorder", len(recoveredSnapshots))
	}

	log.Println("Starting FODC")
	log.Printf("Metrics URL: %s", *metricsURL)
	log.Printf("Health Check URL: %s", *healthCheckURL)
	log.Printf("Flight Recorder Path: %s", *flightRecorderPath)
	log.Printf("Flight Recorder Buffer Size: %d", *flightRecorderBuffer)
	if *rotationInterval > 0 {
		log.Printf("Flight Recorder Rotation Interval: %v", *rotationInterval)
	}

	// Start metrics polling
	metricsChan := make(chan poller.MetricsSnapshot, 10)
	go func() {
		if err := metricsPoller.Start(ctx, metricsChan); err != nil {
			log.Printf("Error polling metrics: %v", err)
		}
	}()

	// Track metrics for health server
	var totalSnapshots int
	var lastSnapshotTime time.Time
	var metricsErrors int

	// Start flight recorder rotation if enabled
	if *rotationInterval > 0 {
		go func() {
			ticker := time.NewTicker(*rotationInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					log.Printf("Rotating flight recorder (clearing old data)...")
					if err := flightRecorder.Clear(); err != nil {
						log.Printf("Error clearing flight recorder: %v", err)
					} else {
						log.Printf("Flight recorder cleared successfully")
						totalSnapshots = 0 // Reset counter
					}
				}
			}
		}()
	}
	// Process events
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case snapshot := <-metricsChan:
				// Record snapshot in flight recorder
				if err := flightRecorder.Record(snapshot); err != nil {
					log.Printf("Warning: Failed to record snapshot in flight recorder: %v", err)
					metricsErrors++
				} else {
					totalSnapshots++
					lastSnapshotTime = snapshot.Timestamp
				}

				// Update health server if in sidecar mode
				if healthServer != nil {
					healthServer.UpdateMetricsHealth(totalSnapshots, metricsErrors, lastSnapshotTime)

					// Update BanyanDB connection status based on errors
					connected := len(snapshot.Errors) == 0
					var err error
					if !connected && len(snapshot.Errors) > 0 {
						err = fmt.Errorf("metrics polling errors: %v", snapshot.Errors)
					}
					healthServer.UpdateBanyanDBHealth(connected, err)
				}
			}
		}
	}()

	// Wait for termination signal
	<-sigChan
	log.Println("Shutting down...")
	cancel()

	// Stop health server if running
	if healthServer != nil {
		if err := healthServer.Stop(); err != nil {
			log.Printf("Error stopping health server: %v", err)
		}
	}

	time.Sleep(1 * time.Second)
}
