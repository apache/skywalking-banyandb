package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/detector"
	"github.com/apache/skywalking-banyandb/fodc/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

const (
	DefaultMetricsURL               = "http://localhost:2121/metrics"
	DefaultPollInterval             = 5 * time.Second
	DefaultHealthCheckURL           = "http://localhost:17913/api/healthz"
	DefaultHealthInterval           = 10 * time.Second
	DefaultDeathRattlePath          = "/tmp/death-rattle"
	DefaultFlightRecorderPath       = "/tmp/fodc-flight-recorder.bin"
	DefaultFlightRecorderBufferSize = 1000
)

func main() {
	var (
		metricsURL           = flag.String("metrics-url", DefaultMetricsURL, "Prometheus metrics endpoint URL")
		pollInterval         = flag.Duration("poll-interval", DefaultPollInterval, "Interval for polling metrics")
		healthCheckURL       = flag.String("health-url", DefaultHealthCheckURL, "Health check endpoint URL")
		healthInterval       = flag.Duration("health-interval", DefaultHealthInterval, "Interval for health checks")
		deathRattlePath      = flag.String("death-rattle-path", DefaultDeathRattlePath, "Path to watch for death rattle file triggers")
		containerName        = flag.String("container", "banyandb", "Container name to monitor")
		alertThreshold       = flag.Float64("alert-threshold", 0.8, "Alert threshold for error rate (0.0-1.0)")
		flightRecorderPath   = flag.String("flight-recorder-path", DefaultFlightRecorderPath, "Path to flight recorder memory-mapped file")
		flightRecorderBuffer = flag.Uint("flight-recorder-buffer", DefaultFlightRecorderBufferSize, "Number of snapshots to buffer in flight recorder")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Initialize components
	metricsPoller := poller.NewMetricsPoller(*metricsURL, *pollInterval)
	deathRattleDetector := detector.NewDeathRattleDetector(*deathRattlePath, *containerName, *healthCheckURL, *healthInterval)
	alertManager := detector.NewAlertManager(*alertThreshold)

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

	log.Println("Starting FODC (Failure Observer and Death Rattle Detector)")
	log.Printf("Metrics URL: %s", *metricsURL)
	log.Printf("Health Check URL: %s", *healthCheckURL)
	log.Printf("Death Rattle Path: %s", *deathRattlePath)
	log.Printf("Container: %s", *containerName)
	log.Printf("Flight Recorder Path: %s", *flightRecorderPath)
	log.Printf("Flight Recorder Buffer Size: %d", *flightRecorderBuffer)

	// Start metrics polling
	metricsChan := make(chan poller.MetricsSnapshot, 10)
	go func() {
		if err := metricsPoller.Start(ctx, metricsChan); err != nil {
			log.Printf("Error polling metrics: %v", err)
		}
	}()

	// Start death rattle detection
	deathRattleChan := make(chan detector.DeathRattleEvent, 10)
	go func() {
		if err := deathRattleDetector.Start(ctx, deathRattleChan); err != nil {
			log.Printf("Error detecting death rattles: %v", err)
		}
	}()

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
				}

				alerts := alertManager.AnalyzeMetrics(snapshot)
				for _, alert := range alerts {
					log.Printf("🚨 ALERT: %s", alert)
				}
			case event := <-deathRattleChan:
				log.Printf("💀 DEATH RATTLE DETECTED: %s - %s", event.Type, event.Message)
				alertManager.HandleDeathRattle(event)
			}
		}
	}()

	// Wait for termination signal
	<-sigChan
	log.Println("Shutting down...")
	cancel()
	time.Sleep(1 * time.Second)
}
