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
	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

const (
	DefaultMetricsURL      = "http://localhost:2121/metrics"
	DefaultPollInterval    = 5 * time.Second
	DefaultHealthCheckURL  = "http://localhost:17913/api/healthz"
	DefaultHealthInterval  = 10 * time.Second
	DefaultDeathRattlePath = "/tmp/death-rattle"
)

func main() {
	var (
		metricsURL      = flag.String("metrics-url", DefaultMetricsURL, "Prometheus metrics endpoint URL")
		pollInterval    = flag.Duration("poll-interval", DefaultPollInterval, "Interval for polling metrics")
		healthCheckURL  = flag.String("health-url", DefaultHealthCheckURL, "Health check endpoint URL")
		healthInterval  = flag.Duration("health-interval", DefaultHealthInterval, "Interval for health checks")
		deathRattlePath = flag.String("death-rattle-path", DefaultDeathRattlePath, "Path to watch for death rattle file triggers")
		containerName   = flag.String("container", "banyandb", "Container name to monitor")
		alertThreshold  = flag.Float64("alert-threshold", 0.8, "Alert threshold for error rate (0.0-1.0)")
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

	log.Println("Starting FODC (Failure Observer and Death Rattle Detector)")
	log.Printf("Metrics URL: %s", *metricsURL)
	log.Printf("Health Check URL: %s", *healthCheckURL)
	log.Printf("Death Rattle Path: %s", *deathRattlePath)
	log.Printf("Container: %s", *containerName)

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

