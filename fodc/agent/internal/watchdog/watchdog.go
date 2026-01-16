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

// Package watchdog implements a watchdog for metrics data.
package watchdog

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// MetricsRecorder is an interface for recording metrics.
// This allows Watchdog to work with any implementation of FlightRecorder.
type MetricsRecorder interface {
	// Update records metrics from a polling cycle.
	Update(rawMetrics []metrics.RawMetric) error
}

const (
	maxRetries     = 3
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
)

// Watchdog periodically polls metrics from BanyanDB and forwards them to Flight Recorder.
type Watchdog struct {
	client        *http.Client
	log           *logger.Logger
	cancel        context.CancelFunc
	recorder      MetricsRecorder
	ctx           context.Context
	urls          []string
	wg            sync.WaitGroup
	mu            sync.RWMutex
	interval      time.Duration
	retryBackoff  time.Duration
	isRunning     bool
	nodeRole       string
	podName        string
	containerNames []string
}

// NewWatchdogWithConfig creates a new Watchdog instance with specified configuration.
func NewWatchdogWithConfig(recorder MetricsRecorder, urls []string, interval time.Duration, nodeRole, podName string, containerNames []string) *Watchdog {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watchdog{
		recorder:       recorder,
		urls:           urls,
		interval:       interval,
		ctx:            ctx,
		cancel:         cancel,
		retryBackoff:   initialBackoff,
		nodeRole:       nodeRole,
		podName:        podName,
		containerNames: containerNames,
	}
}

// Name returns the name of the watchdog service.
func (w *Watchdog) Name() string {
	return "watchdog"
}

// PreRun initializes the watchdog component before running.
func (w *Watchdog) PreRun(_ context.Context) error {
	w.log = logger.GetLogger("watchdog")
	w.client = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	w.log.Info().
		Strs("endpoints", w.urls).
		Dur("interval", w.interval).
		Msg("Watchdog initialized")
	return nil
}

// Serve starts the watchdog polling loop and blocks until stopped.
func (w *Watchdog) Serve() <-chan struct{} {
	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		stopCh := make(chan struct{})
		close(stopCh)
		return stopCh
	}
	w.isRunning = true
	w.mu.Unlock()

	stopCh := make(chan struct{})

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(stopCh)

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		w.log.Info().Msg("Starting watchdog polling loop")
		if pollErr := w.pollAndForward(); pollErr != nil {
			w.log.Warn().Err(pollErr).Msg("Initial metrics poll failed")
		} else {
			w.log.Info().Msg("Initial metrics poll succeeded")
		}

		for {
			select {
			case <-w.ctx.Done():
				w.log.Info().Msg("Watchdog stopping")
				return
			case <-ticker.C:
				w.log.Info().Msg("Starting metrics poll")
				if pollErr := w.pollAndForward(); pollErr != nil {
					w.log.Warn().Err(pollErr).Msg("Metrics poll failed")
				}
			}
		}
	}()

	return stopCh
}

// GracefulStop gracefully stops the watchdog polling loop.
func (w *Watchdog) GracefulStop() {
	w.mu.Lock()
	if !w.isRunning {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	w.log.Info().Msg("Stopping watchdog")
	w.cancel()
	w.wg.Wait()

	if w.client != nil {
		if transport, ok := w.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	w.mu.Lock()
	w.isRunning = false
	w.mu.Unlock()

	w.log.Info().Msg("Watchdog stopped")
}

// pollAndForward polls metrics and forwards them to the recorder.
func (w *Watchdog) pollAndForward() error {
	rawMetrics, pollErr := w.pollMetrics(w.ctx)
	if pollErr != nil {
		return fmt.Errorf("failed to poll metrics: %w", pollErr)
	}

	if w.recorder == nil {
		w.log.Error().Int("count", len(rawMetrics)).Msg("No recorder configured, skipping metrics forwarding")
		return nil
	}

	if updateErr := w.recorder.Update(rawMetrics); updateErr != nil {
		return fmt.Errorf("failed to forward metrics to recorder: %w", updateErr)
	}

	w.log.Info().Int("count", len(rawMetrics)).Msg("Successfully polled and forwarded metrics")
	return nil
}

// pollMetrics fetches raw metrics text from all endpoints and parses them.
func (w *Watchdog) pollMetrics(ctx context.Context) ([]metrics.RawMetric, error) {
	if len(w.urls) == 0 {
		return nil, fmt.Errorf("no metrics endpoints configured")
	}
	allMetrics := make([]metrics.RawMetric, 0)
	var lastErr error
	for idx, url := range w.urls {
		containerName := ""
		if idx < len(w.containerNames) {
			containerName = w.containerNames[idx]
		}
		endpointMetrics, pollErr := w.pollMetricsFromEndpoint(ctx, url, containerName)
		if pollErr != nil {
			w.log.Warn().
				Str("endpoint", url).
				Err(pollErr).
				Msg("Failed to poll metrics from endpoint")
			lastErr = pollErr
			continue
		}
		allMetrics = append(allMetrics, endpointMetrics...)
		w.log.Info().
			Str("endpoint", url).
			Int("count", len(endpointMetrics)).
			Msg("Successfully polled metrics from endpoint")
	}
	if len(allMetrics) == 0 && lastErr != nil {
		return nil, fmt.Errorf("failed to poll metrics from any endpoint: %w", lastErr)
	}
	return allMetrics, nil
}

// pollMetricsFromEndpoint fetches raw metrics text from a single endpoint and parses them.
func (w *Watchdog) pollMetricsFromEndpoint(ctx context.Context, url string, containerName string) ([]metrics.RawMetric, error) {
	var lastErr error
	currentBackoff := w.retryBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(currentBackoff):
			}
			w.log.Info().
				Str("endpoint", url).
				Int("attempt", attempt+1).
				Dur("backoff", currentBackoff).
				Msg("Retrying metrics poll")
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if reqErr != nil {
			lastErr = fmt.Errorf("failed to create request: %w", reqErr)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}
		resp, respErr := w.client.Do(req)
		if respErr != nil {
			lastErr = fmt.Errorf("failed to fetch metrics: %w", respErr)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", readErr)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}
		parsedMetrics, parseErr := metrics.ParseWithAgentLabels(string(body), w.nodeRole, w.podName, containerName)
		if parseErr != nil {
			lastErr = fmt.Errorf("failed to parse metrics: %w", parseErr)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}
		w.mu.Lock()
		w.retryBackoff = initialBackoff
		w.mu.Unlock()
		return parsedMetrics, nil
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// exponentialBackoff calculates the next backoff duration using exponential backoff.
func (w *Watchdog) exponentialBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		next = maxBackoff
	}
	return next
}
