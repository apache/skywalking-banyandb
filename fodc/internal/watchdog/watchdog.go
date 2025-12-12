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

package watchdog

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metrics"
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
	client       *http.Client
	url          string
	interval     time.Duration
	recorder     MetricsRecorder
	log          *logger.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.RWMutex
	isRunning    bool
	retryBackoff time.Duration
}

// NewWatchdogWithConfig creates a new Watchdog instance with specified configuration.
func NewWatchdogWithConfig(recorder MetricsRecorder, url string, interval time.Duration) *Watchdog {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watchdog{
		recorder:     recorder,
		url:          url,
		interval:     interval,
		ctx:          ctx,
		cancel:       cancel,
		retryBackoff: initialBackoff,
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
		Str("endpoint", w.url).
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

		if pollErr := w.pollAndForward(); pollErr != nil {
			w.log.Warn().Err(pollErr).Msg("Initial metrics poll failed")
		}

		for {
			select {
			case <-w.ctx.Done():
				w.log.Info().Msg("Watchdog stopping")
				return
			case <-ticker.C:
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
		w.log.Debug().Int("count", len(rawMetrics)).Msg("No recorder configured, skipping metrics forwarding")
		return nil
	}

	if updateErr := w.recorder.Update(rawMetrics); updateErr != nil {
		return fmt.Errorf("failed to forward metrics to recorder: %w", updateErr)
	}

	w.log.Debug().Int("count", len(rawMetrics)).Msg("Successfully polled and forwarded metrics")
	return nil
}

// pollMetrics fetches raw metrics text from endpoint and parses them.
func (w *Watchdog) pollMetrics(ctx context.Context) ([]metrics.RawMetric, error) {
	var lastErr error
	currentBackoff := w.retryBackoff

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(currentBackoff):
			}
			w.log.Debug().
				Int("attempt", attempt+1).
				Dur("backoff", currentBackoff).
				Msg("Retrying metrics poll")
		}

		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, w.url, nil)
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

		parsedMetrics, parseErr := metrics.Parse(string(body))
		if parseErr != nil {
			lastErr = fmt.Errorf("failed to parse metrics: %w", parseErr)
			currentBackoff = w.exponentialBackoff(currentBackoff)
			continue
		}

		// Reset backoff on success
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
