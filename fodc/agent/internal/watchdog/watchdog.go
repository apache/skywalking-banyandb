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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
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
	// nodeResolveGracePeriod bounds how long the watchdog defers recording while the local
	// node's role is still unresolved. Deferring avoids emitting metrics stamped with an
	// unresolved identity that later turns into duplicate (ghost) series once the role
	// resolves; after the grace period a never-resolving node still has its metrics collected.
	nodeResolveGracePeriod = 5 * time.Minute
)

// roleUnspecified is the string form of an unresolved node role.
var roleUnspecified = databasev1.Role_name[int32(databasev1.Role_ROLE_UNSPECIFIED)]

// Watchdog periodically polls metrics from BanyanDB and forwards them to Flight Recorder.
type Watchdog struct {
	startTime      time.Time
	ctx            context.Context
	recorder       MetricsRecorder
	nodeInfo       func() (role string, labels map[string]string)
	postPollHooks  []func(ctx context.Context)
	log            *logger.Logger
	cancel         context.CancelFunc
	client         *http.Client
	resolvedLabels map[string]string
	nodeRole       string
	podName        string
	resolvedRole   string
	urls           []string
	containerNames []string
	wg             sync.WaitGroup
	interval       time.Duration
	retryBackoff   time.Duration
	mu             sync.RWMutex
	isRunning      bool
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
		startTime:      time.Now(),
		nodeRole:       nodeRole,
		podName:        podName,
		containerNames: containerNames,
	}
}

// SetNodeInfoProvider supplies a live source of the node's role and labels, used to stamp
// metrics at scrape time. This avoids freezing the role/labels at a startup snapshot, which
// is unreliable when the local node has not yet resolved its role when the agent starts.
func (w *Watchdog) SetNodeInfoProvider(fn func() (role string, labels map[string]string)) {
	w.mu.Lock()
	w.nodeInfo = fn
	w.mu.Unlock()
}

// AddPostPollHook registers a callback invoked after each successful metrics poll is forwarded
// to the recorder. Multiple hooks may be registered; they run in registration order on the poll
// goroutine, so each must return quickly and offload any slow work to its own goroutine.
func (w *Watchdog) AddPostPollHook(fn func(ctx context.Context)) {
	w.mu.Lock()
	w.postPollHooks = append(w.postPollHooks, fn)
	w.mu.Unlock()
}

// resolveNodeInfo returns the current node role and labels from the live provider, falling back
// to the static role captured at construction when no provider is set. The first resolved
// identity is cached and "sticks": if the provider later regresses to an unresolved role
// (ROLE_UNSPECIFIED), the cached resolved identity is returned instead. Without this, a regression
// would cause the flight recorder to buffer a duplicate (ghost) series under the unresolved
// identity, which is never evicted.
func (w *Watchdog) resolveNodeInfo() (role string, labels map[string]string) {
	w.mu.RLock()
	fn := w.nodeInfo
	cachedRole, cachedLabels := w.resolvedRole, w.resolvedLabels
	w.mu.RUnlock()
	if fn == nil {
		return w.nodeRole, nil
	}
	role, labels = fn()
	if role != "" && role != roleUnspecified {
		w.mu.Lock()
		w.resolvedRole, w.resolvedLabels = role, labels
		w.mu.Unlock()
		return role, labels
	}
	if cachedRole != "" {
		return cachedRole, cachedLabels
	}
	return role, labels
}

// nodeReadyToRecord reports whether the watchdog should record metrics this cycle. While a live
// node-info provider is configured but the node role has not resolved yet, recording is deferred
// (up to nodeResolveGracePeriod) so the flight recorder never buffers metrics under an unresolved
// identity — those would otherwise linger as duplicate (ghost) series once the role resolves and
// the identity labels change. Once resolved, the gate opens permanently; if the role never
// resolves, the grace period ensures the node's metrics are still recorded.
func (w *Watchdog) nodeReadyToRecord() bool {
	w.mu.RLock()
	fn := w.nodeInfo
	start := w.startTime
	w.mu.RUnlock()
	if fn == nil {
		return true
	}
	// resolveNodeInfo caches and sticks to the first resolved identity, so once the role has
	// resolved this stays true even if the live provider briefly regresses to unspecified.
	if role, _ := w.resolveNodeInfo(); role != "" && role != roleUnspecified {
		return true
	}
	return time.Since(start) >= nodeResolveGracePeriod
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
	if len(w.containerNames) != len(w.urls) && len(w.containerNames) > 0 {
		w.log.Warn().
			Int("urls_count", len(w.urls)).
			Int("container_names_count", len(w.containerNames)).
			Msg("Container names count doesn't match URLs count. Endpoints without matching container names will use empty container_name label")
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
		panicdiag.WithRecovery(w.ctx, panicdiag.RecoveryOptions{
			Component:   "fodc-watchdog",
			Logger:      w.log,
			StateDumper: watchdogStateDumper{watchdog: w},
			ProcessMetadata: map[string]string{
				"pod_name":  w.podName,
				"node_role": w.nodeRole,
			},
		}, nil, func(ctx *context.Context) {
			// baseCtx is the pre-loop context, reused as the starting point for
			// each poll so breadcrumbs reflect only the current iteration.
			baseCtx := *ctx

			ticker := time.NewTicker(w.interval)
			defer ticker.Stop()

			runPoll := func() {
				enrichedCtx, pollErr := w.pollAndForward(baseCtx)
				*ctx = enrichedCtx
				if pollErr != nil {
					w.log.Warn().Err(pollErr).Msg("Metrics poll failed")
				}
			}

			w.log.Info().Msg("Starting watchdog polling loop")
			runPoll()

			for {
				select {
				case <-w.ctx.Done():
					w.log.Info().Msg("Watchdog stopping")
					return
				case <-ticker.C:
					w.log.Info().Msg("Starting metrics poll")
					runPoll()
				}
			}
		})
	}()

	return stopCh
}

type watchdogStateDumper struct {
	watchdog *Watchdog
}

func (w watchdogStateDumper) DumpState(_ context.Context) (any, error) {
	if w.watchdog == nil {
		return nil, fmt.Errorf("watchdog is nil")
	}
	w.watchdog.mu.RLock()
	defer w.watchdog.mu.RUnlock()

	return map[string]any{
		"podName":        w.watchdog.podName,
		"nodeRole":       w.watchdog.nodeRole,
		"urls":           append([]string(nil), w.watchdog.urls...),
		"containerNames": append([]string(nil), w.watchdog.containerNames...),
		"interval":       w.watchdog.interval.String(),
		"retryBackoff":   w.watchdog.retryBackoff.String(),
		"isRunning":      w.watchdog.isRunning,
	}, nil
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
// It returns the enriched context so the caller can update the recovery-visible
// ctx pointer, making breadcrumbs visible if a panic occurs.
func (w *Watchdog) pollAndForward(ctx context.Context) (context.Context, error) {
	if !w.nodeReadyToRecord() {
		w.log.Debug().Msg("Deferring metrics recording until the node role resolves (avoids duplicate series)")
		return ctx, nil
	}
	ctx = panicdiag.WithBreadcrumb(ctx, "poll watchdog metrics", "fodc-watchdog", map[string]string{
		"endpoint_count": fmt.Sprintf("%d", len(w.urls)),
	})

	rawMetrics, pollErr := w.pollMetrics(ctx)
	if pollErr != nil {
		return ctx, fmt.Errorf("failed to poll metrics: %w", pollErr)
	}
	ctx = panicdiag.WithBreadcrumb(ctx, "parsed watchdog metrics", "fodc-watchdog", map[string]string{
		"metric_count": fmt.Sprintf("%d", len(rawMetrics)),
	})

	if w.recorder == nil {
		w.log.Error().Int("count", len(rawMetrics)).Msg("No recorder configured, skipping metrics forwarding")
		return ctx, nil
	}

	if updateErr := w.recorder.Update(rawMetrics); updateErr != nil {
		return ctx, fmt.Errorf("failed to forward metrics to recorder: %w", updateErr)
	}
	ctx = panicdiag.WithBreadcrumb(ctx, "forwarded watchdog metrics", "fodc-watchdog", map[string]string{
		"metric_count": fmt.Sprintf("%d", len(rawMetrics)),
	})

	w.mu.RLock()
	hooks := make([]func(context.Context), len(w.postPollHooks))
	copy(hooks, w.postPollHooks)
	w.mu.RUnlock()
	for _, hook := range hooks {
		hook(ctx)
	}

	w.log.Info().Int("count", len(rawMetrics)).Msg("Successfully polled and forwarded metrics")
	return ctx, nil
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
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("metrics endpoint not found: %s", url)
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
		nodeRole, nodeLabels := w.resolveNodeInfo()
		parsedMetrics, parseErr := metrics.ParseWithNodeLabels(string(body), nodeRole, w.podName, containerName, nodeLabels)
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
