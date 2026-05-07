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

package run

import (
	"context"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// Task tracks a goroutine launched by Go or GoOrDie. Done closes on exit;
// Outcome is nil until then.
type Task struct {
	outcome atomic.Pointer[panicdiag.RecoveryOutcome]
	done    chan struct{}
}

// Done returns a channel closed when the goroutine launched by Go exits.
func (t *Task) Done() <-chan struct{} {
	return t.done
}

// Outcome returns the panic-recovery outcome, or nil if the goroutine has not
// yet exited.
func (t *Task) Outcome() *panicdiag.RecoveryOutcome {
	return t.outcome.Load()
}

// Wait blocks until the goroutine exits and returns the outcome.
func (t *Task) Wait() *panicdiag.RecoveryOutcome {
	<-t.done
	return t.outcome.Load()
}

// goConfig holds resolved options for Go, GoOrDie, and GoWithSignal.
type goConfig struct {
	reporter        panicdiag.Reporter
	stateDumper     panicdiag.StateDumper
	counter         meter.Counter
	processMetadata map[string]string
	artifactRoot    string
	stateLimitBytes int64
}

// Option configures Go, GoOrDie, or GoWithSignal.
type Option func(*goConfig)

// WithReporter installs a non-blocking per-call reporter for recovered panics.
func WithReporter(r panicdiag.Reporter) Option {
	return func(c *goConfig) { c.reporter = r }
}

// WithStateDumper attaches a StateDumper that runs during recovery to capture
// a bounded snapshot of caller-defined state alongside the panic record.
func WithStateDumper(d panicdiag.StateDumper) Option {
	return func(c *goConfig) { c.stateDumper = d }
}

// WithCounter overrides the panic counter for this call. When unset, the
// process-wide counter from panicdiag.SetDefaultPanicCounter is used.
func WithCounter(c meter.Counter) Option {
	return func(g *goConfig) { g.counter = c }
}

// WithProcessMetadata attaches static labels to the panic record.
func WithProcessMetadata(meta map[string]string) Option {
	return func(c *goConfig) { c.processMetadata = meta }
}

// WithArtifactRoot overrides the directory that artifacts are written to.
func WithArtifactRoot(root string) Option {
	return func(c *goConfig) { c.artifactRoot = root }
}

// WithStateLimitBytes caps the size of the deep state dump.
func WithStateLimitBytes(n int64) Option {
	return func(c *goConfig) { c.stateLimitBytes = n }
}

// Go launches fn in a recovered goroutine.
// Panics are logged, reported, and exposed through the returned Task. Use
// GoOrDie for fatal panics and GoWithSignal when a select needs the outcome.
func Go(ctx context.Context, component string, log *logger.Logger, fn func(context.Context), opts ...Option) *Task {
	return launchTask(ctx, component, log, fn, false, opts)
}

// GoOrDie launches fn with recovery, then re-raises recovered panics after
// diagnostics and reporters complete.
func GoOrDie(ctx context.Context, component string, log *logger.Logger, fn func(context.Context), opts ...Option) *Task {
	return launchTask(ctx, component, log, fn, true, opts)
}

// SignalResult bundles fn's typed return with the recovery outcome so the
// parent receives both on a single channel. Value is the zero value of T when
// Outcome.Panicked is true (fn never returned). Outcome is always non-nil.
type SignalResult[T any] struct {
	Outcome *panicdiag.RecoveryOutcome
	Value   T
}

// GoWithSignal launches fn under panic recovery and delivers fn's typed return
// alongside the recovery outcome on the returned channel exactly once, then
// closes the channel. Use it when the launcher needs to react synchronously
// to fn's completion inside a select alongside other channels, such as
// timeouts or parent cancellation, and wants both value and panic in-band.
//
// The channel is buffered (size 1) so the goroutine never blocks on send and
// closes after delivery so a "drain after timeout" pattern is safe. Recovery
// is non-fatal: panics are logged and reported to process-wide hooks like
// with Go, never re-raised. Use Go for fire-and-forget background helpers
// that don't return a value; use GoOrDie when the panic must be fatal.
//
// When fn panics, Value is the zero value of T and the caller should branch
// on result.Outcome.Panicked before reading result.Value.
func GoWithSignal[T any](ctx context.Context, component string, log *logger.Logger, fn func(context.Context) T, opts ...Option) <-chan SignalResult[T] {
	cfg := buildGoConfig(opts)
	ch := make(chan SignalResult[T], 1)
	go func() {
		defer close(ch)
		value, outcome := runRecoveredT(ctx, component, log, fn, cfg)
		ch <- SignalResult[T]{Value: value, Outcome: outcome}
	}()
	return ch
}

// launchTask starts the shared Go and GoOrDie goroutine.
func launchTask(ctx context.Context, component string, log *logger.Logger, fn func(context.Context), repanic bool, opts []Option) *Task {
	cfg := buildGoConfig(opts)
	task := &Task{done: make(chan struct{})}
	go func() {
		defer close(task.done)
		outcome := runRecovered(ctx, component, log, fn, cfg)
		task.outcome.Store(outcome)
		// Store before re-raise so observers can still see the outcome.
		if repanic && outcome.Panicked {
			panic(outcome.PanicValue)
		}
	}()
	return task
}

// buildGoConfig folds caller options into a goConfig.
func buildGoConfig(opts []Option) goConfig {
	cfg := goConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// runRecovered applies pkg/run recovery options and breadcrumbs.
func runRecovered(ctx context.Context, component string, log *logger.Logger, fn func(context.Context), cfg goConfig) *panicdiag.RecoveryOutcome {
	return panicdiag.WithRecovery(ctx, panicdiag.RecoveryOptions{
		Component:       component,
		Logger:          log,
		Counter:         cfg.counter,
		StateDumper:     cfg.stateDumper,
		ProcessMetadata: cfg.processMetadata,
		ArtifactRoot:    cfg.artifactRoot,
		StateLimitBytes: cfg.stateLimitBytes,
	}, cfg.reporter, func(ctxPtr *context.Context) {
		// Let recovery read breadcrumbs added by fn.
		*ctxPtr = panicdiag.WithMutableBreadcrumbs(*ctxPtr)
		fn(*ctxPtr)
	})
}

// runRecoveredT is the generic counterpart to runRecovered: it captures fn's
// typed return value via closure write so callers (GoWithSignal) can deliver
// it alongside the outcome. If fn panics before completing, value remains the
// zero value of T and the caller should branch on outcome.Panicked first.
func runRecoveredT[T any](ctx context.Context, component string, log *logger.Logger, fn func(context.Context) T, cfg goConfig) (T, *panicdiag.RecoveryOutcome) {
	var value T
	outcome := panicdiag.WithRecovery(ctx, panicdiag.RecoveryOptions{
		Component:       component,
		Logger:          log,
		Counter:         cfg.counter,
		StateDumper:     cfg.stateDumper,
		ProcessMetadata: cfg.processMetadata,
		ArtifactRoot:    cfg.artifactRoot,
		StateLimitBytes: cfg.stateLimitBytes,
	}, cfg.reporter, func(ctxPtr *context.Context) {
		*ctxPtr = panicdiag.WithMutableBreadcrumbs(*ctxPtr)
		value = fn(*ctxPtr)
	})
	return value, outcome
}
