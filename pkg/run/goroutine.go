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

// Task tracks a goroutine launched by Go.
//
// Done closes when the goroutine exits. Outcome is nil until then, then holds
// the recovery result. WithRepanic(true) still closes Done, but may leave
// Outcome nil because the panic is re-raised before it can be stored.
type Task struct {
	outcome atomic.Pointer[panicdiag.RecoveryOutcome]
	done    chan struct{}
}

// Done returns a channel closed when the goroutine launched by Go exits.
func (t *Task) Done() <-chan struct{} {
	return t.done
}

// Outcome returns the panic-recovery outcome, or nil if the goroutine has
// not yet exited (or re-raised a panic via WithRepanic).
func (t *Task) Outcome() *panicdiag.RecoveryOutcome {
	return t.outcome.Load()
}

// Wait blocks until the goroutine exits and returns the outcome.
func (t *Task) Wait() *panicdiag.RecoveryOutcome {
	<-t.done
	return t.outcome.Load()
}

// goConfig holds the resolved options for a Go call. All fields are optional
// and, except where noted, fall back to panicdiag's process-wide defaults.
type goConfig struct {
	reporter        panicdiag.Reporter
	stateDumper     panicdiag.StateDumper
	counter         meter.Counter
	processMetadata map[string]string
	artifactRoot    string
	stateLimitBytes int64
	repanic         bool
}

// Option configures a single Go call. Options compose with panicdiag defaults.
type Option func(*goConfig)

// WithReporter installs a non-blocking per-call reporter for recovered panics.
func WithReporter(r panicdiag.Reporter) Option {
	return func(c *goConfig) { c.reporter = r }
}

// WithRepanic re-raises recovered panics after diagnostics. Default is false.
func WithRepanic(repanic bool) Option {
	return func(c *goConfig) { c.repanic = repanic }
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

// Go launches fn in a panic-recovered goroutine.
//
// Recovered panics are logged with diagnostics, reported to process-wide
// panicdiag hooks, and exposed through the returned Task. A per-call reporter
// composes with the process-wide default; callers that need to react to a
// panic should consume Task.Wait or Task.Done.
//
// WithRepanic(true) restores process-fatal behavior after diagnostics. Panic
// counters configured by option or default are incremented with component.
func Go(ctx context.Context, component string, log *logger.Logger, fn func(context.Context), opts ...Option) *Task {
	cfg := goConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	task := &Task{done: make(chan struct{})}
	go func() {
		defer close(task.done)
		outcome := panicdiag.WithRecovery(ctx, panicdiag.RecoveryOptions{
			Component:       component,
			Logger:          log,
			Counter:         cfg.counter,
			StateDumper:     cfg.stateDumper,
			ProcessMetadata: cfg.processMetadata,
			ArtifactRoot:    cfg.artifactRoot,
			StateLimitBytes: cfg.stateLimitBytes,
			Repanic:         cfg.repanic,
		}, cfg.reporter, func(ctxPtr *context.Context) {
			// Install a mutable breadcrumb store so breadcrumbs added inside
			// fn are visible to the recovery defer (which reads *ctxPtr after
			// fn returns or panics).
			*ctxPtr = panicdiag.WithMutableBreadcrumbs(*ctxPtr)
			fn(*ctxPtr)
		})
		task.outcome.Store(outcome)
	}()
	return task
}
