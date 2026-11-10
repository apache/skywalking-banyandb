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

package panicdiag

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// WithRecovery executes fn and recovers panics with diagnostics. fn may update
// the context pointer before recovery reads it. The outcome is always non-nil.
func WithRecovery(ctx context.Context, opts RecoveryOptions, reporter Reporter, fn func(*context.Context)) (outcome *RecoveryOutcome) {
	outcome = &RecoveryOutcome{}
	if ctx == nil {
		//nolint:contextcheck // nil caller context has no parent to inherit from; background is the safe root fallback
		ctx = context.Background()
	}
	if fn == nil {
		return outcome
	}

	log := opts.Logger
	if log == nil {
		log = logger.GetLogger("panicdiag")
	}

	defer func() {
		// Second-tier net: the recovery defer body runs user-provided hooks
		// (Reporter, AbortFunc, StateDumper, Counter) and panicdiag-internal
		// code (artifact writer, logger). User hooks are isolated below via
		// safeCall, but a panic in panicdiag's own code or in a code path we
		// have not yet wrapped must not escape as a fresh panic; that would
		// destroy the original recovered panic value, leave outcome partially
		// populated, and skip whatever ran afterward. Fall back to stderr
		// because the regular logger may itself have been the source.
		defer func() {
			if rec := recover(); rec != nil {
				fmt.Fprintf(os.Stderr,
					"panicdiag: panic in recovery defer (suppressed): %v\n%s\n",
					rec, debug.Stack())
			}
		}()

		panicValue := recover()
		if panicValue == nil {
			return
		}
		result, artifactDone := emitRecovery(ctx, log, opts, reporter, panicValue, debug.Stack())
		outcome.Panicked = true
		outcome.PanicValue = panicValue
		outcome.Result = result
		outcome.ArtifactDone = artifactDone
	}()

	fn(&ctx)
	return
}

// emitRecovery fires every signal panicdiag owes a recovered panic: the panic counter,
// the structured log, the reporters, the abort hook and the artifact. It is shared by
// WithRecovery and by RecoverExternal so a panic recovered elsewhere (a gRPC recovery
// interceptor, for example) is observed identically to one recovered here. stack is the
// caller's captured goroutine stack: callers recover during the panic's unwind and so
// still hold the stack of the panic site, which is worth far more than a synthetic one
// captured after the fact.
func emitRecovery(ctx context.Context, log *logger.Logger, opts RecoveryOptions, reporter Reporter,
	panicValue any, stack []byte,
) (RecoveryResult, chan struct{}) {
	record := &PanicRecord{
		OccurredAt:      time.Now().UTC(),
		Component:       opts.Component,
		PanicValue:      fmt.Sprint(panicValue),
		Recovered:       true,
		GoroutineStack:  string(stack),
		Breadcrumbs:     BreadcrumbsFromContext(ctx),
		ProcessMetadata: cloneStringMap(opts.ProcessMetadata),
	}

	incPanicCounter(log, opts.Counter, opts.Component)

	artifactRoot := opts.ArtifactRoot
	if artifactRoot == "" {
		artifactRoot = DefaultArtifactRoot()
	}

	var (
		stateDump    any
		stateErr     error
		hasStateDump bool
	)
	if opts.StateDumper != nil && artifactRoot != "" {
		hasStateDump = true
		safeCall(log, "state-dumper", func() {
			stateDump, stateErr = opts.StateDumper.DumpState(ctx)
		})
	}

	var artifactDir string
	if artifactRoot != "" {
		artifactDir = NewArtifactWriter(artifactRoot).ArtifactDirPath(record)
	}

	stages := make([]string, len(record.Breadcrumbs))
	for idx, bc := range record.Breadcrumbs {
		stages[idx] = bc.Stage
	}
	log.Error().
		Str("component", opts.Component).
		Str("panic", record.PanicValue).
		Strs("breadcrumbs", stages).
		Str("stack", record.GoroutineStack).
		Str("artifact_dir", artifactDir).
		Msg("recovered panic")

	recoveryResult := RecoveryResult{
		Record:      record,
		ArtifactDir: artifactDir,
	}
	callReporter(ctx, log, reporter, recoveryResult)
	callDefaultAbort(ctx, log, recoveryResult)

	// Signal artifact completion for callers such as GoOrDie.
	artifactDone := make(chan struct{})

	if artifactRoot == "" {
		// No artifact will be written.
		close(artifactDone)
		return recoveryResult, artifactDone
	}
	if sink := defaultArtifactSinkPtr.Load(); sink != nil {
		// Clone so the async worker never mutates caller-visible state.
		sinkRecord := *record
		sinkRecord.StateDump = nil
		sinkJob := ArtifactJob{
			Record:       &sinkRecord,
			RootDir:      artifactRoot,
			StateDump:    stateDump,
			StateErr:     stateErr,
			StateLimit:   opts.StateLimitBytes,
			Logger:       log,
			HasStateDump: hasStateDump,
			Done:         artifactDone,
		}
		if !sink.Submit(sinkJob) {
			log.Warn().Str("component", opts.Component).
				Int("queue_depth", sink.QueueDepth()).
				Msg("artifact sink saturated; dropping artifact")
			// Submit did not take ownership of Done.
			close(artifactDone)
		}
		return recoveryResult, artifactDone
	}
	// Sync fallback preserves caller-visible StateDump status.
	writeArtifactSync(ArtifactJob{
		Record:       record,
		RootDir:      artifactRoot,
		StateDump:    stateDump,
		StateErr:     stateErr,
		StateLimit:   opts.StateLimitBytes,
		Logger:       log,
		HasStateDump: hasStateDump,
		Done:         artifactDone,
	})
	return recoveryResult, artifactDone
}

// RecoverExternal fires panicdiag's standard signals for a panic that the caller has
// already recovered itself, so recovery paths outside WithRecovery -- notably the gRPC
// servers' recovery interceptors -- are not invisible to the panic counter, the crash
// reporters and the artifact writer. Call it from inside the recovering defer (or a
// handler it invokes) so stack still describes the panic site. It never panics: every
// hook it runs is isolated, and a failure inside panicdiag itself falls back to stderr.
//
//nolint:contextcheck // a nil caller context has no parent to inherit from; background is the safe root fallback
func RecoverExternal(ctx context.Context, opts RecoveryOptions, reporter Reporter, panicValue any, stack []byte) (result RecoveryResult) {
	if panicValue == nil {
		return result
	}
	if ctx == nil {
		ctx = context.Background()
	}
	log := opts.Logger
	if log == nil {
		log = logger.GetLogger("panicdiag")
	}
	defer func() {
		// Mirrors WithRecovery's second-tier net: a panic raised while emitting the
		// signals must never escape into the caller's own recovery path.
		if rec := recover(); rec != nil {
			fmt.Fprintf(os.Stderr, "panicdiag: panic in external recovery (suppressed): %v\n%s\n", rec, debug.Stack())
		}
	}()
	if len(stack) == 0 {
		stack = debug.Stack()
	}
	result, _ = emitRecovery(ctx, log, opts, reporter, panicValue, stack)
	return result
}

// GoWithRecovery starts fn in a goroutine protected by WithRecovery. Use
// pkg/run.Go when the launcher needs a Task outcome.
func GoWithRecovery(ctx context.Context, opts RecoveryOptions, reporter Reporter, fn func(*context.Context)) {
	go WithRecovery(ctx, opts, reporter, fn)
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}
