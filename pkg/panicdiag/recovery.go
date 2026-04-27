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
	"runtime/debug"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// WithRecovery executes fn and recovers any panic with structured diagnostics.
// fn receives a pointer to the active context so it can enrich it with
// breadcrumbs; the recovery defer reads through the pointer and therefore
// captures every marker added during fn's execution.
func WithRecovery(ctx context.Context, opts RecoveryOptions, reporter Reporter, fn func(*context.Context)) {
	if ctx == nil {
		//nolint:contextcheck // nil caller context has no parent to inherit from; background is the safe root fallback
		ctx = context.Background()
	}
	if fn == nil {
		return
	}

	log := opts.Logger
	if log == nil {
		log = logger.GetLogger("panicdiag")
	}

	defer func() {
		panicValue := recover()
		if panicValue == nil {
			return
		}

		record := &PanicRecord{
			OccurredAt:      time.Now().UTC(),
			Component:       opts.Component,
			PanicValue:      fmt.Sprint(panicValue),
			Recovered:       true,
			GoroutineStack:  string(debug.Stack()),
			Breadcrumbs:     BreadcrumbsFromContext(ctx),
			ProcessMetadata: cloneStringMap(opts.ProcessMetadata),
		}

		incPanicCounter(opts.Counter, opts.Component)

		artifactRoot := opts.ArtifactRoot
		if artifactRoot == "" {
			artifactRoot = DefaultArtifactRoot()
		}
		var artifactDir string
		artifactWriter := NewArtifactWriter(artifactRoot)
		if artifactRoot != "" {
			writtenDir, writeErr := artifactWriter.Write(record)
			if writeErr != nil {
				log.Error().Err(writeErr).Str("component", opts.Component).Msg("failed to write panic artifacts")
			} else {
				artifactDir = writtenDir
				if opts.StateDumper != nil {
					stateDump, dumpErr := opts.StateDumper.DumpState(ctx)
					if dumpErr != nil {
						record.StateDump = &StateDumpStatus{
							Error: dumpErr.Error(),
						}
					} else {
						truncated, dumpPath, writeDumpErr := artifactWriter.WriteStateDump(artifactDir, stateDump, opts.StateLimitBytes)
						dumpStatus := &StateDumpStatus{Truncated: truncated}
						if writeDumpErr != nil {
							dumpStatus.Error = writeDumpErr.Error()
						} else {
							dumpStatus.Path = dumpPath
						}
						record.StateDump = dumpStatus
					}
				}
			}
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

		callReporter(ctx, reporter, RecoveryResult{
			Record:      record,
			ArtifactDir: artifactDir,
		})
	}()

	fn(&ctx)
}

// GoWithRecovery starts fn in a goroutine protected by WithRecovery.
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
