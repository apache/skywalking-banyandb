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
func WithRecovery(ctx context.Context, opts RecoveryOptions, reporter Reporter, fn func(context.Context)) {
	if ctx == nil {
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

		var artifactDir string
		if opts.ArtifactRoot != "" {
			artifactWriter := NewArtifactWriter(opts.ArtifactRoot)
			writtenDir, writeErr := artifactWriter.Write(record)
			if writeErr != nil {
				log.Error().Err(writeErr).Str("component", opts.Component).Msg("failed to write panic artifacts")
			} else {
				artifactDir = writtenDir
			}
		}

		log.Error().
			Str("component", opts.Component).
			Str("panic", record.PanicValue).
			Str("artifact_dir", artifactDir).
			Msg("recovered panic")

		if reporter != nil {
			reporter(ctx, RecoveryResult{
				Record:      record,
				ArtifactDir: artifactDir,
			})
		}
	}()

	fn(ctx)
}

// GoWithRecovery starts fn in a goroutine protected by WithRecovery.
func GoWithRecovery(ctx context.Context, opts RecoveryOptions, reporter Reporter, fn func(context.Context)) {
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
