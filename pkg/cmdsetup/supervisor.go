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

package cmdsetup

import (
	"context"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// supervisor owns the process-wide panic cancel context and artifact sink.
var (
	supervisorOnce   sync.Once
	supervisorCtx    context.Context
	supervisorCancel context.CancelFunc
	supervisorSink   *panicdiag.ArtifactSink
)

// supervisorShutdownDeadline bounds artifact sink drain during shutdown.
const supervisorShutdownDeadline = 10 * time.Second

// initSupervisor installs the process-wide panic cancel hook and sink.
func initSupervisor() {
	supervisorOnce.Do(func() {
		supervisorCtx, supervisorCancel = context.WithCancel(context.Background())
		panicdiag.SetDefaultAbortFunc(func(_ context.Context, result panicdiag.RecoveryResult) {
			component := ""
			panicValue := ""
			if result.Record != nil {
				component = result.Record.Component
				panicValue = result.Record.PanicValue
			}
			logger.GetLogger("supervisor").Error().
				Str("component", component).
				Str("panic", panicValue).
				Msg("canceling supervising context due to recovered panic")
			supervisorCancel()
		})

		supervisorSink = panicdiag.NewArtifactSink(0)
		supervisorSink.Start()
		panicdiag.SetDefaultArtifactSink(supervisorSink)
	})
}

// SupervisorContext returns the process-wide supervising context. It is
// canceled when any panicdiag-protected goroutine recovers a panic, allowing
// long-running work elsewhere in the process to wind down gracefully.
// The context is initialized by NewRoot's PersistentPreRunE; callers that
// fetch it before initialization receive a never-canceled context.Background.
func SupervisorContext() context.Context {
	if supervisorCtx == nil {
		return context.Background()
	}
	return supervisorCtx
}

// ShutdownSupervisor drains and unregisters the process-wide artifact sink.
func ShutdownSupervisor() {
	if supervisorSink == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), supervisorShutdownDeadline)
	defer cancel()
	if err := supervisorSink.Close(ctx); err != nil {
		logger.GetLogger("supervisor").Warn().
			Err(err).
			Dur("deadline", supervisorShutdownDeadline).
			Msg("artifact sink did not drain before shutdown deadline; pending artifacts dropped")
	}
	panicdiag.SetDefaultArtifactSink(nil)
}
