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

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// supervisor owns a process-wide cancellable context that is signalled when
// any goroutine wrapped by panicdiag.WithRecovery (or run.Go) recovers a
// panic. Subcommands derive their Group.Run contexts from supervisorCtx so
// that callers watching ctx.Done() can wind down gracefully when something
// crashes elsewhere in the process.
var (
	supervisorOnce   sync.Once
	supervisorCtx    context.Context
	supervisorCancel context.CancelFunc
)

// initSupervisor installs the process-wide cancellable supervising context and
// registers a panicdiag.SetDefaultAbortFunc that cancels it. This extends the
// lifecycle-failing default established for run.Group services to every
// goroutine wrapped by panicdiag, including those launched by run.Go that do
// not belong to any Group-managed Service. Idempotent: only the first call
// builds the context.
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
				Msg("cancelling supervising context due to recovered panic")
			supervisorCancel()
		})
	})
}

// SupervisorContext returns the process-wide supervising context. It is
// cancelled when any panicdiag-protected goroutine recovers a panic, allowing
// long-running work elsewhere in the process to wind down gracefully.
// The context is initialised by NewRoot's PersistentPreRunE; callers that
// fetch it before initialisation receive a never-cancelled context.Background.
func SupervisorContext() context.Context {
	if supervisorCtx == nil {
		return context.Background()
	}
	return supervisorCtx
}
