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
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// resetSupervisorForTest clears the package-level supervisor state and the
// panicdiag default abort hook so tests can drive initSupervisor in isolation.
// Tests in this package do not run in parallel, so a simple reset is enough.
func resetSupervisorForTest(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		supervisorOnce = sync.Once{}
		supervisorCtx = nil
		supervisorCancel = nil
		panicdiag.SetDefaultAbortFunc(nil)
	})
	supervisorOnce = sync.Once{}
	supervisorCtx = nil
	supervisorCancel = nil
	panicdiag.SetDefaultAbortFunc(nil)
}

func TestSupervisorContextBeforeInitIsBackground(t *testing.T) {
	resetSupervisorForTest(t)

	ctx := SupervisorContext()
	select {
	case <-ctx.Done():
		t.Fatal("expected uninitialized supervisor context to behave like context.Background")
	default:
	}
}

func TestInitSupervisorCancelsOnRecoveredPanic(t *testing.T) {
	resetSupervisorForTest(t)

	initSupervisor()
	ctx := SupervisorContext()
	if ctx == context.Background() {
		t.Fatal("expected initSupervisor to install a real cancellable context")
	}
	select {
	case <-ctx.Done():
		t.Fatal("supervisor context should not be done before any panic")
	default:
	}

	// A goroutine recovered by panicdiag.WithRecovery should cancel the
	// supervising context via the registered default abort.
	panicdiag.WithRecovery(context.Background(), panicdiag.RecoveryOptions{
		Component: "supervisor-test",
	}, nil, func(_ *context.Context) {
		panic("supervisor-boom")
	})

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected supervisor context to be canceled after a recovered panic")
	}
}

func TestInitSupervisorIsIdempotent(t *testing.T) {
	resetSupervisorForTest(t)

	initSupervisor()
	first := SupervisorContext()
	initSupervisor()
	second := SupervisorContext()
	if first != second {
		t.Fatal("initSupervisor must be idempotent: SupervisorContext should return the same context on repeated init")
	}
}
