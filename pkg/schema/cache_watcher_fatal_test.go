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

package schema

import (
	stderrors "errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/initerror"
	pkglogger "github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// nopCounter is a meter.Counter that discards all increments. Used by the
// fatal-fail subprocess tests so the worker's deferred Inc(1) does not
// nil-panic in the child process.
type nopCounter struct{}

func (nopCounter) Inc(_ float64, _ ...string) {}
func (nopCounter) Delete(_ ...string) bool    { return false }

var _ meter.Counter = nopCounter{}

const (
	// fatalChildEnv signals to the test binary that it is the in-process
	// child for a subprocess fast-fail assertion. Parents re-exec the
	// running test binary with this env set; only the matching child
	// branch executes — the parent does not run the child code path.
	fatalChildEnv          = "BANYAND_TEST_F5_FATAL_CHILD"
	fatalChildModeWatcher  = "watcher"
	fatalChildModeSendSync = "send-sync"
)

// permanentPanicSupplier is a ResourceSchemaSupplier whose OpenResource panics
// with an error-typed permanent error. It exercises the Watcher worker's
// recover() classifier (Step 1) — the panic propagates up through processEvent
// and is caught in the worker's deferred recover(), where IsPermanent classifies
// it as fatal.
type permanentPanicSupplier struct{}

func (permanentPanicSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (permanentPanicSupplier) OpenResource(_ Resource) (IndexListener, error) {
	panic(initerror.AsPermanent(stderrors.New("incompatible version 1.3.0")))
}

// permanentErrorSupplier is a ResourceSchemaSupplier whose OpenResource returns
// a permanent error. It exercises the synchronous-return classifier in
// SendMetadataEvent (Step 2b).
type permanentErrorSupplier struct{}

func (permanentErrorSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (permanentErrorSupplier) OpenResource(_ Resource) (IndexListener, error) {
	return nil, initerror.AsPermanent(stderrors.New("incompatible version 1.3.0"))
}

// transientThenOKSupplier returns a non-permanent error on the first call and
// nil on the second. It is used to verify the transient-retry path stays
// functional after F5 (Test 5.6).
type transientThenOKSupplier struct {
	openCalls atomic.Int32
}

func (s *transientThenOKSupplier) ResourceSchema(_ *commonv1.Metadata) (ResourceSchema, error) {
	return nil, nil
}

func (s *transientThenOKSupplier) OpenResource(_ Resource) (IndexListener, error) {
	calls := s.openCalls.Add(1)
	if calls == 1 {
		return nil, stderrors.New("etcd hiccup")
	}
	return noopIndexListener{}, nil
}

// fatalChildLogger returns a logger whose .Fatal() output goes to stderr so
// the parent test can assert on the exit message via CombinedOutput.
func fatalChildLogger() *pkglogger.Logger {
	_ = pkglogger.Init(pkglogger.Logging{Env: "dev", Level: "info"})
	return pkglogger.GetLogger("test", "schema-cache-fatal")
}

// fatalChildMetrics returns a Metrics instance backed by no-op counters so
// the worker's deferred Inc(1) call does not nil-panic in the child process.
func fatalChildMetrics() *Metrics {
	return &Metrics{
		totalErrs:    nopCounter{},
		totalRetries: nopCounter{},
		totalPanics:  nopCounter{},
	}
}

func runFatalChild(t *testing.T, mode string) string {
	t.Helper()
	// #nosec G204 — os.Args[0] is the test binary path, set by the Go test
	// runner. The mode argument is one of two compile-time string constants
	// declared above.
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), fatalChildEnv+"="+mode)
	out, err := cmd.CombinedOutput()
	require.Error(t, err, "child must exit non-zero; output:\n%s", string(out))
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "child must produce *exec.ExitError; output:\n%s", string(out))
	require.NotZero(t, exitErr.ExitCode(), "child must exit with non-zero code; output:\n%s", string(out))
	return string(out)
}

// TestWatcherHitPermanentError_FailsFast verifies that the Watcher worker's
// recover() classifier escalates an error-typed permanent panic to .Fatal(),
// which exits the process non-zero (Step 1).
func TestWatcherHitPermanentError_FailsFast(t *testing.T) {
	if os.Getenv(fatalChildEnv) == fatalChildModeWatcher {
		runWatcherFatalChild()
		return
	}
	out := runFatalChild(t, fatalChildModeWatcher)
	if !strings.Contains(out, "Watcher hit a permanent error") {
		t.Fatalf("expected stderr to mention 'Watcher hit a permanent error', got:\n%s", out)
	}
}

// runWatcherFatalChild constructs a minimal schemaRepo whose OpenResource
// panics with a permanent error, starts the Watcher, and pushes an event
// through eventCh. The Watcher worker's recover() classifier should escalate
// to .Fatal() within a few hundred milliseconds.
func runWatcherFatalChild() {
	repo := &schemaRepo{
		l:                      fatalChildLogger(),
		resourceSchemaSupplier: permanentPanicSupplier{},
		eventCh:                make(chan MetadataEvent, 1),
		workerNum:              1,
		closer:                 run.NewChannelCloser(),
		metrics:                fatalChildMetrics(),
	}
	repo.Watcher()
	repo.eventCh <- MetadataEvent{
		Typ:  EventAddOrUpdate,
		Kind: EventKindResource,
		Metadata: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 1},
		},
	}
	// Hold the goroutine alive long enough for the worker's defer to run.
	// .Fatal() calls os.Exit(1), so this sleep should never complete.
	time.Sleep(5 * time.Second)
	fmt.Fprintln(os.Stderr, "child did not exit; classifier did not fire")
	os.Exit(2)
}

// TestSendMetadataEventHitPermanentError_FailsFast verifies that the
// synchronous SendMetadataEvent path escalates a permanent error returned
// from processEvent to .Fatal() (Step 2b).
func TestSendMetadataEventHitPermanentError_FailsFast(t *testing.T) {
	if os.Getenv(fatalChildEnv) == fatalChildModeSendSync {
		runSendSyncFatalChild()
		return
	}
	out := runFatalChild(t, fatalChildModeSendSync)
	if !strings.Contains(out, "SendMetadataEvent hit a permanent error") {
		t.Fatalf("expected stderr to mention 'SendMetadataEvent hit a permanent error', got:\n%s", out)
	}
}

// runSendSyncFatalChild calls SendMetadataEvent on a schemaRepo whose
// OpenResource returns a permanent error. The synchronous classifier branch
// at cache.go (Step 2b) should escalate to .Fatal() before SendMetadataEvent
// returns.
func runSendSyncFatalChild() {
	repo := &schemaRepo{
		l:                      fatalChildLogger(),
		resourceSchemaSupplier: permanentErrorSupplier{},
		eventCh:                make(chan MetadataEvent, 1),
		workerNum:              1,
		closer:                 run.NewChannelCloser(),
		metrics:                fatalChildMetrics(),
	}
	repo.SendMetadataEvent(MetadataEvent{
		Typ:  EventAddOrUpdate,
		Kind: EventKindResource,
		Metadata: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 1},
		},
	})
	fmt.Fprintln(os.Stderr, "child did not exit; classifier did not fire")
	os.Exit(2)
}

// TestSendMetadataEvent_TransientErrorRequeues verifies that a non-permanent
// error from processEvent is requeued via eventCh (the existing transient
// path) and not escalated to .Fatal() (Test 5.6 — preserves transient retry).
func TestSendMetadataEvent_TransientErrorRequeues(t *testing.T) {
	supplier := &transientThenOKSupplier{}
	repo := &schemaRepo{
		l:                      testLogger(),
		resourceSchemaSupplier: supplier,
		eventCh:                make(chan MetadataEvent, 1),
		workerNum:              1,
		closer:                 run.NewChannelCloser(),
		metrics:                fatalChildMetrics(),
	}

	evt := MetadataEvent{
		Typ:  EventAddOrUpdate,
		Kind: EventKindResource,
		Metadata: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 1},
		},
	}
	repo.SendMetadataEvent(evt)

	// First processEvent failed with a transient error; SendMetadataEvent should
	// have pushed onto eventCh for the worker (or self) to retry.
	select {
	case requeued := <-repo.eventCh:
		require.Equal(t, EventKindResource, requeued.Kind, "requeued event must be the original")
	case <-time.After(2 * time.Second):
		t.Fatalf("transient error must requeue the event onto eventCh; openCalls=%d", supplier.openCalls.Load())
	}
	require.EqualValues(t, 1, supplier.openCalls.Load(), "first SendMetadataEvent must call OpenResource exactly once")

	// Second pass: drive the requeued event through processEvent directly to
	// confirm OpenResource is reattempted and succeeds.
	require.NoError(t, repo.processEvent(evt))
	require.EqualValues(t, 2, supplier.openCalls.Load(), "second pass must reattempt OpenResource")
}
