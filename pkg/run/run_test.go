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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type stopPanicService struct {
	stopCalled atomic.Bool
}

func (s *stopPanicService) Name() string      { return "stop-panic" }
func (s *stopPanicService) Serve() StopNotify { return nil }
func (s *stopPanicService) GracefulStop() {
	s.stopCalled.Store(true)
	panic("stop-boom")
}

type cleanStopService struct {
	stopCalled atomic.Bool
}

func (c *cleanStopService) Name() string      { return "clean-stop" }
func (c *cleanStopService) Serve() StopNotify { return nil }
func (c *cleanStopService) GracefulStop()     { c.stopCalled.Store(true) }

// TestGracefulStopWithRecovery_RecoversPanic pins that a service whose
// GracefulStop panics during teardown does not crash the process: the panic
// is captured by panicdiag.WithRecovery, an artifact is written, and control
// returns normally so peer services scheduled after this one can still get
// their own GracefulStop calls.
func TestGracefulStopWithRecovery_RecoversPanic(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	log := logger.GetLogger("test")

	panicker := &stopPanicService{}
	require.NotPanics(t, func() {
		gracefulStopWithRecovery(context.Background(), log, panicker)
	}, "gracefulStopWithRecovery must absorb panics from GracefulStop so the teardown loop continues")
	require.True(t, panicker.stopCalled.Load(), "GracefulStop must have been invoked")
}

// TestGracefulStopWithRecovery_NoPanicPath pins that a clean GracefulStop
// runs through the wrapper without altering its observable behavior.
func TestGracefulStopWithRecovery_NoPanicPath(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	log := logger.GetLogger("test")

	clean := &cleanStopService{}
	require.NotPanics(t, func() {
		gracefulStopWithRecovery(context.Background(), log, clean)
	})
	require.True(t, clean.stopCalled.Load())
}

type panicPreRunner struct {
	called atomic.Bool
}

func (p *panicPreRunner) Name() string { return "prerun-panic" }
func (p *panicPreRunner) PreRun(_ context.Context) error {
	p.called.Store(true)
	panic("prerun-boom")
}

type errorPreRunner struct {
	err    error
	called atomic.Bool
}

func (e *errorPreRunner) Name() string { return "prerun-error" }
func (e *errorPreRunner) PreRun(_ context.Context) error {
	e.called.Store(true)
	return e.err
}

type cleanPreRunner struct {
	called atomic.Bool
}

func (c *cleanPreRunner) Name() string { return "prerun-clean" }
func (c *cleanPreRunner) PreRun(_ context.Context) error {
	c.called.Store(true)
	return nil
}

// TestPreRunWithRecovery_RecoversPanic pins panic-to-error behavior.
func TestPreRunWithRecovery_RecoversPanic(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	log := logger.GetLogger("test")

	panicker := &panicPreRunner{}
	var err error
	require.NotPanics(t, func() {
		err = preRunWithRecovery(context.Background(), log, panicker)
	}, "preRunWithRecovery must absorb panics from PreRun so Group.Run can return a structured error")
	require.True(t, panicker.called.Load(), "PreRun must have been invoked")
	require.Error(t, err, "panic must be reported as an error")
	require.Contains(t, err.Error(), "prerun-boom", "error must carry the recovered panic value")
}

// TestPreRunWithRecovery_PassesThroughError pins normal error passthrough.
func TestPreRunWithRecovery_PassesThroughError(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	log := logger.GetLogger("test")

	wantErr := errors.New("prerun-failed")
	preRunner := &errorPreRunner{err: wantErr}
	got := preRunWithRecovery(context.Background(), log, preRunner)
	require.True(t, preRunner.called.Load())
	require.ErrorIs(t, got, wantErr,
		"non-panic errors must surface unchanged so callers can match on them")
}

// TestPreRunWithRecovery_NoPanicCleanPath pins the clean path.
func TestPreRunWithRecovery_NoPanicCleanPath(t *testing.T) {
	t.Helper()

	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "debug"}))
	log := logger.GetLogger("test")

	clean := &cleanPreRunner{}
	require.NoError(t, preRunWithRecovery(context.Background(), log, clean))
	require.True(t, clean.called.Load())
}
