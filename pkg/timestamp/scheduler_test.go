// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package timestamp

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const everySecondCron = cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow

// TestScheduler_CloseCancelsActionContext pins the bug fix described in
// pkg/timestamp/scheduler.go: SchedulerAction's ctx must be canceled
// when the scheduler is closed. Before the fix, task.close only signaled
// t.closer.CloseNotify(), which the run() loop watches between
// invocations — but an in-flight action observing ctx.Done() never saw
// shutdown, so well-behaved actions stalled the close path until the
// 5-minute timeoutCh fired.
//
// Uses a real clock with an every-second cron so the action fires within
// ~1s without needing per-task mock-clock plumbing.
func TestScheduler_CloseCancelsActionContext(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	log := logger.GetLogger("test")

	s := NewScheduler(log, NewClock())

	actionEntered := make(chan struct{}, 1)
	actionExited := make(chan error, 1)

	err := s.Register(context.Background(), "cancellation-probe", everySecondCron,
		"* * * * * *",
		func(ctx context.Context, _ time.Time, _ *logger.Logger) bool {
			select {
			case actionEntered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			actionExited <- ctx.Err()
			return true
		})
	require.NoError(t, err)

	// Wait for the action to enter (real clock fires every second).
	select {
	case <-actionEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("action did not enter within 3s — real-clock cron tick likely missed")
	}

	closeDone := make(chan struct{})
	go func() {
		s.Close()
		close(closeDone)
	}()

	// The action must observe cancellation promptly (well before the
	// 5-minute timeoutCh in run()) and Close must return cleanly.
	select {
	case err := <-actionExited:
		require.ErrorIs(t, err, context.Canceled,
			"action ctx must surface Canceled when scheduler closes")
	case <-time.After(2 * time.Second):
		t.Fatal("SchedulerAction did not observe ctx cancellation within 2s of Scheduler.Close()")
	}

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Scheduler.Close() did not return within 2s after action exited")
	}
}

// TestScheduler_ParentCtxCancelStillPropagates guards that switching the
// action's ctx from t.parentCtx to t.taskCtx (a child via WithCancel)
// did not regress the original behavior: cancellation of the caller's
// parent context must still reach the action.
func TestScheduler_ParentCtxCancelStillPropagates(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	log := logger.GetLogger("test")

	s := NewScheduler(log, NewClock())
	defer s.Close()

	parentCtx, cancelParent := context.WithCancel(context.Background())

	actionEntered := make(chan struct{}, 1)
	actionExited := make(chan error, 1)

	err := s.Register(parentCtx, "parent-cancel-probe", everySecondCron,
		"* * * * * *",
		func(ctx context.Context, _ time.Time, _ *logger.Logger) bool {
			select {
			case actionEntered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			actionExited <- ctx.Err()
			return true
		})
	require.NoError(t, err)

	select {
	case <-actionEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("action did not enter within 3s — real-clock cron tick likely missed")
	}

	cancelParent()

	select {
	case err := <-actionExited:
		require.ErrorIs(t, err, context.Canceled,
			"action ctx must surface Canceled when parent ctx is canceled")
	case <-time.After(2 * time.Second):
		t.Fatal("SchedulerAction did not observe parent ctx cancellation within 2s")
	}
}
