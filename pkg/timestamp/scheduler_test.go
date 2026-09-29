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

// TestScheduler_LongRunningActionTimeoutIsNotAbandoned pins the fix for
// apache/skywalking#14111: crossing task.run()'s 5-minute soft timeout must
// not abandon (or cancel) a still-running action. Before the fix, the
// timeoutCh branch logged "action timed out" at Error, bumped
// TotalTasksTimeout, and returned as if the action had finished -- letting
// the next scheduled tick start while the real action kept running
// detached, its outcome discarded.
//
// Drives a single task through a MockClock end-to-end via Scheduler.Trigger:
// Register gives a mock-clock-backed Scheduler's task its own independent
// MockClock, synced only by Trigger's `c.Set(s.clock.Now())` (see
// scheduler.go's Register/Trigger). Advancing the outer clock and calling
// Trigger again propagates to the task's clock, letting the test cross the
// hard-coded 5-minute timer deterministically without a real 5-minute wait.
func TestScheduler_LongRunningActionTimeoutIsNotAbandoned(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	log := logger.GetLogger("test")

	mc := NewMockClock()
	t0 := time.Now()
	mc.Set(t0)

	s := NewScheduler(log, mc)
	defer s.Close()

	actionEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	actionCtxErr := make(chan error, 1)

	err := s.Register(context.Background(), "long-running", cron.Descriptor, "@every 1h",
		func(ctx context.Context, _ time.Time, _ *logger.Logger) bool {
			select {
			case actionEntered <- struct{}{}:
			default:
			}
			// A later tick also unblocks immediately once release is closed,
			// so its report must never block waiting for a reader: only the
			// first invocation's outcome is read by this test.
			reportCtxErr := func(err error) {
				select {
				case actionCtxErr <- err:
				default:
				}
			}
			select {
			case <-release:
				reportCtxErr(ctx.Err())
				return true
			case <-ctx.Done():
				reportCtxErr(ctx.Err())
				return false
			}
		})
	require.NoError(t, err)

	metrics := s.Metrics()["long-running"]
	require.NotNil(t, metrics)

	// Fire the first scheduled tick.
	mc.Add(time.Hour)
	require.True(t, s.Trigger("long-running"))

	select {
	case <-actionEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("action did not enter within 2s of the first Trigger")
	}

	// Cross the 5-minute soft deadline while the action is still running.
	mc.Add(6 * time.Minute)
	require.True(t, s.Trigger("long-running"))

	require.Eventually(t, func() bool {
		return metrics.TotalTasksTimeout.Load() == 1
	}, 2*time.Second, 10*time.Millisecond,
		"TotalTasksTimeout must increment once the soft deadline is crossed")

	// The task must still be registered (not abandoned/stopped).
	_, _, exist := s.Interval("long-running")
	require.True(t, exist, "the task must remain registered after crossing the soft timeout")

	// Its context must still be alive (not canceled) while the action keeps
	// running: if it had been canceled, the action would immediately take
	// the ctx.Done() branch and report here.
	select {
	case err := <-actionCtxErr:
		t.Fatalf("action must not observe ctx cancellation on a soft timeout, got: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// The core regression: advance well past when a second scheduled tick
	// would be due and Trigger again, all while the first action is still
	// blocked (release has not been closed yet). Before the fix, crossing
	// the soft deadline made the scheduler treat the action as finished and
	// loop straight into arming (and firing) the next tick, invoking the
	// SchedulerAction a second time -- concurrently with the still-running
	// first invocation -- and sending a second value on actionEntered. The
	// fix keeps the scheduler waiting on the first invocation's real
	// outcome, so no second tick can start yet.
	mc.Add(3 * time.Hour)
	require.True(t, s.Trigger("long-running"))

	select {
	case <-actionEntered:
		t.Fatal("a second tick must not start while the first action is still running past the soft timeout")
	case <-time.After(300 * time.Millisecond):
	}

	// Let the action finish and observe its real outcome.
	close(release)

	select {
	case err := <-actionCtxErr:
		require.NoError(t, err, "ctx must never have been canceled by the soft timeout")
	case <-time.After(2 * time.Second):
		t.Fatal("action did not report its outcome within 2s of being released")
	}

	require.Eventually(t, func() bool {
		return metrics.TotalTasksFinished.Load() == 1
	}, 2*time.Second, 10*time.Millisecond,
		"TotalTasksFinished must increment once the action actually completes")

	// The task keeps scheduling normally: a later tick still fires.
	mc.Add(2 * time.Hour)
	require.True(t, s.Trigger("long-running"))

	select {
	case <-actionEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the task stopped scheduling after the timed-out action completed")
	}
}

// TestScheduler_CloseIsBoundedWhenActionIgnoresCtx pins that waiting for an
// action's real outcome does not make shutdown unbounded: Close cancels the
// action's ctx and then waits at most the soft timeout for an action that
// ignores it.
func TestScheduler_CloseIsBoundedWhenActionIgnoresCtx(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	log := logger.GetLogger("test")

	mc := NewMockClock()
	mc.Set(time.Now())
	s := NewScheduler(log, mc)

	actionEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	err := s.Register(context.Background(), "ignores-ctx", cron.Descriptor, "@every 1h",
		func(_ context.Context, _ time.Time, _ *logger.Logger) bool {
			select {
			case actionEntered <- struct{}{}:
			default:
			}
			<-release
			return true
		})
	require.NoError(t, err)

	s.RLock()
	taskClock := s.tasks["ignores-ctx"].clock.(MockClock)
	s.RUnlock()

	mc.Add(time.Hour)
	require.True(t, s.Trigger("ignores-ctx"))
	select {
	case <-actionEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("action did not enter within 2s of the first Trigger")
	}

	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close must give the action a chance to observe cancellation")
	case <-time.After(300 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		taskClock.Add(time.Minute)
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond, "Close must return once the shutdown grace passes")
}
