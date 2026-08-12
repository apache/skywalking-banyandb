// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRecordOutcomeBackoffProgression asserts the exponential schedule doubles from
// mergeBackoffBase up to mergeBackoffCap, and that a success clears it immediately.
func TestRecordOutcomeBackoffProgression(t *testing.T) {
	mc := newMergeLoopControl()
	fixedNow := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	mc.nowFunc = func() time.Time { return fixedNow }

	expected := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second,
		60 * time.Second, 60 * time.Second,
	}
	for i, want := range expected {
		mc.recordOutcome(false)
		got := mc.backoffUntil.Sub(fixedNow)
		require.Equalf(t, want, got, "failure %d: expected backoff delay %s, got %s", i+1, want, got)
	}
	require.Equal(t, len(expected), mc.consecutiveFailures)

	remaining := mc.backoffRemaining()
	require.Greater(t, remaining, time.Duration(0), "backoff should still be active before a success is recorded")

	mc.recordOutcome(true)
	require.Zero(t, mc.consecutiveFailures)
	require.True(t, mc.backoffUntil.IsZero())
	require.Zero(t, mc.backoffRemaining())
}

// TestBackoffWaveBypass asserts wave mode bypasses an active backoff, and that the
// backoff reasserts itself once the wave releases.
func TestBackoffWaveBypass(t *testing.T) {
	mc := newMergeLoopControl()
	fixedNow := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	mc.nowFunc = func() time.Time { return fixedNow }

	mc.recordOutcome(false)
	mc.recordOutcome(false)
	require.Greater(t, mc.backoffRemaining(), time.Duration(0))

	mc.startWave(0, 0)
	require.Zero(t, mc.backoffRemaining(), "wave mode must bypass an active backoff")

	mc.releaseWave()
	require.Greater(t, mc.backoffRemaining(), time.Duration(0), "backoff resumes once the wave releases")
}

// TestDispatcherHonorsBackoff drives mergeControl through two failing dispatch cycles the
// way dispatcherLoop and mergeLaneWorker do (beginDispatch/endDispatch bracketing a
// recordOutcome(false)) and asserts the control state — rather than wall-clock timing —
// reflects an active, growing backoff that a subsequent trigger must honor.
func TestDispatcherHonorsBackoff(t *testing.T) {
	mc := newMergeLoopControl()
	fixedNow := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	mc.nowFunc = func() time.Time { return fixedNow }

	require.NoError(t, mc.enqueue(nil))
	<-mc.trigger
	dispatch, _ := mc.beginDispatch()
	require.True(t, dispatch)
	mc.recordOutcome(false)
	mc.endDispatch()
	require.Equal(t, 1, mc.consecutiveFailures)
	require.Equal(t, time.Second, mc.backoffUntil.Sub(fixedNow))

	require.NoError(t, mc.enqueue(nil))
	<-mc.trigger
	dispatch, _ = mc.beginDispatch()
	require.True(t, dispatch)
	mc.recordOutcome(false)
	mc.endDispatch()
	require.Equal(t, 2, mc.consecutiveFailures)
	require.Equal(t, 2*time.Second, mc.backoffUntil.Sub(fixedNow))
	require.Greater(t, mc.backoffRemaining(), time.Duration(0))
}
