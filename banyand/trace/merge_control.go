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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var errMergeLoopUnavailable = errors.New("merge loop is unavailable")

type mergeLoopControl struct {
	changed            chan struct{}
	trigger            chan struct{}
	version            uint64
	workVersion        uint64
	emptyEpoch         uint64
	emptyWorkVersion   uint64
	waveMaxPartID      uint64
	queued             int
	running            int
	triggerPending     bool
	dispatcherActive   bool
	emptyEpochObserved bool
	stopped            bool
	waveMode           bool
	waveBlocked        bool
	waveDispatchLimit  int
	waveDispatches     int
	mu                 sync.Mutex
}

type mergeLoopState struct {
	changed            <-chan struct{}
	version            uint64
	workVersion        uint64
	emptyEpoch         uint64
	emptyWorkVersion   uint64
	queued             int
	running            int
	triggerPending     bool
	dispatcherActive   bool
	emptyEpochObserved bool
	stopped            bool
	waveMode           bool
	waveBlocked        bool
}

func newMergeLoopControl() *mergeLoopControl {
	return &mergeLoopControl{
		changed: make(chan struct{}),
		trigger: make(chan struct{}, 1),
	}
}

func (mc *mergeLoopControl) notifyLocked() {
	mc.version++
	close(mc.changed)
	mc.changed = make(chan struct{})
}

func (mc *mergeLoopControl) enqueue(closeCh <-chan struct{}) error {
	mc.mu.Lock()
	if mc.stopped {
		mc.mu.Unlock()
		return errMergeLoopUnavailable
	}
	if mc.triggerPending {
		mc.mu.Unlock()
		return nil
	}
	if mc.waveBlocked {
		mc.mu.Unlock()
		return nil
	}
	mc.triggerPending = true
	mc.notifyLocked()
	mc.mu.Unlock()

	select {
	case mc.trigger <- struct{}{}:
		return nil
	case <-closeCh:
		mc.mu.Lock()
		if mc.triggerPending {
			mc.triggerPending = false
			mc.notifyLocked()
		}
		mc.mu.Unlock()
		return errMergeLoopUnavailable
	}
}

func (mc *mergeLoopControl) beginDispatch() (bool, uint64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.triggerPending = false
	if mc.waveBlocked {
		mc.notifyLocked()
		return false, 0
	}
	mc.dispatcherActive = true
	if mc.waveMode {
		// Consume the one wave permit before dispatching so flush notifications
		// cannot queue another production selection cycle in this wave.
		mc.waveBlocked = true
	}
	mc.notifyLocked()
	return true, mc.waveMaxPartID
}

func (mc *mergeLoopControl) endDispatch() {
	mc.mu.Lock()
	mc.dispatcherActive = false
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) observeEmpty(epoch uint64) {
	mc.mu.Lock()
	mc.emptyEpoch = epoch
	mc.emptyWorkVersion = mc.workVersion
	mc.emptyEpochObserved = true
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) addQueued() {
	mc.mu.Lock()
	mc.queued++
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) cancelQueued() {
	mc.mu.Lock()
	mc.queued--
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) startQueued() {
	mc.mu.Lock()
	mc.queued--
	mc.running++
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) finishRunning() {
	mc.mu.Lock()
	mc.running--
	mc.workVersion++
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) notify() {
	mc.mu.Lock()
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) stop() {
	mc.mu.Lock()
	if !mc.stopped {
		mc.stopped = true
		mc.notifyLocked()
	}
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) state() mergeLoopState {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mergeLoopState{
		changed:            mc.changed,
		version:            mc.version,
		workVersion:        mc.workVersion,
		emptyEpoch:         mc.emptyEpoch,
		emptyWorkVersion:   mc.emptyWorkVersion,
		queued:             mc.queued,
		running:            mc.running,
		triggerPending:     mc.triggerPending,
		dispatcherActive:   mc.dispatcherActive,
		emptyEpochObserved: mc.emptyEpochObserved,
		stopped:            mc.stopped,
		waveMode:           mc.waveMode,
		waveBlocked:        mc.waveBlocked,
	}
}

func (mc *mergeLoopControl) blockForWave() {
	mc.mu.Lock()
	mc.waveMode = true
	mc.waveBlocked = true
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) startWave(maxPartID uint64, dispatchLimit int) {
	mc.mu.Lock()
	mc.waveMode = true
	mc.waveBlocked = false
	mc.waveMaxPartID = maxPartID
	mc.waveDispatchLimit = dispatchLimit
	mc.waveDispatches = 0
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) finishWaveDispatch() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !mc.waveMode || mc.waveDispatchLimit < 1 {
		return false
	}
	mc.waveDispatches++
	return mc.waveDispatches >= mc.waveDispatchLimit
}

func (mc *mergeLoopControl) releaseWave() {
	mc.mu.Lock()
	mc.waveMode = false
	mc.waveBlocked = false
	mc.waveMaxPartID = 0
	mc.waveDispatchLimit = 0
	mc.waveDispatches = 0
	mc.notifyLocked()
	mc.mu.Unlock()
}

func (mc *mergeLoopControl) unchanged(version uint64) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.version == version
}

func (tst *tsTable) triggerMerge() error {
	if tst.mergeControl == nil || tst.loopCloser == nil {
		return errMergeLoopUnavailable
	}
	return tst.mergeControl.enqueue(tst.loopCloser.CloseNotify())
}

func (tst *tsTable) blockMergeUntilWave() error {
	if tst.mergeControl == nil || tst.loopCloser == nil {
		return errMergeLoopUnavailable
	}
	tst.mergeControl.blockForWave()
	for {
		state := tst.mergeControl.state()
		quiescent := !state.triggerPending && !state.dispatcherActive && state.queued == 0 && state.running == 0
		if quiescent && tst.mergeInFlightEmpty() && tst.mergeControl.unchanged(state.version) {
			return nil
		}
		select {
		case <-tst.loopCloser.CloseNotify():
			return errMergeLoopUnavailable
		case <-state.changed:
		}
	}
}

func (tst *tsTable) triggerMergeWave(ctx context.Context) error {
	if tst.mergeControl == nil || tst.loopCloser == nil {
		return errMergeLoopUnavailable
	}
	tst.mergeControl.startWave(atomic.LoadUint64(&tst.curPartID), 0)
	if triggerErr := tst.triggerMerge(); triggerErr != nil {
		tst.mergeControl.blockForWave()
		return triggerErr
	}
	for {
		state := tst.mergeControl.state()
		quiescent := !state.triggerPending && !state.dispatcherActive && state.queued == 0 && state.running == 0
		if state.waveBlocked && quiescent && tst.mergeInFlightEmpty() && tst.mergeControl.unchanged(state.version) {
			return nil
		}
		select {
		case <-ctx.Done():
			tst.mergeControl.blockForWave()
			return fmt.Errorf("waiting for one trace merge wave: %w", ctx.Err())
		case <-state.changed:
		}
	}
}

func (tst *tsTable) triggerSingleMergeWave(ctx context.Context) (bool, error) {
	if tst.mergeControl == nil || tst.loopCloser == nil {
		return false, errMergeLoopUnavailable
	}
	before := tst.mergeControl.state().workVersion
	tst.mergeControl.startWave(atomic.LoadUint64(&tst.curPartID), 1)
	if triggerErr := tst.triggerMerge(); triggerErr != nil {
		tst.mergeControl.blockForWave()
		return false, triggerErr
	}
	for {
		state := tst.mergeControl.state()
		quiescent := !state.triggerPending && !state.dispatcherActive && state.queued == 0 && state.running == 0
		if state.waveBlocked && quiescent && tst.mergeInFlightEmpty() && tst.mergeControl.unchanged(state.version) {
			return state.workVersion > before, nil
		}
		select {
		case <-ctx.Done():
			tst.mergeControl.blockForWave()
			return false, fmt.Errorf("waiting for one trace merge: %w", ctx.Err())
		case <-state.changed:
		}
	}
}

func (tst *tsTable) releaseMergeWave() error {
	if tst.mergeControl == nil {
		return errMergeLoopUnavailable
	}
	tst.mergeControl.releaseWave()
	return nil
}

func (tst *tsTable) waitForMergeIdle(ctx context.Context) error {
	if tst.mergeControl == nil {
		return errMergeLoopUnavailable
	}
	for {
		state := tst.mergeControl.state()
		if state.stopped {
			return errMergeLoopUnavailable
		}
		epoch := tst.currentEpoch()
		quiescent := !state.triggerPending && !state.dispatcherActive && state.queued == 0 && state.running == 0
		if state.emptyEpochObserved && state.emptyEpoch == epoch && state.emptyWorkVersion == state.workVersion && quiescent &&
			tst.mergeInFlightEmpty() && tst.mergeControl.unchanged(state.version) && tst.currentEpoch() == epoch {
			return nil
		}
		if quiescent && tst.mergeInFlightEmpty() {
			if state.waveBlocked {
				select {
				case <-ctx.Done():
					return fmt.Errorf("waiting for trace merge idle: %w", ctx.Err())
				case <-state.changed:
				}
				continue
			}
			if triggerErr := tst.triggerMerge(); triggerErr != nil {
				return fmt.Errorf("cannot rescan trace merges while waiting for idle: %w", triggerErr)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for trace merge idle: %w", ctx.Err())
		case <-state.changed:
		}
	}
}

func (tst *tsTable) mergeInFlightEmpty() bool {
	tst.inFlightMu.RLock()
	defer tst.inFlightMu.RUnlock()
	return len(tst.inFlight) == 0
}
