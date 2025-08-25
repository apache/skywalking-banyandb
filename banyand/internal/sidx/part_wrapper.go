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

package sidx

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// partWrapperState represents the state of a partWrapper in its lifecycle.
type partWrapperState int32

const (
	// partStateActive indicates the part is active and available for use.
	partStateActive partWrapperState = iota
	// partStateRemoving indicates the part is being removed but may still have references.
	partStateRemoving
	// partStateRemoved indicates the part has been fully removed and cleaned up.
	partStateRemoved
)

// String returns a string representation of the part state.
func (s partWrapperState) String() string {
	switch s {
	case partStateActive:
		return "active"
	case partStateRemoving:
		return "removing"
	case partStateRemoved:
		return "removed"
	default:
		return fmt.Sprintf("unknown(%d)", int32(s))
	}
}

// partWrapper provides thread-safe reference counting for parts.
// It enables safe concurrent access to parts while managing their lifecycle.
// When the reference count reaches zero, the underlying part is cleaned up.
type partWrapper struct {
	// p is the underlying part. It can be nil for memory parts.
	p *part

	// ref is the atomic reference counter.
	// It starts at 1 when the wrapper is created.
	ref int32

	// state tracks the lifecycle state of the part.
	// State transitions: active -> removing -> removed
	state int32

	// removable indicates if the part should be removed from disk when dereferenced.
	// This is typically true for parts that have been merged or are no longer needed.
	removable atomic.Bool
}

// newPartWrapper creates a new partWrapper with an initial reference count of 1.
// The part starts in the active state.
func newPartWrapper(p *part) *partWrapper {
	pw := &partWrapper{
		p:     p,
		ref:   1,
		state: int32(partStateActive),
	}

	return pw
}

// acquire increments the reference count atomically.
// Returns true if the reference was successfully acquired (part is still active),
// false if the part is being removed or has been removed.
func (pw *partWrapper) acquire() bool {
	// Check state first to avoid unnecessary atomic operations
	if atomic.LoadInt32(&pw.state) != int32(partStateActive) {
		return false
	}

	// Try to increment reference count
	for {
		oldRef := atomic.LoadInt32(&pw.ref)
		if oldRef <= 0 {
			// Reference count is already zero or negative, cannot acquire
			return false
		}

		// Double-check state hasn't changed
		if atomic.LoadInt32(&pw.state) != int32(partStateActive) {
			return false
		}

		// Try to atomically increment
		if atomic.CompareAndSwapInt32(&pw.ref, oldRef, oldRef+1) {
			return true
		}
		// Retry if CAS failed
	}
}

// release decrements the reference count atomically.
// When the reference count reaches zero, the part is cleaned up.
// This method is safe to call multiple times.
func (pw *partWrapper) release() {
	newRef := atomic.AddInt32(&pw.ref, -1)
	if newRef > 0 {
		return
	}

	if newRef < 0 {
		// This shouldn't happen in correct usage, but log it for debugging
		logger.GetLogger().Warn().
			Int32("ref", newRef).
			Str("part", pw.String()).
			Msg("partWrapper reference count went negative")
		return
	}

	// Reference count reached zero, perform cleanup
	pw.cleanup()
}

// cleanup performs the actual cleanup when reference count reaches zero.
// This includes closing the part and potentially removing files from disk.
func (pw *partWrapper) cleanup() {
	// Mark as removed
	atomic.StoreInt32(&pw.state, int32(partStateRemoved))

	if pw.p == nil {
		return
	}

	// Close the part to release file handles
	pw.p.close()

	// Remove from disk if marked as removable
	if pw.removable.Load() && pw.p.fileSystem != nil {
		go func(partPath string, fileSystem interface{}) {
			// Use a goroutine for potentially slow disk operations
			// to avoid blocking the caller
			if fs, ok := fileSystem.(interface{ MustRMAll(string) }); ok {
				fs.MustRMAll(partPath)
			}
		}(pw.p.path, pw.p.fileSystem)
	}
}

// markForRemoval marks the part as removable and transitions it to the removing state.
// Once marked for removal, no new references can be acquired.
// The part will be physically removed when the reference count reaches zero.
func (pw *partWrapper) markForRemoval() {
	// Transition to removing state
	atomic.StoreInt32(&pw.state, int32(partStateRemoving))
	// Mark as removable for cleanup
	pw.removable.Store(true)
}

// ID returns the unique identifier of the part.
// Returns 0 if the part is nil.
func (pw *partWrapper) ID() uint64 {
	if pw.p == nil || pw.p.partMetadata == nil {
		return 0
	}
	return pw.p.partMetadata.ID
}

// refCount returns the current reference count.
// This is primarily for testing and debugging.
func (pw *partWrapper) refCount() int32 {
	return atomic.LoadInt32(&pw.ref)
}

// getState returns the current state of the part.
func (pw *partWrapper) getState() partWrapperState {
	return partWrapperState(atomic.LoadInt32(&pw.state))
}

// isActive returns true if the part is in the active state.
func (pw *partWrapper) isActive() bool {
	return atomic.LoadInt32(&pw.state) == int32(partStateActive)
}

// isRemoving returns true if the part is in the removing state.
func (pw *partWrapper) isRemoving() bool {
	return atomic.LoadInt32(&pw.state) == int32(partStateRemoving)
}

// isRemoved returns true if the part is in the removed state.
func (pw *partWrapper) isRemoved() bool {
	return atomic.LoadInt32(&pw.state) == int32(partStateRemoved)
}

// isMemPart returns true if this wrapper contains a memory part.
func (pw *partWrapper) isMemPart() bool {
	// A memory part typically has no file system path or is stored in memory
	return pw.p != nil && pw.p.path == ""
}

// String returns a string representation of the partWrapper.
func (pw *partWrapper) String() string {
	state := pw.getState()
	refCount := pw.refCount()

	if pw.p == nil {
		return fmt.Sprintf("partWrapper{id=nil, state=%s, ref=%d}", state, refCount)
	}

	return fmt.Sprintf("partWrapper{id=%d, state=%s, ref=%d, path=%s}",
		pw.ID(), state, refCount, pw.p.path)
}
