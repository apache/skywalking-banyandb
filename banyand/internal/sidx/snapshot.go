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
	"strconv"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// snapshot represents an immutable collection of parts at a specific epoch.
// It provides safe concurrent access to parts through reference counting and
// enables queries to work with a consistent view of data.
type snapshot struct {
	// parts contains all active parts sorted by epoch (oldest first)
	parts []*partWrapper

	// ref is the atomic reference counter for safe concurrent access
	ref int32
}

// newSnapshot creates a new snapshot with the given parts and epoch.
// The snapshot starts with a reference count of 1.
func newSnapshot(parts []*partWrapper) *snapshot {
	s := &snapshot{}
	s.parts = append(s.parts[:0], parts...)
	s.ref = 1

	// Acquire references to all parts to ensure they remain valid
	for _, pw := range s.parts {
		if !pw.acquire() {
			// Part is being removed, skip it
			logger.GetLogger().Warn().
				Uint64("part_id", pw.ID()).
				Msg("part unavailable during snapshot creation")
		}
	}

	return s
}

// acquire increments the snapshot reference count.
// Returns true if successful, false if snapshot has been released.
func (s *snapshot) acquire() bool {
	return atomic.AddInt32(&s.ref, 1) > 0
}

// decRef decrements the snapshot reference count (helper for snapshot interface).
func (s *snapshot) decRef() {
	s.release()
}

// release decrements the snapshot reference count.
// When the count reaches zero, all part references are released.
func (s *snapshot) release() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	if newRef < 0 {
		logger.GetLogger().Warn().
			Int32("ref", newRef).
			Msg("snapshot reference count went negative")
		return
	}
	s.reset()
}

// getParts returns parts that potentially contain data within the specified key range.
// This method filters parts based on their key ranges to minimize I/O during queries.
// Parts are returned in epoch order (oldest first) for consistent iteration.
func (s *snapshot) getParts(minKey, maxKey int64) []*partWrapper {
	var result []*partWrapper

	for _, pw := range s.parts {
		part := pw.p
		if part == nil || part.partMetadata == nil {
			continue
		}

		// Check if part's key range overlaps with query range
		partMinKey := part.partMetadata.MinKey
		partMaxKey := part.partMetadata.MaxKey

		// Skip parts that don't overlap with the query range
		// Part overlaps if: partMinKey <= maxKey && partMaxKey >= minKey
		if partMinKey <= maxKey && partMaxKey >= minKey {
			result = append(result, pw)
		}
	}

	return result
}

// getPartsAll returns all active parts in the snapshot.
// This is used when querying without key range restrictions.
func (s *snapshot) getPartsAll() []*partWrapper {
	var result []*partWrapper

	for _, pw := range s.parts {
		if !pw.removable.Load() {
			result = append(result, pw)
		}
	}

	return result
}

// getPartCount returns the number of parts in the snapshot.
func (s *snapshot) getPartCount() int {
	return len(s.getPartsAll())
}

// refCount returns the current reference count (for testing/debugging).
func (s *snapshot) refCount() int32 {
	return atomic.LoadInt32(&s.ref)
}

// validate checks snapshot consistency and part availability.
func (s *snapshot) validate() error {
	if atomic.LoadInt32(&s.ref) <= 0 {
		return fmt.Errorf("snapshot has zero or negative reference count")
	}

	// Validate that parts are sorted by epoch
	for i := 1; i < len(s.parts); i++ {
		prev := s.parts[i-1]
		curr := s.parts[i]

		if prev.p != nil && curr.p != nil &&
			prev.p.partMetadata != nil && curr.p.partMetadata != nil {
			if prev.p.partMetadata.ID > curr.p.partMetadata.ID {
				return fmt.Errorf("parts not sorted by ID: part[%d].ID=%d > part[%d].ID=%d",
					i-1, prev.p.partMetadata.ID, i, curr.p.partMetadata.ID)
			}
		}
	}

	return nil
}

// addPart adds a new part to the snapshot during construction.
// This should only be called before the snapshot is made available to other goroutines.
// After construction, snapshots should be treated as immutable.
func (s *snapshot) addPart(pw *partWrapper) {
	if pw != nil && pw.acquire() {
		s.parts = append(s.parts, pw)
	}
}

// removePart marks a part for removal from future snapshots.
// The part remains accessible in this snapshot until the snapshot is released.
func (s *snapshot) removePart(partID uint64) {
	for _, pw := range s.parts {
		if pw.ID() == partID {
			pw.markForRemoval()
			break
		}
	}
}

// reset clears the snapshot for reuse.
func (s *snapshot) reset() {
	// Release all part references
	for _, pw := range s.parts {
		if pw != nil {
			pw.release()
		}
	}

	s.parts = s.parts[:0]
	s.ref = 0
}

// String returns a string representation of the snapshot.
func (s *snapshot) String() string {
	activeCount := s.getPartCount()
	return fmt.Sprintf("snapshot{parts=%d/%d, ref=%d}",
		activeCount, len(s.parts), s.refCount())
}

func parseEpoch(epochStr string) (uint64, error) {
	return strconv.ParseUint(epochStr, 16, 64)
}

// copyAllTo creates a new snapshot with all parts from current snapshot.
func (s *snapshot) copyAllTo() *snapshot {
	var result snapshot
	result.parts = make([]*partWrapper, len(s.parts))
	result.ref = 1

	// Copy all parts and acquire references
	copy(result.parts, s.parts)
	for _, pw := range result.parts {
		if pw != nil {
			pw.acquire()
		}
	}

	return &result
}

// merge creates a new snapshot by merging flushed parts into the current snapshot.
func (s *snapshot) merge(nextParts map[uint64]*partWrapper) *snapshot {
	var result snapshot
	result.ref = 1
	for i := 0; i < len(s.parts); i++ {
		if n, ok := nextParts[s.parts[i].ID()]; ok {
			result.parts = append(result.parts, n)
			continue
		}
		if s.parts[i].acquire() {
			result.parts = append(result.parts, s.parts[i])
		}
	}
	return &result
}

// remove creates a new snapshot by removing specified parts.
func (s *snapshot) remove(toRemove map[uint64]struct{}) *snapshot {
	var result snapshot
	result.ref = 1

	// Copy parts except those being removed
	for _, pw := range s.parts {
		if _, shouldRemove := toRemove[pw.ID()]; !shouldRemove {
			if pw.acquire() {
				result.parts = append(result.parts, pw)
				continue
			}
			continue
		}
		pw.markForRemoval()
	}

	return &result
}
