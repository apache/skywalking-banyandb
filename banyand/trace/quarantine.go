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

package trace

import "errors"

// quarantineThreshold is the number of consecutive attributable read failures a part
// must accumulate before merge selection excludes it. It tolerates transient IO errors
// while a deterministic decode failure crosses it within three retry cycles.
const quarantineThreshold = 3

// recordUnreadablePart inspects mergeErr for an attributable *unreadablePartError and, if
// found, increments that part's consecutive-failure count. It returns true when mergeErr
// was attributed to a part, regardless of whether the count crossed the quarantine
// threshold on this call. Crossing the threshold logs one Error line and increments the
// quarantine metric.
func (tst *tsTable) recordUnreadablePart(mergeErr error) bool {
	var unreadableErr *unreadablePartError
	if !errors.As(mergeErr, &unreadableErr) {
		return false
	}

	tst.quarantineMu.Lock()
	if tst.quarantineFails == nil {
		tst.quarantineFails = make(map[uint64]int)
	}
	tst.quarantineFails[unreadableErr.partID]++
	fails := tst.quarantineFails[unreadableErr.partID]
	tst.quarantineMu.Unlock()

	if fails == quarantineThreshold {
		tst.l.Error().
			Str("group", tst.group).
			Uint32("shard", uint32(tst.shardID)).
			Uint64("partID", unreadableErr.partID).
			Str("partPath", unreadableErr.partPath).
			Err(unreadableErr.err).
			Msg("part crossed the quarantine threshold; excluding it from merge selection")
		tst.incTotalMergePartQuarantined(1)
	}
	return true
}

// isPartQuarantined reports whether partID has crossed the quarantine threshold and
// should be excluded from merge selection.
func (tst *tsTable) isPartQuarantined(partID uint64) bool {
	tst.quarantineMu.Lock()
	defer tst.quarantineMu.Unlock()
	return tst.quarantineFails[partID] >= quarantineThreshold
}

// hasQuarantinedParts reports whether the registry currently holds any entries, so
// callers on the hot selection path can skip building a liveIDs set when it is empty.
func (tst *tsTable) hasQuarantinedParts() bool {
	tst.quarantineMu.Lock()
	defer tst.quarantineMu.Unlock()
	return len(tst.quarantineFails) > 0
}

// sweepQuarantine removes registry entries for parts no longer present in liveIDs
// (merged away, TTL'd, or deleted). The registry is in-memory only.
func (tst *tsTable) sweepQuarantine(liveIDs map[uint64]struct{}) {
	tst.quarantineMu.Lock()
	defer tst.quarantineMu.Unlock()
	for partID := range tst.quarantineFails {
		if _, live := liveIDs[partID]; !live {
			delete(tst.quarantineFails, partID)
		}
	}
}
