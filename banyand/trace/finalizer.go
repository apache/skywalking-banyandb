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

package trace

import (
	"time"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// runFinalizeRound force-merges this shard's cooled, un-finalized parts through the
// sampler chain (the finalization-sampling backstop). It reuses the hot merge path
// (mergePartsThenSendIntroduction → mergeParts + per-sidx Merge + the serialized
// introducer loop, OQ6) with a pre-built filter and a finalize-generation override,
// so the core AND sidx follow the exact same code the hot merge uses. It NEVER
// acquires mergeMaxConcurrencyCh (that lives in the merge-lane worker, not here), so
// finalize compute cannot starve the hot merge lanes.
//
// Selection is all not-hot (by finalize_grace), not-in-flight parts whose FinalizeGen
// is below the round's target generation Gnext=G+1 (by the invariant that nothing is
// stamped above the stored generation, this is every cooled part). The round is a full
// re-sample of the cooled set; re-sampling already-kept survivors through a
// deterministic sampler is idempotent (DD11), and rounds are hard-capped by the
// scanner (max_finalize_rounds), so the total rewrite volume is bounded.
//
// It returns (true, nil) when a round committed, (false, nil) when there was nothing
// to do or it yielded under pressure, and (false, err) on a merge error (fail-open:
// the segment is left intact and the scanner retries on a later tick). It is invoked
// only by the finalize scanner while the owning segment is incRef-held, so the
// tsTable's introducer loop is alive to apply the introduction.
func (tst *tsTable) runFinalizeRound(samplers []sdk.Sampler, graceNs int64) (bool, error) {
	if len(samplers) == 0 {
		return false, nil
	}
	// Resource gate: never run finalize compute under memory pressure (constraint 1).
	if tst.pm != nil && tst.pm.State() == protector.StateHigh {
		return false, nil
	}
	closeCh := tst.loopCloser.CloseNotify()
	select {
	case <-closeCh:
		return false, nil
	default:
	}

	cur := tst.currentSnapshot()
	if cur == nil {
		return false, nil
	}
	defer cur.decRef()

	gNext := tst.finalizeGenCached.Load() + 1
	now := time.Now().UnixNano()
	// Snapshot the counter at round start. On commit we subtract exactly this much
	// rather than storing 0, so bytes that a concurrent flush accounts DURING the round
	// (late arrivals not part of this round's merge) are preserved for the next round.
	// unsampledBytes is only ever added to elsewhere and reset only here (serialized),
	// so the value never drops below startBytes before the subtract.
	startBytes := tst.unsampledBytes.Load()

	// Select AND pin candidate parts atomically under a single write lock. A concurrent
	// hot-merge dispatch (dispatchAllMerges) is a second selector of the same parts; if
	// selection and pinning were split (RLock check, then Lock add) both could pick the
	// same part and merge it twice, and since snapshot.remove tolerates an absent id the
	// two outputs would both survive — duplicating traces. Doing check+pin+incRef in one
	// Lock (and a matching re-check on the dispatcher side) makes the pin exclusive.
	// Selection: cooled (non-hot by finalize_grace), not already in-flight, and below the
	// round generation (excludes a crashed round's stamped outputs — DD6).
	var parts []*partWrapper
	var needBytes uint64
	tst.inFlightMu.Lock()
	if tst.inFlight == nil {
		tst.inFlight = make(map[uint64]struct{})
	}
	for _, pw := range cur.parts {
		if pw.mp != nil || pw.p.partMetadata.TotalCount < 1 {
			continue
		}
		if _, inFlight := tst.inFlight[pw.ID()]; inFlight {
			continue
		}
		if pw.p.partMetadata.FinalizeGen >= gNext {
			continue
		}
		if pw.p.partMetadata.MaxTimestamp > now-graceNs {
			continue
		}
		pw.incRef()
		tst.inFlight[pw.ID()] = struct{}{}
		parts = append(parts, pw)
		needBytes += pw.p.partMetadata.CompressedSizeBytes
	}
	tst.inFlightMu.Unlock()

	if len(parts) == 0 {
		// Nothing eligible this round (all parts still hot, or already finalized).
		// Do NOT advance the generation or reset the counter.
		return false, nil
	}
	// Unpin + release on EVERY return path below (disk gate, merge error, success).
	defer func() {
		tst.inFlightMu.Lock()
		for _, pw := range parts {
			delete(tst.inFlight, pw.ID())
		}
		tst.inFlightMu.Unlock()
		for _, pw := range parts {
			pw.decRef()
		}
	}()

	// Disk-headroom gate: a full-shard rewrite transiently needs ~the selected parts'
	// compressed size in extra space. Skip the round (an accepted miss) rather than risk
	// pushing the hot write path into disk-full — finalize must never starve the core.
	if tst.freeDiskSpace(tst.root) < needBytes {
		return false, nil
	}

	// Build the finalize filter. Unlike the hot path there is no isMergeHot gate here
	// (eligibility was already decided above with finalize_grace); the chain fails open
	// on any Decide error, retaining the whole batch.
	chain := newMergeChain(tst.group, "", samplers, tst.option.decideTimeoutCircuitBreak)
	filter := &mergeFilter{
		chain:       chain,
		timeout:     tst.option.decideTimeout,
		stageBudget: resolveStageBudget(tst.option),
		forceSlow:   len(chain.projection.Tags) > 0,
	}

	merged := make(map[uint64]struct{}, len(parts))
	for _, pw := range parts {
		merged[pw.ID()] = struct{}{}
	}

	gen := gNext
	if _, err := tst.mergePartsThenSendIntroduction(
		snapshotCreatorMerger, parts, merged, tst.mergeCh, closeCh,
		mergeTypeFinalize, mergeLaneFinalize, &mergeOverrides{filter: filter, finalizeGen: &gen},
	); err != nil {
		// Fail-open: leave the segment intact; the scanner retries on a later tick.
		return false, err
	}

	// Commit the per-shard finalize state AFTER the introduction (DD6 ordering). The
	// in-memory cache and counter are mutated ONLY after the state file is durably
	// written: if persist failed but we still bumped finalizeGenCached, the next round
	// would use gNext+1 and re-include (re-sample) the parts this round just stamped
	// gNext. On failure we leave cache == disk (still G) and return the error; the
	// on-disk parts are stamped gNext, so the retry's predicate (finalizeGen < G+1)
	// excludes them — no double-sample, only the round bookkeeping is retried.
	// The counter subtracts exactly the bytes counted before the round started, so a
	// concurrent flush during the round keeps its contribution for the next round.
	remaining := max(tst.unsampledBytes.Load()-startBytes, 0)
	st := readFinalizeState(tst.fileSystem, tst.root)
	st.FinalizeGeneration = gNext
	st.LastFinalizedAt = time.Now().UTC().Format(time.RFC3339Nano)
	st.FinalizeRounds++
	st.UnsampledBytes = remaining
	if err := writeFinalizeState(tst.fileSystem, tst.root, st); err != nil {
		tst.l.Warn().Err(err).Str("group", tst.group).Msg("cannot persist finalize state; leaving generation unchanged")
		return false, err
	}
	tst.finalizeGenCached.Store(gNext)
	tst.unsampledBytes.Add(-startBytes)
	return true, nil
}
