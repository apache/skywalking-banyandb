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

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// defaultFinalizeScanInterval is the cadence of the background finalize scanner. It
// mirrors the storage idle-check cadence: a backstop does not need tight latency.
const defaultFinalizeScanInterval = 10 * time.Minute

// finalizeScanLoop is the trace-owned periodic finalization-sampling scanner. It runs
// as a single node-wide goroutine, so finalize compute is serialized (concurrency-1)
// and can never fan out to starve the hot merge lanes. On each tick it sweeps every
// finalize-enabled group's cooled segments and runs bounded finalize rounds.
func (sr *schemaRepo) finalizeScanLoop(closer *run.Closer, interval time.Duration) {
	defer closer.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-closer.CloseNotify():
			return
		case <-ticker.C:
			sr.runFinalizeScan(closer.CloseNotify())
		}
	}
}

// runFinalizeScan performs one scan pass over all finalize-enabled groups. It is safe
// to call directly (tests do) without the surrounding loop.
func (sr *schemaRepo) runFinalizeScan(closeCh <-chan struct{}) {
	// A local filesystem to read per-shard finalize.json during the pre-filter, without
	// reopening the segment. finalize.json is always on local disk.
	lfs := fs.NewLocalFileSystem()
	for _, group := range listFinalizeGroups() {
		select {
		case <-closeCh:
			return
		default:
		}
		samplers := lookupSamplers(group)
		if len(samplers) == 0 {
			continue
		}
		graceNs := lookupFinalizeGrace(group)
		if graceNs <= 0 {
			graceNs = int64(sr.finalizeGraceDefault)
		}
		if graceNs <= 0 {
			continue
		}
		sr.scanGroup(group, samplers, graceNs, lookupFinalizeConfig(group, graceNs), lfs, closeCh)
	}
}

// scanGroup peeks the group's cooled segments WITHOUT reopening them, pre-filters on
// per-shard on-disk state so terminal / in-cooldown / max-rounds segments are never
// reopened (this is what keeps a backstop from perturbing the hot node with a reopen
// storm over cold segments), and only reopens the segments that may warrant a round —
// then applies the precise threshold and finalizes warranting shards sequentially.
func (sr *schemaRepo) scanGroup(group string, samplers []sdk.Sampler, graceNs int64, cfg finalizeConfig, lfs fs.FileSystem, closeCh <-chan struct{}) {
	tsdb, err := sr.loadTSDB(group)
	if err != nil || tsdb == nil {
		return
	}
	now := time.Now().UnixNano()
	coolEnd := now - graceNs
	coolRange := timestamp.TimeRange{Start: time.Unix(0, 0), End: time.Unix(0, coolEnd)}
	for _, peek := range tsdb.PeekSegments(coolRange) {
		select {
		case <-closeCh:
			return
		default:
		}
		// Only fully-cooled segments; and skip (without reopening) any whose shards are
		// all terminal / in-cooldown / max-rounds per their on-disk finalize state.
		if peek.End.UnixNano() > coolEnd || !segmentMayWarrant(lfs, peek.ShardPaths, cfg) {
			continue
		}
		// This segment may warrant work: reopen just its range and finalize warranting
		// shards. The precise byte threshold is applied here (warrantsFinalize) against
		// the reopened snapshot.
		segs, selErr := tsdb.SelectSegments(timestamp.TimeRange{Start: peek.Start, End: peek.End}, true)
		if selErr != nil {
			sr.l.Warn().Err(selErr).Str("group", group).Msg("finalize scan: reopen segment failed")
			continue
		}
		sr.finalizeReopened(group, segs, samplers, graceNs, cfg, coolEnd)
	}
}

// finalizeReopened runs bounded finalize rounds on the warranting shards of the reopened
// segments, then DecRefs every segment on all paths.
func (sr *schemaRepo) finalizeReopened(group string, segs []storage.Segment[*tsTable, option],
	samplers []sdk.Sampler, graceNs int64, cfg finalizeConfig, coolEnd int64,
) {
	defer func() {
		for _, seg := range segs {
			seg.DecRef()
		}
	}()
	for _, seg := range segs {
		// SelectSegments may return a boundary neighbor; guard on the cool window again.
		if seg.GetTimeRange().End.UnixNano() > coolEnd {
			continue
		}
		tables, _ := seg.Tables()
		for _, tst := range tables {
			if !warrantsFinalize(tst, cfg) {
				continue
			}
			if _, ferr := tst.runFinalizeRound(samplers, graceNs); ferr != nil {
				sr.l.Warn().Err(ferr).Str("group", group).Msg("finalize round failed (fail-open)")
			}
		}
	}
}

// segmentMayWarrant reports whether any of a cooled segment's shards is worth reopening,
// judged purely from on-disk finalize state (no segment reopen). It is a conservative
// superset of warrantsFinalize: it never skips a shard that would actually warrant.
func segmentMayWarrant(lfs fs.FileSystem, shardPaths []string, cfg finalizeConfig) bool {
	for _, shardPath := range shardPaths {
		if shardMayWarrant(readFinalizeState(lfs, shardPath), cfg) {
			return true
		}
	}
	return false
}

// shardMayWarrant is the reopen pre-filter for one shard. It skips only the cases that
// are authoritative from on-disk state regardless of whether the shard is open: terminal,
// the hard round cap, and an un-elapsed cooldown. A never-finalized shard always warrants;
// a finalized one past its cooldown is reopened so the precise byte threshold can run
// against the live snapshot (the on-disk unsampled counter can lag an open shard).
func shardMayWarrant(st finalizeState, cfg finalizeConfig) bool {
	if st.Terminal || st.FinalizeRounds >= cfg.maxRounds {
		return false
	}
	if st.FinalizeGeneration == 0 || st.LastFinalizedAt == "" {
		return true
	}
	last, perr := time.Parse(time.RFC3339Nano, st.LastFinalizedAt)
	if perr != nil {
		return true
	}
	return time.Since(last) >= time.Duration(cfg.cooldownNs)
}

// warrantsFinalize decides whether a shard warrants another finalize round. It reads
// the per-shard persisted state (scanner path — off the hot path, so metadata I/O is
// fine) and applies the threshold: never-finalized always warrants; otherwise the
// newly-arrived unsampled bytes must cross max(FLOOR, RATIO*total); a cooldown caps the
// frequency; and max_finalize_rounds is a hard terminal cap.
func warrantsFinalize(tst *tsTable, cfg finalizeConfig) bool {
	st := readFinalizeState(tst.fileSystem, tst.root)
	if st.Terminal {
		return false
	}
	if st.FinalizeRounds >= cfg.maxRounds {
		// Hard lifetime cap reached: mark terminal so the shard is never scanned again.
		st.Terminal = true
		if err := writeFinalizeState(tst.fileSystem, tst.root, st); err != nil {
			tst.l.Warn().Err(err).Str("group", tst.group).Msg("cannot mark finalize terminal")
		}
		return false
	}
	if st.LastFinalizedAt != "" {
		if last, perr := time.Parse(time.RFC3339Nano, st.LastFinalizedAt); perr == nil {
			if time.Since(last) < time.Duration(cfg.cooldownNs) {
				return false
			}
		}
	}
	if st.FinalizeGeneration == 0 {
		return true
	}
	total := shardTotalUncompressed(tst)
	threshold := int64(cfg.floorBytes)
	if r := int64(cfg.ratio * float64(total)); r > threshold {
		threshold = r
	}
	return tst.unsampledBytes.Load() >= threshold
}

// shardTotalUncompressed sums the uncompressed span bytes of the shard's current file
// parts, the denominator for the RATIO threshold.
func shardTotalUncompressed(tst *tsTable) int64 {
	s := tst.currentSnapshot()
	if s == nil {
		return 0
	}
	defer s.decRef()
	var total int64
	for _, pw := range s.parts {
		if pw.mp == nil {
			total += int64(pw.p.partMetadata.UncompressedSpanSizeBytes)
		}
	}
	return total
}
