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

package grpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// memberRole identifies the role under which a watched-set member was discovered.
type memberRole int

const (
	roleLiaison memberRole = iota
	roleData
)

func (r memberRole) String() string {
	switch r {
	case roleLiaison:
		return "liaison"
	case roleData:
		return "data"
	default:
		return "unknown_role"
	}
}

// member is a single cluster member in the frozen watched set built at the
// start of an Await* call.
type member struct {
	name   string
	role   memberRole
	isSelf bool
}

// laggardName returns the addressable laggard identifier per the plan's
// `<role>-<Metadata.Name>` convention.
func (m member) laggardName() string {
	return m.role.String() + "-" + m.name
}

// snapshotMembers builds the frozen watched set from the receiving liaison's
// in-process self plus the tier1 (peer-liaison) and tier2 (data-node) Active
// route tables. Dedup is by Metadata.Name with the first-seen tier winning,
// so a hybrid host running both roles is probed exactly once: once via self
// if it is the receiving liaison, otherwise via tier1.
//
// Standalone fallback: when both tier route tables are empty (the local
// pipeline returns an empty RouteTable) the watched set degenerates to
// {self}, which the probe loop handles uniformly with the multi-member case.
func (b *barrierService) snapshotMembers() []member {
	seen := map[string]struct{}{}
	var watched []member

	if b.selfName != nil {
		if name := b.selfName(); name != "" {
			seen[name] = struct{}{}
			watched = append(watched, member{name: name, role: roleLiaison, isSelf: true})
		}
	}

	addFromTier := func(provider func() queue.Client, role memberRole) {
		if provider == nil {
			return
		}
		client := provider()
		if client == nil {
			return
		}
		rt := client.GetRouteTable()
		if rt == nil {
			return
		}
		for _, name := range rt.GetActive() {
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			watched = append(watched, member{name: name, role: role})
		}
	}
	addFromTier(b.peerLiaisons, roleLiaison)
	addFromTier(b.dataNodes, roleData)

	return watched
}

// probeResult is the outcome of a single per-iteration probe of one member.
//
// ready=true means the member is at or above the target revision (or returned
// codes.Unimplemented, which the cross-version policy treats as ready). When
// ready=false the (rev, err) pair carries whatever the probe last observed —
// rev=0 + err!=nil indicates a transient RPC failure that the next iteration
// retries; rev>0 + err==nil indicates the member is online but behind.
type probeResult struct {
	err    error
	member member
	rev    int64
	ready  bool
}

// evictedLaggardReason is the laggard.reason value attached to a member that
// transitioned Active → Evictable while a barrier call was in flight. The
// cluster has already decided the node is unreliable; the barrier defers to
// that decision rather than try to override it, but surfaces the eviction
// once so callers see why the watched set shrank mid-call.
const evictedLaggardReason = "evicted_during_poll"

// membershipView is a snapshot of the tier1 + tier2 route tables collapsed
// into name-keyed sets. The barrier loop refreshes this each iteration to
// detect Active → Evictable / Active → Removed transitions on the frozen
// member set built at call start.
type membershipView struct {
	active    map[string]struct{}
	evictable map[string]struct{}
}

// currentMembership reads tier1 and tier2 route tables and merges their
// Active and Evictable lists into name sets. Both providers contribute to
// both sets — dedup is by name, so a hybrid host appearing in both tiers
// counts once. ConnManager.GetRouteTable (pkg/grpchelper/connmanager.go:308)
// holds the manager's RLock for the duration of the read and returns a
// freshly allocated *databasev1.RouteTable whose Active/Evictable slices
// are built per call, so concurrent invocation from the barrier loop is
// race-free without additional locking at this layer.
func (b *barrierService) currentMembership() membershipView {
	view := membershipView{
		active:    map[string]struct{}{},
		evictable: map[string]struct{}{},
	}
	addFromTier := func(provider func() queue.Client) {
		if provider == nil {
			return
		}
		client := provider()
		if client == nil {
			return
		}
		rt := client.GetRouteTable()
		if rt == nil {
			return
		}
		for _, n := range rt.GetActive() {
			view.active[n] = struct{}{}
		}
		for _, n := range rt.GetEvictable() {
			view.evictable[n] = struct{}{}
		}
	}
	addFromTier(b.peerLiaisons)
	addFromTier(b.dataNodes)
	return view
}

// aliveSet returns a name-keyed set of the supplied alive slice for O(1)
// post-probe filtering.
func aliveSet(alive []member) map[string]struct{} {
	out := make(map[string]struct{}, len(alive))
	for _, m := range alive {
		out[m.name] = struct{}{}
	}
	return out
}

// pruneRevResults filters probeResult slice to only entries whose member
// is still in the alive set. Used by the post-probe membership refresh in
// awaitRevisionAppliedCluster to avoid reporting an in-flight evicted
// member as a normal revision laggard on the timeout/cancel return path.
func pruneRevResults(results []probeResult, set map[string]struct{}) []probeResult {
	out := results[:0]
	for _, r := range results {
		if _, ok := set[r.member.name]; ok {
			out = append(out, r)
		}
	}
	return out
}

// pruneKeyResults is the keyProbeResult sibling of pruneRevResults.
func pruneKeyResults(results []keyProbeResult, set map[string]struct{}) []keyProbeResult {
	out := results[:0]
	for _, r := range results {
		if _, ok := set[r.member.name]; ok {
			out = append(out, r)
		}
	}
	return out
}

// pruneStillPresentResults is the stillPresentResult sibling of
// pruneRevResults.
func pruneStillPresentResults(results []stillPresentResult, set map[string]struct{}) []stillPresentResult {
	out := results[:0]
	for _, r := range results {
		if _, ok := set[r.member.name]; ok {
			out = append(out, r)
		}
	}
	return out
}

// applyTransitions walks the alive members against the current membership
// view and partitions them into "still alive" (kept for the next probe) and
// "newly evicted" (surfaced as one-time laggards with reason="evicted_during_poll").
// Members whose names disappear from BOTH active and evictable are treated
// as having left the cluster (graceful leave) and are dropped without a
// laggard entry, per the frozen-snapshot policy. Self is always kept — the
// in-process member never transitions.
func applyTransitions(alive []member, view membershipView, lastRev map[string]int64) (kept []member, newlyEvicted []*schemav1.NodeLaggard) {
	for _, m := range alive {
		if m.isSelf {
			kept = append(kept, m)
			continue
		}
		if _, evicted := view.evictable[m.name]; evicted {
			newlyEvicted = append(newlyEvicted, &schemav1.NodeLaggard{
				Node:               m.laggardName(),
				CurrentModRevision: lastRev[m.name],
				Reason:             evictedLaggardReason,
			})
			continue
		}
		if _, active := view.active[m.name]; !active {
			// Absent from both sets: graceful leave. Drop silently per
			// the frozen-snapshot leave-during-poll branch.
			continue
		}
		kept = append(kept, m)
	}
	return kept, newlyEvicted
}

// awaitRevisionAppliedCluster runs the cluster-wide fan-out for
// AwaitRevisionApplied. The receiving liaison's own cache is probed
// in-process; peers are probed via clusterv1.NodeSchemaStatusService over the
// *grpc.ClientConn borrowed from queue.Client. The frozen watched set built
// at call start is shrunk mid-call when members transition Active → Evictable
// (recorded as one-time `evicted_during_poll` laggards) or Active → Removed
// (silent leave). Late joiners — nodes that enter Active after the snapshot
// — are excluded; they show up in subsequent calls.
func (b *barrierService) awaitRevisionAppliedCluster(ctx context.Context, req *schemav1.AwaitRevisionAppliedRequest) (*schemav1.AwaitRevisionAppliedResponse, error) {
	frozen := b.snapshotMembers()
	if len(frozen) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no active cluster members")
	}

	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	alive := frozen
	lastRev := map[string]int64{}
	var evictedLaggards []*schemav1.NodeLaggard
	interval := barrierInitInterval
	var lastResults []probeResult
	for {
		view := b.currentMembership()
		var newlyEvicted []*schemav1.NodeLaggard
		alive, newlyEvicted = applyTransitions(alive, view, lastRev)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)

		if len(alive) == 0 {
			// Every frozen member departed mid-call. The remaining cluster
			// has no objection to the target revision; surface the eviction
			// notices and return applied=true so callers can act.
			return &schemav1.AwaitRevisionAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}

		lastResults = b.probeMembers(pollCtx, alive, req.GetMinRevision(), deadline)
		for _, r := range lastResults {
			lastRev[r.member.name] = r.rev
		}
		if allReady(lastResults) {
			return &schemav1.AwaitRevisionAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		// Post-probe membership refresh: a member can transition Active →
		// Evictable between probe completion and the timeout/cancel branch.
		// Without this second snapshot the member would be reported as a
		// normal revision laggard instead of a one-time eviction laggard,
		// and if it was the last blocked member the call would return
		// applied=false when applied=true is correct.
		view = b.currentMembership()
		alive, newlyEvicted = applyTransitions(alive, view, lastRev)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)
		lastResults = pruneRevResults(lastResults, aliveSet(alive))
		if len(alive) == 0 || allReady(lastResults) {
			return &schemav1.AwaitRevisionAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: append(revisionLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: append(revisionLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// probeMembers runs one parallel iteration of GetMaxRevision probes against
// the watched set. Each per-member probe context inherits the call-wide
// deadline (shared, not divided across N members) so the loop's wall-clock is
// bounded by req.timeout regardless of fan-out width.
func (b *barrierService) probeMembers(ctx context.Context, members []member, minRev int64, deadline time.Time) []probeResult {
	results := make([]probeResult, len(members))
	var wg sync.WaitGroup
	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = b.probeOne(probeCtx, members[idx], minRev)
		}(i)
	}
	wg.Wait()
	return results
}

// probeOne returns the per-member outcome for one iteration.
func (b *barrierService) probeOne(ctx context.Context, m member, minRev int64) probeResult {
	if m.isSelf {
		c := b.cache()
		if c == nil {
			return probeResult{member: m}
		}
		rev := c.GetMaxModRevision()
		return probeResult{member: m, rev: rev, ready: rev >= minRev}
	}

	tier := b.peerLiaisons
	if m.role == roleData {
		tier = b.dataNodes
	}
	if tier == nil {
		return probeResult{member: m, err: errors.New("no tier client wired")}
	}
	client := tier()
	if client == nil {
		return probeResult{member: m, err: errors.New("tier client unavailable")}
	}
	statusClient, err := client.NewNodeSchemaStatusClient(m.name)
	if err != nil {
		return probeResult{member: m, err: err}
	}
	resp, rpcErr := statusClient.GetMaxRevision(ctx, &clusterv1.GetMaxRevisionRequest{})
	if rpcErr != nil {
		// Cross-version policy: a Phase-1 peer that does not implement
		// NodeSchemaStatusService returns codes.Unimplemented; treat that
		// member as ready (assume max_revision = ∞) so partial-upgrade
		// clusters do not deadlock all barrier callers.
		if status.Code(rpcErr) == codes.Unimplemented {
			return probeResult{member: m, ready: true}
		}
		// Any other RPC error counts as a transient laggard for this
		// iteration only — the next backoff iteration retries it.
		return probeResult{member: m, err: rpcErr}
	}
	rev := resp.GetMaxModRevision()
	return probeResult{member: m, rev: rev, ready: rev >= minRev}
}

// keyProbeResult is the per-iteration outcome of a GetKeyRevisions probe for
// one member. missingKeys carries only the keys that failed the apply check
// (absent or below their target revision). ready=true when missingKeys is
// empty after the probe; err!=nil means the probe is a transient laggard for
// this iteration only.
type keyProbeResult struct {
	err         error
	missingKeys []*schemav1.SchemaKey
	member      member
	ready       bool
}

// barrierKeyChunkSize matches the plan  chunking policy: each peer probe
// for AwaitSchemaApplied / AwaitSchemaDeleted issues at most 1000 keys per
// RPC. The server-side cap (nodeStatusMaxKeys = 10000) is the absolute limit;
// 1000 is a soft chunking threshold that keeps individual RPC payloads well
// under typical gRPC max-message-size defaults and fans the work out across
// multiple round-trips for large requests.
const barrierKeyChunkSize = 1000

// awaitSchemaAppliedCluster runs the cluster-wide fan-out for
// AwaitSchemaApplied. Self is probed in-process via the cache; peers are
// probed via GetKeyRevisions over the *grpc.ClientConn borrowed from
// queue.Client. Per-member probes chunk keys at barrierKeyChunkSize per RPC,
// each chunk inheriting the call-wide deadline (shared, not divided across
// chunks or members). Frozen-snapshot semantics — eviction / leave / late
// joiner exclusion — match awaitRevisionAppliedCluster's behavior.
func (b *barrierService) awaitSchemaAppliedCluster(ctx context.Context, req *schemav1.AwaitSchemaAppliedRequest) (*schemav1.AwaitSchemaAppliedResponse, error) {
	if len(req.GetKeys()) > barrierMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", barrierMaxKeys)
	}
	frozen := b.snapshotMembers()
	if len(frozen) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no active cluster members")
	}

	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	alive := frozen
	var evictedLaggards []*schemav1.NodeLaggard
	interval := barrierInitInterval
	var lastResults []keyProbeResult
	for {
		view := b.currentMembership()
		var newlyEvicted []*schemav1.NodeLaggard
		alive, newlyEvicted = applyTransitionsApplied(alive, view)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)

		if len(alive) == 0 {
			return &schemav1.AwaitSchemaAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}

		lastResults = b.probeKeyRevisions(pollCtx, alive, req.GetKeys(), req.GetMinRevisions(), deadline)
		if allKeysApplied(lastResults) {
			return &schemav1.AwaitSchemaAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		// Post-probe membership refresh — see awaitRevisionAppliedCluster
		// for the rationale. Same race window applies to the per-key
		// barrier: a member that flips Active → Evictable after the
		// GetKeyRevisions probe finishes but before the timeout branch
		// must surface as an eviction laggard, not as a missing-keys
		// laggard.
		view = b.currentMembership()
		alive, newlyEvicted = applyTransitionsApplied(alive, view)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)
		lastResults = pruneKeyResults(lastResults, aliveSet(alive))
		if len(alive) == 0 || allKeysApplied(lastResults) {
			return &schemav1.AwaitSchemaAppliedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitSchemaAppliedResponse{
				Applied:  false,
				Laggards: append(keyLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitSchemaAppliedResponse{
				Applied:  false,
				Laggards: append(keyLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// applyTransitionsApplied is the AwaitSchemaApplied / AwaitSchemaDeleted
// sibling of applyTransitions. Eviction laggards for the per-key barriers
// carry only the role-prefixed name and Reason; missing_keys /
// still_present_keys are intentionally empty because the cluster has
// already removed the member from the watched set, so its outstanding key
// state is no longer something the call is waiting on.
func applyTransitionsApplied(alive []member, view membershipView) (kept []member, newlyEvicted []*schemav1.NodeLaggard) {
	for _, m := range alive {
		if m.isSelf {
			kept = append(kept, m)
			continue
		}
		if _, evicted := view.evictable[m.name]; evicted {
			newlyEvicted = append(newlyEvicted, &schemav1.NodeLaggard{
				Node:   m.laggardName(),
				Reason: evictedLaggardReason,
			})
			continue
		}
		if _, active := view.active[m.name]; !active {
			continue
		}
		kept = append(kept, m)
	}
	return kept, newlyEvicted
}

// probeKeyRevisions runs one parallel iteration of GetKeyRevisions probes
// against the watched set. Each per-member probe chunks the key list into
// blocks of barrierKeyChunkSize and aggregates per-chunk responses into a
// single missingKeys slice; the call-wide deadline is shared across all
// chunks of all members.
func (b *barrierService) probeKeyRevisions(ctx context.Context, members []member, keys []*schemav1.SchemaKey, minRevs []int64, deadline time.Time) []keyProbeResult {
	results := make([]keyProbeResult, len(members))
	var wg sync.WaitGroup
	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = b.probeKeysOne(probeCtx, members[idx], keys, minRevs)
		}(i)
	}
	wg.Wait()
	return results
}

// probeKeysOne returns the per-member outcome for one iteration of
// AwaitSchemaApplied's per-key probe. Self uses an in-process cache scan;
// peers use GetKeyRevisions chunked at barrierKeyChunkSize.
func (b *barrierService) probeKeysOne(ctx context.Context, m member, keys []*schemav1.SchemaKey, minRevs []int64) keyProbeResult {
	if m.isSelf {
		c := b.cache()
		if c == nil {
			// Fail-closed: nil cache → every key is missing so the loop
			// keeps polling until the cache is online.
			missing := make([]*schemav1.SchemaKey, len(keys))
			copy(missing, keys)
			return keyProbeResult{member: m, missingKeys: missing}
		}
		var missing []*schemav1.SchemaKey
		for idx, key := range keys {
			propID := schemaKeyToPropID(key)
			rev, ok := c.GetKeyModRevision(propID)
			var minRev int64
			if idx < len(minRevs) {
				minRev = minRevs[idx]
			}
			if !ok || (minRev > 0 && rev < minRev) {
				missing = append(missing, key)
			}
		}
		return keyProbeResult{member: m, missingKeys: missing, ready: len(missing) == 0}
	}

	tier := b.peerLiaisons
	if m.role == roleData {
		tier = b.dataNodes
	}
	if tier == nil {
		return keyProbeResult{member: m, err: errors.New("no tier client wired")}
	}
	client := tier()
	if client == nil {
		return keyProbeResult{member: m, err: errors.New("tier client unavailable")}
	}
	statusClient, err := client.NewNodeSchemaStatusClient(m.name)
	if err != nil {
		return keyProbeResult{member: m, err: err}
	}

	var missing []*schemav1.SchemaKey
	for chunkStart := 0; chunkStart < len(keys); chunkStart += barrierKeyChunkSize {
		chunkEnd := min(chunkStart+barrierKeyChunkSize, len(keys))
		chunkKeys := keys[chunkStart:chunkEnd]
		resp, rpcErr := statusClient.GetKeyRevisions(ctx, &clusterv1.GetKeyRevisionsRequest{Keys: chunkKeys})
		if rpcErr != nil {
			// Cross-version: a Phase-1 peer answers Unimplemented; treat
			// that member as ready (assume every key applied).
			if status.Code(rpcErr) == codes.Unimplemented {
				return keyProbeResult{member: m, ready: true}
			}
			// Other RPC errors are transient laggards for this iteration.
			return keyProbeResult{member: m, err: rpcErr}
		}
		revisions := resp.GetRevisions()
		for i, kr := range revisions {
			globalIdx := chunkStart + i
			var minRev int64
			if globalIdx < len(minRevs) {
				minRev = minRevs[globalIdx]
			}
			if !kr.GetPresent() || (minRev > 0 && kr.GetModRevision() < minRev) {
				missing = append(missing, chunkKeys[i])
			}
		}
	}
	return keyProbeResult{member: m, missingKeys: missing, ready: len(missing) == 0}
}

// stillPresentResult is the per-iteration outcome of a GetAbsentKeys probe
// for one member during AwaitSchemaDeleted. stillPresent carries the keys
// the member's cache has not yet removed; ready=true when every requested
// key is absent (stillPresent is empty).
type stillPresentResult struct {
	err          error
	stillPresent []*schemav1.SchemaKey
	member       member
	ready        bool
}

// awaitSchemaDeletedCluster runs the cluster-wide fan-out for
// AwaitSchemaDeleted. Per the  differences table the only deltas from
// AwaitSchemaApplied are the per-member probe RPC (GetAbsentKeys instead of
// GetKeyRevisions) and the decision rule (`every key absent on every member`
// instead of `every key present at or above target rev`). Membership
// snapshot, chunking, shared deadline, cross-version, and transient-error
// policies are identical.
func (b *barrierService) awaitSchemaDeletedCluster(ctx context.Context, req *schemav1.AwaitSchemaDeletedRequest) (*schemav1.AwaitSchemaDeletedResponse, error) {
	if len(req.GetKeys()) > barrierMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", barrierMaxKeys)
	}
	frozen := b.snapshotMembers()
	if len(frozen) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no active cluster members")
	}

	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	alive := frozen
	var evictedLaggards []*schemav1.NodeLaggard
	interval := barrierInitInterval
	var lastResults []stillPresentResult
	for {
		view := b.currentMembership()
		var newlyEvicted []*schemav1.NodeLaggard
		alive, newlyEvicted = applyTransitionsApplied(alive, view)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)

		if len(alive) == 0 {
			return &schemav1.AwaitSchemaDeletedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}

		lastResults = b.probeAbsentKeys(pollCtx, alive, req.GetKeys(), deadline)
		if allKeysAbsent(lastResults) {
			return &schemav1.AwaitSchemaDeletedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		// Post-probe membership refresh — see awaitRevisionAppliedCluster.
		// A member that flips Active → Evictable after GetAbsentKeys
		// finishes must surface as an eviction laggard, not as a
		// still-present-keys laggard.
		view = b.currentMembership()
		alive, newlyEvicted = applyTransitionsApplied(alive, view)
		evictedLaggards = append(evictedLaggards, newlyEvicted...)
		lastResults = pruneStillPresentResults(lastResults, aliveSet(alive))
		if len(alive) == 0 || allKeysAbsent(lastResults) {
			return &schemav1.AwaitSchemaDeletedResponse{Applied: true, Laggards: evictedLaggards}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitSchemaDeletedResponse{
				Applied:  false,
				Laggards: append(stillPresentLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitSchemaDeletedResponse{
				Applied:  false,
				Laggards: append(stillPresentLaggards(lastResults), evictedLaggards...),
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// probeAbsentKeys runs one parallel iteration of GetAbsentKeys probes
// against the watched set, chunking keys at barrierKeyChunkSize with a
// shared call-wide deadline.
func (b *barrierService) probeAbsentKeys(ctx context.Context, members []member, keys []*schemav1.SchemaKey, deadline time.Time) []stillPresentResult {
	results := make([]stillPresentResult, len(members))
	var wg sync.WaitGroup
	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for i := range members {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = b.probeAbsentOne(probeCtx, members[idx], keys)
		}(i)
	}
	wg.Wait()
	return results
}

// probeAbsentOne returns the per-member outcome for one iteration of the
// AwaitSchemaDeleted probe.
func (b *barrierService) probeAbsentOne(ctx context.Context, m member, keys []*schemav1.SchemaKey) stillPresentResult {
	if m.isSelf {
		c := b.cache()
		if c == nil {
			// Fail-closed: nil cache → claim every key still present so
			// the loop keeps polling until the cache is online. Mirrors
			// Phase 1's collectPresentKeys nil-cache contract.
			present := make([]*schemav1.SchemaKey, len(keys))
			copy(present, keys)
			return stillPresentResult{member: m, stillPresent: present}
		}
		var present []*schemav1.SchemaKey
		for _, key := range keys {
			propID := schemaKeyToPropID(key)
			if _, ok := c.GetKeyModRevision(propID); ok {
				present = append(present, key)
			}
		}
		return stillPresentResult{member: m, stillPresent: present, ready: len(present) == 0}
	}

	tier := b.peerLiaisons
	if m.role == roleData {
		tier = b.dataNodes
	}
	if tier == nil {
		return stillPresentResult{member: m, err: errors.New("no tier client wired")}
	}
	client := tier()
	if client == nil {
		return stillPresentResult{member: m, err: errors.New("tier client unavailable")}
	}
	statusClient, err := client.NewNodeSchemaStatusClient(m.name)
	if err != nil {
		return stillPresentResult{member: m, err: err}
	}

	var present []*schemav1.SchemaKey
	for chunkStart := 0; chunkStart < len(keys); chunkStart += barrierKeyChunkSize {
		chunkEnd := min(chunkStart+barrierKeyChunkSize, len(keys))
		chunkKeys := keys[chunkStart:chunkEnd]
		resp, rpcErr := statusClient.GetAbsentKeys(ctx, &clusterv1.GetAbsentKeysRequest{Keys: chunkKeys})
		if rpcErr != nil {
			if status.Code(rpcErr) == codes.Unimplemented {
				return stillPresentResult{member: m, ready: true}
			}
			return stillPresentResult{member: m, err: rpcErr}
		}
		present = append(present, resp.GetStillPresentKeys()...)
	}
	return stillPresentResult{member: m, stillPresent: present, ready: len(present) == 0}
}

// allKeysAbsent reports whether every member's most recent probe found
// every requested key absent (i.e. removed from its cache).
func allKeysAbsent(results []stillPresentResult) bool {
	for _, r := range results {
		if !r.ready {
			return false
		}
	}
	return true
}

// stillPresentLaggards builds the laggards list for the timeout response,
// each entry carrying the per-node still_present_keys observed in the
// final iteration.
func stillPresentLaggards(results []stillPresentResult) []*schemav1.NodeLaggard {
	laggards := make([]*schemav1.NodeLaggard, 0)
	for _, r := range results {
		if r.ready {
			continue
		}
		laggards = append(laggards, &schemav1.NodeLaggard{
			Node:             r.member.laggardName(),
			StillPresentKeys: r.stillPresent,
		})
	}
	return laggards
}

// allKeysApplied reports whether every member's most recent probe found
// every key applied at or above its target revision.
func allKeysApplied(results []keyProbeResult) bool {
	for _, r := range results {
		if !r.ready {
			return false
		}
	}
	return true
}

// keyLaggards builds the laggards list for the timeout response. Members
// that succeeded this iteration are excluded so the list contains only
// the actual stragglers (each carrying their per-node missing_keys).
func keyLaggards(results []keyProbeResult) []*schemav1.NodeLaggard {
	laggards := make([]*schemav1.NodeLaggard, 0)
	for _, r := range results {
		if r.ready {
			continue
		}
		laggards = append(laggards, &schemav1.NodeLaggard{
			Node:        r.member.laggardName(),
			MissingKeys: r.missingKeys,
		})
	}
	return laggards
}

// allReady reports whether every member's most recent probe returned ready.
func allReady(results []probeResult) bool {
	for _, r := range results {
		if !r.ready {
			return false
		}
	}
	return true
}

// revisionLaggards builds the laggards list for the timeout response,
// preserving the watched-set order. Cross-version-ready and ready-this-pass
// members are excluded so the list contains only the actual stragglers.
func revisionLaggards(results []probeResult) []*schemav1.NodeLaggard {
	laggards := make([]*schemav1.NodeLaggard, 0)
	for _, r := range results {
		if r.ready {
			continue
		}
		laggards = append(laggards, &schemav1.NodeLaggard{
			Node:               r.member.laggardName(),
			CurrentModRevision: r.rev,
		})
	}
	return laggards
}
