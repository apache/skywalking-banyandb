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

// awaitRevisionAppliedCluster runs the cluster-wide fan-out for
// AwaitRevisionApplied. The receiving liaison's own cache is probed
// in-process; peers are probed via clusterv1.NodeSchemaStatusService over the
// *grpc.ClientConn borrowed from queue.Client.
func (b *barrierService) awaitRevisionAppliedCluster(ctx context.Context, req *schemav1.AwaitRevisionAppliedRequest) (*schemav1.AwaitRevisionAppliedResponse, error) {
	members := b.snapshotMembers()
	if len(members) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no active cluster members")
	}

	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	interval := barrierInitInterval
	var lastResults []probeResult
	for {
		lastResults = b.probeMembers(pollCtx, members, req.GetMinRevision(), deadline)
		if allReady(lastResults) {
			return &schemav1.AwaitRevisionAppliedResponse{Applied: true}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: revisionLaggards(lastResults),
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: revisionLaggards(lastResults),
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
