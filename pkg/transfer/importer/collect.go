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

package importer

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// targetsLimit bounds the Targets calls in flight to one liaison.
const targetsLimit = 8

// barrierProbeTimeout bounds the read-only schema barrier probe of the preflight; tests shorten it.
var barrierProbeTimeout = 5 * time.Second

// Collect reads, read-only, everything the import decides on from the target cluster through
// the liaisons, the first of which is the primary: the schema of every artifact group the
// target has (all eight kinds); the Targets routes of the (group, stage) pairs the artifact's
// work items touch in groups the import leaves as they are, asked of every liaison, and the
// problems of those routes (RouteIssues, Blockers);
// for every group a resource of which would be overwritten, whether it holds data (a Plan of
// those groups; a failed Plan leaves them unknown, so overwriting them needs --force); and whether every
// node has applied the target's current schema revision (a best-effort barrier probe whose
// laggards become Notes).
func Collect(ctx context.Context, liaisons []*cluster.Liaison, info *cluster.Info, art *Artifact, opts SchemaOptions) (*TargetState, error) {
	conn := liaisons[0].Conn
	state := NewTargetState()
	state.DataNodes = info.DataNodes
	state.Standalone = info.Standalone
	groups, err := cluster.FetchGroups(ctx, conn)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if name := g.GetMetadata().GetName(); art.Groups[name] != nil {
			state.Groups[name] = g
		}
	}
	if err = listResources(ctx, conn, state); err != nil {
		return nil, err
	}
	// The dispositions --force would allow: a group whose overwrite needs --force is still probed
	// for data.
	probe := opts
	probe.Force = true
	actions := DiffSchemas(&art.Schemas, state, probe).Actions
	if err = collectRoutes(ctx, liaisons, art, state, rewrittenGroups(actions)); err != nil {
		return nil, err
	}
	collectNonEmpty(ctx, conn, state, actions)
	probeBarrier(ctx, conn, state)
	return state, nil
}

// rewrittenGroups returns the groups the actions create or overwrite.
func rewrittenGroups(actions []SchemaAction) map[string]bool {
	out := map[string]bool{}
	for i := range actions {
		a := &actions[i]
		if _, ok := a.Spec.(*commonv1.Group); ok && (a.Disposition == DispositionCreate || a.Disposition == DispositionOverwrite) {
			out[a.Group] = true
		}
	}
	return out
}

// listGroupsLimit bounds the groups whose resources are listed at once.
const listGroupsLimit = 8

// listResources lists every resource of the state's groups into its schemas, several groups
// at a time.
func listResources(ctx context.Context, conn *grpc.ClientConn, state *TargetState) error {
	var mu sync.Mutex
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(listGroupsLimit)
	for _, g := range state.Groups {
		eg.Go(func() error {
			err := schemasync.ListGroup(egCtx, conn, g, func(kind string, m proto.Message) error {
				mu.Lock()
				defer mu.Unlock()
				return state.Add(kind, m)
			})
			if err != nil {
				return exitcode.Exit(exitcode.For(err), "target: %w", err)
			}
			return nil
		})
	}
	return eg.Wait()
}

// collectRoutes asks every liaison for the route of every (group, stage) of the work items
// whose group the target has or the import creates or overwrites, one key per call so an
// error belongs to its key. A group the import creates or overwrites is routed by the liaison
// with its stored schema, which is missing or about to change, so its answer only tells
// whether the liaison serves Targets: the group has no route until its schema is written,
// only a Note, and the re-check after the schema write routes it. The primary's answers are
// the Routes: a route is keyed by the stage it was asked for and by the stage it reports,
// which differ when it fell back to hot. A route that cannot be written to becomes a
// RouteIssue of the key asked for: a routing error (InvalidArgument, FailedPrecondition,
// NotFound), inactive nodes, a group being deleted, or another liaison answering differently.
// A liaison that does not serve Targets (Unimplemented, Unavailable, PermissionDenied,
// Unauthenticated) is a Blocker: the push spreads over every liaison. Any other failure is
// returned.
func collectRoutes(ctx context.Context, liaisons []*cluster.Liaison, art *Artifact, state *TargetState, rewritten map[string]bool) error {
	// A manifest BuildWorkItems rejects is an artifact error Decide reports.
	items, _ := BuildWorkItems(art.Manifest)
	seen := map[RouteKey]bool{}
	var keys []RouteKey
	for i := range items {
		k := NewRouteKey(items[i].Group, items[i].Stage)
		_, has := state.Groups[k.Group]
		if seen[k] || (!has && !rewritten[k.Group]) {
			continue
		}
		seen[k] = true
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}
	slices.SortFunc(keys, func(a, b RouteKey) int {
		return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage))
	})
	issues := map[RouteKey][]string{}
	primary := map[RouteKey]*transferv1.TargetsResponse{}
	for li, l := range liaisons {
		answers, err := askTargets(ctx, l, keys)
		if err != nil {
			return err
		}
		if blocker := liaisonBlocker(l, answers); blocker != "" {
			state.Blockers = append(state.Blockers, blocker)
			continue
		}
		for i, k := range keys {
			a := answers[i]
			if rewritten[k.Group] {
				continue
			}
			if a.err != nil {
				issues[k] = append(issues[k], fmt.Sprintf("liaison %s: %s", l.Addr, status.Convert(a.err).Message()))
				continue
			}
			r := a.route
			if ids := r.GetInactiveNodeIds(); len(ids) > 0 {
				issues[k] = append(issues[k], fmt.Sprintf("liaison %s: data node(s) [%s] of the route are not active", l.Addr, strings.Join(ids, " ")))
			}
			if r.GetGroupDeleting() {
				issues[k] = append(issues[k], fmt.Sprintf("liaison %s: group %s is being deleted", l.Addr, k.Group))
			}
			if li == 0 {
				primary[k] = r
				continue
			}
			if p, ok := primary[k]; ok && routeSignature(p) != routeSignature(r) {
				issues[k] = append(issues[k], fmt.Sprintf("liaisons disagree: %s routes %s, %s routes %s",
					liaisons[0].Addr, routeSignature(p), l.Addr, routeSignature(r)))
			}
		}
	}
	for k, list := range issues {
		state.RouteIssues[k] = strings.Join(transfer.UniqueSorted(list), "; ")
	}
	state.Notes = transfer.UniqueSorted(state.Notes)
	for _, k := range keys {
		r, ok := primary[k]
		if !ok {
			continue
		}
		state.Routes[k] = r
		// A fallback route does not replace the route asked for its own stage.
		if reported := NewRouteKey(r.GetGroup(), r.GetStage()); state.Routes[reported] == nil {
			state.Routes[reported] = r
		}
	}
	return nil
}

// targetsAnswer is the answer of one liaison for one key.
type targetsAnswer struct {
	route *transferv1.TargetsResponse
	err   error
}

// askTargets asks a liaison for the route of every key, one key per call, several at a time.
// A status error is the key's answer.
func askTargets(ctx context.Context, l *cluster.Liaison, keys []RouteKey) ([]targetsAnswer, error) {
	client := transferv1.NewImportServiceClient(l.Conn)
	answers := make([]targetsAnswer, len(keys))
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(targetsLimit)
	for i, k := range keys {
		eg.Go(func() error {
			resp, err := client.Targets(egCtx, &transferv1.TargetsRequest{Group: k.Group, Stage: k.Stage})
			if err != nil && ctx.Err() != nil {
				return context.Cause(ctx)
			}
			answers[i] = targetsAnswer{route: resp, err: err}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	for i, a := range answers {
		if a.err == nil {
			continue
		}
		code := status.Code(a.err)
		if code != codes.InvalidArgument && code != codes.FailedPrecondition && code != codes.NotFound && !slices.Contains(blockerCodes, code) {
			return nil, exitcode.Exit(exitcode.For(a.err), "import targets of %s/%s via %s: %w", keys[i].Group, keys[i].Stage, l.Addr, a.err)
		}
	}
	return answers, nil
}

// blockerCodes are the Targets answers of a liaison that cannot take part in the import.
var blockerCodes = []codes.Code{codes.Unimplemented, codes.Unavailable, codes.PermissionDenied, codes.Unauthenticated}

// liaisonBlocker explains why a liaison cannot take part in the import, from the first answer
// that shows it does not serve Targets; "" when none does.
func liaisonBlocker(l *cluster.Liaison, answers []targetsAnswer) string {
	for _, a := range answers {
		if a.err == nil {
			continue
		}
		st := status.Convert(a.err)
		if slices.Contains(blockerCodes, st.Code()) {
			return fmt.Sprintf("liaison %s cannot serve the import: ImportService.Targets answered %s: %s; "+
				"every liaison takes import streams, so each must serve ImportService and allow it", l.Addr, st.Code(), st.Message())
		}
	}
	return ""
}

// routeSignature renders what a route places where, for comparing the answers of liaisons.
func routeSignature(r *transferv1.TargetsResponse) string {
	shards := slices.Clone(r.GetShards())
	slices.SortFunc(shards, func(a, b *transferv1.TargetsResponse_Shard) int {
		return cmp.Compare(a.GetShardId(), b.GetShardId())
	})
	parts := make([]string, 0, len(shards))
	for _, s := range shards {
		parts = append(parts, fmt.Sprintf("%d:%s", s.GetShardId(), strings.Join(transfer.UniqueSorted(s.GetNodeIds()), ",")))
	}
	return fmt.Sprintf("stage=%s shards=%d copies=%d [%s]", r.GetStage(), r.GetShardNum(), r.GetCopies(), strings.Join(parts, " "))
}

// collectNonEmpty fills NonEmptyGroups for the groups an overwrite would change, from one
// Plan of those groups.
func collectNonEmpty(ctx context.Context, conn *grpc.ClientConn, state *TargetState, actions []SchemaAction) {
	catalogs := map[string]commonv1.Catalog{}
	for _, a := range actions {
		if a.Disposition == DispositionOverwrite {
			catalogs[a.Group] = state.Groups[a.Group].GetCatalog()
		}
	}
	if len(catalogs) == 0 {
		return
	}
	groups := slices.Sorted(maps.Keys(catalogs))
	result, err := cluster.RunPlan(ctx, conn, groupSelectors(catalogs))
	if err != nil {
		for _, g := range groups {
			state.NonEmptyGroups[g] = NonEmptyState{Reason: fmt.Sprintf("Plan is not available: %v", err)}
		}
		return
	}
	maps.Copy(state.NonEmptyGroups, NonEmptyFromPlan(groups, result.Frames, result.UnreachableNodes))
}

// probeBarrier asks, read-only and briefly, whether every node has applied the newest schema
// revision of the target's listed resources; the nodes that lag become Notes, since rows
// routed to them may meet a schema they do not know yet. A liaison without the barrier
// leaves an Info line instead.
func probeBarrier(ctx context.Context, conn *grpc.ClientConn, state *TargetState) {
	rev := state.maxModRevision()
	if rev == 0 {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, barrierProbeTimeout+5*time.Second)
	defer cancel()
	resp, err := schemav1.NewSchemaBarrierServiceClient(conn).AwaitRevisionApplied(callCtx,
		&schemav1.AwaitRevisionAppliedRequest{MinRevision: rev, Timeout: durationpb.New(barrierProbeTimeout)})
	if err != nil {
		state.Info = append(state.Info, fmt.Sprintf("schema barrier probe skipped: %s", status.Convert(err).Message()))
		return
	}
	if resp.GetApplied() {
		return
	}
	for _, l := range resp.GetLaggards() {
		node := l.GetNode()
		if node == "" {
			node = "the target"
		}
		note := fmt.Sprintf("schema barrier: %s has applied schema revision %d, not yet %d; rows routed to it may fail until it catches up",
			node, l.GetCurrentModRevision(), rev)
		if l.GetReason() != "" {
			note += " (" + l.GetReason() + ")"
		}
		state.Notes = append(state.Notes, note)
	}
}

// groupSelectors returns one selector per catalog naming the given groups, from the schema.
func groupSelectors(groups map[string]commonv1.Catalog) []*transferv1.Selector {
	byCatalog := map[commonv1.Catalog][]string{}
	for g, c := range groups {
		byCatalog[c] = append(byCatalog[c], g)
	}
	out := make([]*transferv1.Selector, 0, len(byCatalog))
	for c, gs := range byCatalog {
		slices.Sort(gs)
		out = append(out, &transferv1.Selector{Catalog: c, Groups: gs})
	}
	slices.SortFunc(out, func(a, b *transferv1.Selector) int {
		return cmp.Compare(transfer.CatalogName(a.GetCatalog()), transfer.CatalogName(b.GetCatalog()))
	})
	return out
}
