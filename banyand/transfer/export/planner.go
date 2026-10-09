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

package export

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Planner produces the read-only export inventory of this node, either from the live data
// directories or from an export-session snapshot.
type Planner struct {
	meta       metadata.Repo
	backends   Backends
	sessions   *sessionManager
	nodeLabels map[string]string
	l          *logger.Logger
}

// NewPlanner wires a Planner. nodeLabels drive lifecycle-stage resolution.
func NewPlanner(meta metadata.Repo, backends Backends, nodeLabels map[string]string, l *logger.Logger) *Planner {
	return &Planner{
		meta:       meta,
		backends:   backends,
		sessions:   newSessionManager(backends, l),
		nodeLabels: nodeLabels,
		l:          l,
	}
}

// PlanInventory serves one PlanRequest. Without a session it walks the live data
// directories of the groups the schema registry knows; ReadSession walks (and renews) the
// session snapshot and takes its group list from the snapshot itself; CreateSession snapshots
// first and, when it preempted anything, closes the stream with a summary frame carrying those
// ids after the group frames (the liaison announces the session id itself). Every emitted unit
// frame covers exactly one group and leaves node_id empty for the liaison to stamp.
func (p *Planner) PlanInventory(ctx context.Context, req *transferv1.PlanRequest, emit func(*transferv1.PlanResponse) error) error {
	roots, summary, err := p.resolveRoots(ctx, req)
	if err != nil {
		return err
	}
	if err = p.planRoots(ctx, req, roots, emit); err != nil {
		if summary != nil {
			// The preempted sessions are gone even though the walk failed: report them
			// best-effort as the last frame before the error.
			if emitErr := emit(summary); emitErr != nil {
				p.l.Warn().Err(emitErr).Msg("cannot report the preempted sessions of a failed create")
			}
		}
		return err
	}
	if summary != nil {
		return emit(summary)
	}
	return nil
}

// planRoots emits one frame per selected group under roots. In session mode it finally
// checks that the session survived the walk, since a snapshot removed meanwhile reads as
// missing directories rather than as an error.
func (p *Planner) planRoots(ctx context.Context, req *transferv1.PlanRequest, roots map[commonv1.Catalog]string,
	emit func(*transferv1.PlanResponse) error,
) error {
	groups, err := p.planGroups(ctx, req, roots)
	if err != nil {
		return err
	}
	for _, g := range selectGroups(groups, req.GetSelectors()) {
		if err = ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		root, ok := roots[g.GetCatalog()]
		if !ok {
			continue // this catalog has no root on this node
		}
		frame, buildErr := p.planGroup(ctx, g, root)
		if buildErr != nil {
			if errors.Is(buildErr, context.Canceled) || errors.Is(buildErr, context.DeadlineExceeded) {
				return status.FromContextError(buildErr).Err()
			}
			return status.Errorf(codes.Internal, "plan group %s: %v", g.GetMetadata().GetName(), buildErr)
		}
		if frame == nil {
			continue
		}
		if err = emit(frame); err != nil {
			return err
		}
	}
	if req.GetSession() != nil {
		return p.sessions.verify(sessionID(req))
	}
	return nil
}

// resolveRoots picks the groups root of every catalog for this request: the live
// directories, an existing session's snapshot (renewing it), or a freshly created one.
func (p *Planner) resolveRoots(ctx context.Context, req *transferv1.PlanRequest) (map[commonv1.Catalog]string, *transferv1.PlanResponse, error) {
	switch sess := req.GetSession().(type) {
	case nil:
		roots := make(map[commonv1.Catalog]string, len(p.backends))
		for c, b := range p.backends {
			roots[c] = b.GetDataPath()
		}
		return roots, nil, nil
	case *transferv1.PlanRequest_Create:
		if sess.Create.GetId() == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "create session requires the session id the liaison generated")
		}
		created, err := p.sessions.create(ctx, sess.Create.GetId(), p.selectedCatalogs(req.GetSelectors()), sess.Create.GetPreempt())
		if err != nil {
			return nil, nil, err
		}
		if len(created.preempted) == 0 {
			return created.roots, nil, nil
		}
		return created.roots, &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{
			Summary: &transferv1.PlanSummary{PreemptedSessionIds: created.preempted},
		}}, nil
	case *transferv1.PlanRequest_Read:
		if sess.Read.GetId() == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "read session requires the session id")
		}
		roots, err := p.sessions.locateAndHeartbeat(sess.Read.GetId())
		return roots, nil, err
	default:
		return nil, nil, status.Errorf(codes.InvalidArgument, "unknown session kind %T", sess)
	}
}

// Sessions serves one SessionsRequest with the single frame this node answers. ACTION_LIST
// answers `session` with the lease held here, or `none`. ACTION_HEARTBEAT extends the lease of
// session_id and answers `done`; a session this node does not hold is answered as `error` so
// the liaison can tell which nodes lost it, while an expired or unreadable lease fails the
// call (FailedPrecondition). ACTION_RELEASE drops the session, idempotently, and answers
// `done` when the node held it and `none` when it did not; a failure is answered as `error`
// too, since a half-released session must stay visible rather than fail the fan-out.
func (p *Planner) Sessions(req *transferv1.SessionsRequest) (*transferv1.SessionsResponse, error) {
	id := req.GetSessionId()
	if req.GetAction() != transferv1.SessionsRequest_ACTION_LIST && id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "action %s requires the session id", req.GetAction())
	}
	switch req.GetAction() {
	case transferv1.SessionsRequest_ACTION_LIST:
		lease, err := p.sessions.probe()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list export sessions: %v", err)
		}
		if lease == nil {
			return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}}, nil
		}
		return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Session{Session: lease}}, nil
	case transferv1.SessionsRequest_ACTION_HEARTBEAT:
		_, err := p.sessions.locateAndHeartbeat(id)
		switch {
		case err == nil:
			return doneFrame(), nil
		case status.Code(err) == codes.NotFound:
			return errorFrame(err), nil
		default:
			return nil, err
		}
	case transferv1.SessionsRequest_ACTION_RELEASE:
		held, err := p.sessions.release(id)
		if err != nil {
			return errorFrame(err), nil
		}
		if !held {
			return noneFrame(), nil
		}
		return doneFrame(), nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown sessions action %s", req.GetAction())
	}
}

func noneFrame() *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}}
}

func doneFrame() *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Done{Done: &transferv1.Ack{}}}
}

func errorFrame(err error) *transferv1.SessionsResponse {
	return &transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Error{Error: status.Convert(err).Message()}}
}

// selectedCatalogs returns the catalogs a selector list touches; empty means all four.
func (p *Planner) selectedCatalogs(selectors []*transferv1.Selector) map[commonv1.Catalog]struct{} {
	out := map[commonv1.Catalog]struct{}{}
	if len(selectors) == 0 {
		for c := range p.backends {
			out[c] = struct{}{}
		}
		return out
	}
	for _, s := range selectors {
		if _, ok := p.backends[s.GetCatalog()]; ok {
			out[s.GetCatalog()] = struct{}{}
		}
	}
	return out
}

// sessionID is the id of the session a request drives, empty for a dry run.
func sessionID(req *transferv1.PlanRequest) string {
	if c := req.GetCreate(); c != nil {
		return c.GetId()
	}
	return req.GetRead().GetId()
}

// planGroups lists the groups a request inventories: the group directories under the root of
// every catalog the selectors name (every catalog without selectors) that the schema registry
// still knows. Only the root differs between the modes: the live data directory for a dry run,
// the session snapshot otherwise. A directory whose group the registry no longer knows is
// skipped with a warning. A selector naming a catalog without a root is refused: in a session
// that catalog holds no snapshot, and silently skipping it would hide a half-lost session.
func (p *Planner) planGroups(ctx context.Context, req *transferv1.PlanRequest, roots map[commonv1.Catalog]string) ([]*commonv1.Group, error) {
	for _, s := range req.GetSelectors() {
		if _, ok := roots[s.GetCatalog()]; !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "export session %s has no snapshot for catalog %s on this node",
				sessionID(req), s.GetCatalog())
		}
	}
	registered, err := p.meta.GroupRegistry().ListGroup(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list groups: %v", err)
	}
	known := make(map[commonv1.Catalog]map[string]*commonv1.Group, len(roots))
	for _, g := range registered {
		if known[g.GetCatalog()] == nil {
			known[g.GetCatalog()] = map[string]*commonv1.Group{}
		}
		known[g.GetCatalog()][g.GetMetadata().GetName()] = g
	}
	var out []*commonv1.Group
	for _, catalog := range slices.Sorted(maps.Keys(p.selectedCatalogs(req.GetSelectors()))) {
		root, ok := roots[catalog]
		if !ok {
			continue
		}
		entries, readErr := readDirOrEmpty(root)
		if readErr != nil {
			return nil, status.Errorf(codes.Internal, "list groups of %s under %s: %v", catalog, root, readErr)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			g, registeredGroup := known[catalog][e.Name()]
			if !registeredGroup {
				p.l.Warn().Str("group", e.Name()).Stringer("catalog", catalog).Str("dir", root).
					Msg("skipping a group directory the schema registry does not know")
				continue
			}
			out = append(out, g)
		}
	}
	sortGroups(out)
	return out, nil
}

func sortGroups(groups []*commonv1.Group) {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].GetCatalog() != groups[j].GetCatalog() {
			return groups[i].GetCatalog() < groups[j].GetCatalog()
		}
		return groups[i].GetMetadata().GetName() < groups[j].GetMetadata().GetName()
	})
}

// selectGroups narrows groups to what the selectors name: a selector without groups takes
// the whole catalog. No selectors means every group.
func selectGroups(groups []*commonv1.Group, selectors []*transferv1.Selector) []*commonv1.Group {
	if len(selectors) == 0 {
		return groups
	}
	wholeCatalog := make(map[commonv1.Catalog]bool, len(selectors))
	named := make(map[commonv1.Catalog]map[string]struct{}, len(selectors))
	for _, s := range selectors {
		if len(s.GetGroups()) == 0 {
			wholeCatalog[s.GetCatalog()] = true
			continue
		}
		if named[s.GetCatalog()] == nil {
			named[s.GetCatalog()] = map[string]struct{}{}
		}
		for _, g := range s.GetGroups() {
			named[s.GetCatalog()][g] = struct{}{}
		}
	}
	var out []*commonv1.Group
	for _, g := range groups {
		if wholeCatalog[g.GetCatalog()] {
			out = append(out, g)
			continue
		}
		if _, ok := named[g.GetCatalog()][g.GetMetadata().GetName()]; ok {
			out = append(out, g)
		}
	}
	return out
}

// planGroup builds the single frame of one group under root, or nil when the group has no
// data there.
func (p *Planner) planGroup(ctx context.Context, g *commonv1.Group, root string) (*transferv1.PlanResponse, error) {
	name := g.GetMetadata().GetName()
	stage, err := p.resolveStage(g)
	if err != nil {
		return nil, err
	}
	groupDir := filepath.Join(root, name)
	var units []*transferv1.UnitInventory
	var read partReadFunc
	if pr, ok := p.backends[g.GetCatalog()].(PartReader); ok {
		read = pr.ReadPartMetadata
	}
	switch g.GetCatalog() {
	case commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_TRACE:
		units, err = statTSDBGroup(groupDir, g.GetCatalog(), name, false, read)
	case commonv1.Catalog_CATALOG_MEASURE:
		hasSegments, segErr := hasSegmentDir(groupDir)
		if segErr != nil {
			return nil, segErr
		}
		indexMode := false
		if hasSegments { // a group without segments has nothing to count: skip the registry
			if indexMode, err = p.hasIndexModeMeasure(ctx, name); err != nil {
				return nil, err
			}
		}
		units, err = statTSDBGroup(groupDir, g.GetCatalog(), name, indexMode, read)
	case commonv1.Catalog_CATALOG_PROPERTY:
		var unit *transferv1.UnitInventory
		unit, err = statPropertyGroup(groupDir, name)
		if unit != nil {
			units = []*transferv1.UnitInventory{unit}
		}
	default:
		return nil, fmt.Errorf("unsupported catalog %s", g.GetCatalog())
	}
	if err != nil {
		return nil, err
	}
	if len(units) == 0 {
		return nil, nil
	}
	return &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{Stage: stage, Units: units}}}, nil
}

// resolveStage returns the matched lifecycle stage name, or "" for the default tier.
func (p *Planner) resolveStage(g *commonv1.Group) (string, error) {
	ro := g.GetResourceOpts()
	if ro == nil || len(ro.GetStages()) == 0 || len(p.nodeLabels) == 0 {
		return "", nil
	}
	_, matched, _, err := pub.ResolveStageResourceOpts(ro, p.nodeLabels)
	if err != nil {
		return "", err
	}
	return matched.GetName(), nil
}

// hasIndexModeMeasure reports whether the group holds an index-mode measure, whose rows live
// only in the segment-level series index. The index also carries the series documents of the
// group's regular measures, so the resulting doc_count is an upper bound of the rows.
func (p *Planner) hasIndexModeMeasure(ctx context.Context, group string) (bool, error) {
	measures, err := p.meta.MeasureRegistry().ListMeasure(ctx, schema.ListOpt{Group: group})
	if err != nil {
		return false, err
	}
	for _, m := range measures {
		if m.GetIndexMode() {
			return true, nil
		}
	}
	return false, nil
}
