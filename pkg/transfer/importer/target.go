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
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// RouteKey keys a route by group and lower-case target stage ("hot" for the default stage).
type RouteKey struct {
	Group string
	Stage string
}

// NewRouteKey returns the key of a group's stage, lower-casing the stage and mapping "" to "hot".
func NewRouteKey(group, stage string) RouteKey {
	return RouteKey{Group: group, Stage: normalizeStage(stage)}
}

// normalizeStage is the one stage normalization of the importer: "" is the default stage and
// names are lower-cased, as the liaison matches them.
func normalizeStage(stage string) string {
	return strings.ToLower(transfer.NormalizeStage(stage))
}

// routeView reads a Targets route; the zero value has no node.
type routeView struct {
	r *transferv1.TargetsResponse
}

func routeOf(routes map[RouteKey]*transferv1.TargetsResponse, group, stage string) routeView {
	return routeView{r: routes[NewRouteKey(group, stage)]}
}

func (v routeView) targets(shard uint32) []string {
	for _, s := range v.r.GetShards() {
		if s.GetShardId() == shard {
			return s.GetNodeIds()
		}
	}
	return nil
}

// nodes returns the distinct data nodes of every shard, sorted; nil for none.
func (v routeView) nodes() []string {
	var out []string
	for _, s := range v.r.GetShards() {
		out = append(out, s.GetNodeIds()...)
	}
	return transfer.UniqueSorted(out)
}

// NonEmptyState tells whether a target group holds data, from an ExportService.Plan of it.
type NonEmptyState struct {
	// Reason explains an unknown state, e.g. "Plan is not available" or "unreachable nodes [a]".
	Reason string
	// Known is false when the Plan failed or some node was unreachable: an overwrite of the group then needs --force.
	Known bool
	// NonEmpty is true when any shard reports rows or parts, or any segment or property shard documents.
	NonEmpty bool
}

// TargetState is everything the importer read from the target cluster: its schema, the routes
// of the (group, stage) pairs the import touches, its data nodes and which groups hold data.
// The runtime fills it; DiffSchemas and Decide only read it.
type TargetState struct {
	Schemas
	// Routes are the primary liaison's Targets answers keyed by (group, lower-case stage): the
	// stage a route reports and, when it differs (a fallback to hot), the stage that was asked for.
	Routes map[RouteKey]*transferv1.TargetsResponse
	// RouteIssues explain, by the (group, stage) asked for, why a route cannot be written to:
	// a routing error, inactive nodes, a group being deleted, or liaisons that disagree.
	RouteIssues map[RouteKey]string
	// Blockers are target problems that refuse the whole import, e.g. a liaison without ImportService.
	Blockers []string
	// Notes are warnings found while collecting, e.g. nodes lagging behind the schema revision.
	Notes []string
	// Info are remarks for the log only, e.g. a skipped barrier probe.
	Info []string
	// NonEmptyGroups is keyed by group name; a missing group counts as unknown (fail closed).
	NonEmptyGroups map[string]NonEmptyState
	DataNodes      []*databasev1.Node
	Standalone     bool
}

// NewTargetState returns a TargetState with every map allocated.
func NewTargetState() *TargetState {
	return &TargetState{
		Schemas:        NewSchemas(),
		Routes:         map[RouteKey]*transferv1.TargetsResponse{},
		RouteIssues:    map[RouteKey]string{},
		NonEmptyGroups: map[string]NonEmptyState{},
	}
}

// NonEmptyFromPlan derives the NonEmptyState of groups from a Plan of them: a group is
// non-empty when any of its shards reports total_count or parts_count above zero, its segment
// level reports documents, or a property shard reports documents. Every group is unknown when
// some node was unreachable, since that node's data is not counted.
func NonEmptyFromPlan(groups []string, frames []*transferv1.UnitFrame, unreachable []string) map[string]NonEmptyState {
	out := make(map[string]NonEmptyState, len(groups))
	if len(unreachable) > 0 {
		for _, g := range groups {
			out[g] = NonEmptyState{Reason: "Plan could not reach nodes [" + strings.Join(unreachable, " ") + "]"}
		}
		return out
	}
	for _, g := range groups {
		out[g] = NonEmptyState{Known: true}
	}
	mark := func(group string) {
		if st, ok := out[group]; ok {
			st.NonEmpty = true
			out[group] = st
		}
	}
	for _, f := range frames {
		for _, u := range f.GetUnits() {
			if p := u.GetProperty(); p != nil {
				for _, s := range p.GetShards() {
					if s.GetDocCount() > 0 {
						mark(p.GetGroup())
					}
				}
				continue
			}
			seg := u.GetSegment()
			group := seg.GetUnit().GetGroup()
			if seg.GetSegmentLevel().GetDocCount() > 0 {
				mark(group)
			}
			for _, s := range seg.GetShards() {
				if s.GetTotalCount() > 0 || s.GetPartsCount() > 0 {
					mark(group)
				}
			}
		}
	}
	return out
}

type withMetadata interface {
	GetMetadata() *commonv1.Metadata
}

func maxRevision[K comparable, T withMetadata](m map[K]T, cur int64) int64 {
	for _, v := range m {
		cur = max(cur, v.GetMetadata().GetModRevision())
	}
	return cur
}

// maxModRevision is the newest mod revision of the listed schema resources.
func (s *TargetState) maxModRevision() int64 {
	rev := maxRevision(s.Groups, 0)
	rev = maxRevision(s.Streams, rev)
	rev = maxRevision(s.Measures, rev)
	rev = maxRevision(s.Traces, rev)
	rev = maxRevision(s.Properties, rev)
	rev = maxRevision(s.IndexRules, rev)
	rev = maxRevision(s.IndexRuleBindings, rev)
	return maxRevision(s.TopNs, rev)
}
