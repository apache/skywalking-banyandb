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
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

// The gates that reject a work item.
const (
	GateSchema   = "SCHEMA"
	GateTTL      = "TTL"
	GateCoverage = "COVERAGE"
	// GateRoute refuses a work item whose target route cannot be written to.
	GateRoute = "ROUTE"
	// GateRows refuses a work item with rows the liaison would refuse.
	GateRows = "ROWS"
)

// Flags are the command flags that change the decision.
type Flags struct {
	ForceTTL       bool
	AllSources     bool
	StrictCoverage bool
	// SchemaOnly writes the schema only, so the data gates refuse nothing.
	SchemaOnly bool
}

// Inputs is everything Decide reads.
type Inputs struct {
	// Now is the reference time of the TTL gate; it never shows in the output.
	Now      time.Time
	Artifact *Artifact
	// Target is the predicted target (PredictState): groups to be created carry the artifact's spec.
	Target *TargetState
	// Endpoint names the liaison for the header, e.g. "liaison-dst:17912 (ROLE_LIAISON)".
	Endpoint string
	Actions  []SchemaAction
	Flags    Flags
}

// Header describes the target and the artifact.
type Header struct {
	Endpoint        string `json:"endpoint"`
	Input           string `json:"input"`
	Format          string `json:"format"`
	ExportedAt      string `json:"exportedAt"`
	ArtifactVersion int    `json:"artifactVersion"`
	DataNodes       int    `json:"dataNodes"`
	Standalone      bool   `json:"standalone"`
}

// Entry is a work item the import pushes. Its JSON form adds the key and rows of the item.
type Entry struct {
	// TargetStage is the lower-case stage the rows go to; "" for property.
	TargetStage string `json:"targetStage"`
	// TargetNodes are the distinct data nodes of the target route, sorted.
	TargetNodes []string `json:"targetNodes"`
	// Item holds only the chosen sources.
	Item   WorkItem `json:"item"`
	Copies uint32   `json:"copies"`
	// TTLExpired marks an item the TTL gate would reject and --force-ttl let through.
	TTLExpired bool `json:"ttlExpired,omitempty"`
}

// Rejection is a work item a gate refused. Its JSON form adds the key and rows of the item.
type Rejection struct {
	Gate        string   `json:"gate"`
	Reason      string   `json:"reason"`
	TargetStage string   `json:"targetStage"`
	TargetNodes []string `json:"targetNodes"`
	Item        WorkItem `json:"item"`
}

// MarshalJSON renders the entry with the key and rows of its item.
func (e Entry) MarshalJSON() ([]byte, error) {
	type plain Entry
	return json.Marshal(struct {
		Key string `json:"key"`
		plain
		Rows uint64 `json:"rows"`
	}{Key: e.Item.Key(), plain: plain(e), Rows: e.Item.Rows()})
}

// MarshalJSON renders the rejection with the key and rows of its item.
func (r Rejection) MarshalJSON() ([]byte, error) {
	type plain Rejection
	return json.Marshal(struct {
		Key string `json:"key"`
		plain
		Rows uint64 `json:"rows"`
	}{Key: r.Item.Key(), plain: plain(r), Rows: r.Item.Rows()})
}

// MultiSourceDecision is the source selection of a work item with more than one non-empty source.
type MultiSourceDecision struct {
	Key       string           `json:"key"`
	Decisions []SourceDecision `json:"decisions"`
}

// Plan is the decision of an import: what is pushed where, what is refused and why.
type Plan struct {
	Entries     []Entry               `json:"entries"`
	Rejected    []Rejection           `json:"rejected"`
	MultiSource []MultiSourceDecision `json:"multiSource"`
	Schema      []SchemaAction        `json:"schema"`
	// GateDetails explain the work items the gates refuse or let through expired, one line each;
	// the schema gate needs none, the schema says why.
	GateDetails []string `json:"gateDetails"`
	Warnings    []string `json:"warnings"`
	// Errors are artifact defects that keep the work items from being built; any refuses the import.
	Errors []string `json:"errors,omitempty"`
	// Blockers are target problems that refuse the whole import, e.g. a liaison without ImportService.
	Blockers []string `json:"blockers,omitempty"`
	// RowCheck is the result of the read-only row precheck; nil when it did not run.
	RowCheck *RowCheck `json:"rowCheck,omitempty"`
	Header   Header    `json:"header"`
	// SchemaOnly marks a --schema-only import: only a blocking schema action or an artifact error refuses it.
	SchemaOnly bool `json:"schemaOnly,omitempty"`
}

// SchemaBlocked reports whether any schema action refuses the import.
func (p *Plan) SchemaBlocked() bool {
	return slices.ContainsFunc(p.Schema, func(a SchemaAction) bool { return a.Disposition.Blocking() })
}

// Refused reports whether the import must stop before writing anything: a blocking schema
// action, an artifact error, or, unless only the schema is imported, a target blocker, a
// --strict-coverage, route or rows rejection, or every work item rejected.
func (p *Plan) Refused() bool {
	if p.SchemaBlocked() || len(p.Errors) > 0 {
		return true
	}
	if p.SchemaOnly {
		return false
	}
	if len(p.Blockers) > 0 || p.rejectedBy(GateCoverage) || p.rejectedBy(GateRoute) || p.rejectedBy(GateRows) {
		return true
	}
	return len(p.Entries) == 0 && len(p.Rejected) > 0
}

func (p *Plan) rejectedBy(gate string) bool {
	return slices.ContainsFunc(p.Rejected, func(r Rejection) bool { return r.Gate == gate })
}

// Decide is the pure decision of an import. It builds the work items of the artifact, rejects
// those the schema gate or the TTL gate refuses, selects the sources of the rest and maps
// each to its target stage, copies and nodes. Output is sorted, so identical
// inputs give an identical plan whatever the time.
func Decide(in Inputs) *Plan {
	m := in.Artifact.Manifest
	p := &Plan{
		Header: Header{
			Endpoint: in.Endpoint, Input: in.Artifact.Dir, Format: m.Format, ExportedAt: m.ExportedAt,
			ArtifactVersion: m.ArtifactVersion, DataNodes: len(in.Target.DataNodes), Standalone: in.Target.Standalone,
		},
		Entries: []Entry{}, Rejected: []Rejection{}, MultiSource: []MultiSourceDecision{},
		Schema: append([]SchemaAction{}, in.Actions...), GateDetails: []string{}, Warnings: slices.Concat(in.Artifact.Warnings, in.Target.Notes),
		Blockers: slices.Clone(in.Target.Blockers), SchemaOnly: in.Flags.SchemaOnly,
	}
	items, err := BuildWorkItems(m)
	if err != nil {
		p.Errors = append(p.Errors, err.Error())
	}
	for i := range items {
		decideItem(in, p, &items[i])
	}
	slices.SortFunc(p.Entries, func(a, b Entry) int { return compareItems(a.Item, b.Item) })
	slices.SortFunc(p.Rejected, func(a, b Rejection) int {
		return cmp.Or(compareItems(a.Item, b.Item), cmp.Compare(a.Gate, b.Gate))
	})
	slices.SortFunc(p.MultiSource, func(a, b MultiSourceDecision) int { return cmp.Compare(a.Key, b.Key) })
	p.GateDetails = nonNil(transfer.UniqueSorted(p.GateDetails))
	p.Warnings = nonNil(transfer.UniqueSorted(p.Warnings))
	return p
}

// nonNil keeps an empty list a list in json and yaml.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func decideItem(in Inputs, p *Plan, item *WorkItem) {
	key := item.Key()
	var chosen []string
	if item.Kind == ShardItem {
		inputs := item.SourceInputs()
		sel := SelectSources(item.Catalog, inputs)
		if in.Flags.AllSources {
			sel = AllSources(inputs)
		}
		chosen = sel.Chosen
		if countNonEmpty(sel.Decisions) > 1 {
			p.MultiSource = append(p.MultiSource, MultiSourceDecision{Key: key, Decisions: sel.Decisions})
			if in.Flags.AllSources && len(chosen) > 1 {
				p.Warnings = append(p.Warnings, fmt.Sprintf("%s: --all-sources pushes %d sources; replicated rows are written more than once", key, len(chosen)))
			}
		}
		for _, w := range sel.Warnings {
			p.Warnings = append(p.Warnings, key+": "+w)
		}
	} else {
		// Segment-level and property items are pushed from every source: the index-mode rows
		// are deduplicated by version and the target keeps each property's latest revision.
		for _, s := range item.Sources {
			if len(s.Files) > 0 {
				chosen = append(chosen, s.Node)
			}
		}
	}
	picked := item.WithSources(chosen)
	if !slices.ContainsFunc(picked.Sources, func(s WorkSource) bool { return len(s.Files) > 0 }) {
		return
	}
	targetStage := mapStage(in, p, &picked)
	copies, nodes := resolveRoute(in, p, &picked, targetStage)
	reject := func(gate, reason string) {
		p.Rejected = append(p.Rejected, Rejection{
			Item: picked, TargetStage: targetStage, TargetNodes: nodes, Gate: gate, Reason: reason,
		})
	}
	if reason := schemaRejection(p.Schema, &picked); reason != "" {
		reject(GateSchema, reason)
		return
	}
	if issue := in.Target.RouteIssues[NewRouteKey(item.Group, item.Stage)]; issue != "" {
		reject(GateRoute, issue)
		p.GateDetails = append(p.GateDetails, fmt.Sprintf("%s: the target route cannot be written to: %s", key, issue))
		return
	}
	var ttlDetailLine string
	if item.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
		ttlDetailLine = ttlGate(in, p, &picked, targetStage)
		if ttlDetailLine != "" && !in.Flags.ForceTTL {
			p.GateDetails = append(p.GateDetails, ttlDetailLine)
			reject(GateTTL, "every row is older than the target stage's retention")
			return
		}
	}
	// Index-mode segment items of a measure are deduplicated by version and the target keeps
	// each property's latest revision, so only the other items can duplicate rows when several
	// sources are pushed.
	duplicable := item.Kind == ShardItem || (item.Kind == SegmentItem && item.Catalog != commonv1.Catalog_CATALOG_MEASURE)
	if in.Flags.StrictCoverage && duplicable && len(picked.Sources) > 1 {
		reason := fmt.Sprintf("%d sources would be pushed and --strict-coverage refuses multi-source units", len(picked.Sources))
		reject(GateCoverage, reason)
		p.GateDetails = append(p.GateDetails, fmt.Sprintf("%s: %s", key, reason))
		return
	}
	expired := ttlDetailLine != ""
	if expired {
		p.GateDetails = append(p.GateDetails, ttlDetailLine)
		p.Warnings = append(p.Warnings, key+": imported although expired (--force-ttl); the rows are invisible to queries and removed by the next retention run")
	}
	p.Entries = append(p.Entries, Entry{
		Item: picked, TargetStage: targetStage, Copies: copies, TargetNodes: nodes, TTLExpired: expired,
	})
}

func countNonEmpty(ds []SourceDecision) int {
	n := 0
	for _, d := range ds {
		if d.Reason != ReasonEmpty {
			n++
		}
	}
	return n
}

// mapStage maps the item's source stage to its target stage. A group the target has answers
// with the stage of its Targets route, which falls back to hot for a stage the group lacks (and
// for every stage on a standalone target). Without a route (a group the import creates or
// overwrites, routed only once its schema is written), the stage is matched case-insensitively
// against the predicted group, with the same fallbacks.
// Property has no stage.
func mapStage(in Inputs, p *Plan, item *WorkItem) string {
	if item.Catalog == commonv1.Catalog_CATALOG_PROPERTY {
		return ""
	}
	stage, target := normalizeStage(item.Stage), transfer.DefaultStageName
	if r, ok := in.Target.Routes[NewRouteKey(item.Group, item.Stage)]; ok {
		target = normalizeStage(r.GetStage())
	} else if !in.Target.Standalone && stageIndex(in.Target.Groups[item.Group].GetResourceOpts(), stage) >= 0 {
		target = stage
	}
	switch {
	case target == stage:
	case in.Target.Groups[item.Group] == nil:
		// The target has no such group: the schema refuses the item, and no stage of it is
		// missing on its own.
	case in.Target.Standalone:
		p.Warnings = append(p.Warnings, fmt.Sprintf("group %s: the standalone target routes stage %q to %s", item.Group, item.Stage, target))
	default:
		p.Warnings = append(p.Warnings, fmt.Sprintf("group %s: the target has no stage %q; its units go to %s", item.Group, item.Stage, target))
	}
	return target
}

// schemaRejection explains why the schema refuses an item: a blocking action in its group.
func schemaRejection(actions []SchemaAction, item *WorkItem) string {
	var ids []string
	for i := range actions {
		a := &actions[i]
		if a.Group != item.Group || !a.Disposition.Blocking() {
			continue
		}
		ids = append(ids, string(a.Disposition)+" "+a.ID())
	}
	return strings.Join(ids, ", ")
}

// ttlGate returns the detail line of an item every row of which is older than now − the
// cumulative TTL of the target stage, and "" otherwise. A standalone target matches no stage
// and keeps the group ttl only.
func ttlGate(in Inputs, p *Plan, item *WorkItem, targetStage string) string {
	ro := in.Target.Groups[item.Group].GetResourceOpts()
	if ro.GetTtl() == nil {
		return ""
	}
	ttlStage := targetStage
	if in.Target.Standalone {
		ttlStage = ""
	}
	ttl, err := CumulativeTTL(ro, ttlStage)
	if err != nil {
		p.Warnings = append(p.Warnings, fmt.Sprintf("%s: TTL gate skipped: %v", item.Key(), err))
		return ""
	}
	newest, err := newestTime(item)
	if err != nil {
		p.Warnings = append(p.Warnings, fmt.Sprintf("%s: TTL gate skipped: %v", item.Key(), err))
		return ""
	}
	if newest == 0 || newest >= in.Now.Add(-ttl).UnixNano() {
		return ""
	}
	return ttlDetail(item, targetStage, newest, ttl, ttlBreakdown(ro, ttlStage), in.Flags.ForceTTL)
}

// newestTime is the newest row time of an item: the largest maxTimestamp of its sources'
// shards, or, for segment-level files, the last instant of the source segment (BuildWorkItems
// resolved it, or recorded why it could not).
func newestTime(item *WorkItem) (int64, error) {
	var newest int64
	for _, s := range item.Sources {
		newest = max(newest, s.MaxTimestamp)
	}
	if newest != 0 || item.Segment == "" {
		return newest, nil
	}
	return 0, item.timeErr
}

// resolveRoute returns the copies and target nodes of an item from the Targets answer, or the
// copies from the predicted group when the target has no route for it: one on a
// standalone target, which keeps a single copy of every non-property group.
func resolveRoute(in Inputs, p *Plan, item *WorkItem, targetStage string) (uint32, []string) {
	stage := normalizeStage(targetStage)
	r, ok := in.Target.Routes[NewRouteKey(item.Group, stage)]
	if !ok {
		g := in.Target.Groups[item.Group]
		if in.Target.Standalone && g.GetCatalog() != commonv1.Catalog_CATALOG_PROPERTY {
			return 1, []string{}
		}
		ro := g.GetResourceOpts()
		replicas := ro.GetReplicas()
		if idx := stageIndex(ro, stage); idx >= 0 {
			replicas = ro.GetStages()[idx].GetReplicas()
		}
		return replicas + 1, []string{}
	}
	nodes := nonNil(routeView{r: r}.nodes())
	if uint32(len(nodes)) < r.GetCopies() {
		p.Warnings = append(p.Warnings, fmt.Sprintf("group %s stage %s: %d copies wanted but only %d node(s) match; each row is written to every one of them",
			item.Group, stage, r.GetCopies(), len(nodes)))
	}
	return r.GetCopies(), nodes
}

// ComparePlans lists how b differs from a in what is pushed where and what the gates refuse;
// the schema dispositions are not compared, since a CREATE becomes OK once written. An entry
// whose route was unknown (a group the import creates or overwrites, routed only once written)
// prints copies=? nodes=?, which match any copies and nodes of the same entry in the other plan.
func ComparePlans(a, b *Plan) []string {
	as, bs := planLines(a), planLines(b)
	inA, inB := newLineSet(as), newLineSet(bs)
	var out []string
	for _, l := range as {
		if !inB.matches(l) {
			out = append(out, "- "+l)
		}
	}
	for _, l := range bs {
		if !inA.matches(l) {
			out = append(out, "+ "+l)
		}
	}
	return out
}

// routeFields is the part of an entry line an unknown route leaves open.
var routeFields = regexp.MustCompile(`copies=\S+ nodes=\S+`)

const unknownRoute = "copies=? nodes=?"

// lineSet holds plan lines exactly, with their route fields masked, and the masked lines that
// carried an unknown route.
type lineSet struct {
	exact, masked, wildcard map[string]struct{}
}

func newLineSet(lines []string) lineSet {
	set := lineSet{exact: map[string]struct{}{}, masked: map[string]struct{}{}, wildcard: map[string]struct{}{}}
	for _, l := range lines {
		set.exact[l] = struct{}{}
		m := routeFields.ReplaceAllLiteralString(l, unknownRoute)
		set.masked[m] = struct{}{}
		if strings.Contains(l, unknownRoute) {
			set.wildcard[m] = struct{}{}
		}
	}
	return set
}

// matches reports whether l equals a line of the set, the unknown route of either side matching any.
func (s lineSet) matches(l string) bool {
	if _, ok := s.exact[l]; ok {
		return true
	}
	m := routeFields.ReplaceAllLiteralString(l, unknownRoute)
	if _, ok := s.wildcard[m]; ok {
		return true
	}
	_, ok := s.masked[m]
	return ok && strings.Contains(l, unknownRoute)
}

func planLines(p *Plan) []string {
	lines := make([]string, 0, len(p.Entries)+len(p.Rejected))
	for _, e := range p.Entries {
		nodes := make([]string, 0, len(e.Item.Sources))
		for _, s := range e.Item.Sources {
			nodes = append(nodes, s.Node)
		}
		route := unknownRoute
		if len(e.TargetNodes) > 0 {
			route = fmt.Sprintf("copies=%d nodes=[%s]", e.Copies, strings.Join(e.TargetNodes, ","))
		}
		lines = append(lines, fmt.Sprintf("entry %s -> stage=%s %s sources=[%s] rows=%d ttlExpired=%t",
			e.Item.Key(), stageLabel(e.TargetStage), route, strings.Join(nodes, ","), e.Item.Rows(), e.TTLExpired))
	}
	for _, r := range p.Rejected {
		lines = append(lines, fmt.Sprintf("rejected %s gate=%s: %s", r.Item.Key(), r.Gate, r.Reason))
	}
	return lines
}

func stageLabel(stage string) string {
	if stage == "" {
		return "-"
	}
	return stage
}
