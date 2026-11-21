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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

const (
	// ProgressFileName is the default progress file of an import, below its artifact directory;
	// --checkpoint names another path, such as one per target cluster.
	ProgressFileName = config.ImportProgressFileName
	// cursorSide names the side file of a CSV import's cursors (resume.SidePath).
	cursorSide = "cursor"
	// planSide names the side file of the plan the first run recorded and of its baseline
	// (resume.SidePath): written with the plan, written again to add the baseline once it is
	// counted, and later only to append the items a gate refused then and accepts now, or to add
	// to the baseline the groups or target nodes it lacks.
	planSide        = "plan"
	progressKind    = "import-progress"
	progressVersion = 2
)

// ProgressPath is the progress file of an import: --checkpoint, or ProgressFileName below the
// artifact directory.
func ProgressPath(opts config.Import) string {
	if opts.Checkpoint != "" {
		return opts.Checkpoint
	}
	return filepath.Join(opts.Input, ProgressFileName)
}

// progressState is the progress file of an import: what the first run decided and counted,
// and what every run since put into the target, per work item and per payload or CSV position.
type progressState struct {
	// baseline is the baseline the plan side file holds, read with the plan; nil while none is
	// recorded.
	baseline  *Counts
	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// FinishedAt is when a run pushed the last work item; a later run pushes nothing.
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	// Inflight is the native payload each worker sent and has no answer for yet.
	Inflight map[string]inflightPayload `json:"inflight"`
	// Done is the progress of every work item that has any, by WorkItem.Key.
	Done     map[string]*itemRecord `json:"done"`
	Bindings importBindings         `json:"bindings"`
	Format   string                 `json:"format"`
	// BaselineAt is when the baseline, the target's counts before the first push, was counted;
	// nil while the plan side file records none.
	BaselineAt *time.Time `json:"baselineAt,omitempty"`
	// Pushed are the rows every run put into the target, per (catalog, group, stage, kind),
	// for --verify; Duplicates the rows pushed again that may have landed already.
	Pushed     []rowCount `json:"pushed"`
	Duplicates []rowCount `json:"duplicates,omitempty"`
	// Plan is the plan the plan side file holds, read when the progress file is loaded.
	Plan []plannedItem `json:"-"`
	// PlanDigest is the digest of the plan side file (resume.WriteSide); "" while none is written.
	PlanDigest string `json:"planDigest,omitempty"`
	// PlanNextDigest is the digest of the plan side file being rewritten (tracker.rewritePlan): a
	// crash between the rewrite and the next write of this file leaves a side file of this digest.
	PlanNextDigest string `json:"planNextDigest,omitempty"`
	// SkippedPayloads name what resumeDuplicationPolicy skip left out and no verify has passed
	// since: every run forces --verify while any is listed.
	SkippedPayloads []string `json:"skippedPayloads,omitempty"`
	resume.Header
	// FoldSeq counts the folds of the CSV cursors into this file; the cursor side file carries
	// the FoldSeq it was written at and is newer than this file only when it is not lower.
	FoldSeq uint64 `json:"foldSeq,omitempty"`
	// NoBaseline records that the first run could not count the target before its push and
	// needed no baseline: a verify forced later has nothing to compare against.
	NoBaseline bool `json:"noBaseline,omitempty"`
	// VerifyPending records that the run which finished the push was to verify it and no verify
	// has passed since: a later run verifies again instead of only saying it is done.
	VerifyPending bool `json:"verifyPending,omitempty"`
}

// importBindings are what must not change between the run that wrote a progress file and the
// run that resumes it, in clear so that a refusal can say what differs. The target is the
// exception: it is identified by its liaisons and data nodes, which may change in part (see
// diffBindings).
type importBindings struct {
	Options        map[string]string `json:"options"`
	ManifestDigest string            `json:"manifestDigest"`
	// Liaisons are the names of the target's liaisons, sorted; a standalone target's own name.
	Liaisons []string `json:"liaisons"`
	// TargetNodes are the names of the target's data nodes, sorted; a standalone target's own name.
	TargetNodes []string `json:"targetNodes"`
}

// plannedItem is a work item as the first run planned it.
type plannedItem struct {
	Key         string   `json:"key"`
	TargetStage string   `json:"targetStage"`
	Sources     []string `json:"sources"`
	TargetNodes []string `json:"targetNodes"`
}

// inflightPayload is a native payload sent and not answered yet.
type inflightPayload struct {
	Key    string   `json:"key"`
	Source string   `json:"source"`
	Kind   string   `json:"kind"`
	Nodes  []string `json:"nodes"`
	PartID uint64   `json:"partID,omitempty"`
	Rows   uint64   `json:"rows,omitempty"`
}

// itemRecord is the progress of one work item: its native payloads, or its CSV cursor.
type itemRecord struct {
	// Parts and Blobs key the native payloads by "<source>/<part id>" and "<source>/<KIND>".
	Parts  map[string]*payloadRecord `json:"parts,omitempty"`
	Blobs  map[string]*payloadRecord `json:"blobs,omitempty"`
	Cursor *csvCursor                `json:"cursor"`
	// Landed are, once a native stream or trace item is complete, the rows of its parts that
	// landed per target node, and Tainted the nodes where a part was pushed again or left out:
	// all a resume needs of a complete item's parts, which are dropped then.
	Landed  map[string]uint64 `json:"landed,omitempty"`
	Tainted []string          `json:"tainted,omitempty"`
	// Complete marks an item every payload or batch of which is done.
	Complete bool `json:"complete"`
}

// payloadRecord is where a native payload landed: Nodes stored it, Missing failed it before its
// completion was sent, CompletionSent failed it after (it may be stored there), Skipped were
// left out by resumeDuplicationPolicy skip, and Repushed had it pushed again by the policy
// although it may have landed there already. Causes names, per node of CompletionSent, why the
// liaison reported it failed (failureCauseName); a node the liaison stopped waiting for
// (causeNoAnswer) may still have been committing the payload, so counts read as not landed
// cannot decide it.
type payloadRecord struct {
	Causes         map[string]string `json:"causes,omitempty"`
	Nodes          []string          `json:"nodes"`
	Missing        []string          `json:"missing"`
	CompletionSent []string          `json:"completionSent,omitempty"`
	Skipped        []string          `json:"skipped,omitempty"`
	Repushed       []string          `json:"repushed,omitempty"`
	Rows           uint64            `json:"rows,omitempty"`
}

// eachPayload calls fn with every native payload of the record, part telling a part from a blob.
func (r *itemRecord) eachPayload(fn func(id string, p *payloadRecord, part bool)) {
	for _, id := range slices.Sorted(maps.Keys(r.Parts)) {
		fn(id, r.Parts[id], true)
	}
	for _, id := range slices.Sorted(maps.Keys(r.Blobs)) {
		fn(id, r.Blobs[id], false)
	}
}

// done tells whether the payload needs nothing more.
func (r *payloadRecord) done() bool { return len(r.Missing) == 0 && len(r.CompletionSent) == 0 }

func (r *payloadRecord) clone() *payloadRecord {
	return &payloadRecord{
		Nodes: slices.Clone(r.Nodes), Missing: slices.Clone(r.Missing), CompletionSent: slices.Clone(r.CompletionSent),
		Causes: maps.Clone(r.Causes), Skipped: slices.Clone(r.Skipped), Repushed: slices.Clone(r.Repushed), Rows: r.Rows,
	}
}

// clone copies the record and its payloads.
func (r *itemRecord) clone() *itemRecord {
	cp := &itemRecord{Complete: r.Complete, Landed: maps.Clone(r.Landed), Tainted: slices.Clone(r.Tainted)}
	if r.Parts != nil {
		cp.Parts = make(map[string]*payloadRecord, len(r.Parts))
		for id, p := range r.Parts {
			cp.Parts[id] = p.clone()
		}
	}
	if r.Blobs != nil {
		cp.Blobs = make(map[string]*payloadRecord, len(r.Blobs))
		for id, p := range r.Blobs {
			cp.Blobs[id] = p.clone()
		}
	}
	cp.Cursor = r.Cursor.clone()
	return cp
}

// compact keeps of a complete item's parts what a resume needs: the rows landed per node and
// the tainted nodes. Its blobs and cursor are dropped.
func (r *itemRecord) compact() {
	for _, p := range r.Parts {
		for _, n := range p.Nodes {
			if r.Landed == nil {
				r.Landed = map[string]uint64{}
			}
			r.Landed[n] += p.Rows
		}
		r.Tainted = append(r.Tainted, p.Skipped...)
		r.Tainted = append(r.Tainted, p.Repushed...)
	}
	if len(r.Tainted) > 0 {
		r.Tainted = transfer.UniqueSorted(r.Tainted)
	}
	r.Parts, r.Blobs, r.Cursor = nil, nil, nil
}

// csvCursor is the last acknowledged record of a CSV work item: every record up to Record of
// File, and every file before it in the item's order, is stored. A cursor without a file marks
// an item started with nothing acknowledged yet. Rows counts the rows acknowledged so far.
// Sent is the last record of the last batch sent, recorded before the batch is sent: the rows
// after the cursor up to Sent may have landed, the rows after Sent were never sent.
// SendLimit is how many rows of the item the run may send, as of the last synced write of the
// progress file: a batch ending after it is sent only once a synced write has raised it, so that
// whatever the unsynced cursor side file loses, the rows that may have landed are the rows after
// the cursor up to SendLimit. The synced write of a run that ends clears it. Rows is its unit,
// as the rows ahead of the sender have no position yet.
type csvCursor struct {
	Sent      *csvMark `json:"sent,omitempty"`
	File      string   `json:"file,omitempty"`
	Record    uint64   `json:"record,omitempty"`
	Rows      uint64   `json:"rows"`
	SendLimit uint64   `json:"sendLimit,omitempty"`
	// limitWindow marks a cursor whose window the cursor side file could not tell, so that Sent
	// was set to SendLimit when the progress file was read (progressState.foldCursorSide).
	limitWindow bool
}

// csvMark is a position of a CSV work item: the record Record of File, with Rows rows of the
// item up to it.
type csvMark struct {
	File   string `json:"file"`
	Record uint64 `json:"record"`
	Rows   uint64 `json:"rows"`
}

func (c *csvCursor) clone() *csvCursor {
	if c == nil {
		return nil
	}
	cp := *c
	if c.Sent != nil {
		s := *c.Sent
		cp.Sent = &s
	}
	return &cp
}

// window is how many rows after the cursor were sent: they may have landed already.
func (c *csvCursor) window() uint64 {
	if c.Sent == nil || c.Sent.Rows <= c.Rows {
		return 0
	}
	return c.Sent.Rows - c.Rows
}

// rowCount is a number of rows of one PushKey; Payloads name what a duplicate count may come
// from.
type rowCount struct {
	Catalog  string   `json:"catalog"`
	Group    string   `json:"group"`
	Stage    string   `json:"stage"`
	Kind     string   `json:"kind"`
	Payloads []string `json:"payloads,omitempty"`
	Rows     uint64   `json:"rows"`
}

func pushKeyOf(r rowCount) (PushKey, error) {
	catalog, ok := transfer.CatalogByName(r.Catalog)
	if !ok {
		return PushKey{}, fmt.Errorf("unknown catalog %q", r.Catalog)
	}
	var kind WorkItemKind
	switch r.Kind {
	case ShardItem.String():
		kind = ShardItem
	case SegmentItem.String():
		kind = SegmentItem
	case PropertyItem.String():
		kind = PropertyItem
	default:
		return PushKey{}, fmt.Errorf("unknown work item kind %q", r.Kind)
	}
	return PushKey{streamKey: streamKey{catalog: catalog, group: r.Group, stage: r.Stage}, Kind: kind}, nil
}

func rowCountOf(k PushKey, n uint64) rowCount {
	return rowCount{Catalog: transfer.CatalogName(k.catalog), Group: k.group, Stage: k.stage, Kind: k.Kind.String(), Rows: n}
}

// rowCounts renders a PushKey map in a stable order.
func rowCounts(m map[PushKey]uint64) []rowCount {
	out := make([]rowCount, 0, len(m))
	for k, n := range m {
		out = append(out, rowCountOf(k, n))
	}
	slices.SortFunc(out, func(a, b rowCount) int {
		return cmp.Or(cmp.Compare(a.Catalog, b.Catalog), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage), cmp.Compare(a.Kind, b.Kind))
	})
	return out
}

// pushedMap reads recorded row counts back.
func pushedMap(counts []rowCount) (map[PushKey]uint64, error) {
	out := map[PushKey]uint64{}
	for _, c := range counts {
		k, err := pushKeyOf(c)
		if err != nil {
			return nil, err
		}
		out[k] += c.Rows
	}
	return out, nil
}

// recordedCounts is Counts in JSON: a struct key cannot be a JSON object key.
type recordedCounts struct {
	// Properties are the documents per property group; PropertiesCounted tells they were
	// counted (a baseline recorded by an older version lacks them).
	Properties        map[string]int64 `json:"properties,omitempty"`
	Shards            []countRecord    `json:"shards"`
	Segments          []countRecord    `json:"segments,omitempty"`
	Unreachable       []string         `json:"unreachable,omitempty"`
	Answered          []string         `json:"answered"`
	PropertiesCounted bool             `json:"propertiesCounted,omitempty"`
}

type countRecord struct {
	Segments map[string]int64 `json:"segments"`
	Catalog  string           `json:"catalog"`
	Group    string           `json:"group"`
	Stage    string           `json:"stage"`
	Node     string           `json:"node"`
	Shard    uint32           `json:"shard"`
}

func recordCounts(c *Counts) *recordedCounts {
	conv := func(m map[CountKey]map[string]int64) []countRecord {
		out := make([]countRecord, 0, len(m))
		for k, segs := range m {
			out = append(out, countRecord{Catalog: transfer.CatalogName(k.Catalog), Group: k.Group, Stage: k.Stage, Node: k.Node, Shard: k.Shard, Segments: segs})
		}
		slices.SortFunc(out, func(a, b countRecord) int {
			return cmp.Or(cmp.Compare(a.Catalog, b.Catalog), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Stage, b.Stage),
				cmp.Compare(a.Node, b.Node), cmp.Compare(a.Shard, b.Shard))
		})
		return out
	}
	return &recordedCounts{
		Shards: conv(c.Shards), Segments: conv(c.Segments), Unreachable: slices.Clone(c.Unreachable), Answered: slices.Clone(c.Answered),
		Properties: maps.Clone(c.Properties), PropertiesCounted: c.PropertiesCounted,
	}
}

func (r *recordedCounts) counts() (*Counts, error) {
	c := &Counts{
		Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}, Unreachable: slices.Clone(r.Unreachable),
		Answered: slices.Clone(r.Answered), Properties: maps.Clone(r.Properties), PropertiesCounted: r.PropertiesCounted,
	}
	for _, part := range []struct {
		dst map[CountKey]map[string]int64
		src []countRecord
	}{{c.Shards, r.Shards}, {c.Segments, r.Segments}} {
		for _, rec := range part.src {
			catalog, ok := transfer.CatalogByName(rec.Catalog)
			if !ok {
				return nil, fmt.Errorf("baseline: unknown catalog %q", rec.Catalog)
			}
			k := CountKey{Catalog: catalog, Group: rec.Group, Stage: rec.Stage, Node: rec.Node, Shard: rec.Shard}
			for seg, n := range rec.Segments {
				addCount(part.dst, k, seg, n)
			}
		}
	}
	return c, nil
}

// bindingsOf records the artifact's manifest (its digest), the target's liaisons and data nodes
// and the options that change what is pushed.
func bindingsOf(manifestBytes []byte, info *cluster.Info, opts *config.Import) importBindings {
	var liaisons []string
	if info != nil {
		liaisons = transfer.UniqueSorted(info.Liaisons)
	}
	return importBindings{
		ManifestDigest: resume.DigestBytes(manifestBytes),
		Liaisons:       liaisons,
		TargetNodes:    info.DataNodeNames(),
		Options: map[string]string{
			"allSources":    strconv.FormatBool(opts.AllSources),
			"includeSchema": strconv.FormatBool(opts.IncludeSchema),
		},
	}
}

// anotherClusterHint is how to import an artifact into a cluster other than the one a progress
// file records.
const anotherClusterHint = "to import this artifact into another cluster give --checkpoint <another path>"

// otherTargetHint is how to import an artifact into a cluster other than the one a progress file
// records, or into the same one again.
const otherTargetHint = anotherClusterHint + "; to import it again into this cluster give --restart"

// diffBindings names every binding of the progress file this invocation differs in, field by
// field, "" when none does. The target is the cluster the progress file records when some of its
// liaisons and some of its data nodes still are liaisons and data nodes of the target: liaisons
// and data nodes may be added, removed or restarted, but a cluster sharing none of them is another
// one. Every target node of an item of the recorded plan that is not complete must also still be
// a data node of the target. A progress file without the liaisons is of an older format and
// cannot be checked. A schema-only import reads no progress file, so the schemaOnly option an
// earlier version recorded is not compared.
func diffBindings(state *progressState, current importBindings) string {
	rec := state.Bindings
	liaisons, nodes := targetDiffs(state, current)
	return resume.DiffBindings([]resume.Binding{
		{Name: "manifestDigest", Recorded: rec.ManifestDigest, Current: current.ManifestDigest, Hint: "the artifact's manifest.json"},
		{Name: "liaisons", Hint: "the liaisons of the target cluster", Details: liaisons},
		{Name: "targetNodes", Hint: "the data nodes of the target cluster", Details: nodes},
		{Name: "options", Hint: "--all-sources, --include-schema", Details: resume.OptionDiffs(recordedOptions(state), current.Options, defaultOptions(), nil)},
	})
}

// defaultOptions are the bound options at their defaults, which a refusal marks "(default)".
func defaultOptions() map[string]string {
	opts := &config.Import{}
	config.Defaults(opts)
	return bindingsOf(nil, nil, opts).Options
}

// recordedOptions are the options the progress file records, without the schemaOnly option an
// earlier version recorded.
func recordedOptions(state *progressState) map[string]string {
	recorded := maps.Clone(state.Bindings.Options)
	delete(recorded, "schemaOnly")
	return recorded
}

// targetDiffs says how the target differs from the one the progress file records, by its
// liaisons and by its data nodes; both are empty for the same target.
func targetDiffs(state *progressState, current importBindings) (liaisons, nodes []string) {
	rec := state.Bindings
	switch {
	case len(rec.Liaisons) == 0:
		liaisons = append(liaisons, "the progress file records no liaisons (it was written by an earlier version); its target cannot be checked")
	case !intersects(rec.Liaisons, current.Liaisons):
		liaisons = append(liaisons, fmt.Sprintf("recorded %v, now %v: none of the recorded liaisons is a liaison of the target", rec.Liaisons, current.Liaisons))
	}
	if !intersects(rec.TargetNodes, current.TargetNodes) {
		nodes = append(nodes, fmt.Sprintf("recorded %v, now %v: none of the recorded data nodes is a data node of the target", rec.TargetNodes, current.TargetNodes))
	}
	var needed []string
	for _, p := range state.Plan {
		if !state.complete(p.Key) {
			needed = append(needed, p.TargetNodes...)
		}
	}
	for _, n := range resume.Missing(needed, current.TargetNodes) {
		nodes = append(nodes, "target node "+n+" of the recorded import is no longer a data node of the target")
	}
	return liaisons, nodes
}

// intersects tells whether a and b share a value.
func intersects(a, b []string) bool {
	return slices.ContainsFunc(a, func(v string) bool { return slices.Contains(b, v) })
}

// recordedPlan is the content of the plan side file: the plan and, once counted, the baseline.
type recordedPlan struct {
	Baseline *recordedCounts `json:"baseline,omitempty"`
	Plan     []plannedItem   `json:"plan"`
}

// planRecord is the content of the plan side file for plan and baseline (nil: none recorded).
func planRecord(plan []plannedItem, baseline *Counts) recordedPlan {
	rec := recordedPlan{Plan: plan}
	if baseline != nil {
		rec.Baseline = recordCounts(baseline)
	}
	return rec
}

// writePlan writes the plan side file of the progress file at path, with the baseline when it is
// not nil, and returns its digest.
func writePlan(path string, plan []plannedItem, baseline *Counts) (string, error) {
	digest, err := resume.WriteSide(resume.SidePath(path, planSide), planRecord(plan, baseline))
	if err != nil {
		return "", fmt.Errorf("write the plan of progress file %s: %w", path, err)
	}
	return digest, nil
}

// rewritePlan rewrites the plan side file of the progress file with plan and baseline: the digest
// it will have is announced first (with announce, which may change more), so that a crash in
// between leaves a side file that one of the two digests of the progress file names
// (readSideNext), and is recorded (with commit) once the side file is written. what names the
// change in errors.
func (t *tracker) rewritePlan(what string, plan []plannedItem, baseline *Counts, announce, commit func(s *progressState)) error {
	data, digest, err := resume.EncodeSide(planRecord(plan, baseline))
	if err != nil {
		return namingProgress(fmt.Errorf("encode the %s of progress file %s: %w", what, t.path, err))
	}
	if err = t.sync(func(s *progressState) {
		s.PlanNextDigest = digest
		announce(s)
	}); err != nil {
		return err
	}
	if err = resume.WriteSideBytes(resume.SidePath(t.path, planSide), data); err != nil {
		return namingProgress(fmt.Errorf("write the %s of progress file %s: %w", what, t.path, err))
	}
	return t.sync(func(s *progressState) {
		s.PlanDigest, s.PlanNextDigest = digest, ""
		commit(s)
	})
}

// appendPlan appends items to the recorded plan (rewritePlan). The import is no longer finished:
// it pushes the items now.
func (t *tracker) appendPlan(state *progressState, items []plannedItem) error {
	plan := append(slices.Clone(state.Plan), items...)
	return t.rewritePlan("plan", plan, state.baseline, func(*progressState) {},
		func(s *progressState) { s.Plan, s.FinishedAt = plan, nil })
}

// recordBaseline records the baseline counted at the first push (rewritePlan).
func (t *tracker) recordBaseline(state *progressState, baseline *Counts, at time.Time) error {
	if err := t.rewritePlan("baseline", state.Plan, baseline, func(*progressState) {},
		func(s *progressState) { s.BaselineAt = &at }); err != nil {
		return err
	}
	state.baseline = baseline
	return nil
}

// extendBaseline replaces the recorded baseline with merged, which adds to it the counts of groups
// or target nodes it lacks (rewritePlan).
func (t *tracker) extendBaseline(state *progressState, merged *Counts) error {
	if err := t.rewritePlan("baseline", state.Plan, merged, func(*progressState) {}, func(*progressState) {}); err != nil {
		return err
	}
	state.baseline = merged
	return nil
}

// planOf records the entries as planned.
func planOf(entries []Entry) []plannedItem {
	out := make([]plannedItem, len(entries))
	for i := range entries {
		e := &entries[i]
		sources := make([]string, len(e.Item.Sources))
		for j, s := range e.Item.Sources {
			sources[j] = s.Node
		}
		out[i] = plannedItem{Key: e.Item.Key(), Sources: sources, TargetStage: e.TargetStage, TargetNodes: slices.Clone(e.TargetNodes)}
	}
	return out
}

// kindName names a native payload kind in a payload key: SERIES_INDEX, ELEMENT_INDEX, PROPERTY.
func kindName(k nativefmt.Kind) string { return strings.TrimPrefix(k.String(), "KIND_") }

// payloadRef names one native payload of a work item.
type payloadRef struct {
	// id is "<source>/<part id>" for a part, "<source>/<KIND>" for any other payload.
	id     string
	source string
	kind   nativefmt.Kind
	partID uint64
	rows   uint64
}

func (r payloadRef) blob() bool { return r.kind != nativefmt.KindPart }

func partRef(source string, partID, rows uint64) payloadRef {
	return payloadRef{id: source + "/" + strconv.FormatUint(partID, 10), source: source, kind: nativefmt.KindPart, partID: partID, rows: rows}
}

func blobRef(source string, kind nativefmt.Kind) payloadRef {
	return payloadRef{id: source + "/" + kindName(kind), source: source, kind: kind}
}

// Test hooks: how the progress file and the cursor side file are written and removed. A failed
// write of the progress file names it. The cursor side file is replaced unsynced, through a
// temporary file named as one of its own so that a crash leaves nothing claimProgress misses: a
// power loss may lose it (the progress file is then used).
var (
	writeProgress = func(path string, v any) error { return namingProgress(resume.WriteFile(path, v)) }
	removeCursors = resume.RemoveFile
	writeCursors  = resume.ReplaceFileUnsynced
)

// tracker records the progress of an import in its progress file: one persister owns the file,
// and the CSV cursors of every batch sent and acknowledged go to the unsynced side file
// <path without .json>-cursor.json, folded into the progress file when an item starts, every checkpointEveryBatches
// acknowledged batches and when an item completes. Each fold first rewrites the side file with
// the next FoldSeq and then writes that FoldSeq into the progress file, so a side file is newer
// than its progress file exactly when its FoldSeq is not lower. A nil tracker records nothing.
type tracker struct {
	p *resume.Persister[progressState]
	// pushed, cursors, sideWritten, closed, seq and acks are guarded by mu, which also orders the
	// side file writes and the changes sent to the persister. Nobody waits for the progress file
	// to be written while holding mu: the changes are queued under it, and the persister writes them
	// in that order; only a full queue makes the one queueing wait.
	pushed  map[PushKey]uint64
	cursors map[string]*csvCursor
	// queued is the SendLimit of each CSV item as queued for the progress file, and durable as of
	// the last synced write (flush); both are guarded by mu.
	queued  map[string]uint64
	durable map[string]uint64
	path    string
	every   int
	acks    int
	seq     uint64
	// window and batchRows are the batches in flight and the rows of a batch of the push: with
	// every, how far ahead of the last fold the sender may be (sendHeadroom).
	window    int
	batchRows int
	mu        sync.Mutex
	// sideWritten records that the side file was written by this run.
	sideWritten bool
	// closed records that close ran.
	closed bool
}

// cursorState is the content of the cursor side file: the cursors of the started CSV items and
// every pushed count, as of fold FoldSeq or later.
type cursorState struct {
	Cursors map[string]*csvCursor `json:"cursors"`
	Pushed  []rowCount            `json:"pushed"`
	FoldSeq uint64                `json:"foldSeq"`
}

// startTracker starts the persister of state at path; onError gets the first write failure.
func startTracker(ctx context.Context, path string, state *progressState, every int, onError func(error)) (*tracker, error) {
	state.init()
	// loadProgress validated the pushed counts of a progress file read back.
	pushed, err := pushedMap(state.Pushed)
	if err != nil {
		return nil, fmt.Errorf("progress file %s: %w", path, err)
	}
	t := &tracker{
		path: path, pushed: pushed, cursors: map[string]*csvCursor{}, every: max(1, every), seq: state.FoldSeq,
		queued: map[string]uint64{}, durable: map[string]uint64{}, batchRows: 1,
	}
	t.p = resume.NewPersister(ctx, state, func(s *progressState) error {
		s.UpdatedAt = time.Now()
		return writeProgress(path, s)
	}, onError)
	return t, nil
}

// sync applies change and waits until it is on disk.
func (t *tracker) sync(change func(*progressState)) error {
	if t == nil {
		return nil
	}
	if err := t.p.Sync(change); err != nil {
		return fmt.Errorf("record the progress: %w", err)
	}
	return nil
}

// init gives a progress state read back the maps it may lack.
func (s *progressState) init() {
	if s.Done == nil {
		s.Done = map[string]*itemRecord{}
	}
	if s.Inflight == nil {
		s.Inflight = map[string]inflightPayload{}
	}
}

// record is the record of the item key, created when there is none.
func (s *progressState) record(key string) *itemRecord {
	r := s.Done[key]
	if r == nil {
		r = &itemRecord{}
		s.Done[key] = r
	}
	return r
}

// complete tells whether the item key is recorded complete.
func (s *progressState) complete(key string) bool {
	r := s.Done[key]
	return r != nil && r.Complete
}

// applyCursors sets the cursors of the items not complete.
func (s *progressState) applyCursors(cursors map[string]*csvCursor) {
	for k, c := range cursors {
		if c == nil {
			continue
		}
		if r := s.record(k); !r.Complete {
			r.Cursor = c
		}
	}
}

// payloadSending records the payload a worker is about to send, and waits until it is on disk:
// a crash from here on leaves it in the ambiguous set.
func (t *tracker) payloadSending(worker int, e *Entry, ref payloadRef, nodes []string) error {
	if t == nil {
		return nil
	}
	in := inflightPayload{Key: e.Item.Key(), Source: ref.source, Kind: kindName(ref.kind), PartID: ref.partID, Rows: ref.rows, Nodes: slices.Clone(nodes)}
	return t.sync(func(s *progressState) { s.Inflight[strconv.Itoa(worker)] = in })
}

// payloadAnswered records where a payload landed after an answer; once the payload is done, a
// part's rows count as pushed. It causes no write of its own: the next synced write writes it,
// such as the worker's next payloadSending, and until then the payload stays in flight in the
// file, which a resume reconciles or pushes again. An answer that names a node the liaison stopped
// waiting for is synced at once (persist): a resume must not read that node as not landed.
func (t *tracker) payloadAnswered(worker int, e *Entry, ref payloadRef, rec *payloadRecord, countRows bool) error {
	if t == nil {
		return nil
	}
	key, snapshot := e.Item.Key(), rec.clone()
	persist := false
	for _, c := range snapshot.Causes {
		persist = persist || causeMayCommitLater(c)
	}
	t.locked(func() {
		var pushed []rowCount
		if countRows && ref.kind == nativefmt.KindPart {
			t.pushed[PushKey{streamKey: entryStreamKey(e), Kind: e.Item.Kind}] += ref.rows
			pushed = rowCounts(t.pushed)
		}
		change := func(s *progressState) {
			delete(s.Inflight, strconv.Itoa(worker))
			setPayload(s.record(key), ref, snapshot)
			if pushed != nil {
				s.Pushed = pushed
			}
		}
		if persist {
			t.p.Update(change)
		} else {
			t.p.UpdateLazy(change)
		}
	})
	if persist {
		return t.flush()
	}
	return nil
}

// locked runs f under mu.
func (t *tracker) locked(f func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f()
}

// flush waits until everything recorded so far, lazy answers too, is on disk; the send limits
// queued before it are durable then.
func (t *tracker) flush() error {
	if t == nil {
		return nil
	}
	var queued map[string]uint64
	t.locked(func() { queued = maps.Clone(t.queued) })
	if err := t.sync(nil); err != nil {
		return err
	}
	t.locked(func() {
		for k, v := range queued {
			t.durable[k] = max(t.durable[k], v)
		}
	})
	return nil
}

// sendWindow tells the tracker the batches in flight and the rows of a batch of the push.
func (t *tracker) sendWindow(inflight, batchRows int) {
	if t == nil {
		return
	}
	t.locked(func() { t.window, t.batchRows = inflight, max(1, batchRows) })
}

// sendHeadroom is how many rows the send limit of an item is put ahead of the last row sent when
// the progress file is written: the batches acknowledged before the next fold and those in
// flight, so that the sender seldom has to wait for a synced write of its own.
func (t *tracker) sendHeadroom() uint64 { return uint64(t.every+t.window) * uint64(t.batchRows) }

// itemComplete marks a work item done, folding the CSV cursors first and compacting its parts.
// A CSV item waits until it is on disk: the side file written by the fold still holds the cursor
// of the item, which covers all its rows then, so a crash before the progress file is written
// leaves a side file whose pushed counts hold the item's rows and whose cursor completes the item
// on a resume; the cursor is dropped once the progress file records the item complete. A native
// item does not wait: its completion is written with the next synced write, like the answers of
// its payloads, and a crash before leaves its last payload in flight, which a resume resolves.
func (t *tracker) itemComplete(key string) error {
	if t == nil {
		return nil
	}
	var csv bool
	var err error
	t.locked(func() {
		_, csv = t.cursors[key]
		err = t.foldLocked(!csv, func(s *progressState) {
			r := s.record(key)
			r.Complete = true
			r.compact()
		})
	})
	if err != nil || !csv {
		return err
	}
	if err = t.flush(); err != nil {
		return err
	}
	t.locked(func() { delete(t.cursors, key) })
	return nil
}

// csvStarted records that a CSV item is being pushed from cursor (nil from its first record) and
// that its first batch, ending at sent, is about to be sent, and waits until it is on disk.
func (t *tracker) csvStarted(key string, from *csvCursor, sent csvMark) error {
	if t == nil {
		return nil
	}
	var err error
	t.locked(func() {
		c := from.clone()
		if c == nil {
			c = &csvCursor{}
		}
		c.Sent, c.SendLimit, c.limitWindow = &sent, 0, false
		t.cursors[key] = c
		err = t.foldLocked(false, nil)
	})
	if err != nil {
		return err
	}
	return t.flush()
}

// csvUnsent records, unsynced, that the last batch of a CSV item recorded as sending was not sent:
// its Sent mark goes back to prev, nil when nothing of the item was sent before.
func (t *tracker) csvUnsent(key string, prev *csvMark) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.cursors[key]
	if c == nil {
		return nil
	}
	c.Sent = nil
	if prev != nil {
		p := *prev
		c.Sent = &p
	}
	return t.writeSideLocked()
}

// csvSending records that a batch of a CSV item ending at sent is about to be sent: unsynced
// while it ends within the send limit the progress file holds, else after a synced write of the
// progress file that raises the limit.
func (t *tracker) csvSending(key string, sent csvMark) error {
	if t == nil {
		return nil
	}
	var wait bool
	var err error
	t.locked(func() {
		c := t.cursors[key]
		if c == nil {
			c = &csvCursor{}
			t.cursors[key] = c
		}
		c.Sent = &sent
		wait = sent.Rows > t.durable[key]
		if wait && sent.Rows > t.queued[key] {
			err = t.foldLocked(false, nil)
		} else {
			err = t.writeSideLocked()
		}
	})
	if err != nil || !wait {
		return err
	}
	return t.flush()
}

// csvAcked moves the cursor of a CSV item to the last row of an acknowledged batch of n rows,
// counts the rows, and rewrites the side file; every checkpointEveryBatches batches the cursors
// are folded into the progress file, and it waits until they are on disk.
func (t *tracker) csvAcked(e *Entry, k PushKey, last rowPos, n int) error {
	if t == nil {
		return nil
	}
	var fold bool
	var err error
	t.locked(func() {
		key := e.Item.Key()
		c := t.cursors[key]
		if c == nil {
			c = &csvCursor{}
			t.cursors[key] = c
		}
		c.File, c.Record, c.Rows = last.file, last.record, c.Rows+uint64(n)
		t.pushed[k] += uint64(n)
		t.acks++
		if fold = t.acks%t.every == 0; fold {
			err = t.foldLocked(false, nil)
		} else {
			err = t.writeSideLocked()
		}
	})
	if err != nil || !fold {
		return err
	}
	return t.flush()
}

// writeSideLocked replaces the cursor side file with the cursors and the pushed counts, unsynced
// (writeCursors).
func (t *tracker) writeSideLocked() error { return t.writeSideWith(rowCounts(t.pushed)) }

// writeSideWith is writeSideLocked with pushed, the pushed counts, computed already.
func (t *tracker) writeSideWith(pushed []rowCount) error {
	side := resume.SidePath(t.path, cursorSide)
	data, err := resume.EncodeJSON(cursorState{Cursors: t.cursors, Pushed: pushed, FoldSeq: t.seq})
	if err == nil {
		err = writeCursors(side, data)
	}
	if err != nil {
		return namingProgress(fmt.Errorf("write %s: %w", side, err))
	}
	t.sideWritten = true
	return nil
}

// raiseLimit puts the send limit of the cursor c, of the item key, sendHeadroom rows ahead of the
// last row sent or acknowledged; a tracker that is closing clears it, as no batch is sent any more.
func (t *tracker) raiseLimit(c *csvCursor) {
	if t.closed {
		c.SendLimit = 0
		return
	}
	base := c.Rows
	if c.Sent != nil {
		base = max(base, c.Sent.Rows)
	}
	c.SendLimit = max(c.SendLimit, base+t.sendHeadroom())
}

// foldLocked rewrites the side file with the next FoldSeq and queues the write of the cursors, the
// pushed counts and change into the progress file under it, without waiting for it: flush waits.
// A lazy fold causes no write of its own: the next write writes it.
func (t *tracker) foldLocked(lazy bool, change func(*progressState)) error {
	t.seq++
	for _, c := range t.cursors {
		t.raiseLimit(c)
	}
	// Neither the side file nor the progress file changes it: both get the same.
	pushed, seq := rowCounts(t.pushed), t.seq
	if t.sideWritten || len(t.cursors) > 0 {
		if err := t.writeSideWith(pushed); err != nil {
			return err
		}
	}
	cursors := make(map[string]*csvCursor, len(t.cursors))
	for k, c := range t.cursors {
		cursors[k] = c.clone()
		// The limit is queued with the fold below: flush makes it durable.
		t.queued[k] = c.SendLimit
	}
	fold := func(s *progressState) {
		s.applyCursors(cursors)
		s.Pushed, s.FoldSeq = pushed, seq
		if change != nil {
			change(s)
		}
	}
	if lazy {
		t.p.UpdateLazy(fold)
	} else {
		t.p.Update(fold)
	}
	return nil
}

// resolved writes, in one synced write, what resolving the ambiguous set of a resume decided:
// the new records of the items it changed, the rows now counted as pushed, the duplicates and
// the payloads skip left out; it drops the in-flight payloads of the items planned (byKey) and
// keeps those of the items left out of this run, under a key no worker uses.
func (t *tracker) resolved(records map[string]*itemRecord, added map[PushKey]uint64, dups []rowCount, skipped []string, byKey map[string]*Entry) error {
	t.locked(func() {
		for k, n := range added {
			t.pushed[k] += n
		}
		t.seq++
		pushed, seq := rowCounts(t.pushed), t.seq
		t.p.Update(func(s *progressState) {
			for key, r := range records {
				s.Done[key] = r
			}
			for w, in := range s.Inflight {
				delete(s.Inflight, w)
				if byKey[in.Key] == nil {
					s.Inflight[keptInflightKey(in)] = in
				}
			}
			s.Pushed, s.FoldSeq = pushed, seq
			s.Duplicates = dups
			s.SkippedPayloads = transfer.UniqueSorted(append(s.SkippedPayloads, skipped...))
		})
	})
	return t.flush()
}

// keptInflightKey keys an in-flight payload kept for a later run by the payload, not by a
// worker, so that no worker of this run overwrites it.
func keptInflightKey(in inflightPayload) string {
	return fmt.Sprintf("kept:%s/%s/%s/%d", in.Key, in.Source, in.Kind, in.PartID)
}

// pushedTotals is every row pushed so far, cumulative over the runs.
func (t *tracker) pushedTotals() map[PushKey]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return maps.Clone(t.pushed)
}

// close folds what is left, writes what is queued and stops the persister; it returns the first
// write failure. Once everything the side file held is in the progress file, the side file is
// removed. It is safe to call more than once: a later close does nothing and returns nil, the first
// having reported what failed.
func (t *tracker) close() error {
	if t == nil {
		return nil
	}
	var closed bool
	var err error
	t.locked(func() {
		closed, t.closed = t.closed, true
		if !closed && len(t.cursors) > 0 {
			err = t.foldLocked(false, nil)
		}
	})
	if closed {
		return nil
	}
	if err == nil {
		err = t.flush()
	}
	t.locked(func() {
		if t.sideWritten && err == nil {
			side := resume.SidePath(t.path, cursorSide)
			if removeErr := removeCursors(side); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = namingProgress(fmt.Errorf("remove %s: %w", side, removeErr))
			}
		}
		t.sideWritten = false
		clear(t.cursors)
	})
	if closeErr := t.p.Close(); closeErr != nil {
		return fmt.Errorf("record the progress: %w", closeErr)
	}
	return err
}

// readCursors reads the cursor side file of the progress file at path. It returns nil, and lost
// true, when the side file cannot tell where the previous run stopped although it may have been
// written: it is missing, empty or all NUL (a power loss leaves the unsynced file so), or it is
// older than the progress file (its FoldSeq is lower than foldSeq); the cursors of the progress
// file are used then, with the rows up to their send limit as the rows that may have landed.
// A side file that cannot be read or parsed is an error: its cursors may be newer than the progress
// file's, whose older Sent marks would leave rows that may have landed out of the window a resume
// treats as ambiguous.
func readCursors(path string, foldSeq uint64) (cs *cursorState, lost bool, err error) {
	side := resume.SidePath(path, cursorSide)
	data, err := os.ReadFile(side)
	if errors.Is(err, os.ErrNotExist) || (err == nil && len(bytes.Trim(data, " \t\r\n\x00")) == 0) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cursor side file %s: %w", side, err)
	}
	cs = &cursorState{}
	if err = json.Unmarshal(data, cs); err != nil {
		return nil, false, fmt.Errorf("cursor side file %s is unusable: %w", side, err)
	}
	if _, err = pushedMap(cs.Pushed); err != nil {
		return nil, false, fmt.Errorf("cursor side file %s is unusable: %w", side, err)
	}
	if cs.FoldSeq < foldSeq {
		return nil, true, nil
	}
	return cs, false, nil
}
