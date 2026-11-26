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
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// currentBindings digests this invocation: the artifact's manifest file, the target and the
// options.
func currentBindings(opts *config.Import, info *cluster.Info) (importBindings, error) {
	raw, err := os.ReadFile(filepath.Join(opts.Input, manifest.FileName))
	if err != nil {
		return importBindings{}, exitcode.Exit(exitcode.ExitRuntime, "read the manifest of %s: %w", opts.Input, err)
	}
	return bindingsOf(raw, info, opts), nil
}

// newProgress is the progress file of a fresh import.
func newProgress(art *Artifact, bindings importBindings) *progressState {
	now := time.Now()
	format := manifest.FormatCSV
	if art.Native() {
		format = manifest.FormatNative
	}
	return &progressState{
		Header: resume.Header{Kind: progressKind, Version: progressVersion}, Format: format, StartedAt: now, UpdatedAt: now,
		Bindings: bindings, Inflight: map[string]inflightPayload{}, Done: map[string]*itemRecord{}, Pushed: []rowCount{}, Plan: []plannedItem{},
	}
}

// loadProgress reads the progress file at path and its side files and checks them against this
// invocation; the cursors of the cursor side file, when it is not older than the progress file,
// replace them, and the plan side file (plan and baseline) is read and checked against its digest. Anything
// unusable exits exitcode.ExitResume and changes nothing.
func loadProgress(path string, bindings importBindings) (*progressState, error) {
	state := &progressState{}
	if err := resume.Read(path, progressKind, progressVersion, state); err != nil {
		if version := new(resume.VersionError); errors.As(err, &version) {
			return nil, exitcode.Exit(exitcode.ExitResume, "an earlier import left the progress file %s, but another version of bydbctl wrote it "+
				"(progress file version %d, this bydbctl reads version %d), so it cannot be resumed; nothing was written. "+
				"Resume it with the bydbctl version that wrote it; or %s", path, version.Found, version.Want, otherVersionHint)
		}
		return nil, exitcode.Exit(exitcode.For(err), "%w; nothing was written. To import anyway, %s", err, separateHint)
	}
	// The bindings are compared with the plan's target nodes; a plan that cannot be read is
	// reported after them, which say more when the progress file is another import's.
	planErr := readPlan(path, state)
	if diff := diffBindings(state, bindings); diff != "" {
		return nil, exitcode.Exit(exitcode.ExitResume,
			"the progress file %s was written by the import of another artifact, target or options; nothing was written. It differs in:\n%s%s",
			path, diff, bindingHints(state, bindings))
	}
	if planErr != nil {
		return nil, exitcode.Exit(exitcode.For(planErr), "%w; nothing was written. To import anyway, %s", planErr, separateHint)
	}
	if err := validateProgress(state); err != nil {
		return nil, exitcode.Exit(exitcode.ExitResume, "the progress file %s is unusable: %w; nothing was written. To import anyway, %s", path, err, separateHint)
	}
	if err := state.foldCursorSide(path); err != nil {
		return nil, exitcode.Exit(exitcode.ExitResume, "%w; nothing was written. To import anyway, %s", err, separateHint)
	}
	return state, nil
}

// foldCursorSide gives the state read back the maps it may lack and, when the cursor side file of
// the progress file at path is not older than it, that file's cursors and pushed counts. When the
// side file is lost, the cursors of the progress file that record a send limit above their
// acknowledged rows are marked: the rows up to the limit may have landed (classify).
func (s *progressState) foldCursorSide(path string) error {
	s.init()
	cs, lost, err := readCursors(path, s.FoldSeq)
	if err != nil {
		return err
	}
	if lost {
		for _, r := range s.Done {
			if r.Cursor != nil && !r.Complete && r.Cursor.SendLimit > r.Cursor.Rows {
				r.Cursor.limitWindow = true
			}
		}
		return nil
	}
	s.applyCursors(cs.Cursors)
	s.Pushed = cs.Pushed
	return nil
}

// readSideNext reads the side file name of the progress file at path, checked against *digest,
// or against *next, the digest of a rewrite a crash interrupted (tracker.rewritePlan), which then
// becomes *digest; adopted reports that. *next is cleared once the side file is read.
func readSideNext[T any](path, name string, digest, next *string) (v *T, adopted bool, err error) {
	side := resume.SidePath(path, name)
	v = new(T)
	err = resume.ReadSide(side, *digest, v)
	if err != nil && *next != "" {
		alt := new(T)
		if resume.ReadSide(side, *next, alt) == nil {
			v, err, adopted = alt, nil, true
			*digest = *next
		}
	}
	if err != nil {
		return nil, false, err
	}
	*next = ""
	return v, adopted, nil
}

// readPlan reads the plan side file of the progress file at path into state (readSideNext): the
// plan and the baseline. A progress file that records no plan has none, and records no progress. A baseline the side file
// holds while the progress file records none (a crash before its time was recorded, so before
// anything was pushed) is not used: the next push counts the target again. A plan whose rewrite
// a crash interrupted may have appended items (tracker.appendPlan): with an item not done, the
// import is not finished, whatever the progress file says.
func readPlan(path string, state *progressState) error {
	state.Plan, state.baseline = nil, nil
	if state.PlanDigest == "" {
		// A run writes the plan side file before the progress file that names it, and records a
		// baseline or any progress only after: a file with either and no plan was not written by one.
		if state.BaselineAt != nil || len(state.Pushed) > 0 {
			return exitcode.Exit(exitcode.ExitResume, "the progress file %s records progress but names no plan side file; %s", path, importStartOverHint(path))
		}
		return nil
	}
	rec, adopted, err := readSideNext[recordedPlan](path, planSide, &state.PlanDigest, &state.PlanNextDigest)
	if err != nil {
		return err
	}
	if adopted && slices.ContainsFunc(rec.Plan, func(p plannedItem) bool { return !state.complete(p.Key) }) {
		state.FinishedAt = nil
	}
	state.Plan = rec.Plan
	if state.Plan == nil {
		state.Plan = []plannedItem{}
	}
	if state.BaselineAt == nil {
		return nil
	}
	if rec.Baseline == nil {
		return exitcode.Exit(exitcode.ExitResume, "the progress file %s records a baseline taken at %s, but its plan side file holds none",
			path, state.BaselineAt.Local().Format(time.RFC3339))
	}
	if state.baseline, err = rec.Baseline.counts(); err != nil {
		return exitcode.Exit(exitcode.ExitResume, "the baseline of the progress file %s is unusable: %w", path, err)
	}
	return nil
}

// separateHint is how to run an import apart from a progress file it cannot resume, and its cost.
const separateHint = "give --checkpoint <another path> to run this import separately, or --restart to start this one over; " +
	"the stream and trace data an earlier run already imported into this target is then stored twice"

// otherVersionHint is how to go on from a progress file another version of bydbctl wrote, and
// what it costs.
const otherVersionHint = "give --restart to import from scratch (the progress file and its side files are kept as .bak files, " +
	"and the stream and trace data already imported into this target is then stored twice); " +
	"or give --checkpoint <another path> to run this import separately"

// bindingHints says, per binding that differs, how to resume the progress file, and once at the
// end what else to do.
func bindingHints(state *progressState, current importBindings) string {
	var hints []string
	if state.Bindings.ManifestDigest != current.ManifestDigest {
		hints = append(hints, "the artifact is not the one this progress file was written for (or its manifest.json changed): import that artifact to resume")
	}
	if len(resume.OptionDiffs(recordedOptions(state), current.Options, nil, nil)) > 0 {
		hints = append(hints, "--all-sources or --include-schema differ: run with the first run's values to resume")
	}
	if liaisons, nodes := targetDiffs(state, current); len(liaisons)+len(nodes) > 0 {
		hints = append(hints, "the target is not the cluster this progress file was written for: "+otherTargetHint)
	}
	if len(hints) == 0 {
		return ""
	}
	return strings.Join(append(hints, "otherwise "+separateHint), "\n")
}

// validateProgress checks what a resume reads of a progress file that JSON alone does not:
// every record and payload is present, every payload id and kind is one a run writes, and every
// count names a known catalog and kind. A failure cause this client does not know is taken as
// unspecified, as the liaison's own is.
func validateProgress(state *progressState) error {
	for _, counts := range [][]rowCount{state.Pushed, state.Duplicates} {
		if _, err := pushedMap(counts); err != nil {
			return err
		}
	}
	for key, r := range state.Done {
		if r == nil {
			return fmt.Errorf("done %q has no record", key)
		}
		var err error
		r.eachPayload(func(id string, p *payloadRecord, part bool) {
			switch {
			case err != nil:
			case p == nil:
				err = fmt.Errorf("done %q: payload %q has no record", key, id)
			default:
				if _, refErr := refOf(id, p, part); refErr != nil {
					err = fmt.Errorf("done %q: %w", key, refErr)
					return
				}
			}
		})
		if err != nil {
			return err
		}
	}
	for w, in := range state.Inflight {
		if in.Key == "" || in.Source == "" {
			return fmt.Errorf("inflight %q names no work item or source", w)
		}
		if _, err := inflightRef(in); err != nil {
			return fmt.Errorf("inflight %q: %w", w, err)
		}
	}
	return nil
}

// ambiguousPayload is a payload whose landing the progress file does not know: a native payload
// in flight or failed after its completion was sent, on nodes, or the rows after the cursor of an
// unfinished CSV item.
type ambiguousPayload struct {
	entry  *Entry
	cursor *csvCursor
	// action is reconcile, re-push, or the duplication policy, from source; fallback is the
	// policy, from fallbackSource, of a part reconciliation cannot decide.
	action         resumeAction
	source         string
	fallback       config.DuplicationPolicy
	fallbackSource string
	// landed, notLanded and undecided are what reconciliation decided per node.
	landed, notLanded, undecided []string
	nodes                        []string
	// causes are why the liaison reported each node failed (payloadRecord.Causes): a node that
	// may have committed the part later (causeMayCommitLater) is not read as not landed.
	causes map[string]string
	ref    payloadRef
	// skipRefused marks a payload whose skip was turned into block: warn or push continue it.
	skipRefused bool
}

// resumeAction is what a resume does with an ambiguous payload: reconcile it with the counts of
// the target, push it again, or what a duplication policy says.
type resumeAction struct {
	// policy is the duplication policy of an action of kind actionKindPolicy.
	policy config.DuplicationPolicy
	kind   actionKind
}

type actionKind uint8

const (
	actionKindPolicy actionKind = iota
	actionKindReconcile
	actionKindRepush
)

var (
	actionReconcile = resumeAction{kind: actionKindReconcile}
	actionRepush    = resumeAction{kind: actionKindRepush}
)

// policyAction is the action a duplication policy stands for.
func policyAction(p config.DuplicationPolicy) resumeAction {
	return resumeAction{kind: actionKindPolicy, policy: p}
}

// is tells whether the action is the duplication policy p.
func (a resumeAction) is(p config.DuplicationPolicy) bool {
	return a.kind == actionKindPolicy && a.policy == p
}

func (a resumeAction) String() string {
	switch a.kind {
	case actionKindReconcile:
		return "reconcile"
	case actionKindRepush:
		return "re-push"
	default:
		return string(a.policy)
	}
}

// name renders the payload for the summary and the warnings.
func (a *ambiguousPayload) name() string {
	key := a.entry.Item.Key()
	if a.cursor != nil {
		return key + " " + cursorWindow(a.cursor)
	}
	what := kindName(a.ref.kind)
	if a.ref.kind == nativefmt.KindPart {
		what = "part " + strings.TrimPrefix(a.ref.id, a.ref.source+"/")
	}
	return fmt.Sprintf("%s %s of %s on %s", key, what, a.ref.source, strings.Join(a.nodes, ","))
}

// cursorWindow names the rows of a CSV item sent after its cursor: they may have landed. The
// file of the last row sent is named only when it is not the cursor's.
func cursorWindow(c *csvCursor) string {
	from := "rows from its first row"
	if c.File != "" {
		from = fmt.Sprintf("rows after %s record %d", c.File, c.Record)
	}
	if c.Sent == nil {
		return from
	}
	if c.limitWindow {
		return fmt.Sprintf("%s up to its recorded send limit (%d row(s))", from, c.window())
	}
	to := c.Sent.File + " "
	if c.Sent.File == c.File {
		to = ""
	}
	return fmt.Sprintf("%s up to %srecord %d (%d row(s) sent)", from, to, c.Sent.Record, c.window())
}

// rows is how many rows pushing the payload again may duplicate per copy: for a CSV item the
// rows sent after its cursor.
func (a *ambiguousPayload) rows() uint64 {
	if a.cursor != nil {
		return a.cursor.window()
	}
	return a.ref.rows
}

// line is the AMBIGUOUS line of the payload; a reconciled one names the policy of what
// reconciliation cannot decide.
func (a *ambiguousPayload) line() string {
	what := a.action.String()
	if a.source != "" {
		what += " (" + a.source + ")"
	}
	if a.action == actionReconcile {
		what += ", else " + string(a.fallback) + " (" + a.fallbackSource + ")"
	}
	return fmt.Sprintf("  %s: %s", a.name(), what)
}

// resumeActionOf is what resume does with an ambiguous payload of e; false when the duplication
// policy decides.
func resumeActionOf(e *Entry, ref payloadRef, csv bool, sampled map[string]bool) (resumeAction, bool) {
	dupRows := storesDuplicates(e.Item.Catalog)
	switch {
	case csv && !dupRows:
		return actionRepush, true
	case csv:
		return resumeAction{}, false
	case ref.kind == nativefmt.KindPart && dupRows && (e.Item.Catalog != commonv1.Catalog_CATALOG_TRACE || !sampled[e.Item.Group]):
		return actionReconcile, true
	case ref.kind == nativefmt.KindElementIndex || (ref.kind == nativefmt.KindPart && dupRows):
		return resumeAction{}, false
	default:
		return actionRepush, true
	}
}

// classify lists the ambiguous payloads of the planned entries and what resume does with each:
// a stream or trace part is reconciled unless its group samples spans in merges; a measure part,
// a series index and a property shard are pushed again; an element index and the rows after the
// cursor of a CSV stream or trace item follow the duplication policy; the rows after the cursor
// of any other CSV item are pushed again. Of a CSV item only the rows sent after the cursor are
// ambiguous: those after the last batch sent were never sent. skip is refused (block) wherever
// verify cannot check what it leaves out: the rows of a trace group sampling spans in merges, an
// element index, whose documents verify does not count, and anything of an import whose first run
// recorded no baseline. A CSV item whose cursor covers all its rows is completed; one that sent
// nothing after its cursor is not ambiguous. policySource names where the duplication policy came
// from (Sources.Label), "" for the flag.
func classify(state *progressState, entries []Entry, opts *config.Import, sampled map[string]bool, policySource string) []*ambiguousPayload {
	byKey := plannedByKey(entries)
	var out []*ambiguousPayload
	add := func(e *Entry, ref payloadRef, nodes []string, cursor *csvCursor) *ambiguousPayload {
		a := &ambiguousPayload{entry: e, ref: ref, nodes: nodes, cursor: cursor}
		a.fallback, a.fallbackSource = opts.DuplicationPolicy, cmp.Or(policySource, "--resume-duplication-policy")
		var ok bool
		if a.action, ok = resumeActionOf(e, ref, cursor != nil, sampled); !ok {
			a.action, a.source = policyAction(a.fallback), a.fallbackSource
		}
		if a.action.is(config.DuplicationSkip) {
			if why := skipUncheckable(state, e, ref, cursor != nil, sampled); why != "" {
				a.action, a.skipRefused = policyAction(config.DuplicationBlock), true
				a.source = a.fallbackSource + "; skip is refused: " + why
			}
		}
		out = append(out, a)
		return a
	}
	for _, key := range slices.Sorted(maps.Keys(state.Done)) {
		r, e := state.Done[key], byKey[key]
		if e == nil || r.Complete {
			continue
		}
		if r.Cursor != nil {
			c := r.Cursor
			if c.limitWindow {
				c = c.clone()
				c.Sent = &csvMark{Rows: sendLimitOf(e, c)}
			}
			if !cursorCovers(e, c) && c.window() > 0 {
				add(e, payloadRef{}, nil, c)
			}
			continue
		}
		r.eachPayload(func(id string, p *payloadRecord, part bool) {
			if len(p.CompletionSent) > 0 {
				// validateProgress checked every payload id when the progress file was loaded.
				ref, _ := refOf(id, p, part)
				a := add(e, ref, slices.Clone(p.CompletionSent), nil)
				a.causes = maps.Clone(p.Causes)
			}
		})
	}
	for _, w := range slices.Sorted(maps.Keys(state.Inflight)) {
		in := state.Inflight[w]
		e := byKey[in.Key]
		if e == nil || state.complete(in.Key) {
			continue
		}
		// validateProgress checked every payload kind when the progress file was loaded.
		ref, _ := inflightRef(in)
		var landed []string
		if r := state.Done[in.Key]; r != nil {
			if p := payloadOf(r, ref); p != nil {
				landed = p.Nodes
			}
		}
		// A payload recorded in flight to no node may have gone to any target node of its item.
		sentTo := in.Nodes
		if len(sentTo) == 0 {
			sentTo = e.TargetNodes
		}
		var nodes []string
		for _, n := range sentTo {
			if !slices.Contains(landed, n) {
				nodes = append(nodes, n)
			}
		}
		if len(nodes) == 0 {
			continue
		}
		// A payload in flight may also have failed on other nodes after its completion before.
		if i := slices.IndexFunc(out, func(a *ambiguousPayload) bool { return a.entry == e && a.ref.id == ref.id && a.cursor == nil }); i >= 0 {
			out[i].nodes = transfer.UniqueSorted(append(out[i].nodes, nodes...))
			continue
		}
		add(e, ref, nodes, nil)
	}
	return out
}

// cursorSideLostNote says why the rows of a CSV item up to its send limit are ambiguous.
const cursorSideLostNote = "the cursor side file is missing, empty or older than the progress file; " +
	"the rows up to the recorded send limit are treated as possibly landed"

// sendLimitOf is the row of the item e up to which the rows sent after the cursor c may have
// landed, when the cursor side file is lost: the send limit the progress file records, at most the
// rows of the item.
func sendLimitOf(e *Entry, c *csvCursor) uint64 {
	if total := e.Item.Rows(); total > 0 {
		return min(c.SendLimit, total)
	}
	return c.SendLimit
}

// errWindowEnd stops the scan of the rows of a send-limit window at its last row.
var errWindowEnd = errors.New("end of the send-limit window")

// locateLimitWindows gives the windows skip leaves out, which the send limit bounds by rows, the
// position of their last row: the rows after the cursor, read as the push reads them, up to the
// limit. Reading the rows to count them is the only way to find the position, as the window's
// rows were never read.
func locateLimitWindows(ctx context.Context, art *Artifact, target *Schemas, amb []*ambiguousPayload) error {
	var entries []Entry
	var windows []*ambiguousPayload
	for _, a := range amb {
		if a.cursor != nil && a.cursor.limitWindow && a.action.is(config.DuplicationSkip) {
			entries = append(entries, *a.entry)
			windows = append(windows, a)
		}
	}
	if len(windows) == 0 {
		return nil
	}
	builders, err := newBuilders(art, target, entries)
	if err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	return scanLimitWindows(ctx, art.Dir, builders, windows)
}

// scanLimitWindows reads the rows of every window up to the row its Sent mark names and moves the
// mark to the position of that row.
func scanLimitWindows(ctx context.Context, dir string, builders map[builderKey]*rowBuilder, windows []*ambiguousPayload) error {
	for _, a := range windows {
		item, c := &a.entry.Item, a.cursor
		var seen uint64
		last := csvMark{File: c.File, Record: c.Record, Rows: c.Rows}
		b := builders[builderKey{catalog: item.Catalog, group: item.Group}]
		err := b.itemRows(ctx, dir, item, &csvCursor{File: c.File, Record: c.Record}, func(r row) error {
			seen++
			last = csvMark{File: r.file, Record: r.record, Rows: c.Rows + seen}
			if last.Rows >= c.Sent.Rows {
				return errWindowEnd
			}
			return nil
		})
		if err != nil && !errors.Is(err, errWindowEnd) {
			return exitcode.Exit(exitcode.ExitRuntime, "%s: %w", item.Key(), err)
		}
		c.Sent = &last
	}
	return nil
}

// skipUncheckable says why verify cannot check what skip would leave out of a payload, "" when it
// can.
func skipUncheckable(state *progressState, e *Entry, ref payloadRef, csv bool, sampled map[string]bool) string {
	switch {
	case state.NoBaseline:
		return "the first run recorded no baseline, so --verify cannot check what skip would leave out"
	case !csv && ref.kind == nativefmt.KindElementIndex:
		return "--verify does not count element index documents, so nothing would check what skip would leave out"
	case e.Item.Catalog == commonv1.Catalog_CATALOG_TRACE && sampled[e.Item.Group] && (csv || ref.kind == nativefmt.KindPart):
		return "trace group " + e.Item.Group + " samples spans in merges, so --verify cannot check what skip would leave out"
	}
	return ""
}

func plannedByKey(entries []Entry) map[string]*Entry {
	out := make(map[string]*Entry, len(entries))
	for i := range entries {
		out[entries[i].Item.Key()] = &entries[i]
	}
	return out
}

// cursorCovers tells whether the cursor of a CSV item acknowledges every row of the item.
func cursorCovers(e *Entry, c *csvCursor) bool {
	total := e.Item.Rows()
	return c.File != "" && total > 0 && c.Rows >= total
}

// refOf rebuilds the reference of a recorded payload from its id, "<source>/<part id>" for a
// part and "<source>/<KIND>" for any other payload.
func refOf(id string, p *payloadRecord, part bool) (payloadRef, error) {
	i := strings.LastIndex(id, "/")
	if i <= 0 {
		return payloadRef{}, fmt.Errorf("payload id %q is not <source>/<part id or kind>", id)
	}
	source, rest := id[:i], id[i+1:]
	if part {
		partID, err := strconv.ParseUint(rest, 10, 64)
		if err != nil {
			return payloadRef{}, fmt.Errorf("payload id %q: part id: %w", id, err)
		}
		return partRef(source, partID, p.Rows), nil
	}
	kind, ok := kindByName(rest)
	if !ok || kind == nativefmt.KindPart {
		return payloadRef{}, fmt.Errorf("payload id %q: unknown payload kind %q", id, rest)
	}
	return blobRef(source, kind), nil
}

func inflightRef(in inflightPayload) (payloadRef, error) {
	kind, ok := kindByName(in.Kind)
	if !ok {
		return payloadRef{}, fmt.Errorf("unknown payload kind %q", in.Kind)
	}
	if kind == nativefmt.KindPart {
		return partRef(in.Source, in.PartID, in.Rows), nil
	}
	return blobRef(in.Source, kind), nil
}

// kindByName is the payload kind a name such as SERIES_INDEX names.
func kindByName(name string) (nativefmt.Kind, bool) {
	v, ok := transferv1.ImportPartsRequest_Kind_value["KIND_"+name]
	if !ok || nativefmt.Kind(v) == transferv1.ImportPartsRequest_KIND_UNSPECIFIED {
		return 0, false
	}
	return nativefmt.Kind(v), true
}

func payloadOf(r *itemRecord, ref payloadRef) *payloadRecord {
	if ref.blob() {
		return r.Blobs[ref.id]
	}
	return r.Parts[ref.id]
}

// summarize is the resume summary of a progress file over the planned entries.
func summarize(path string, state *progressState, entries []Entry, amb []*ambiguousPayload) resume.Summary {
	s := resume.Summary{Path: path, UpdatedAt: state.UpdatedAt, Now: time.Now(), Noun: "work item(s)"}
	for i := range entries {
		e := &entries[i]
		var files int
		var size uint64
		for _, src := range e.Item.Sources {
			files += len(src.Files)
			for _, f := range src.Files {
				size += f.Bytes
			}
		}
		done := state.complete(e.Item.Key())
		s.Add(e.Item.CatalogName(), e.Item.Group, stageLabel(e.TargetStage), done, files, size, size)
		if done {
			s.DoneFiles += files
			s.DoneBytes += size
		} else {
			s.LeftRows += rowsLeft(state, e)
		}
	}
	if added := newItems(state, entries); len(added) > 0 {
		s.Extra = append(s.Extra, fmt.Sprintf("NEW %d work item(s) a gate refused when the plan was recorded are accepted now; "+
			"they are added to the recorded plan and imported:", len(added)))
		for _, e := range added {
			s.Extra = append(s.Extra, "  "+e.Item.Key())
		}
	}
	if len(amb) > 0 {
		s.Extra = append(s.Extra, fmt.Sprintf("AMBIGUOUS %d payload(s) may have landed already:", len(amb)))
		for _, a := range amb {
			s.Extra = append(s.Extra, a.line())
		}
		if slices.ContainsFunc(amb, func(a *ambiguousPayload) bool { return a.cursor != nil && a.cursor.limitWindow }) {
			s.Extra = append(s.Extra, "NOTE "+cursorSideLostNote)
		}
	}
	return s
}

// rowsLeft is the rows of e that a CSV cursor does not acknowledge yet.
func rowsLeft(state *progressState, e *Entry) uint64 {
	total := e.Item.Rows()
	if r := state.Done[e.Item.Key()]; r != nil && r.Cursor != nil {
		return total - min(total, r.Cursor.Rows)
	}
	return total
}

// remaining is plan without the entries the progress file records as complete, and without the
// rejections of the items it records as complete: a gate refusing them now has nothing left to hold.
func remaining(plan *Plan, state *progressState) *Plan {
	left := *plan
	left.Entries = remainingEntries(plan.Entries, state)
	left.Rejected = slices.DeleteFunc(slices.Clone(plan.Rejected), func(r Rejection) bool { return state.complete(r.Item.Key()) })
	return &left
}

// remainingEntries are the entries the progress file does not record as complete.
func remainingEntries(entries []Entry, state *progressState) []Entry {
	var out []Entry
	for _, e := range entries {
		if !state.complete(e.Item.Key()) {
			out = append(out, e)
		}
	}
	return out
}

// comparePlans checks the new plan against the one the progress file records: every recorded
// item is planned again from the same sources, or refused by a gate (its progress is kept),
// and an item with progress not complete yet keeps its target stage and nodes. An item not in the
// recorded plan is one a gate refused then and accepts now, such as with --force-ttl: it has no
// progress, so it is appended to the plan (newItems) and pushed, unless the progress file records
// progress of it. checkNodes is false while the routes of groups the import writes are unknown. It
// returns one line per difference, "" when the plans agree.
func comparePlans(state *progressState, plan *Plan, checkNodes bool) string {
	if len(state.Plan) == 0 {
		return ""
	}
	byKey := plannedByKey(plan.Entries)
	rejected := make(map[string]bool, len(plan.Rejected))
	for _, r := range plan.Rejected {
		rejected[r.Item.Key()] = true
	}
	recorded := state.plannedKeys()
	var lines []string
	for _, p := range state.Plan {
		e := byKey[p.Key]
		if e == nil {
			if rejected[p.Key] {
				continue
			}
			lines = append(lines, "  "+p.Key+": no longer planned")
			continue
		}
		sources := make([]string, len(e.Item.Sources))
		for i, s := range e.Item.Sources {
			sources[i] = s.Node
		}
		if !slices.Equal(sources, p.Sources) {
			lines = append(lines, fmt.Sprintf("  %s: sources [%s], recorded [%s]", p.Key, strings.Join(sources, " "), strings.Join(p.Sources, " ")))
		}
		if state.complete(p.Key) || !hasProgress(state, p.Key) || (!checkNodes && len(e.TargetNodes) == 0) {
			continue
		}
		if e.TargetStage != p.TargetStage || !slices.Equal(e.TargetNodes, p.TargetNodes) {
			lines = append(lines, fmt.Sprintf("  %s: target %s [%s], recorded %s [%s]", p.Key, stageLabel(e.TargetStage), strings.Join(e.TargetNodes, " "),
				stageLabel(p.TargetStage), strings.Join(p.TargetNodes, " ")))
		}
	}
	for k := range byKey {
		if !recorded[k] && hasProgress(state, k) {
			lines = append(lines, "  "+k+": not in the recorded plan, but the progress file records progress of it")
		}
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// newItems are the entries the recorded plan does not hold: items a gate refused when the plan
// was recorded and accepts now. A progress file that records no plan has none.
func newItems(state *progressState, entries []Entry) []Entry {
	if state == nil || len(state.Plan) == 0 {
		return nil
	}
	recorded := state.plannedKeys()
	var out []Entry
	for _, e := range entries {
		if !recorded[e.Item.Key()] {
			out = append(out, e)
		}
	}
	return out
}

// plannedKeys are the keys of the items of the recorded plan.
func (s *progressState) plannedKeys() map[string]bool {
	out := make(map[string]bool, len(s.Plan))
	for _, p := range s.Plan {
		out[p.Key] = true
	}
	return out
}

func hasProgress(state *progressState, key string) bool {
	if state.Done[key] != nil {
		return true
	}
	for _, in := range state.Inflight {
		if in.Key == key {
			return true
		}
	}
	return false
}

// resumePlan is what a resumed push skips and repeats, after the ambiguous set is resolved.
type resumePlan struct {
	// records are the native payloads of every item with progress.
	records map[string]*itemRecord
	// cursors are where the CSV items start.
	cursors map[string]*csvCursor
	// tolerance are the rows pushed again that may have landed already, per stream key, for verify.
	tolerance map[streamKey]*verifyTolerance
	// skipped names the payloads left out by the skip policy: verify is then forced.
	skipped []string
}

// verifyTolerance is how many rows of a stream key a resumed import pushed again although they
// may have landed already, and which payloads they are.
type verifyTolerance struct {
	payloads []string
	rows     uint64
}

// payload is the record of a native payload of an item, nil when the item has none.
func (rp *resumePlan) payload(key string, ref payloadRef) *payloadRecord {
	if rp == nil || rp.records[key] == nil {
		return nil
	}
	return payloadOf(rp.records[key], ref)
}

func (rp *resumePlan) cursor(key string) *csvCursor {
	if rp == nil {
		return nil
	}
	return rp.cursors[key]
}

// resolveResume resolves the ambiguous set once before a resumed push: one reconciliation decides
// every stream and trace part it can, seeded with the parts recorded as landed and with the keys
// whose counts earlier policy decisions left unknown; the policy decides the rest, block halting
// the import (exit 7) before anything is pushed. What is decided, and the rows it counts as
// pushed, is written to the progress file in one synced write before the push. rejected are the
// items this run does not push although the progress file records them (a gate refused them).
func resolveResume(ctx context.Context, log io.Writer, t *tracker, state *progressState, entries []Entry, rejected []Rejection, amb []*ambiguousPayload,
	rec *reconciler,
) (*resumePlan, error) {
	rp := &resumePlan{records: map[string]*itemRecord{}, cursors: map[string]*csvCursor{}}
	byKey := plannedByKey(entries)
	if rec != nil {
		seedLanded(rec, state, knownItems(state, entries, rejected), byKey)
	}
	reconcileAmbiguous(ctx, log, rec, amb)
	if ctx.Err() != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "import interrupted while resolving the ambiguous payloads: %w", context.Cause(ctx))
	}
	if err := checkBlocked(log, amb); err != nil {
		return nil, err
	}
	// Nothing writes the state meanwhile: the push has not started, every write so far was synced.
	res := &resolution{state: state, added: map[PushKey]uint64{}, records: map[string]*itemRecord{}}
	for _, a := range amb {
		res.apply(log, rec, a)
	}
	for key, e := range byKey {
		if r := state.Done[key]; r != nil && !r.Complete && r.Cursor != nil && cursorCovers(e, r.Cursor) {
			res.record(key).Complete, res.record(key).Cursor = true, nil
		}
	}
	rp.skipped = transfer.UniqueSorted(slices.Concat(state.SkippedPayloads, res.skipped))
	dups := slices.Concat(state.Duplicates, res.duplicates)
	rp.tolerance = toleranceOf(dups)
	if err := t.resolved(res.records, res.added, dups, res.skipped, byKey); err != nil {
		return nil, exitcode.Exit(exitcode.ExitRuntime, "%w", err)
	}
	rp.copyRecords(state, byKey)
	return rp, nil
}

// knownItems are the items the progress file may record, by key: those planned, and those a gate
// refused this time, each at the target stage the progress file records for it: what it records
// landed where the item was pushed, which may differ from the stage planned now.
func knownItems(state *progressState, entries []Entry, rejected []Rejection) map[string]*Entry {
	out := plannedByKey(entries)
	stages := map[string]string{}
	for _, p := range state.Plan {
		stages[p.Key] = p.TargetStage
	}
	for key, e := range out {
		if stage, ok := stages[key]; ok && stage != e.TargetStage {
			out[key] = &Entry{Item: e.Item, TargetStage: stage, TargetNodes: e.TargetNodes}
		}
	}
	for _, r := range rejected {
		key := r.Item.Key()
		if out[key] != nil {
			continue
		}
		stage, ok := stages[key]
		if !ok {
			stage = r.TargetStage
		}
		out[key] = &Entry{Item: r.Item, TargetStage: stage}
	}
	return out
}

// reconcileAmbiguous decides every part whose action is reconcile, and logs each decision. Without
// a reconciler every such node is undecided; so is a node the liaison stopped waiting for that
// reads as not landed, since it may have committed the part after the run ended.
func reconcileAmbiguous(ctx context.Context, log io.Writer, rec *reconciler, amb []*ambiguousPayload) {
	var queries []reconcileQuery
	var asked []*ambiguousPayload
	for _, a := range amb {
		if a.action != actionReconcile {
			continue
		}
		if rec == nil {
			a.undecided = a.nodes
			continue
		}
		for _, node := range a.nodes {
			queries = append(queries, reconcileQuery{Key: entryTargetKey(a.entry, node), ID: a.ref.partID, Rows: a.ref.rows})
			asked = append(asked, a)
		}
	}
	if len(queries) > 0 {
		for j, d := range rec.decideAll(ctx, queries) {
			a, node := asked[j], queries[j].Key.Node
			if d.Outcome == reconcileNotLanded && causeMayCommitLater(a.causes[node]) {
				d.Outcome, d.Reason = reconcileUndecidable, "the liaison stopped waiting for the node, which may have committed the part since"
			}
			line := reconcileLine(a.entry.Item.Key(), a.ref.partID, a.ref.source, node, d, a.ref.rows)
			switch d.Outcome {
			case reconcileLanded:
				a.landed = append(a.landed, node)
				rec.recordLanded(queries[j].Key, a.ref.rows)
			case reconcileNotLanded:
				a.notLanded = append(a.notLanded, node)
			case reconcileUndecidable:
				line += ": " + d.Reason
				a.undecided = append(a.undecided, node)
			}
			fmt.Fprintln(log, line)
		}
	}
	// A reconciled part is named by the nodes still unknown.
	for _, a := range amb {
		if a.action == actionReconcile {
			a.nodes = a.undecided
		}
	}
}

// policy is the action, the duplication policy or the re-push, that applies to the nodes of the
// payload still unknown, and where it comes from.
func (a *ambiguousPayload) policy() (resumeAction, string) {
	if a.action == actionReconcile {
		return policyAction(a.fallback), a.fallbackSource
	}
	return a.action, a.source
}

// unknown tells whether anything of the payload is still unknown after reconciliation.
func (a *ambiguousPayload) unknown() bool { return len(a.nodes) > 0 || a.cursor != nil }

// checkBlocked halts the import (exit 7) when the block policy applies to any payload still unknown.
func checkBlocked(log io.Writer, amb []*ambiguousPayload) error {
	var blocked []string
	for _, a := range amb {
		if policy, source := a.policy(); policy.is(config.DuplicationBlock) && a.unknown() {
			blocked = append(blocked, fmt.Sprintf("%s (%s)", a.name(), source))
		}
	}
	if len(blocked) == 0 {
		return nil
	}
	duplicationBlocked(log, blocked)
	policies := "warn|push|skip"
	if slices.ContainsFunc(amb, func(a *ambiguousPayload) bool { return a.skipRefused && a.unknown() }) {
		policies = "warn|push (skip stays refused where a line says so)"
	}
	return exitcode.Exit(exitcode.ExitHalted, "the import is halted by resumeDuplicationPolicy block (listed above); what was imported before is valid. "+
		"To continue, use --resume-duplication-policy %s and run the same command again", policies)
}

// resolution collects what resolving the ambiguous set changes in the progress file: new records
// of the items it changes, built from copies of the state's, the rows they count as pushed, the
// rows a re-push may duplicate and the payloads skip leaves out.
type resolution struct {
	state      *progressState
	added      map[PushKey]uint64
	records    map[string]*itemRecord
	duplicates []rowCount
	skipped    []string
}

// record is the new record of the item key, a copy of the state's.
func (res *resolution) record(key string) *itemRecord {
	if r := res.records[key]; r != nil {
		return r
	}
	r := &itemRecord{}
	if old := res.state.Done[key]; old != nil {
		r = old.clone()
	}
	res.records[key] = r
	return r
}

// apply applies the outcome of one ambiguous payload: the nodes decided by reconciliation, and the
// policy (or the re-push) for the rest. A stream or trace part pushed again or left out by the
// policy taints its key on those nodes: the counts there are off by an unknown number of rows.
func (res *resolution) apply(log io.Writer, rec *reconciler, a *ambiguousPayload) {
	policy, source := a.policy()
	key := a.entry.Item.Key()
	pk := PushKey{streamKey: entryStreamKey(a.entry), Kind: a.entry.Item.Kind}
	skip := policy.is(config.DuplicationSkip)
	if a.unknown() {
		res.applyPolicy(log, a, pk, policy, source)
	}
	r := res.record(key)
	if a.cursor != nil {
		if skip {
			// Only the rows that may have landed are left out: the push goes on after the last of them.
			res.added[pk] += a.rows()
			sent := a.cursor.Sent
			r.Cursor = &csvCursor{File: sent.File, Record: sent.Record, Rows: sent.Rows}
			if cursorCovers(a.entry, r.Cursor) {
				r.Complete, r.Cursor = true, nil
			}
			return
		}
		// The window is decided and its duplicates counted: the window of a later resume holds only
		// what this run sends again, so every resume adds its own duplicates.
		if r.Cursor != nil {
			r.Cursor.Sent, r.Cursor.SendLimit, r.Cursor.limitWindow = nil, 0, false
		}
		return
	}
	ref, nodes := a.ref, a.nodes
	p := payloadOf(r, ref)
	if p == nil {
		p = &payloadRecord{Rows: ref.rows}
		setPayload(r, ref, p)
	}
	p.Nodes = transfer.UniqueSorted(append(p.Nodes, a.landed...))
	p.Missing = transfer.UniqueSorted(append(p.Missing, a.notLanded...))
	if skip {
		p.Skipped = transfer.UniqueSorted(append(p.Skipped, nodes...))
		p.Missing = slices.DeleteFunc(p.Missing, func(n string) bool { return slices.Contains(nodes, n) })
	} else {
		p.Missing = transfer.UniqueSorted(append(p.Missing, nodes...))
		if ref.kind == nativefmt.KindPart && (policy.is(config.DuplicationWarn) || policy.is(config.DuplicationPush)) {
			p.Repushed = transfer.UniqueSorted(append(p.Repushed, nodes...))
		}
	}
	p.CompletionSent, p.Causes = nil, nil
	p.Missing = slices.DeleteFunc(p.Missing, func(n string) bool { return slices.Contains(p.Nodes, n) })
	if p.done() && ref.kind == nativefmt.KindPart {
		res.added[pk] += ref.rows
	}
	if rec != nil && ref.kind == nativefmt.KindPart && policy != actionRepush {
		for _, n := range nodes {
			rec.taint(entryTargetKey(a.entry, n))
		}
	}
}

// applyPolicy logs what the policy does with a payload still unknown and records the rows a
// re-push may duplicate, or the payload skip leaves out.
func (res *resolution) applyPolicy(log io.Writer, a *ambiguousPayload, pk PushKey, policy resumeAction, source string) {
	switch {
	case policy.is(config.DuplicationWarn), policy.is(config.DuplicationPush):
		if policy.is(config.DuplicationWarn) {
			warnDuplicationPushed(log, a.name(), source, a.rows())
		}
		dupRows := storesDuplicates(a.entry.Item.Catalog) && (a.cursor != nil || a.ref.kind == nativefmt.KindPart)
		if dupRows && a.rows() > 0 {
			res.duplicate(pk, a)
		}
	case policy.is(config.DuplicationSkip):
		warnDuplicationSkipped(log, a.name(), source)
		res.skipped = append(res.skipped, a.name())
	case policy == actionRepush:
		// A measure point pushed again overwrites the one that may have landed, but both versions
		// are counted until a merge: verify tolerates them as it tolerates a stream re-push.
		// An index-mode measure is not counted by rows.
		if a.entry.Item.Catalog == commonv1.Catalog_CATALOG_MEASURE && a.entry.Item.Kind == ShardItem &&
			(a.cursor != nil || a.ref.kind == nativefmt.KindPart) && a.rows() > 0 {
			res.duplicate(pk, a)
		}
	}
}

// duplicate records the rows pushing a payload again may store twice.
func (res *resolution) duplicate(pk PushKey, a *ambiguousPayload) {
	dup := rowCountOf(pk, a.rows())
	dup.Payloads = []string{a.name()}
	res.duplicates = append(res.duplicates, dup)
}

// copyRecords keeps a copy of the progress of every unfinished planned item for the push.
func (rp *resumePlan) copyRecords(state *progressState, byKey map[string]*Entry) {
	for key, r := range state.Done {
		if byKey[key] == nil || r.Complete {
			continue
		}
		// The cursor is kept apart; Landed and Tainted are of complete items only.
		cp := r.clone()
		cp.Cursor, cp.Landed, cp.Tainted = nil, nil, nil
		rp.records[key] = cp
		if r.Cursor != nil {
			rp.cursors[key] = r.Cursor.clone()
		}
	}
}

func setPayload(r *itemRecord, ref payloadRef, p *payloadRecord) {
	if ref.blob() {
		if r.Blobs == nil {
			r.Blobs = map[string]*payloadRecord{}
		}
		r.Blobs[ref.id] = p
		return
	}
	if r.Parts == nil {
		r.Parts = map[string]*payloadRecord{}
	}
	r.Parts[ref.id] = p
}

// toleranceOf sums the duplicate counts per stream key, for verify.
func toleranceOf(dups []rowCount) map[streamKey]*verifyTolerance {
	out := map[streamKey]*verifyTolerance{}
	for _, d := range dups {
		k, err := pushKeyOf(d)
		if err != nil {
			continue
		}
		t := out[k.streamKey]
		if t == nil {
			t = &verifyTolerance{}
			out[k.streamKey] = t
		}
		t.rows += d.Rows
		t.payloads = append(t.payloads, d.Payloads...)
	}
	return out
}

// seedLanded records in rec the rows of every stream and trace part the progress file records
// as landed, per target key, so that the counts expected at a key include them, whether the
// item is planned this time or not (items, see knownItems); and it taints every key where a
// part was pushed again or left out by the policy, whose counts are then off by an unknown
// number of rows, and every key where a part of an item not planned this time (not in planned)
// may have landed: nothing resolves those parts in this run.
func seedLanded(rec *reconciler, state *progressState, items, planned map[string]*Entry) {
	for _, in := range state.Inflight {
		e := items[in.Key]
		if e == nil || planned[in.Key] != nil || in.Kind != kindName(nativefmt.KindPart) || !reconcilable(e.Item.Catalog, nativefmt.KindPart) {
			continue
		}
		for _, node := range in.Nodes {
			rec.taint(entryTargetKey(e, node))
		}
	}
	for key, r := range state.Done {
		e := items[key]
		if e == nil || !reconcilable(e.Item.Catalog, nativefmt.KindPart) {
			continue
		}
		at := func(node string) targetKey { return entryTargetKey(e, node) }
		for _, p := range r.Parts {
			for _, node := range p.Nodes {
				rec.recordLanded(at(node), p.Rows)
			}
			for _, node := range slices.Concat(p.Skipped, p.Repushed) {
				rec.taint(at(node))
			}
			if planned[key] == nil {
				for _, node := range p.CompletionSent {
					rec.taint(at(node))
				}
			}
		}
		// A complete item keeps only its totals.
		for node, rows := range r.Landed {
			rec.recordLanded(at(node), rows)
		}
		for _, node := range r.Tainted {
			rec.taint(at(node))
		}
	}
}
