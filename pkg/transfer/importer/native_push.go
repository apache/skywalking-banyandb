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
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// partRetryAttempts is how many times a payload is pushed to the target nodes that failed it.
var partRetryAttempts = 3

// maxMetaFileSize bounds the metadata file of a part read into memory.
const maxMetaFileSize = 1 << 20

// PushParts imports the entries of a native artifact: it reads the part files out of the
// containers of every chosen source and relays them through ImportParts, one payload per part
// (with its trace secondary index parts), per series index, per element index and per property
// shard. The series indexes of every segment go first, in a round of their own, so that no
// part is queryable before the index that names its series; parts, element indexes and
// property shards follow. Within a round a pool of workers pushes the entries, worker i through
// liaison i % L. A payload that some target nodes failed is pushed again to those nodes only,
// up to partRetryAttempts times; one that still misses a node fails the import (exit 3), which
// lists the work items done and not done. The rows of the parts that landed are counted per
// (catalog, group, target stage) for verify.
func PushParts(ctx context.Context, liaisons []*cluster.Liaison, art *Artifact, entries []Entry, opts PushOptions) (*PushResult, error) {
	if opts.Log == nil {
		opts.Log = io.Discard
	}
	p := &partPusher{art: art, opts: opts, liaisons: liaisons, rows: map[PushKey]uint64{}, done: map[string]bool{}}
	p.limiter = ratelimit.NewClient(opts.PartRateLimit, int(opts.PartChunkSize))
	if opts.Progress != nil {
		// Built over every entry, both rounds, so each group counts all its items.
		p.progress = newImportProgress(opts.Progress, entries)
	}
	result := &PushResult{Rows: p.rows}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var indexes, rest []Entry
	for _, e := range entries {
		if e.Item.Kind == SegmentItem {
			indexes = append(indexes, e)
		} else {
			rest = append(rest, e)
		}
	}
	for _, round := range [][]Entry{indexes, rest} {
		if runCtx.Err() != nil {
			break
		}
		p.runRound(runCtx, cancel, round)
	}
	return finishPush(runCtx, opts.errLog(), entries, p.done, result, opts.track, "stream and trace parts and element indexes")
}

// partPusher is the state shared by the workers of PushParts.
type partPusher struct {
	art      *Artifact
	limiter  *rate.Limiter
	progress *importProgress
	rows     map[PushKey]uint64
	done     map[string]bool
	liaisons []*cluster.Liaison
	// keys serializes the shard items that map to the same target (segment, shard).
	keys keyLocks
	opts PushOptions
	mu   sync.Mutex
}

// runRound pushes entries with a pool of workers; the first failure cancels the round and the
// rounds after it.
func (p *partPusher) runRound(ctx context.Context, cancel context.CancelCauseFunc, entries []Entry) {
	workers, _ := WorkerCount(p.opts.Parallelism, entries)
	if workers == 0 {
		return
	}
	work := make(chan int, len(entries))
	for i := range entries {
		work <- i
	}
	close(work)
	runWorkers(ctx, cancel, workers, "import-parts-worker", func(ctx context.Context, i int) {
		for idx := range work {
			if ctx.Err() != nil {
				return
			}
			if err := p.pushEntry(ctx, i, &entries[idx]); err != nil {
				if ctx.Err() == nil {
					cancel(fmt.Errorf("%s: %w", entries[idx].Item.Key(), err))
				}
				return
			}
		}
	})
}

// pushEntry pushes every chosen source of one work item.
func (p *partPusher) pushEntry(ctx context.Context, worker int, e *Entry) error {
	if e.Item.Kind == ShardItem && reconcilable(e.Item.Catalog, nativefmt.KindPart) {
		// Two source stages falling back to one target stage map to the same target key; their
		// parts are pushed one after another, so at most one part per key is in flight while the
		// counts of the key may decide a part.
		key := entryTargetKey(e, "")
		defer p.keys.lock(key)()
		// A worker that waited for the key while the run was canceled records nothing in flight.
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if p.opts.reconciler != nil {
			// The counts of the shard move while the item pushes: a part of another segment of the
			// shard reconciled meanwhile names it when it cannot be decided.
			defer p.opts.reconciler.pushing(key)()
		}
	}
	item := &itemProgress{}
	p.progress.itemStarted(e, item)
	var rows uint64
	for i := range e.Item.Sources {
		n, err := p.pushSource(ctx, worker, e, &e.Item.Sources[i], item)
		if err != nil {
			return fmt.Errorf("source %s: %w", e.Item.Sources[i].Node, err)
		}
		rows += n
	}
	if err := p.opts.track.itemComplete(e.Item.Key()); err != nil {
		return err
	}
	p.mu.Lock()
	p.done[e.Item.Key()] = true
	p.rows[PushKey{streamKey: entryStreamKey(e), Kind: e.Item.Kind}] += rows
	p.mu.Unlock()
	p.progress.itemDone(e, item)
	if p.opts.Hook != nil {
		p.opts.Hook(fmt.Sprintf("item %s rows=%d", e.Item.Key(), rows))
	}
	return nil
}

// pushSource pushes the containers of one source of a work item and returns the rows of the
// parts that landed.
func (p *partPusher) pushSource(ctx context.Context, worker int, e *Entry, src *WorkSource, ip *itemProgress) (uint64, error) {
	containers, err := containerVolumes(src.Files)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", errCorrupt, err)
	}
	var rows uint64
	for _, prefix := range slices.Sorted(maps.Keys(containers)) {
		vs, openErr := p.openContainer(containers[prefix])
		if openErr != nil {
			return rows, openErr
		}
		n, pushErr := p.pushContainer(ctx, worker, e, src, prefix, vs, ip)
		rows += n
		closeErr := vs.Close()
		if pushErr != nil {
			return rows, pushErr
		}
		if closeErr != nil {
			return rows, fmt.Errorf("container %s: %w", prefix, closeErr)
		}
	}
	return rows, nil
}

func (p *partPusher) openContainer(files []manifest.File) (*bnc.VolumeSet, error) {
	vs, err := openVolumes(p.art.Dir, files)
	if err != nil {
		return nil, fmt.Errorf("%w: open container %s: %w", errCorrupt, files[0].Path, err)
	}
	return vs, nil
}

// pushContainer pushes one container: the series index of a segment item, the parts and the
// element index of a shard item, or the index of a property shard. The rows of each part
// count towards the item's progress once the part is acknowledged.
func (p *partPusher) pushContainer(ctx context.Context, worker int, e *Entry, src *WorkSource, prefix string, vs *bnc.VolumeSet,
	ip *itemProgress,
) (uint64, error) {
	item := &e.Item
	init := &transferv1.ImportPartsRequest_Init{
		Catalog: item.Catalog, Group: item.Group, Stage: e.TargetStage, ShardId: item.Shard, SegmentVersion: item.SegmentVersion,
	}
	switch item.Kind {
	case SegmentItem:
		if src.MinTimestamp <= 0 {
			return 0, fmt.Errorf("the series index of %s has no time range", prefix)
		}
		init.Kind, init.ShardIds = nativefmt.KindSeriesIndex, item.Shards
		init.Parts = []*transferv1.ImportPartsRequest_PartInfo{{MinTimestamp: src.MinTimestamp, MaxTimestamp: src.MaxTimestamp}}
		_, err := p.pushPayload(ctx, worker, e, blobRef(src.Node, init.Kind), init, payloadFiles(vs, newestFirst(vs.Files()), 0))
		return 0, err
	case PropertyItem:
		init.Kind = nativefmt.KindProperty
		_, err := p.pushPayload(ctx, worker, e, blobRef(src.Node, init.Kind), init, payloadFiles(vs, vs.Files(), 0))
		return 0, err
	case ShardItem:
	}
	layout := vs.Shard()
	// An element index that cannot be pushed fails the shard before any of its parts is stored,
	// so a rerun does not store those parts twice.
	if len(layout.ElementIndex) > 0 {
		if src.MinTimestamp <= 0 {
			return 0, fmt.Errorf("the element index of %s has no time range", prefix)
		}
		if err := nativefmt.Supports(item.Catalog, nativefmt.KindElementIndex); err != nil {
			return 0, err
		}
	}
	if len(layout.Orphans) > 0 {
		warnOrphanFiles(p.opts.Log, item.Key(), len(layout.Orphans), prefix)
	}
	var rows uint64
	for _, g := range layout.Parts {
		partInit, files, err := partPayload(vs, init, &g)
		if err != nil {
			return rows, err
		}
		partRows := partInit.GetParts()[0].GetTotalCount()
		sent, pushErr := p.pushPayload(ctx, worker, e, partRef(src.Node, g.PartID, partRows), partInit, files)
		if pushErr != nil {
			return rows, fmt.Errorf("part %016x of %s: %w", g.PartID, src.Node, pushErr)
		}
		if sent {
			rows += partRows
		}
		p.progress.acked(e, ip, int(partRows))
	}
	if len(layout.ElementIndex) > 0 {
		idx := cloneInit(init)
		idx.Kind = nativefmt.KindElementIndex
		idx.Parts = []*transferv1.ImportPartsRequest_PartInfo{{MinTimestamp: src.MinTimestamp, MaxTimestamp: src.MaxTimestamp}}
		if _, err := p.pushPayload(ctx, worker, e, blobRef(src.Node, idx.Kind), idx, payloadFiles(vs, newestFirst(layout.ElementIndex), 0)); err != nil {
			return rows, fmt.Errorf("element index: %w", err)
		}
	}
	return rows, nil
}

// partPayload builds the init and the files of one part: the core part first, then the
// secondary index parts sharing its id, each described by its metadata file.
func partPayload(vs *bnc.VolumeSet, base *transferv1.ImportPartsRequest_Init, g *bnc.PartGroup) (*transferv1.ImportPartsRequest_Init, []payloadFile, error) {
	init := cloneInit(base)
	init.Kind = nativefmt.KindPart
	var files []payloadFile
	var core *transferv1.ImportPartsRequest_PartInfo
	for i, sub := range g.Subs {
		meta, err := vs.ReadFile(sub.Meta, maxMetaFileSize)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: read %s: %w", errCorrupt, sub.Meta.Path, err)
		}
		info, err := nativefmt.ParsePartInfo(g.PartID, sub.PartType, meta, core)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", errCorrupt, err)
		}
		if i == 0 {
			core = info
		}
		init.Parts = append(init.Parts, info)
		files = append(files, payloadFiles(vs, sub.Files, uint32(i))...)
	}
	return init, files, nil
}

// cloneInit copies init without its parts and retry nodes, which every payload sets itself.
func cloneInit(init *transferv1.ImportPartsRequest_Init) *transferv1.ImportPartsRequest_Init {
	out := proto.Clone(init).(*transferv1.ImportPartsRequest_Init)
	out.Parts, out.RetryNodes = nil, nil
	return out
}

// newestFirst orders the files of a native index by descending name, so the newest segment
// is introduced first: the series index receiver keeps the first copy of a document it is sent,
// and a document a newer segment rewrote should win over its older copy. This approximates the
// source only: the deletion masks of its generation manifest are not sent, so a document the
// source masked without a newer copy is live again on the target.
func newestFirst(files []bnc.File) []bnc.File {
	out := slices.Clone(files)
	slices.SortFunc(out, func(a, b bnc.File) int { return strings.Compare(b.Path, a.Path) })
	return out
}

// payloadFile is one file of a payload and the part it belongs to.
type payloadFile struct {
	vs   *bnc.VolumeSet
	file bnc.File
	part uint32
}

func payloadFiles(vs *bnc.VolumeSet, files []bnc.File, part uint32) []payloadFile {
	out := make([]payloadFile, len(files))
	for i, f := range files {
		out[i] = payloadFile{vs: vs, file: f, part: part}
	}
	return out
}

// pushPayload sends one payload and pushes it again to the target nodes that failed it.
// nativefmt decides which catalogs take a payload kind, such as an element index; any other
// fails the import. A payload the progress file records as done is not sent, and one that
// missed nodes is sent to those nodes only; sent reports whether it was sent and stored. Where
// the payload landed is recorded after every answer.
func (p *partPusher) pushPayload(ctx context.Context, worker int, e *Entry, ref payloadRef, init *transferv1.ImportPartsRequest_Init,
	files []payloadFile,
) (sent bool, err error) {
	item := &e.Item
	if err = nativefmt.Supports(init.GetCatalog(), init.GetKind()); err != nil {
		return false, err
	}
	var sendable []payloadFile
	for _, f := range files {
		_, skip, nameErr := nativefmt.SyncFileName(init.GetCatalog(), init.GetKind(), nativefmt.PartTypeAt(init, f.part), path.Base(f.file.Path))
		if nameErr != nil {
			return false, fmt.Errorf("%w: %s: %w", errCorrupt, f.file.Path, nameErr)
		}
		if !skip {
			sendable = append(sendable, f)
		}
	}
	if len(sendable) == 0 {
		return true, nil
	}
	rec, init, done := p.resumed(item, ref, init)
	if done {
		return false, nil
	}
	// A stream or trace part that may have landed on a failed target is reconciled from the
	// target's counts; it needs the reconciler's baseline.
	reconcile := p.opts.reconciler != nil && reconcilable(init.GetCatalog(), init.GetKind()) && len(init.GetParts()) > 0
	backoff := transfer.RetryBase
	for attempt := 0; ; attempt++ {
		nodes := init.GetRetryNodes()
		if len(nodes) == 0 {
			nodes = e.TargetNodes
		}
		resp, sendErr := p.sendRecorded(ctx, worker, attempt, e, ref, rec, init, sendable, nodes)
		if sendErr != nil {
			return false, sendErr
		}
		a := p.answers(item, init, resp, nodes, rec, reconcile)
		failed, errs, ambiguous := a.failed, a.errs, a.ambiguous
		// A stream or trace part, or an element index, whose completion reached a target may be
		// stored there already; pushing it again would store its rows twice. A stream or trace part
		// is reconciled from the counts; any other such failure is final.
		if len(ambiguous) > 0 && nativefmt.Duplicable(init.GetCatalog(), init.GetKind()) {
			ambiguousErr := fmt.Errorf("%s failed on %s after the completion was sent, so the payload may be stored on them already: %s",
				init.GetKind(), strings.Join(ambiguous, ", "), strings.Join(errs, "; "))
			if !reconcile {
				return false, errors.Join(ambiguousErr, p.opts.track.payloadAnswered(worker, e, ref, rec, false))
			}
			outcomes, reconcileErr := p.reconcileAmbiguous(ctx, item, ref.source, init, ambiguous, rec.Causes)
			// What was decided is recorded even when another node is undecidable.
			for n := range outcomes {
				rec.answered(n, true, false, "")
			}
			if reconcileErr != nil {
				// The part may still land on the nodes left undecided: no later part at their keys
				// is decided from counts it may or may not hold, whichever worker pushes it.
				for _, n := range ambiguous {
					if !outcomes[n] {
						p.opts.reconciler.taint(payloadTargetKey(item, init, n))
					}
				}
				return false, errors.Join(fmt.Errorf("%w; %w", ambiguousErr, reconcileErr), p.opts.track.payloadAnswered(worker, e, ref, rec, false))
			}
			failed, errs = withoutLanded(failed, errs, outcomes)
		}
		// requested_copies is per shard; a series index's targets span several shards.
		if attempt == 0 && len(init.GetRetryNodes()) == 0 && init.GetKind() != nativefmt.KindSeriesIndex && uint32(len(resp.GetTargets())) < resp.GetRequestedCopies() {
			warnFewerCopies(p.opts.Log, item.Key(), resp.GetRequestedCopies(), init.GetKind(), len(resp.GetTargets()))
		}
		if len(failed) == 0 {
			if err = p.opts.track.payloadAnswered(worker, e, ref, rec, true); err != nil {
				return false, err
			}
			return true, nil
		}
		if err = p.opts.track.payloadAnswered(worker, e, ref, rec, false); err != nil {
			return false, err
		}
		if attempt+1 >= partRetryAttempts {
			return false, fmt.Errorf("%s not written to %d node(s) after %d attempt(s): %s", init.GetKind(), len(failed), attempt+1, strings.Join(errs, "; "))
		}
		// Where the payload landed is on disk before the back-off: a crash meanwhile does not leave
		// the nodes that stored it ambiguous.
		if err = p.opts.track.flush(); err != nil {
			return false, err
		}
		warnPayloadRetry(p.opts.Log, item.Key(), init.GetKind(), strings.Join(errs, "; "), backoff)
		if err = transfer.SleepCtx(ctx, backoff); err != nil {
			return false, err
		}
		backoff = transfer.NextBackoff(backoff)
		init = proto.Clone(init).(*transferv1.ImportPartsRequest_Init)
		init.RetryNodes = failed
	}
}

// payloadAnswers are the target nodes an answer names as failed, with their errors, and those of
// them that failed after the completion was sent: rec.Causes tells why each of these failed.
type payloadAnswers struct {
	failed, errs, ambiguous []string
}

// answers records the answer of every target of a payload sent to nodes in rec, and the rows of a
// part that landed in the reconciler when reconcile.
func (p *partPusher) answers(item *WorkItem, init *transferv1.ImportPartsRequest_Init, resp *transferv1.ImportPartsResponse, nodes []string,
	rec *payloadRecord, reconcile bool,
) payloadAnswers {
	var a payloadAnswers
	for _, t := range resp.GetTargets() {
		if !slices.Contains(nodes, t.GetNode()) {
			warnUnpredictedNode(p.opts.Log, item.Key(), init.GetKind(), t.GetNode(), strings.Join(nodes, ", "))
		}
		if t.GetSuccess() && reconcile {
			p.opts.reconciler.recordLanded(payloadTargetKey(item, init, t.GetNode()), init.GetParts()[0].GetTotalCount())
		}
		cause := failureCauseName(t.GetFailureCause())
		// A target that refused the payload gave a definite answer, whenever it did: it is
		// missing there and pushed again, not reconciled.
		completionSent := t.GetCompletionSent() && cause != causeRefused
		rec.answered(t.GetNode(), t.GetSuccess(), completionSent, cause)
		if t.GetSuccess() {
			continue
		}
		a.failed = append(a.failed, t.GetNode())
		a.errs = append(a.errs, t.GetNode()+": "+t.GetError())
		if completionSent {
			a.ambiguous = append(a.ambiguous, t.GetNode())
		}
	}
	return a
}

// sendRecorded records the payload in flight to nodes and sends it. A canceled run sends nothing
// more, so it leaves nothing in flight it did not send. When no attempt sent the completion,
// every target dropped the payload: it is recorded missing there. When a stop cut the wait for
// the answer after the completion short, every node is recorded with no_answer: it may still
// commit the payload. Without any answer otherwise the payload stays in flight in the progress
// file: a resume treats it as ambiguous.
func (p *partPusher) sendRecorded(ctx context.Context, worker, attempt int, e *Entry, ref payloadRef, rec *payloadRecord,
	init *transferv1.ImportPartsRequest_Init, sendable []payloadFile, nodes []string,
) (*transferv1.ImportPartsResponse, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if len(nodes) == 0 {
		// A resume treats the payload as in flight to the nodes recorded: it must name them.
		return nil, fmt.Errorf("%s has no target node to push to: the plan routed it nowhere", init.GetKind())
	}
	if err := p.opts.track.payloadSending(worker, e, ref, nodes); err != nil {
		return nil, err
	}
	resp, completed, err := p.sendPayload(ctx, worker+attempt, init, sendable)
	switch {
	case err != nil && !completed:
		for _, n := range nodes {
			rec.answered(n, false, false, "")
		}
		err = errors.Join(err, p.opts.track.payloadAnswered(worker, e, ref, rec, false))
	case errors.Is(err, errNoAnswerAfterStop):
		// Like a target the liaison stopped waiting for, every node may still commit the payload.
		for _, n := range nodes {
			rec.answered(n, false, true, causeNoAnswer)
		}
		err = errors.Join(err, p.opts.track.payloadAnswered(worker, e, ref, rec, false))
	}
	return resp, err
}

// resumed starts the record of a payload from what the progress file holds: a payload done is not
// sent again, and one that missed nodes is sent to those nodes only.
func (p *partPusher) resumed(item *WorkItem, ref payloadRef, init *transferv1.ImportPartsRequest_Init,
) (*payloadRecord, *transferv1.ImportPartsRequest_Init, bool) {
	prev := p.opts.resume.payload(item.Key(), ref)
	if prev == nil {
		return &payloadRecord{Rows: ref.rows}, init, false
	}
	if prev.done() {
		return nil, init, true
	}
	rec := prev.clone()
	init = proto.Clone(init).(*transferv1.ImportPartsRequest_Init)
	init.RetryNodes = slices.Clone(rec.Missing)
	return rec, init, false
}

// answered records one target's answer: stored, failed before the completion was sent, or
// failed after it, for cause (a failureCauseName, "" when the liaison named none). The latest
// answer of a node replaces what an earlier one said, its cause too; only a node of
// CompletionSent keeps a cause.
func (r *payloadRecord) answered(node string, success, completionSent bool, cause string) {
	drop := func(list []string) []string { return slices.DeleteFunc(list, func(n string) bool { return n == node }) }
	r.Nodes, r.Missing, r.CompletionSent = drop(r.Nodes), drop(r.Missing), drop(r.CompletionSent)
	delete(r.Causes, node)
	if len(r.Causes) == 0 {
		r.Causes = nil
	}
	switch {
	case success:
		r.Nodes = transfer.UniqueSorted(append(r.Nodes, node))
	case completionSent:
		r.CompletionSent = transfer.UniqueSorted(append(r.CompletionSent, node))
		if cause != "" {
			if r.Causes == nil {
				r.Causes = map[string]string{}
			}
			r.Causes[node] = cause
		}
	default:
		r.Missing = transfer.UniqueSorted(append(r.Missing, node))
	}
}

// reconcileAmbiguous decides from the target's counts whether the stream or trace part of init
// landed on each ambiguous node, and logs each decision: outcomes holds the nodes the part landed
// on, each true; in the run no node is decided as not landed. A node it cannot decide for fails
// the payload; so does a node that reads as not landed: its stream broke or the liaison stopped waiting for it after the
// completion (causes names why, per node), and a data node commits a part whose completion it
// received whatever the stream, so it may still be committing the part. A resume decides it later.
func (p *partPusher) reconcileAmbiguous(ctx context.Context, item *WorkItem, source string, init *transferv1.ImportPartsRequest_Init,
	ambiguous []string, causes map[string]string,
) (outcomes map[string]bool, err error) {
	part := init.GetParts()[0]
	queries := make([]reconcileQuery, len(ambiguous))
	for i, node := range ambiguous {
		queries[i] = reconcileQuery{Key: payloadTargetKey(item, init, node), ID: part.GetId(), Rows: part.GetTotalCount()}
	}
	decisions := p.opts.reconciler.decideAll(ctx, queries)
	outcomes = map[string]bool{}
	var undecided []string
	for i, d := range decisions {
		node := ambiguous[i]
		if d.Outcome == reconcileNotLanded {
			d.Outcome, d.Reason = reconcileUndecidable, notLandedInRun(causes[node])
		}
		line := reconcileLine(item.Key(), part.GetId(), source, node, d, part.GetTotalCount())
		if d.Outcome == reconcileLanded {
			outcomes[node] = true
			p.opts.reconciler.recordLanded(queries[i].Key, part.GetTotalCount())
		} else {
			line += ": " + d.Reason
			undecided = append(undecided, fmt.Sprintf("%s %s (%s): %s", node, d.Outcome, d, d.Reason))
		}
		fmt.Fprintln(p.opts.Log, line)
	}
	if len(undecided) > 0 {
		return outcomes, fmt.Errorf("reconciliation: %s", strings.Join(undecided, "; "))
	}
	return outcomes, nil
}

// notLandedInRun is why a part that reads as not landed on a node whose answer failed after the
// completion for cause is left to a resume: the node may still be committing it.
func notLandedInRun(cause string) string {
	switch cause {
	case causeNoAnswer:
		return "the liaison stopped waiting for the node, which may still be committing the part; a resume decides it"
	case causeStreamBroken:
		return "the connection to the node broke after the completion, and the node may still be committing the part; a resume decides it"
	default:
		return "the node failed after the completion, and it may still be committing the part; a resume decides it"
	}
}

// The failure causes of a target as the progress file names them (payloadRecord.Causes).
const (
	causeUnreachable  = "unreachable"
	causeRefused      = "refused"
	causeStreamBroken = "stream_broken"
	causeNoAnswer     = "no_answer"
)

// failureCauseName names cause for the progress file: "" for FAILURE_CAUSE_UNSPECIFIED and for a
// value this client does not know, which it takes as unspecified.
func failureCauseName(cause transferv1.ImportPartsResponse_FailureCause) string {
	switch cause {
	case transferv1.ImportPartsResponse_FAILURE_CAUSE_UNREACHABLE:
		return causeUnreachable
	case transferv1.ImportPartsResponse_FAILURE_CAUSE_REFUSED:
		return causeRefused
	case transferv1.ImportPartsResponse_FAILURE_CAUSE_STREAM_BROKEN:
		return causeStreamBroken
	case transferv1.ImportPartsResponse_FAILURE_CAUSE_NO_ANSWER:
		return causeNoAnswer
	default:
		return ""
	}
}

// causeMayCommitLater tells whether a node that failed after the completion for cause may still
// be committing the payload after its answer, so counts that read it as not landed cannot decide
// it, in the run or on a resume.
func causeMayCommitLater(cause string) bool { return cause == causeNoAnswer }

// withoutLanded drops from failed (and errs, the matching messages) the nodes the part landed on.
func withoutLanded(failed, errs []string, outcomes map[string]bool) (stillFailed, stillErrs []string) {
	for i, node := range failed {
		if !outcomes[node] {
			stillFailed = append(stillFailed, node)
			stillErrs = append(stillErrs, errs[i])
		}
	}
	return stillFailed, stillErrs
}

// payloadTargetKey is the target key of a part payload on node.
func payloadTargetKey(item *WorkItem, init *transferv1.ImportPartsRequest_Init, node string) targetKey {
	return partTargetKey(init.GetCatalog(), init.GetGroup(), init.GetStage(), item.Segment, init.GetShardId(), node)
}

// entryTargetKey is the target key of a part of the entry on node; "" leaves the node out.
func entryTargetKey(e *Entry, node string) targetKey {
	return partTargetKey(e.Item.Catalog, e.Item.Group, e.TargetStage, e.Item.Segment, e.Item.Shard, node)
}

// partTargetKey is the target key of a part of (catalog, group, target stage, segment, shard)
// on node; "" leaves the node out.
func partTargetKey(catalog commonv1.Catalog, group, stage, segment string, shard uint32, node string) targetKey {
	return targetKey{Catalog: catalog, Group: group, Stage: normalizeStage(stage), Segment: segment, Shard: shard, Node: node}
}

// sendPayload opens an ImportParts stream through a liaison and sends one payload. A stream
// the liaison refuses, as unavailable, busy or not knowing the group yet, is opened again on the
// next liaison within --send-retry-timeout, unless a stream or trace part may already be stored:
// see retryablePayload. completed reports whether any attempt sent the completion frame: without
// it no target can have committed the payload.
func (p *partPusher) sendPayload(ctx context.Context, next int, init *transferv1.ImportPartsRequest_Init, files []payloadFile,
) (resp *transferv1.ImportPartsResponse, completed bool, err error) {
	deadline := time.Now().Add(p.opts.SendRetryTimeout)
	backoff := transfer.RetryBase
	for {
		via := p.liaisons[next%len(p.liaisons)]
		var sent bool
		resp, sent, err = p.sendOnce(ctx, via, init, files)
		completed = completed || sent
		if err == nil {
			return resp, completed, nil
		}
		if ctx.Err() != nil {
			if errors.Is(err, errNoAnswerAfterStop) {
				return nil, completed, fmt.Errorf("%w: %w", context.Cause(ctx), err)
			}
			return nil, completed, context.Cause(ctx)
		}
		code, reason, _ := failureOf(err)
		sentDuplicable := sent && nativefmt.Duplicable(init.GetCatalog(), init.GetKind())
		if !retryablePayload(code, reason, sentDuplicable) {
			if sentDuplicable {
				return nil, completed, fmt.Errorf("via liaison %s, after the completion was sent, so the payload may be stored on its targets already: %w", via.Addr, err)
			}
			return nil, completed, fmt.Errorf("via liaison %s: %w", via.Addr, err)
		}
		if time.Now().Add(backoff).After(deadline) {
			return nil, completed, fmt.Errorf("via liaison %s: %w", via.Addr, err)
		}
		warnLiaisonRefused(p.opts.Log, via.Addr, init.GetKind(), err, backoff)
		if sleepErr := transfer.SleepCtx(ctx, backoff); sleepErr != nil {
			return nil, completed, sleepErr
		}
		backoff = transfer.NextBackoff(backoff)
		if !schemaNotFound(code, reason) {
			next++
		}
	}
}

// retryablePayload tells whether a payload the liaison refused may be sent again as a whole.
// Before its completion is sent no target can have committed it: a data node drops a part it
// did not complete. After it, the liaison may have relayed the completion and every target
// committed the part before the stream broke; a series index segment introduced twice and a
// measure part of the same versions are deduplicated, but a stream or trace part or an element
// index sent again stores its rows/documents twice. So when sentDuplicable, a completed
// duplicable payload, an UNAVAILABLE or ABORTED without a detail, which a broken stream
// reports, is final. A schema
// not live yet and load shedding come before the liaison relays anything and stay retryable;
// a frame over the liaison's message size limit never fits, so it is final.
func retryablePayload(code codes.Code, reason string, sentDuplicable bool) bool {
	if reason == transfer.ImportReasonMessageTooLarge {
		return false
	}
	if schemaNotFound(code, reason) || code == codes.ResourceExhausted {
		return true
	}
	if code == codes.Unavailable || code == codes.Aborted {
		return reason != "" || !sentDuplicable
	}
	return false
}

// sendOnce sends one payload on one ImportParts stream: init, the files in chunks of
// --part-chunk-size bytes, then the completion. completed reports whether the completion frame
// went out, after which the targets may have committed the payload whatever the error. A stop
// (ctx ending) before the completion cancels the stream at once; once the completion is sent, the
// stream waits up to drainTimeout for the answer, so that where the payload landed is recorded
// instead of left ambiguous.
func (p *partPusher) sendOnce(ctx context.Context, via *cluster.Liaison, init *transferv1.ImportPartsRequest_Init,
	files []payloadFile,
) (resp *transferv1.ImportPartsResponse, completed bool, err error) {
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopping := p.drainOnStop(ctx, init, cancel)
	defer stopping.release()
	//nolint:contextcheck // the stream outlives ctx by up to drainTimeout to collect the answer of a sent completion
	stream, err := transferv1.NewImportServiceClient(via.Conn).ImportParts(streamCtx)
	if err != nil {
		return nil, false, err
	}
	send := func(req *transferv1.ImportPartsRequest) error {
		if sendErr := stream.Send(req); sendErr != nil {
			if errors.Is(sendErr, io.EOF) {
				// The liaison answered early, every target having failed, or ended the stream.
				return errEarlyAnswer
			}
			return sendErr
		}
		return nil
	}
	sendAll := func() error {
		if sendErr := send(&transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Init_{Init: init}}); sendErr != nil {
			return sendErr
		}
		c := &chunker{size: int(p.opts.PartChunkSize), limiter: p.limiter, send: send}
		for _, f := range files {
			if addErr := c.addFile(ctx, f); addErr != nil {
				return addErr
			}
		}
		if flushErr := c.flush(ctx, true); flushErr != nil {
			return flushErr
		}
		if !stopping.completing(ctx) {
			return context.Cause(ctx)
		}
		// A Send that fails did not deliver the frame: the completion never reached the liaison.
		return send(&transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Completion_{
			Completion: &transferv1.ImportPartsRequest_Completion{TotalBytes: c.total, TotalChunks: c.chunks},
		}})
	}
	if err = sendAll(); err != nil {
		resp, err = closeEarly(stream, err)
		return resp, false, err
	}
	resp, err = stream.CloseAndRecv()
	if err != nil && stopping.drainedOut() {
		err = fmt.Errorf("%w: %w", errNoAnswerAfterStop, err)
	}
	return resp, true, err
}

var (
	errEarlyAnswer = errors.New("the liaison ended the stream early")
	// errNoAnswerAfterStop is a payload whose answer did not come within drainTimeout of a stop
	// after its completion was sent.
	errNoAnswerAfterStop = errors.New("no answer came within the drain timeout after the completion was sent")
)

// payloadStop is how a stop reaches the stream of one payload: before the completion it cancels the
// stream at once, after it the stream is canceled drainTimeout later, unless the answer came first.
type payloadStop struct {
	timer   *time.Timer
	stopAll func() bool
	mu      sync.Mutex
	stopped bool
	sending bool
	// drained records that drainTimeout ran out before the answer came, and canceled the stream.
	drained bool
	// released records that the stream is done: a stop callback that runs after it does nothing.
	released bool
}

// drainOnStop arranges what a stop of ctx does to the stream that cancel cancels.
func (p *partPusher) drainOnStop(ctx context.Context, init *transferv1.ImportPartsRequest_Init, cancel context.CancelFunc) *payloadStop {
	s := &payloadStop{}
	drain := drainTimeout
	s.stopAll = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.released {
			return
		}
		s.stopped = true
		if !s.sending {
			cancel()
			return
		}
		waitingForPayload(p.opts.Log, drain, init.GetKind(), init.GetGroup(), cmpStage(init.GetStage()), init.GetShardId(), interrupted(ctx))
		s.timer = time.AfterFunc(drain, func() {
			s.mu.Lock()
			s.drained = true
			s.mu.Unlock()
			cancel()
		})
	})
	return s
}

// drainedOut tells whether drainTimeout ran out before the answer of the sent completion came.
func (s *payloadStop) drainedOut() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drained
}

// completing marks the completion as about to be sent; false when the run is stopping already,
// so that nothing is completed after a stop: ctx is checked too, as the stop callback may not have
// run yet.
func (s *payloadStop) completing(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || ctx.Err() != nil {
		s.stopped = true
		return false
	}
	s.sending = true
	return true
}

// release ends the watch of the stop once the stream is done.
func (s *payloadStop) release() {
	s.stopAll()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = true
	if s.timer != nil {
		s.timer.Stop()
	}
}

// cmpStage names the default stage for messages.
func cmpStage(stage string) string {
	if stage == "" {
		return transfer.DefaultStageName
	}
	return stage
}

// closeEarly reads the answer of a stream that ended while sending: the response of a liaison
// that answered early, or the status it failed with.
func closeEarly(stream transferv1.ImportService_ImportPartsClient, err error) (*transferv1.ImportPartsResponse, error) {
	if !errors.Is(err, errEarlyAnswer) {
		return nil, err
	}
	return stream.CloseAndRecv()
}

// chunker packs the files of a payload into chunks: each chunk holds up to size bytes and lists
// the slices of the files it holds, in order.
type chunker struct {
	limiter *rate.Limiter
	send    func(*transferv1.ImportPartsRequest) error
	buf     []byte
	slices  []*transferv1.ImportPartsRequest_FileSlice
	size    int
	total   uint64
	chunks  uint32
}

// addFile appends one file, sending every chunk that fills up.
func (c *chunker) addFile(ctx context.Context, f payloadFile) (err error) {
	r, err := f.vs.Open(f.file)
	if err != nil {
		return err
	}
	defer func() {
		// The read of the last byte of a piece checks its CRC-32; Close reports a failure a
		// full read dropped.
		if closeErr := r.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("%w: %s: %w", errCorrupt, f.file.Path, closeErr)
		}
	}()
	name := path.Base(f.file.Path)
	remaining := f.file.Size
	// A zero-length file still needs its slice: the receiver creates it.
	if remaining == 0 {
		c.slices = append(c.slices, &transferv1.ImportPartsRequest_FileSlice{Name: name, PartIndex: f.part})
		return nil
	}
	for remaining > 0 {
		if c.buf == nil {
			c.buf = make([]byte, 0, c.size)
		}
		n := int(min(int64(c.size-len(c.buf)), remaining))
		start := len(c.buf)
		c.buf = c.buf[:start+n]
		if _, err = io.ReadFull(r, c.buf[start:]); err != nil {
			return fmt.Errorf("%w: read %s: %w", errCorrupt, f.file.Path, err)
		}
		c.slices = append(c.slices, &transferv1.ImportPartsRequest_FileSlice{Name: name, Size: uint32(n), PartIndex: f.part})
		remaining -= int64(n)
		if len(c.buf) == c.size {
			if err = c.flush(ctx, false); err != nil {
				return err
			}
		}
	}
	// The read past the end checks the CRC-32 of the last piece.
	if _, err = r.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("file is longer than %d bytes", f.file.Size)
		}
		return fmt.Errorf("%w: %s: %w", errCorrupt, f.file.Path, err)
	}
	return nil
}

// flush sends the chunk being filled; last sends it even when it holds only empty files, or
// when nothing was sent yet.
func (c *chunker) flush(ctx context.Context, last bool) error {
	if len(c.buf) == 0 && len(c.slices) == 0 && (!last || c.chunks > 0) {
		return nil
	}
	if err := ratelimit.Wait(ctx, c.limiter, len(c.buf)); err != nil {
		return err
	}
	data := c.buf
	c.buf = nil
	req := &transferv1.ImportPartsRequest{Frame: &transferv1.ImportPartsRequest_Chunk_{
		Chunk: &transferv1.ImportPartsRequest_Chunk{Data: data, Files: c.slices},
	}}
	c.slices = nil
	c.total += uint64(len(data))
	c.chunks++
	return c.send(req)
}

// openVolumes opens the volumes of one container, files in volume order, under artifact dir.
func openVolumes(dir string, files []manifest.File) (*bnc.VolumeSet, error) {
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = filepath.Join(dir, filepath.FromSlash(f.Path))
	}
	return bnc.OpenVolumes(paths)
}

// containerVolumes groups the .bnc volumes of a source's files by container prefix, each in
// volume order; other files (the segment metadata) are not pushed. A .bnc file not named as a
// volume is an error.
func containerVolumes(files []manifest.File) (map[string][]manifest.File, error) {
	out := map[string][]manifest.File{}
	for _, f := range files {
		prefix, _, ok := bnc.ParseVolumeName(f.Path)
		switch {
		case ok:
			out[prefix] = append(out[prefix], f)
		case strings.HasSuffix(f.Path, bnc.VolumeExt):
			return nil, fmt.Errorf("artifact file %s is not named as a container volume", f.Path)
		}
	}
	for _, vols := range out {
		slices.SortFunc(vols, func(a, b manifest.File) int { return strings.Compare(a.Path, b.Path) })
	}
	return out, nil
}
