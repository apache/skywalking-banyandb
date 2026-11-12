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
	if opts.PartRateLimit > 0 {
		p.limiter = rate.NewLimiter(rate.Limit(opts.PartRateLimit), max(int(opts.PartRateLimit), int(opts.PartChunkSize)))
	}
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
	return finishPush(runCtx, opts.Log, entries, p.done, result, "stream and trace parts and element indexes")
}

// partPusher is the state shared by the workers of PushParts.
type partPusher struct {
	art      *Artifact
	limiter  *rate.Limiter
	progress *importProgress
	rows     map[PushKey]uint64
	done     map[string]bool
	liaisons []*cluster.Liaison
	opts     PushOptions
	mu       sync.Mutex
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
		return 0, p.pushPayload(ctx, worker, item, init, payloadFiles(vs, newestFirst(vs.Files()), 0))
	case PropertyItem:
		init.Kind = nativefmt.KindProperty
		return 0, p.pushPayload(ctx, worker, item, init, payloadFiles(vs, vs.Files(), 0))
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
		fmt.Fprintf(p.opts.Log, "WARN %s: %d file(s) of incomplete parts in %s are skipped, a receiver cannot load them\n",
			item.Key(), len(layout.Orphans), prefix)
	}
	var rows uint64
	for _, g := range layout.Parts {
		partInit, files, err := partPayload(vs, init, &g)
		if err != nil {
			return rows, err
		}
		if err = p.pushPayload(ctx, worker, item, partInit, files); err != nil {
			return rows, fmt.Errorf("part %016x: %w", g.PartID, err)
		}
		partRows := partInit.GetParts()[0].GetTotalCount()
		rows += partRows
		p.progress.acked(e, ip, int(partRows))
	}
	if len(layout.ElementIndex) > 0 {
		idx := cloneInit(init)
		idx.Kind = nativefmt.KindElementIndex
		idx.Parts = []*transferv1.ImportPartsRequest_PartInfo{{MinTimestamp: src.MinTimestamp, MaxTimestamp: src.MaxTimestamp}}
		if err := p.pushPayload(ctx, worker, item, idx, payloadFiles(vs, newestFirst(layout.ElementIndex), 0)); err != nil {
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
// fails the import.
func (p *partPusher) pushPayload(ctx context.Context, worker int, item *WorkItem, init *transferv1.ImportPartsRequest_Init, files []payloadFile) error {
	if err := nativefmt.Supports(init.GetCatalog(), init.GetKind()); err != nil {
		return err
	}
	var sendable []payloadFile
	for _, f := range files {
		_, skip, err := nativefmt.SyncFileName(init.GetCatalog(), init.GetKind(), nativefmt.PartTypeAt(init, f.part), path.Base(f.file.Path))
		if err != nil {
			return fmt.Errorf("%w: %s: %w", errCorrupt, f.file.Path, err)
		}
		if !skip {
			sendable = append(sendable, f)
		}
	}
	if len(sendable) == 0 {
		return nil
	}
	backoff := transfer.RetryBase
	for attempt := 0; ; attempt++ {
		resp, err := p.sendPayload(ctx, worker+attempt, init, sendable)
		if err != nil {
			return err
		}
		var failed, errs, ambiguous []string
		for _, t := range resp.GetTargets() {
			if !t.GetSuccess() {
				failed = append(failed, t.GetNode())
				errs = append(errs, t.GetNode()+": "+t.GetError())
				if t.GetCompletionSent() {
					ambiguous = append(ambiguous, t.GetNode())
				}
			}
		}
		// A stream or trace part, or an element index, whose completion reached a target may be
		// stored there already; pushing it again would store its rows twice, so that failure is final.
		if len(ambiguous) > 0 && nativefmt.Duplicable(init.GetCatalog(), init.GetKind()) {
			return fmt.Errorf("%s failed on %s after the completion was sent, so the payload may be stored on them already: %s",
				init.GetKind(), strings.Join(ambiguous, ", "), strings.Join(errs, "; "))
		}
		// requested_copies is per shard; a series index's targets span several shards.
		if attempt == 0 && init.GetKind() != nativefmt.KindSeriesIndex && uint32(len(resp.GetTargets())) < resp.GetRequestedCopies() {
			fmt.Fprintf(p.opts.Log, "WARN %s: the target stage asks for %d copies, the liaison routed %s to %d node(s)\n",
				item.Key(), resp.GetRequestedCopies(), init.GetKind(), len(resp.GetTargets()))
		}
		if len(failed) == 0 {
			return nil
		}
		if attempt+1 >= partRetryAttempts {
			return fmt.Errorf("%s not written to %d node(s) after %d attempt(s): %s", init.GetKind(), len(failed), attempt+1, strings.Join(errs, "; "))
		}
		fmt.Fprintf(p.opts.Log, "WARN %s: %s failed on %s; pushing it again to them in %s\n",
			item.Key(), init.GetKind(), strings.Join(errs, "; "), backoff)
		if err := transfer.SleepCtx(ctx, backoff); err != nil {
			return err
		}
		backoff = transfer.NextBackoff(backoff)
		init = proto.Clone(init).(*transferv1.ImportPartsRequest_Init)
		init.RetryNodes = failed
	}
}

// sendPayload opens an ImportParts stream through a liaison and sends one payload. A stream
// the liaison refuses, as unavailable, busy or not knowing the group yet, is opened again on the
// next liaison within --send-retry-timeout, unless a stream or trace part may already be stored:
// see retryablePayload.
func (p *partPusher) sendPayload(ctx context.Context, next int, init *transferv1.ImportPartsRequest_Init, files []payloadFile) (*transferv1.ImportPartsResponse, error) {
	deadline := time.Now().Add(p.opts.SendRetryTimeout)
	backoff := transfer.RetryBase
	for {
		via := p.liaisons[next%len(p.liaisons)]
		resp, completed, err := p.sendOnce(ctx, via, init, files)
		if err == nil {
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		code, reason, _ := failureOf(err)
		sentDuplicable := completed && nativefmt.Duplicable(init.GetCatalog(), init.GetKind())
		if !retryablePayload(code, reason, sentDuplicable) {
			if sentDuplicable {
				return nil, fmt.Errorf("via liaison %s, after the completion was sent, so the payload may be stored on its targets already: %w", via.Addr, err)
			}
			return nil, fmt.Errorf("via liaison %s: %w", via.Addr, err)
		}
		if time.Now().Add(backoff).After(deadline) {
			return nil, fmt.Errorf("via liaison %s: %w", via.Addr, err)
		}
		fmt.Fprintf(p.opts.Log, "WARN liaison %s refused %s (%v); retrying in %s\n", via.Addr, init.GetKind(), err, backoff)
		if sleepErr := transfer.SleepCtx(ctx, backoff); sleepErr != nil {
			return nil, sleepErr
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
// went out, after which the targets may have committed the payload whatever the error.
func (p *partPusher) sendOnce(ctx context.Context, via *cluster.Liaison, init *transferv1.ImportPartsRequest_Init,
	files []payloadFile,
) (resp *transferv1.ImportPartsResponse, completed bool, err error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
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
	return resp, true, err
}

var errEarlyAnswer = errors.New("the liaison ended the stream early")

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
