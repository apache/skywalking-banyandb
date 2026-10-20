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

package exporter

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

var (
	// exportRetryBase and exportRetryMax bound the back-off of opening an Export stream that
	// the server refused as busy or unavailable (--send-retry-timeout); tests shorten them.
	exportRetryBase = time.Second
	exportRetryMax  = 30 * time.Second
)

// plannedUnit is one unit a node exports: a segment unit (stream, measure, trace) or a
// property group's shards on that node, and what the node streamed for it.
type plannedUnit struct {
	segment  *transferv1.SegmentInventory
	property *transferv1.PropertyInventory
	stage    string
	files    []receivedFile
	warnings []string
	elapsed  time.Duration
}

// receivedFile is one file written and verified, with where it belongs in the manifest.
type receivedFile struct {
	rowCounts    map[string]uint64
	file         manifest.File
	shardID      uint32
	segmentLevel bool
}

func (u *plannedUnit) catalog() commonv1.Catalog {
	if u.property != nil {
		return commonv1.Catalog_CATALOG_PROPERTY
	}
	return u.segment.GetUnit().GetCatalog()
}

func (u *plannedUnit) group() string {
	if u.property != nil {
		return u.property.GetGroup()
	}
	return u.segment.GetUnit().GetGroup()
}

// label names the unit in messages: catalog/group[/seg-suffix].
func (u *plannedUnit) label() string { return u.name("/") }

// displayName names the unit in progress lines: catalog/group[ seg-suffix].
func (u *plannedUnit) displayName() string { return u.name(" ") }

// name is catalog/group, followed by sep and the segment for a segment unit.
func (u *plannedUnit) name(sep string) string {
	n := transfer.CatalogName(u.catalog()) + "/" + u.group()
	if u.segment != nil {
		n += sep + "seg-" + u.segment.GetUnit().GetSegmentSuffix()
	}
	return n
}

// request is the Export frame asking for this unit: the Plan's segment unit unchanged, or a
// property unit with the shards the node reported.
func (u *plannedUnit) request() *transferv1.ExportRequest {
	if u.property != nil {
		return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Property{
			Property: &transferv1.PropertyUnit{Group: u.property.GetGroup(), ShardIds: u.shardIDs()},
		}}
	}
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Segment{Segment: u.segment.GetUnit()}}
}

// shardIDs are the shards the unit asks for.
func (u *plannedUnit) shardIDs() []uint32 {
	if u.property == nil {
		return u.segment.GetUnit().GetShardIds()
	}
	shards := make([]uint32, 0, len(u.property.GetShards()))
	for _, s := range u.property.GetShards() {
		shards = append(shards, s.GetShardId())
	}
	return shards
}

// pathPrefix is the prefix every relative path of this unit must start with.
func (u *plannedUnit) pathPrefix() string {
	return transfer.CatalogName(u.catalog()) + "/" + csvfmt.EscapePathComponent(u.group()) + "/"
}

// nodeJob is everything one worker exports from one data node.
type nodeJob struct {
	nodeID string
	dir    string
	units  []*plannedUnit
}

// nodeExport drives one node's Export stream. It starts on liaisons[next] and moves to the
// following liaison each time opening the stream is refused.
type nodeExport struct {
	job      *nodeJob
	init     *transferv1.ExportRequest_Init
	schema   *schemasync.Result // nil when the schema is not exported
	idle     *idleTimer
	progress *nodeProgress
	log      io.Writer
	root     string // <output>/nodes/<node-dir>
	relRoot  string // nodes/<node-dir>
	liaisons []*Liaison
	next     int
	retryFor time.Duration
	idleFor  time.Duration
}

// liaison is the liaison the next open attempt goes through.
func (n *nodeExport) liaison() *Liaison { return n.liaisons[n.next%len(n.liaisons)] }

// run opens the stream, exports every unit in order and closes it cleanly.
func (n *nodeExport) run(ctx context.Context) error {
	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errIdle := fmt.Errorf("node %s sent nothing for %s (--node-idle-timeout)", n.job.nodeID, n.idleFor)
	n.idle = newIdleTimer(n.idleFor, func() { cancel(errIdle) })
	defer n.idle.stop()
	err := n.export(streamCtx)
	if err != nil && context.Cause(streamCtx) == errIdle { //nolint:errorlint // identity of our own sentinel
		return Exit(ExitRuntime, "%w", errIdle)
	}
	return err
}

func (n *nodeExport) export(ctx context.Context) error {
	stream, err := n.open(ctx)
	if err != nil {
		return err
	}
	for _, u := range n.job.units {
		if err = stream.Send(u.request()); err != nil {
			return n.streamErr(stream, fmt.Sprintf("send unit %s", u.label()), err)
		}
		n.idle.touch()
		if err = n.receiveUnit(stream, u); err != nil {
			return err
		}
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("node %s: close the export stream: %w", n.job.nodeID, err)
	}
	resp, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("node %s: end of export stream: %w", n.job.nodeID, err)
	}
	return Exit(ExitRuntime, "node %s: unexpected %T frame after the last unit", n.job.nodeID, resp.GetFrame())
}

// streamErr turns a Send failure into the stream's status: Send reports io.EOF when the
// server ended the stream, and the reason comes from Recv.
func (n *nodeExport) streamErr(stream transferv1.ExportService_ExportClient, what string, err error) error {
	if errors.Is(err, io.EOF) {
		if _, recvErr := stream.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) {
			err = recvErr
		}
	}
	return fmt.Errorf("node %s: %s: %w", n.job.nodeID, what, err)
}

// open opens the Export stream and waits for ready. UNAVAILABLE, RESOURCE_EXHAUSTED and
// NOT_FOUND (a liaison that cannot see the node) move on to the next liaison and are retried
// with a doubling back-off until the retry window (--send-retry-timeout) is used up.
func (n *nodeExport) open(ctx context.Context) (transferv1.ExportService_ExportClient, error) {
	deadline := time.Now().Add(n.retryFor)
	backoff := exportRetryBase
	for {
		via := n.liaison()
		stream, err := n.openOnce(ctx, via)
		if err == nil {
			return stream, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("node %s: open export stream: %w", n.job.nodeID, context.Cause(ctx))
		}
		code := status.Code(err)
		if (code != codes.ResourceExhausted && code != codes.Unavailable && code != codes.NotFound) || time.Now().Add(backoff).After(deadline) {
			return nil, err
		}
		n.next++
		fmt.Fprintf(n.log, "node %s is busy or unreachable via %s (%s); retrying via %s in %s\n", n.job.nodeID, via.Addr, code, n.liaison().Addr, backoff)
		n.idle.pause()
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("node %s: waiting to retry the export stream: %w", n.job.nodeID, context.Cause(ctx))
		case <-timer.C:
		}
		n.idle.resume()
		backoff = min(backoff*2, exportRetryMax)
	}
}

// openOnce opens the stream through one liaison and waits for the ready frame.
func (n *nodeExport) openOnce(ctx context.Context, via *Liaison) (transferv1.ExportService_ExportClient, error) {
	n.idle.touch()
	stream, err := transferv1.NewExportServiceClient(via.Conn).Export(ctx)
	if err != nil {
		return nil, fmt.Errorf("node %s: open export stream via %s: %w", n.job.nodeID, via.Addr, unsupported(err))
	}
	if err = stream.Send(&transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Init_{Init: n.init}}); err != nil {
		return nil, n.streamErr(stream, "send init via "+via.Addr, unsupported(err))
	}
	resp, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("stream ended before ready")
		}
		return nil, fmt.Errorf("node %s: export init via %s: %w", n.job.nodeID, via.Addr, unsupported(err))
	}
	if resp.GetReady() == nil {
		return nil, Exit(ExitRuntime, "node %s: the first export frame must be ready, got %T", n.job.nodeID, resp.GetFrame())
	}
	n.idle.touch()
	return stream, nil
}

// receiveUnit reads the frames of one unit until unit_finished, writing and verifying its
// files, timing the unit and reporting the progress.
func (n *nodeExport) receiveUnit(stream transferv1.ExportService_ExportClient, u *plannedUnit) (err error) {
	var cur *partialFile
	defer func() {
		if cur != nil {
			cur.abandon()
		}
	}()
	started := false
	var start time.Time
	orderErr := func(frame any) error {
		return Exit(ExitRuntime, "node %s unit %s: unexpected %T frame", n.job.nodeID, u.label(), frame)
	}
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return Exit(ExitRuntime, "node %s unit %s: the stream ended before unit_finished", n.job.nodeID, u.label())
			}
			return fmt.Errorf("node %s unit %s: %w", n.job.nodeID, u.label(), recvErr)
		}
		n.idle.touch()
		switch f := resp.GetFrame().(type) {
		case *transferv1.ExportResponse_UnitStarted:
			if started {
				return orderErr(f)
			}
			started, start = true, time.Now()
			n.progress.unitStarted(u, f.UnitStarted.GetTotalShards(), f.UnitStarted.GetTotalParts())
		case *transferv1.ExportResponse_FileStarted:
			if !started || cur != nil {
				return orderErr(f)
			}
			if cur, err = n.startFile(u, f.FileStarted); err != nil {
				return err
			}
			n.progress.fileStarted(cur.rel, f.FileStarted.GetShardId(), f.FileStarted.GetShardIndex(), f.FileStarted.GetSegmentLevel())
		case *transferv1.ExportResponse_Chunk:
			if cur == nil {
				return orderErr(f)
			}
			if err = cur.write(f.Chunk.GetContent()); err != nil {
				return fmt.Errorf("node %s: write %s: %w", n.job.nodeID, cur.tmp, err)
			}
			n.progress.chunk(len(f.Chunk.GetContent()), f.Chunk.GetRows())
		case *transferv1.ExportResponse_FileFinished:
			if cur == nil {
				return orderErr(f)
			}
			rf, finishErr := n.finishFile(u, cur, f.FileFinished)
			if finishErr != nil {
				return finishErr
			}
			cur = nil
			u.files = append(u.files, rf)
			n.progress.fileFinished(rf.rowCounts, rf.file.Bytes)
		case *transferv1.ExportResponse_UnitFinished:
			if !started || cur != nil {
				return orderErr(f)
			}
			u.warnings = append(u.warnings, f.UnitFinished.GetWarnings()...)
			u.elapsed = time.Since(start)
			n.progress.unitFinished(u.elapsed)
			return nil
		default:
			return orderErr(f)
		}
	}
}

// partialFile is the .tmp file being received.
type partialFile struct {
	f            *os.File
	crc          hash.Hash32
	tmp, final   string
	rel          string // slash path relative to the node directory
	size         uint64
	shardID      uint32
	segmentLevel bool
}

func (p *partialFile) write(b []byte) error {
	if _, err := p.f.Write(b); err != nil {
		return err
	}
	_, _ = p.crc.Write(b)
	p.size += uint64(len(b))
	return nil
}

// abandon closes the file and leaves the .tmp in place for inspection; it is never renamed.
func (p *partialFile) abandon() { _ = p.f.Close() }

// safeRelativePath checks a node-supplied relative path: it must be relative, free of ".."
// and backslashes, and lie under the unit's <catalog>/<group>/ prefix. It returns the cleaned
// slash path.
func safeRelativePath(raw, prefix string) (string, error) {
	if raw == "" {
		return "", errors.New("empty relative path")
	}
	if strings.Contains(raw, `\`) || path.IsAbs(raw) || filepath.IsAbs(raw) || filepath.VolumeName(raw) != "" {
		return "", fmt.Errorf("relative path %q is not a relative slash path", raw)
	}
	for _, part := range strings.Split(raw, "/") {
		if part == ".." {
			return "", fmt.Errorf("relative path %q leaves its directory", raw)
		}
	}
	clean := path.Clean(raw)
	if !strings.HasPrefix(clean, prefix) || clean == strings.TrimSuffix(prefix, "/") {
		return "", fmt.Errorf("relative path %q is outside %s", raw, prefix)
	}
	return clean, nil
}

func (n *nodeExport) startFile(u *plannedUnit, fs *transferv1.FileStarted) (*partialFile, error) {
	rel, err := safeRelativePath(fs.GetRelativePath(), u.pathPrefix())
	if err != nil {
		return nil, Exit(ExitRuntime, "node %s unit %s: %w", n.job.nodeID, u.label(), err)
	}
	if !fs.GetSegmentLevel() && !slices.Contains(u.shardIDs(), fs.GetShardId()) {
		return nil, Exit(ExitRuntime, "node %s unit %s: file %s is of shard %d, which the unit did not ask for",
			n.job.nodeID, u.label(), rel, fs.GetShardId())
	}
	final := filepath.Join(n.root, filepath.FromSlash(rel))
	if _, statErr := os.Lstat(final); !errors.Is(statErr, os.ErrNotExist) {
		return nil, Exit(ExitRuntime, "node %s unit %s: %s already exists", n.job.nodeID, u.label(), final)
	}
	if err = os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return nil, fmt.Errorf("node %s: %w", n.job.nodeID, err)
	}
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", n.job.nodeID, err)
	}
	return &partialFile{
		f: f, crc: crc32.NewIEEE(), tmp: tmp, final: final, rel: rel,
		shardID: fs.GetShardId(), segmentLevel: fs.GetSegmentLevel(),
	}, nil
}

// finishFile verifies the size, the CRC-32 and, when the schema was exported, the schema
// revisions the node encoded with, then makes the file durable under its final name.
func (n *nodeExport) finishFile(u *plannedUnit, p *partialFile, ff *transferv1.FileFinished) (receivedFile, error) {
	if ff.GetBytes() != p.size || ff.GetCrc32() != p.crc.Sum32() {
		return receivedFile{}, Exit(ExitRuntime, "node %s: %s is corrupt: received %d bytes crc32 %s, the node announced %d bytes crc32 %s",
			n.job.nodeID, p.rel, p.size, manifest.FormatCRC32(p.crc.Sum32()), ff.GetBytes(), manifest.FormatCRC32(ff.GetCrc32()))
	}
	if err := n.checkSchemaRevisions(u, p, ff); err != nil {
		return receivedFile{}, err
	}
	err := p.f.Sync()
	if closeErr := p.f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(p.tmp, p.final)
	}
	if err == nil {
		err = syncDir(filepath.Dir(p.final))
	}
	if err != nil {
		return receivedFile{}, fmt.Errorf("node %s: persist %s: %w", n.job.nodeID, p.rel, err)
	}
	return receivedFile{
		file: manifest.File{
			Path: n.relRoot + "/" + p.rel, Bytes: p.size, CRC32: manifest.FormatCRC32(p.crc.Sum32()),
			EntryCount: ff.GetEntryCount(),
		},
		rowCounts:    ff.GetRowCounts(),
		shardID:      p.shardID,
		segmentLevel: p.segmentLevel,
	}, nil
}

// checkSchemaRevisions compares the schema a file was encoded with to the exported schema;
// without an exported schema there is nothing to compare with. Columns are bound by
// position and the layout is built from every resource of the group, so the node must
// report exactly the group's exported resources, each at its exported mod revision.
func (n *nodeExport) checkSchemaRevisions(u *plannedUnit, p *partialFile, ff *transferv1.FileFinished) error {
	if n.schema == nil {
		return nil
	}
	want := n.schema.GroupRevisions(u.catalog(), u.group())
	delete(want, csvfmt.TopNResultMeasure)
	got := ff.GetSchemaModRevisions()
	names := slices.Sorted(maps.Keys(want))
	for name := range got {
		if _, ok := want[name]; !ok {
			names = append(names, name)
		}
	}
	for _, name := range names {
		wantRev, exported := want[name]
		gotRev, encoded := got[name]
		if exported && encoded && wantRev == gotRev {
			continue
		}
		return Exit(ExitRuntime,
			"schema of %s %s/%s changed during export: the node encoded %s with mod revision %s, the exported schema has %s; export again",
			transfer.CatalogName(u.catalog()), u.group(), name, p.rel, revisionLabel(gotRev, encoded), revisionLabel(wantRev, exported))
	}
	return nil
}

// revisionLabel spells a mod revision, or "missing" for a resource one side does not have.
func revisionLabel(rev int64, ok bool) string {
	if !ok {
		return "missing"
	}
	return strconv.FormatInt(rev, 10)
}

// syncDir makes a directory's new entries durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	return err
}
