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

// Package csv produces the CSV artifact of an export unit from a session snapshot and
// frames the files into Export stream responses.
package csv

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// Directory and file names of the snapshot layout; they mirror the storage engines.
const (
	segDirPrefix    = "seg-"
	shardDirPrefix  = "shard-"
	sidxDirName     = "sidx"
	segmentFileName = "segment"
)

// localFS is the file system the part readers open snapshot files through.
var localFS = pkgfs.NewLocalFileSystem()

// Producer writes every unit of one Export stream as CSV: one CSV stream per
// shard, rows of every resource of the group in it, told apart by the _name column.
type Producer struct {
	ctx       context.Context
	meta      metadata.Repo
	indexMode IndexModeReader
	settings  *streamSettings
}

// NewProducer creates the CSV producer of one Export stream. init carries the stream
// settings (compression, file size, annotations and rate limit); send writes a frame to
// the stream. indexMode reads index-mode measure rows and may be nil.
func NewProducer(ctx context.Context, meta metadata.Repo, indexMode IndexModeReader, init *transferv1.ExportRequest_Init,
	send func(*transferv1.ExportResponse) error,
) *Producer {
	limit := init.GetRateLimit()
	return &Producer{
		ctx:       ctx,
		meta:      meta,
		indexMode: indexMode,
		settings: &streamSettings{
			bytes:       newByteLimiter(limit.GetBytesPerSecond()),
			rows:        newRowLimiter(limit.GetRowsPerSecond()),
			send:        send,
			maxFileSize: init.GetMaxFileSize(),
			gzip:        init.GetCompress() == transferv1.Compress_COMPRESS_GZIP,
			annotations: init.GetCsvAnnotations(),
		},
	}
}

// Segment produces a stream, measure or trace unit and returns its warnings and row count.
// segDir is <root>/<group>/seg-<suffix>, and every requested shard directory under it exists.
func (p *Producer) Segment(unit *transferv1.SegmentUnit, segDir string) ([]string, uint64, error) {
	run := newUnitRun(p.ctx, p.meta, p.settings)
	var err error
	switch unit.GetCatalog() {
	case commonv1.Catalog_CATALOG_STREAM:
		err = produceStreamCSV(run, unit, segDir)
	case commonv1.Catalog_CATALOG_MEASURE:
		err = produceMeasureCSV(run, unit, segDir, p.indexMode)
	case commonv1.Catalog_CATALOG_TRACE:
		err = produceTraceCSV(run, unit, segDir)
	default:
		err = status.Errorf(codes.InvalidArgument, "catalog %s has no segment units", unit.GetCatalog())
	}
	if err != nil {
		return nil, 0, err
	}
	return run.finish(len(unit.GetShardIds()) > 0), run.rows, nil
}

// Property produces a property unit and returns its warnings and row count. groupDir is
// <root>/<group>, and every requested shard directory under it exists.
func (p *Producer) Property(unit *transferv1.PropertyUnit, groupDir string) ([]string, uint64, error) {
	run := newUnitRun(p.ctx, p.meta, p.settings)
	if err := producePropertyCSV(run, unit, groupDir); err != nil {
		return nil, 0, err
	}
	return run.finish(len(unit.GetShardIds()) > 0), run.rows, nil
}

// streamSettings are the effective settings of one Export stream.
type streamSettings struct {
	bytes       *rate.Limiter
	rows        *rate.Limiter
	send        func(*transferv1.ExportResponse) error
	maxFileSize uint64
	gzip        bool
	annotations bool
}

// unitRun is the state of the unit being produced: the stream it writes to and the
// warnings it collects for UnitFinished.
type unitRun struct {
	ctx      context.Context
	meta     metadata.Repo
	settings *streamSettings
	// wrote holds the resources that wrote at least one row.
	wrote    map[string]struct{}
	dropped  map[string]map[string]uint64
	unknown  map[string]uint64
	warnings []string
	rows     uint64
	// oversized and largest count the rows larger than max_file_size, each in a file of its
	// own, and the encoded length of the largest.
	oversized uint64
	largest   uint64
}

func newUnitRun(ctx context.Context, meta metadata.Repo, settings *streamSettings) *unitRun {
	return &unitRun{
		ctx:      ctx,
		meta:     meta,
		settings: settings,
		wrote:    map[string]struct{}{},
		dropped:  map[string]map[string]uint64{},
		unknown:  map[string]uint64{},
	}
}

// dropTag counts a stored tag value the resource's schema no longer declares.
func (r *unitRun) dropTag(resource, tag string) { r.dropTagN(resource, tag, 1) }

// dropTagN counts n stored values of tag dropped for resource.
func (r *unitRun) dropTagN(resource, tag string, n uint64) {
	if r.dropped[resource] == nil {
		r.dropped[resource] = map[string]uint64{}
	}
	r.dropped[resource][tag] += n
}

// dropRow counts a row whose resource cannot be determined or is not in the schema.
func (r *unitRun) dropRow(reason string) {
	r.unknown[reason]++
}

// fileWriter is one CSV stream of the unit (one shard, or the segment-level file).
type fileWriter struct {
	run  *unitRun
	w    *csvfmt.Writer
	sink *frameSink
}

// newWriter opens the CSV stream of one shard (segmentLevel false) or of the segment.
// revisions maps each resource of the layout to its schema ModRevision.
func (r *unitRun) newWriter(layout *csvfmt.Layout, pathPrefix string, shardID, shardIndex uint32, segmentLevel bool,
	revisions map[string]int64,
) *fileWriter {
	sink := &frameSink{
		run:          r,
		settings:     r.settings,
		revisions:    revisions,
		shardID:      shardID,
		shardIndex:   shardIndex,
		segmentLevel: segmentLevel,
	}
	w := csvfmt.NewWriter(sink, csvfmt.WriterOptions{
		Layout:      layout,
		PathPrefix:  pathPrefix,
		MaxFileSize: r.settings.maxFileSize,
		Gzip:        r.settings.gzip,
		Annotations: r.settings.annotations,
	})
	return &fileWriter{run: r, w: w, sink: sink}
}

// writeRow waits for the row bucket and appends one row. Before a wait it sends the rows the
// file holds that were not sent yet: right away when the wait is earlyFlushWait or longer, so
// a low row rate shows each row as it is written, and otherwise once they are earlyFlush old.
// Either way a throttled stream keeps sending.
func (fw *fileWriter) writeRow(resource string, cells []string) error {
	if l := fw.run.settings.rows; l != nil && fw.sink.rows > fw.sink.sentRows {
		if wait := rowWait(l); wait >= earlyFlushWait || wait > 0 && time.Since(fw.sink.sent) >= earlyFlush {
			if err := fw.w.Flush(); err != nil {
				return err
			}
			if err := fw.sink.flush(); err != nil {
				return err
			}
		}
	}
	if err := waitN(fw.run.ctx, fw.run.settings.rows, 1); err != nil {
		return err
	}
	if err := fw.w.WriteRow(resource, cells); err != nil {
		return err
	}
	fw.sink.rows++
	fw.run.wrote[resource] = struct{}{}
	return nil
}

// close finishes the open file and folds the writer's statistics into the unit.
func (fw *fileWriter) close() error {
	if err := fw.w.Close(); err != nil {
		return err
	}
	fw.run.rows += fw.w.Rows()
	rows, largest := fw.w.Oversized()
	fw.run.oversized += rows
	fw.run.largest = max(fw.run.largest, largest)
	return nil
}

// finish returns the warnings of the unit: dropped rows and values, rows larger than
// max_file_size, and a unit whose shards held parts but yielded no row. A unit without shards
// (a segment created ahead of time, or an index-mode one with nothing indexed) is silent.
func (r *unitRun) finish(listedShards bool) []string {
	out := r.warnings
	for _, reason := range slices.Sorted(maps.Keys(r.unknown)) {
		out = append(out, fmt.Sprintf("dropped %d rows: %s", r.unknown[reason], reason))
	}
	for _, resource := range slices.Sorted(maps.Keys(r.dropped)) {
		tags := r.dropped[resource]
		var total uint64
		for _, n := range tags {
			total += n
		}
		out = append(out, fmt.Sprintf("%s: dropped %d stored values of tags or fields the schema does not declare, or declares with another type: %s",
			resource, total, strings.Join(slices.Sorted(maps.Keys(tags)), ", ")))
	}
	if r.oversized > 0 {
		out = append(out, fmt.Sprintf("%d row(s) larger than max_file_size %d got a file of their own; the largest is %d bytes before compression",
			r.oversized, r.settings.maxFileSize, r.largest))
	}
	if listedShards && r.rows == 0 {
		out = append(out, "unit has no rows")
	}
	return out
}

// frameSink is the csvfmt.Sink of one fileWriter: it frames the on-disk bytes into
// file_started / chunk / file_finished, buffering chunks of up to chunkLimit bytes. A
// chunk waits for the byte bucket before it is sent, so a slow client or a low rate
// stops the reading.
type frameSink struct {
	sent         time.Time // when the last chunk was sent
	run          *unitRun
	settings     *streamSettings
	revisions    map[string]int64
	buf          []byte
	rows         uint64 // rows written into the open file
	sentRows     uint64 // rows of the open file the last chunk carried
	shardID      uint32
	shardIndex   uint32
	segmentLevel bool
}

func (s *frameSink) StartFile(relativePath string, seq uint32) error {
	s.rows, s.sentRows = 0, 0
	return s.settings.send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{
		RelativePath: relativePath,
		ShardId:      s.shardID,
		SegmentLevel: s.segmentLevel,
		ShardIndex:   s.shardIndex,
		Seq:          seq,
	}}})
}

func (s *frameSink) Write(p []byte) error {
	limit := chunkLimit(s.settings.bytes)
	for len(p) > 0 {
		if s.buf == nil {
			s.buf = make([]byte, 0, limit)
		}
		n := min(len(p), limit-len(s.buf))
		s.buf = append(s.buf, p[:n]...)
		p = p[n:]
		if len(s.buf) == limit {
			if err := s.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *frameSink) flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	if err := waitN(s.run.ctx, s.settings.bytes, len(s.buf)); err != nil {
		return err
	}
	// The frame owns its bytes: gRPC may still hold the message after Send returns.
	content := s.buf
	s.buf = nil
	s.sent, s.sentRows = time.Now(), s.rows
	return s.settings.send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: content, Rows: s.rows}}})
}

func (s *frameSink) FinishFile(size uint64, crc uint32, rowCounts map[string]uint64) error {
	if err := s.flush(); err != nil {
		return err
	}
	// Every resource the layout was built from, rows or not: the client checks the whole
	// layout against the schema it saved.
	return s.settings.send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: &transferv1.FileFinished{
		Bytes:              size,
		Crc32:              crc,
		RowCounts:          rowCounts,
		SchemaModRevisions: maps.Clone(s.revisions),
	}}})
}

// readOnlyFS is the file system the part readers open snapshot files through. It never
// writes; ReadDir reports a failure instead of panicking (a preempted session can remove
// the directory mid-read). The part readers skip some files they cannot open, so the first
// failure to list, open or read is kept for the caller to check; only a missing file may be
// opened or read without failing, as the readers probe optional files that way.
type readOnlyFS struct {
	pkgfs.FileSystem
	err error
}

func newReadOnlyFS() *readOnlyFS {
	return &readOnlyFS{FileSystem: localFS}
}

func (f *readOnlyFS) keep(err error) {
	if err != nil && f.err == nil {
		f.err = err
	}
}

// keepPresent keeps err unless it reports a missing file.
func (f *readOnlyFS) keepPresent(err error) {
	if !errors.Is(err, fs.ErrNotExist) {
		f.keep(err)
	}
}

func (f *readOnlyFS) ReadDir(dirname string) []pkgfs.DirEntry {
	entries, err := f.ReadDirLimit(dirname, 0)
	f.keep(err)
	return entries
}

func (f *readOnlyFS) OpenFile(name string) (pkgfs.File, error) {
	file, err := f.FileSystem.OpenFile(name)
	f.keepPresent(err)
	return file, err
}

func (f *readOnlyFS) Read(name string) ([]byte, error) {
	data, err := f.FileSystem.Read(name)
	f.keepPresent(err)
	return data, err
}

// shardPrefix is the path prefix of one shard's files in a segment unit.
func shardPrefix(unit *transferv1.SegmentUnit, shard uint32) string {
	return fmt.Sprintf("%s/%s/%s%s/%s%d", transfer.CatalogName(unit.GetCatalog()), csvfmt.EscapePathComponent(unit.GetGroup()),
		segDirPrefix, unit.GetSegmentSuffix(), shardDirPrefix, shard)
}

// fixedColumns finds the index of every fixed column of a layout by kind.
func fixedColumns(layout *csvfmt.Layout) map[csvfmt.ColumnKind]int {
	out := map[csvfmt.ColumnKind]int{}
	for i, c := range layout.Columns {
		if c.Kind != csvfmt.KindTag && c.Kind != csvfmt.KindField {
			out[c.Kind] = i
		}
	}
	return out
}

// resetRow refills cells with the null cell so one slice serves every row.
func resetRow(cells []string) {
	for i := range cells {
		cells[i] = csvfmt.NullCell
	}
}

// readError maps a failure while reading the snapshot to the stream status: a file that
// vanished means the session was preempted (FailedPrecondition), a canceled stream keeps
// its context status, gRPC statuses pass through, anything else is Internal.
func readError(what string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, fs.ErrNotExist) {
		return status.Errorf(codes.FailedPrecondition, "%s: the export session snapshot is gone (preempted or released): %v", what, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", what, err)
}
