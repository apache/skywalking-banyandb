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

package lifecycle

import (
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// orphanPolicy decides what row-replay does with rows whose schema was deleted
// from the registry: archive them to JSONL (default) or drop them.
type orphanPolicy uint8

const (
	orphanArchive orphanPolicy = iota
	orphanDiscard
)

func parseOrphanPolicy(s string) (orphanPolicy, error) {
	switch s {
	case "archive":
		return orphanArchive, nil
	case "discard":
		return orphanDiscard, nil
	default:
		return 0, fmt.Errorf("invalid orphan policy %q (want archive|discard)", s)
	}
}

// orphanConfig is the resolved policy + archive root threaded from the service.
type orphanConfig struct {
	rootDir string
	policy  orphanPolicy
}

// sourceLoc locates the source part an orphan row came from. Stage is metadata
// only (not part of the path); segment/shard/part build the file path.
type sourceLoc struct {
	Stage   string `json:"stage"`
	Segment string `json:"segment"`
	Part    string `json:"part_id"`
	Shard   uint32 `json:"shard"`
}

// typedValue is a schema-free value with its explicit BanyanDB value type.
type typedValue struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// archiveRecord is one orphan row as written to JSONL. Fields is nil for stream;
// ElementID is set only for stream.
type archiveRecord struct {
	Tags        map[string]typedValue `json:"tags,omitempty"`
	IndexedTags map[string][]string   `json:"indexed_tags,omitempty"`
	Fields      map[string]typedValue `json:"fields,omitempty"`
	ElementID   string                `json:"element_id,omitempty"`
	Timestamp   string                `json:"timestamp"`
	Group       string                `json:"group"`
	Measure     string                `json:"measure"`
	Catalog     string                `json:"catalog"`
	Source      sourceLoc             `json:"source"`
	Entity      []string              `json:"entity"`
	SeriesID    uint64                `json:"series_id"`
	TimeNanos   int64                 `json:"timestamp_unix_nano"`
	// Version is measure-only (a nanosecond timestamp, always > 0); stream records
	// omit it rather than emit a misleading "version": 0.
	Version int64 `json:"version,omitempty"`
}

// orphanArchiver is created per (group, catalog) replayer; it owns the manifest
// aggregation across that group's segments and serializes archive writes.
type orphanArchiver struct {
	l        *logger.Logger
	segments map[string]*segManifestState
	group    string
	catalog  string
	cfg      orphanConfig
	mu       sync.Mutex
}

func newOrphanArchiver(cfg orphanConfig, group, catalog string, l *logger.Logger) *orphanArchiver {
	return &orphanArchiver{cfg: cfg, group: group, catalog: catalog, l: l, segments: make(map[string]*segManifestState)}
}

// groupDir is the archive root for this group: rootDir already points at the
// catalog's archive subdir (e.g. <catalog-root>/archive), so the per-segment
// layout below adds only the group — the catalog is recorded inside each record
// and manifest rather than as a path level.
func (a *orphanArchiver) groupDir() string {
	return filepath.Join(a.cfg.rootDir, a.group)
}

func (a *orphanArchiver) partFilePath(loc sourceLoc) string {
	return filepath.Join(a.groupDir(), "seg-"+loc.Segment, fmt.Sprintf("shard-%d", loc.Shard), "part-"+loc.Part+".jsonl.gz")
}

func (a *orphanArchiver) manifestPath(segment string) string {
	return filepath.Join(a.groupDir(), "seg-"+segment, "manifest.json")
}

// valueWithType renders a decoded TagValue as a schema-free typed value.
func valueWithType(tv *modelv1.TagValue) typedValue {
	switch v := tv.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return typedValue{Type: "STR", Value: v.Str.GetValue()}
	case *modelv1.TagValue_Int:
		return typedValue{Type: "INT64", Value: v.Int.GetValue()}
	case *modelv1.TagValue_StrArray:
		return typedValue{Type: "STR_ARRAY", Value: v.StrArray.GetValue()}
	case *modelv1.TagValue_IntArray:
		return typedValue{Type: "INT_ARRAY", Value: v.IntArray.GetValue()}
	case *modelv1.TagValue_BinaryData:
		return typedValue{Type: "BINARY", Value: v.BinaryData}
	case *modelv1.TagValue_Timestamp:
		return typedValue{Type: "TIMESTAMP", Value: v.Timestamp.AsTime().UTC().Format(time.RFC3339Nano)}
	case *modelv1.TagValue_Null:
		return typedValue{Type: "NULL", Value: nil}
	default:
		if tv.GetValue() == nil {
			return typedValue{Type: "NULL", Value: nil}
		}
		// A populated but unrecognized oneof: surface it as UNKNOWN rather than
		// silently masquerading as NULL.
		return typedValue{Type: "UNKNOWN", Value: nil}
	}
}

// entityValueString renders one decoded entity TagValue faithfully (no binary
// loss): strings as-is, ints via strconv, binary as base64, arrays joined by
// their faithful element renderings, null/empty as "".
func entityValueString(tv *modelv1.TagValue) string {
	switch v := tv.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return v.Str.GetValue()
	case *modelv1.TagValue_Int:
		return strconv.FormatInt(v.Int.GetValue(), 10)
	case *modelv1.TagValue_BinaryData:
		return "base64:" + base64.StdEncoding.EncodeToString(v.BinaryData)
	case *modelv1.TagValue_StrArray:
		return strings.Join(v.StrArray.GetValue(), ",")
	case *modelv1.TagValue_IntArray:
		parts := make([]string, 0, len(v.IntArray.GetValue()))
		for _, n := range v.IntArray.GetValue() {
			parts = append(parts, strconv.FormatInt(n, 10))
		}
		return strings.Join(parts, ",")
	case *modelv1.TagValue_Timestamp:
		return v.Timestamp.AsTime().UTC().Format(time.RFC3339Nano)
	default:
		return ""
	}
}

// segManifestState holds per-part tallies for one segment so the manifest can be
// rebuilt idempotently when a part is (re)archived.
type segManifestState struct {
	parts map[string]map[string]manifestCount
}

type measureTally struct {
	series map[uint64]struct{}
	rows   int
}

// manifestFile is the on-disk per-segment manifest. Parts is the idempotent
// per-part breakdown the summary (Measures/Total*) is rebuilt from; the per-part
// SeriesIDs lists let the summary report the true distinct series count (a series
// appearing in several parts is counted once) and stay resume-safe.
type manifestFile struct {
	Parts       map[string]map[string]manifestCount `json:"_parts"`
	Group       string                              `json:"group"`
	Catalog     string                              `json:"catalog"`
	SourceStage string                              `json:"source_stage"`
	Segment     string                              `json:"segment"`
	GeneratedAt string                              `json:"generated_at"`
	Policy      string                              `json:"policy"`
	Measures    []manifestMeasure                   `json:"measures"`
	TotalRows   int                                 `json:"total_rows"`
	TotalSeries int                                 `json:"total_series"`
}

type manifestMeasure struct {
	Measure string   `json:"measure"`
	Files   []string `json:"files"`
	Rows    int      `json:"rows"`
	Series  int      `json:"series"`
}

// manifestCount records one (part, measure) breakdown: the row count and the
// sorted real series-ID list. Persisting the IDs (not a bare count) lets the
// summary union them across parts for a true distinct count and survive a resume.
type manifestCount struct {
	SeriesIDs []uint64 `json:"series_ids"`
	Rows      int      `json:"rows"`
}

// orphanPartWriter writes one source part's orphan rows. For discard, f is nil
// and only counts are kept (never persisted).
type orphanPartWriter struct {
	a     *orphanArchiver
	f     *os.File
	gw    *gzip.Writer
	enc   *json.Encoder
	tally map[string]*measureTally
	loc   sourceLoc
}

// archiving reports whether this writer persists rows (archive policy) or only
// tallies them (discard policy). Derived from the parent archiver's config.
func (w *orphanPartWriter) archiving() bool { return w.a.cfg.policy == orphanArchive }

// runPart opens the archive writer for loc, runs body (the part replay, which
// appends orphan rows via the passed writer), and commits the manifest on success
// or aborts (no manifest, partial file removed) on failure. A commit error is
// surfaced as a part error so the part is retried and the manifest never
// under-reports. Shared by the measure and stream replayers.
func (a *orphanArchiver) runPart(loc sourceLoc, body func(*orphanPartWriter) error) error {
	pw := a.partWriter(loc)
	if bodyErr := body(pw); bodyErr != nil {
		_ = pw.close(false)
		return bodyErr
	}
	if cerr := pw.close(true); cerr != nil {
		return fmt.Errorf("finalize orphan archive for %s: %w", loc.Part, cerr)
	}
	return nil
}

// partWriter builds a writer for one source part. It never fails: the archive
// file is opened lazily on the first orphan row (see appendRow), so a part with
// no orphan rows — the common case in a healthy migration — never touches the
// filesystem and no manifest is written for an orphan-free segment. An open
// failure surfaces from the first appendRow (and thus aborts the part).
func (a *orphanArchiver) partWriter(loc sourceLoc) *orphanPartWriter {
	return &orphanPartWriter{a: a, loc: loc, tally: make(map[string]*measureTally)}
}

func (w *orphanPartWriter) appendRow(rec *archiveRecord) error {
	// Open the archive file lazily on the first row so orphan-free parts create no
	// file or manifest. Open+encode FIRST so a failed write is never counted as an
	// archived orphan row; only a durably-encoded row is tallied. For discard
	// (not archiving) there is no write, so the tally always succeeds.
	if w.archiving() && w.f == nil {
		if err := w.openFile(); err != nil {
			return err
		}
	}
	if w.f != nil {
		if err := w.enc.Encode(rec); err != nil {
			return fmt.Errorf("encode archive row: %w", err)
		}
	}
	t := w.tally[rec.Measure]
	if t == nil {
		t = &measureTally{series: make(map[uint64]struct{})}
		w.tally[rec.Measure] = t
	}
	t.rows++
	t.series[rec.SeriesID] = struct{}{}
	return nil
}

// openFile creates the part's archive directory and file, truncating any prior
// content so a re-replayed part rewrites cleanly (idempotent on resume).
func (w *orphanPartWriter) openFile() error {
	path := w.a.partFilePath(w.loc)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create archive dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open archive file %s: %w", path, err)
	}
	// JSONL is highly repetitive (constant group/series fields per row), so the
	// archive is gzip-compressed on disk (~37x smaller); read it with
	// `gunzip -c file.jsonl.gz | jq` (or `zcat`/`zgrep` on Linux).
	w.f = f
	w.gw = gzip.NewWriter(f)
	w.enc = json.NewEncoder(w.gw)
	return nil
}

// close finalizes the part archive. The file handle (if any) is always closed.
// When commit is true the part completed, so its tallies are folded into the
// segment manifest and the manifest is rewritten; the first of {file-close error,
// manifest error} is returned. When commit is false the part aborted mid-way, so
// the manifest is NOT folded (it must never reflect a partial part) and the
// partial .jsonl is best-effort removed; nil is returned because the part's real
// error already propagates. Discard (f == nil) is a no-op.
func (w *orphanPartWriter) close(commit bool) error {
	if w.f == nil {
		return nil
	}
	// Close the gzip writer first to flush its buffer + trailer into the file, then
	// close the file. A buffered write error surfaces here (not at appendRow), so
	// close's error is what makes an archive failure fatal to the part.
	closeErr := w.gw.Close()
	if ferr := w.f.Close(); closeErr == nil {
		closeErr = ferr
	}
	if !commit {
		_ = os.Remove(w.a.partFilePath(w.loc))
		return nil
	}
	if closeErr != nil {
		return fmt.Errorf("close archive file: %w", closeErr)
	}
	return w.a.foldAndWriteManifest(w.loc, w.tally)
}

func (a *orphanArchiver) foldAndWriteManifest(loc sourceLoc, tally map[string]*measureTally) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := a.segments[loc.Segment]
	if st == nil {
		st = &segManifestState{parts: make(map[string]map[string]manifestCount)}
		if loaded := a.loadManifestParts(loc.Segment); loaded != nil {
			st.parts = loaded
		}
		a.segments[loc.Segment] = st
	}
	// Store the real per-part series-ID lists (not a bare count) so the summary can
	// union them across parts for a true distinct count and a resume can re-derive
	// the same totals.
	counts := make(map[string]manifestCount, len(tally))
	for measure, t := range tally {
		ids := make([]uint64, 0, len(t.series))
		for id := range t.series {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		counts[measure] = manifestCount{Rows: t.rows, SeriesIDs: ids}
	}
	partKey := fmt.Sprintf("shard-%d/part-%s.jsonl.gz", loc.Shard, loc.Part)
	st.parts[partKey] = counts // overwrite -> idempotent on resume
	return a.writeManifest(loc, st)
}

// loadManifestParts reseeds per-part counts from an existing manifest so a resume
// run keeps prior parts' entries.
func (a *orphanArchiver) loadManifestParts(segment string) map[string]map[string]manifestCount {
	b, err := os.ReadFile(a.manifestPath(segment))
	if err != nil {
		// First run for this segment has no manifest yet; any other read error means
		// prior tallies are about to be lost on the next write, so make it visible.
		if !os.IsNotExist(err) && a.l != nil {
			a.l.Warn().Err(err).Str("segment", segment).Msg("cannot read existing orphan manifest; prior tallies will be overwritten")
		}
		return nil
	}
	var m manifestFile
	if err := json.Unmarshal(b, &m); err != nil {
		if a.l != nil {
			a.l.Warn().Err(err).Str("segment", segment).Msg("corrupt orphan manifest; prior tallies will be overwritten")
		}
		return nil
	}
	return m.Parts
}

func (a *orphanArchiver) writeManifest(loc sourceLoc, st *segManifestState) error {
	byMeasure := make(map[string]*manifestMeasure)
	// distinctSeries unions each measure's series IDs across all its parts so a
	// series spanning several parts is counted exactly once.
	distinctSeries := make(map[string]map[uint64]struct{})
	totalRows := 0
	for partKey, byMeasureCounts := range st.parts {
		for measure, c := range byMeasureCounts {
			mm := byMeasure[measure]
			if mm == nil {
				mm = &manifestMeasure{Measure: measure}
				byMeasure[measure] = mm
				distinctSeries[measure] = make(map[uint64]struct{})
			}
			mm.Rows += c.Rows
			mm.Files = append(mm.Files, partKey)
			for _, id := range c.SeriesIDs {
				distinctSeries[measure][id] = struct{}{}
			}
			totalRows += c.Rows
		}
	}
	// total_series is the sum of per-measure distinct counts: a series belongs to
	// exactly one measure, so there is no cross-measure overlap to deduplicate.
	totalSeries := 0
	names := make([]string, 0, len(byMeasure))
	for n := range byMeasure {
		names = append(names, n)
	}
	sort.Strings(names)
	measures := make([]manifestMeasure, 0, len(names))
	for _, n := range names {
		mm := byMeasure[n]
		mm.Series = len(distinctSeries[n])
		totalSeries += mm.Series
		sort.Strings(mm.Files)
		measures = append(measures, *mm)
	}
	policy := "archive"
	if a.cfg.policy == orphanDiscard {
		policy = "discard"
	}
	m := manifestFile{
		Group: a.group, Catalog: a.catalog, SourceStage: loc.Stage, Segment: loc.Segment,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339), Policy: policy,
		Measures: measures, Parts: st.parts, TotalRows: totalRows, TotalSeries: totalSeries,
	}
	out, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	// Write-then-rename so a crash mid-write never leaves a truncated manifest that
	// a resume would read as empty (silently dropping prior parts' tallies). Rename
	// is atomic on the same filesystem; the tmp file is a sibling of the target.
	path := a.manifestPath(loc.Segment)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit manifest: %w", err)
	}
	return nil
}

// orphanVerb returns the past-tense verb describing the orphan policy for log messages.
func orphanVerb(p orphanPolicy) string {
	if p == orphanDiscard {
		return "discarded"
	}
	return "archived"
}
