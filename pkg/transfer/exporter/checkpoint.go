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
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/output"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

const (
	// ProgressFileName is the progress file of an export below its output directory, unless
	// --checkpoint names another path.
	ProgressFileName = ".export-progress.json"
	progressKind     = "export-progress"
	// progressVersion is the format version of the progress file and of its plan side file. Any
	// change to the format of either raises it by one. A run writes the side file only after the
	// version of the progress file was accepted, or when it starts the progress file itself, so
	// that version covers the side file too; a progress file of another version is refused
	// (exit 6) before the side file is read, and resumes only with the bydbctl that wrote it or
	// is given up (the session released, the directory emptied).
	progressVersion = 2
	// planSide names the side file of the progress file that holds the plan (resume.SidePath).
	planSide = "plan"
)

// ProgressPath is the progress file of an export: --checkpoint, or ProgressFileName below the
// output directory; "" when neither is given.
func ProgressPath(opts config.Export) string {
	if opts.Checkpoint != "" {
		return opts.Checkpoint
	}
	if opts.Output == "" {
		return ""
	}
	return filepath.Join(opts.Output, ProgressFileName)
}

// CheckResume checks the output directory and the progress file of an export before anything is
// contacted; every failure is a usage error (exit 1). A --checkpoint that names a directory, or
// the output directory itself, is refused. For a dry run, which writes nothing and previews a
// resume only, it drops the output directory and the progress file the environment sets: they
// are defaults of real exports. A progress file from the environment is kept for an output
// directory given as a flag or in plan.yaml, as where that export records its progress. An
// output directory or a progress file given so is refused unless the progress file it names
// exists to preview.
func CheckResume(sources config.Sources, opts *config.Export) error {
	if opts.DryRun {
		if !sources.Explicit(&opts.Output) {
			opts.Output = ""
		}
		if !sources.Explicit(&opts.Checkpoint) && opts.Output == "" {
			opts.Checkpoint = ""
		}
	}
	if err := config.CheckCheckpoint(sources, &opts.Checkpoint, opts.Output, ProgressFileName); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	if err := resume.CheckSideCollision(ProgressPath(*opts)); err != nil {
		return exitcode.Exit(exitcode.ExitUsage, "%w", err)
	}
	if !opts.DryRun {
		return nil
	}
	progress := ProgressPath(*opts)
	if progress == "" || resume.Exists(progress) {
		return nil
	}
	for _, setting := range []*string{&opts.Output, &opts.Checkpoint} {
		if *setting != "" && sources.Explicit(setting) {
			return exitcode.Exit(exitcode.ExitUsage, "%s cannot be combined with --dry-run, which writes nothing "+
				"(it previews a resume only, and there is no progress file at %s)", sources.Label(setting), progress)
		}
	}
	return nil
}

// exportProgress is the progress file of an export. It records what a resumed run cannot rebuild
// from the output directory: the session, the plan (in the side file planSide, written once,
// with its digest here), the base manifest's groups and every finished unit with its files.
type exportProgress struct {
	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// PlanDigest is the resume.DigestBytes of the plan side file, "" while none is written.
	PlanDigest string `json:"planDigest,omitempty"`
	// Inflight is the unit each node was reading at the last write, for whoever looks at the file
	// while the export runs: a resume never reads it, it finds a unit unfinished unless it is done.
	Inflight    map[string]inflightUnit `json:"inflight"`
	Session     progressSession         `json:"session"`
	Bindings    exportBindings          `json:"bindings"`
	Format      string                  `json:"format"`
	OutputDir   string                  `json:"outputDir"`
	SchemaFiles []manifest.File         `json:"schemaFiles"`
	Groups      []manifest.Group        `json:"groups"`
	Done        []doneUnit              `json:"done"`
	// SourceTimezones are the time zones of the cluster's data nodes when the export started,
	// which the manifest records even of a node that has left since; old files lack them.
	SourceTimezones map[string]string `json:"sourceTimezones,omitempty"`
	resume.Header
	// ElapsedMs is the time the runs of this export took, up to the last write of the file.
	ElapsedMs int64 `json:"elapsedMs"`
}

type progressSession struct {
	ID string `json:"id"`
}

// exportBindings are what must not change between the run that wrote a progress file and the
// run that resumes it: digests, with the normalized values they digest so that a refusal can
// say what differs. The source nodes are the exception, recorded only: the data nodes with units
// left must still be members of the cluster (checkMembers), which may have gained nodes. The
// options are compared by value, without maxFileSize (boundOptions), so their digest is recorded
// only too.
type exportBindings struct {
	Options           map[string]string `json:"options"`
	SelectorsDigest   string            `json:"selectorsDigest"`
	SourceNodesDigest string            `json:"sourceNodesDigest"` // recorded only
	OptionsDigest     string            `json:"optionsDigest"`     // recorded only
	Selectors         []string          `json:"selectors"`
	SourceNodes       []string          `json:"sourceNodes"`
}

// recordedPlan is the folded PlanResult of the session, each units frame as protojson.
type recordedPlan struct {
	Frames        []json.RawMessage `json:"frames"`
	AnsweredNodes []string          `json:"answeredNodes"`
}

type inflightUnit struct {
	StartedAt time.Time `json:"startedAt"`
	Unit      string    `json:"unit"`
}

// doneUnit is one finished unit of one node, with the files it wrote in the order they came.
type doneUnit struct {
	Node      string     `json:"node"`
	Unit      string     `json:"unit"`
	Files     []doneFile `json:"files"`
	Warnings  []string   `json:"warnings"`
	ElapsedMs int64      `json:"elapsedMs"`
}

type doneFile struct {
	RowCounts map[string]uint64 `json:"rowCounts"`
	manifest.File
	Shard        uint32 `json:"shard"`
	SegmentLevel bool   `json:"segmentLevel"`
}

func recordPlan(plan *cluster.PlanResult) (*recordedPlan, error) {
	rec := &recordedPlan{AnsweredNodes: plan.AnsweredNodes, Frames: make([]json.RawMessage, 0, len(plan.Frames))}
	for _, f := range plan.Frames {
		b, err := protojson.Marshal(f)
		if err != nil {
			return nil, fmt.Errorf("encode the plan of node %s: %w", f.GetNodeId(), err)
		}
		rec.Frames = append(rec.Frames, b)
	}
	return rec, nil
}

func (r *recordedPlan) result() (*cluster.PlanResult, error) {
	res := &cluster.PlanResult{AnsweredNodes: r.AnsweredNodes}
	for i, raw := range r.Frames {
		f := &transferv1.UnitFrame{}
		if err := protojson.Unmarshal(raw, f); err != nil {
			return nil, fmt.Errorf("plan frame %d: %w", i, err)
		}
		res.Frames = append(res.Frames, f)
	}
	return res, nil
}

// bindingsOf digests the selectors, the cluster's data nodes and the options that change the
// bytes of a file. Rate limits, parallelism, timeouts and liaisons may change between runs.
func bindingsOf(opts config.Export, info *cluster.Info) exportBindings {
	selectors := make([]string, 0, len(opts.Selectors))
	for _, s := range opts.Selectors {
		selectors = append(selectors, "catalog="+transfer.CatalogName(s.GetCatalog())+",groups="+
			strings.Join(transfer.UniqueSorted(s.GetGroups()), ";"))
	}
	options := map[string]string{
		"format":         formatLabel(opts.Format),
		"compress":       compressLabel(opts.Compress),
		"maxFileSize":    strconv.FormatUint(opts.MaxFileSize, 10),
		"csvAnnotations": strconv.FormatBool(opts.CSVAnnotations),
		"includeSchema":  strconv.FormatBool(opts.IncludeSchema),
	}
	nodes := info.DataNodeNames()
	return exportBindings{
		Selectors:         transfer.UniqueSorted(selectors),
		SelectorsDigest:   resume.DigestSet(selectors),
		SourceNodes:       nodes,
		SourceNodesDigest: resume.DigestSet(nodes),
		Options:           options,
		OptionsDigest:     resume.DigestOptions(options),
	}
}

// maxFileSizeOption is the option a resumed export may change: it only decides where the files
// of a unit roll over, a resumed unit is exported again from its start, and the manifest does not
// record it.
const maxFileSizeOption = "maxFileSize"

// boundOptions are the options a resumed export must keep.
func boundOptions(options map[string]string) map[string]string {
	bound := maps.Clone(options)
	delete(bound, maxFileSizeOption)
	return bound
}

// defaultOptions are the options at their defaults for an export of format, which a refusal marks
// "(default)": a native export is never compressed, so none is its default compression.
func defaultOptions(format transferv1.Format) map[string]string {
	opts := &config.Export{}
	config.Defaults(opts)
	opts.Format = format
	if format == transferv1.Format_FORMAT_NATIVE {
		opts.Compress = transferv1.Compress_COMPRESS_UNSPECIFIED
	}
	_ = CheckFormat(opts)
	return bindingsOf(*opts, nil).Options
}

// showOption renders the byte count of maxFileSize in IEC units.
func showOption(name, value string) string {
	n, err := strconv.ParseUint(value, 10, 64)
	switch {
	case name != maxFileSizeOption || err != nil:
		return value
	case n == 0:
		return "0 (unlimited)"
	}
	return output.HumanBytes(n)
}

// rebindMaxFileSize records the --max-file-size of this run in state when it differs from the
// recorded one, and warns that the files of the units already exported keep the old limit. It
// does nothing when no unit is left.
func rebindMaxFileSize(w io.Writer, state *exportProgress, opts config.Export, left int) {
	recorded := state.Bindings.Options
	current := strconv.FormatUint(opts.MaxFileSize, 10)
	if left == 0 || recorded[maxFileSizeOption] == current {
		return
	}
	diff := resume.OptionDiffs(map[string]string{maxFileSizeOption: recorded[maxFileSizeOption]},
		map[string]string{maxFileSizeOption: current}, defaultOptions(opts.Format), showOption)
	warnf(w, "--max-file-size differs from the earlier run(s) (%s): the files of the %d unit(s) already exported keep their sizes; "+
		"the %d unit(s) left are written with the new limit", strings.Join(diff, "; "), len(state.Done), left)
	options := maps.Clone(recorded)
	if options == nil {
		options = map[string]string{}
	}
	options[maxFileSizeOption] = current
	state.Bindings.Options, state.Bindings.OptionsDigest = options, resume.DigestOptions(options)
}

// planNodes names the data nodes of a plan: those that answered and those that hold units.
func planNodes(plan *cluster.PlanResult) []string {
	nodes := slices.Clone(plan.AnsweredNodes)
	for _, f := range plan.Frames {
		nodes = append(nodes, f.GetNodeId())
	}
	return transfer.UniqueSorted(nodes)
}

// checkBindings names every binding of the progress file at path this invocation differs in, ""
// when none does. Without an output directory (a dry run that names only the progress file) the
// recorded one is taken. The data nodes are checked apart (checkMembers).
func checkBindings(state *exportProgress, opts config.Export, path string) string {
	cur := bindingsOf(opts, nil)
	outputDir := state.OutputDir
	if opts.Output != "" {
		abs, err := filepath.Abs(opts.Output)
		if err != nil {
			abs = opts.Output
		}
		outputDir = abs
	}
	var outputHint string
	if opts.Checkpoint != "" {
		outputHint = "this --checkpoint records the export into that directory; a new export needs another --checkpoint, or " + path +
			" deleted after its session is released"
	}
	rec := state.Bindings
	var selectorDetails, optionDetails []string
	if rec.SelectorsDigest != cur.SelectorsDigest {
		selectorDetails = resume.SetDiffs(rec.Selectors, cur.Selectors)
	}
	optionDetails = resume.OptionDiffs(boundOptions(rec.Options), boundOptions(cur.Options), defaultOptions(opts.Format), showOption)
	return resume.DiffBindings([]resume.Binding{
		{Name: "outputDir", Recorded: state.OutputDir, Current: outputDir, Hint: outputHint},
		{
			Name: "selectorsDigest", Recorded: rec.SelectorsDigest, Current: cur.SelectorsDigest, Hint: "--selector / export.selectors",
			Details: selectorDetails,
		},
		{Name: "options", Hint: "format, compress, csv-annotations, include-schema", Details: optionDetails},
	})
}

// key keys a unit within its node: catalog/group/stage/segment, property/group for a
// property unit.
func (u *plannedUnit) key() string {
	if u.property != nil {
		return transfer.CatalogName(u.catalog()) + "/" + u.group()
	}
	return strings.Join([]string{transfer.CatalogName(u.catalog()), u.group(), u.stage, u.segment.GetUnit().GetSegmentSuffix()}, "/")
}

// estimatedBytes is what the plan estimates the unit's files take.
func (u *plannedUnit) estimatedBytes() uint64 {
	size := u.segment.GetSegmentLevel().GetEstimatedBytes()
	for _, s := range u.segment.GetShards() {
		size += s.GetEstimatedCompressedBytes()
	}
	for _, s := range u.property.GetShards() {
		size += s.GetEstimatedBytes()
	}
	return size
}

// doneOf records a finished unit.
func doneOf(node string, u *plannedUnit) doneUnit {
	d := doneUnit{Node: node, Unit: u.key(), Files: make([]doneFile, 0, len(u.files)), Warnings: slices.Clone(u.warnings), ElapsedMs: u.elapsed.Milliseconds()}
	if d.Warnings == nil {
		d.Warnings = []string{}
	}
	for _, f := range u.files {
		rows := f.rowCounts
		if rows == nil {
			rows = map[string]uint64{}
		}
		d.Files = append(d.Files, doneFile{
			File: f.file, RowCounts: rows, Shard: f.shardID, SegmentLevel: f.segmentLevel,
		})
	}
	return d
}

// markDone restores the finished units of the progress file into the jobs; a finished unit the
// plan does not hold makes the progress file unusable.
func markDone(jobs []*nodeJob, done []doneUnit) error {
	units := map[string]*plannedUnit{}
	for _, job := range jobs {
		for _, u := range job.units {
			units[job.nodeID+"\x00"+u.key()] = u
		}
	}
	for _, d := range done {
		u, ok := units[d.Node+"\x00"+d.Unit]
		if !ok {
			return fmt.Errorf("it records unit %s of node %s as done, which the plan does not hold", d.Unit, d.Node)
		}
		if u.done {
			return fmt.Errorf("it records unit %s of node %s as done twice", d.Unit, d.Node)
		}
		u.done = true
		u.warnings = slices.Clone(d.Warnings)
		u.elapsed = time.Duration(d.ElapsedMs) * time.Millisecond
		u.files = nil
		for _, f := range d.Files {
			var rows map[string]uint64
			if len(f.RowCounts) > 0 {
				rows = f.RowCounts
			}
			u.files = append(u.files, receivedFile{
				file: f.File, rowCounts: rows, shardID: f.Shard, segmentLevel: f.SegmentLevel,
			})
		}
	}
	return nil
}

// summarize is the resume summary of a progress file; jobs is nil while no plan is recorded.
func summarize(path string, state *exportProgress, jobs []*nodeJob) resume.Summary {
	s := resume.Summary{
		Path: path, UpdatedAt: state.UpdatedAt, Now: time.Now(), Session: state.Session.ID, Command: "export",
		Noun: "unit(s)",
	}
	for _, d := range state.Done {
		s.DoneFiles += len(d.Files)
		for _, f := range d.Files {
			s.DoneBytes += f.Bytes
		}
	}
	if jobs == nil {
		s.Done = len(state.Done)
		s.LeftUnknown = "unknown: no plan is recorded yet; it is read from the export session when the export resumes"
		return s
	}
	for _, job := range jobs {
		for _, u := range job.units {
			stage := u.stage
			if u.property != nil {
				stage = "-"
			}
			var doneBytes, leftBytes uint64
			if u.done {
				for _, f := range u.files {
					doneBytes += f.file.Bytes
				}
			} else {
				leftBytes = u.estimatedBytes()
			}
			s.Add(transfer.CatalogName(u.catalog()), u.group(), stage, u.done, len(u.files), doneBytes, leftBytes)
		}
	}
	return s
}

// writeProgress persists a progress file; tests make it fail.
var writeProgress = resume.WriteFile

// tracker feeds the progress file's persister from the workers. A nil tracker records nothing.
type tracker struct {
	p *resume.Persister[exportProgress]
	// state is what p persists; it is read only once p is closed.
	state *exportProgress
	// failErr is the first write failure, the cause the run is canceled with; set by the
	// persister, it is read once p is closed.
	failErr error
	path    string
}

// startTracker starts the persister of state at path; the first write failure cancels the run
// (cancelRun) with the error close returns too.
// Every write adds the time since start, when this run began, to the time the earlier runs took.
func startTracker(ctx context.Context, path string, state *exportProgress, start time.Time, cancelRun context.CancelCauseFunc) *tracker {
	if state.Inflight == nil {
		state.Inflight = map[string]inflightUnit{}
	}
	if state.Done == nil {
		state.Done = []doneUnit{}
	}
	prior := time.Duration(state.ElapsedMs) * time.Millisecond
	// A finished unit costs only its export again when a crash loses its record, while the
	// file, which lists every file of every finished unit, grows with the export: its records
	// are written at most once per progressWriteInterval. What must be durable is synced
	// (the session, the plan) or written by close (before the manifest).
	t := &tracker{path: path, state: state}
	t.p = resume.NewPersisterWith(ctx, state, func(s *exportProgress) error {
		s.UpdatedAt = time.Now()
		s.ElapsedMs = (prior + time.Since(start)).Milliseconds()
		return writeProgress(path, s)
	}, func(err error) {
		t.failErr = exitcode.Exit(exitcode.ExitRuntime, "write progress file %s: %w", path, err)
		cancelRun(t.failErr)
	}, resume.PersisterOptions{MinInterval: progressWriteInterval, CancelableUpdates: true})
	return t
}

// progressWriteInterval spaces the writes of finished units; a test changes it.
var progressWriteInterval = time.Second

// recordPlan writes the plan side file once and then the progress file with its digest: no
// Export stream opens before both are on disk.
func (t *tracker) recordPlan(rec *recordedPlan) error {
	digest, err := resume.WriteSide(resume.SidePath(t.path, planSide), rec)
	if err != nil {
		return fmt.Errorf("write the plan of progress file %s: %w", t.path, err)
	}
	return t.sync(func(s *exportProgress) {
		s.PlanDigest = digest
		s.Inflight = map[string]inflightUnit{}
	})
}

// readPlan reads the plan side file of the progress file at path, checked against the digest
// the progress file records; nil when none is recorded yet.
func readPlan(path string, state *exportProgress) (*recordedPlan, error) {
	if state.PlanDigest == "" {
		return nil, nil
	}
	rec := &recordedPlan{}
	if err := resume.ReadSide(resume.SidePath(path, planSide), state.PlanDigest, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// sync applies change and waits until it is on disk.
func (t *tracker) sync(change func(*exportProgress)) error {
	if err := t.p.Sync(change); err != nil {
		return fmt.Errorf("write progress file %s: %w", t.path, err)
	}
	return nil
}

func (t *tracker) unitStarted(node string, u *plannedUnit) {
	if t == nil {
		return
	}
	in := inflightUnit{Unit: u.key(), StartedAt: time.Now()}
	// Not worth a write of its own: a resume finds the unit unfinished either way.
	t.p.UpdateLazy(func(s *exportProgress) { s.Inflight[node] = in })
}

func (t *tracker) unitFinished(node string, u *plannedUnit) {
	if t == nil {
		return
	}
	d := doneOf(node, u)
	t.p.Update(func(s *exportProgress) {
		s.Done = append(s.Done, d)
		delete(s.Inflight, node)
	})
}

// close writes what is queued and stops the persister; it returns the first write failure.
func (t *tracker) close() error {
	if t == nil {
		return nil
	}
	if err := t.p.Close(); err != nil {
		// The persister told onError of every failure it returns.
		return t.failErr
	}
	return nil
}
