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
	"fmt"
	"os"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// ProgressView is a read-only view of an import progress file and its side files as a resume
// reads them, without checking them against an invocation: for tests and checks.
type ProgressView struct {
	// FinishedAt is when a run pushed the last work item, nil before.
	FinishedAt *time.Time
	// Done is the progress of every work item that has any, by work item key.
	Done map[string]ItemView
	// Inflight is the native payload each worker sent and has no answer for yet.
	Inflight map[string]InflightView
	// Plan is the plan of the plan side file, none before it is written.
	Plan []PlannedItemView
	// Pushed are the rows every run put into the target, Duplicates those pushed again that may
	// have landed already.
	Pushed     []RowCountView
	Duplicates []RowCountView
	// Liaisons and TargetNodes are the target the progress file was written for.
	Liaisons    []string
	TargetNodes []string
}

// ItemView is the progress of one work item.
type ItemView struct {
	// SentWindow is how many rows of a started CSV item were sent after its cursor and not
	// acknowledged: they may have landed.
	SentWindow uint64
	Complete   bool
	// Started tells a CSV item with a cursor that is not complete.
	Started bool
}

// InflightView is a native payload sent and not answered.
type InflightView struct {
	Key  string
	Rows uint64
	// Part tells a part from the other payloads.
	Part bool
}

// PlannedItemView is a work item as the first run planned it.
type PlannedItemView struct {
	Key         string
	Sources     []string
	TargetNodes []string
}

// RowCountView is a number of rows of one (catalog, group, stage, kind); the catalog is its name, such as "stream".
type RowCountView struct {
	Catalog string
	Group   string
	Stage   string
	Kind    string
	Rows    uint64
}

// ReadProgress reads the progress file at path with its plan and cursor side files the way a
// resume does (the cursors of a cursor side file not older than the progress file replace those
// of the items not complete, and its pushed counts the progress file's), but does not compare
// its bindings with any target. A missing progress file is an error that wraps os.ErrNotExist, and
// one a resume refuses as unusable is an error too.
func ReadProgress(path string) (*ProgressView, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	state := &progressState{}
	if err := resume.Read(path, progressKind, progressVersion, state); err != nil {
		return nil, err
	}
	if err := readPlan(path, state); err != nil {
		return nil, err
	}
	if err := validateProgress(state); err != nil {
		return nil, fmt.Errorf("the progress file %s is unusable: %w", path, err)
	}
	if err := state.foldCursorSide(path); err != nil {
		return nil, err
	}
	return viewOf(state), nil
}

func viewOf(state *progressState) *ProgressView {
	v := &ProgressView{
		FinishedAt: state.FinishedAt, Done: make(map[string]ItemView, len(state.Done)), Inflight: make(map[string]InflightView, len(state.Inflight)),
		Pushed: rowCountViews(state.Pushed), Duplicates: rowCountViews(state.Duplicates),
		Liaisons: state.Bindings.Liaisons, TargetNodes: state.Bindings.TargetNodes,
	}
	for key, r := range state.Done {
		it := ItemView{Complete: r.Complete, Started: !r.Complete && r.Cursor != nil}
		if it.Started {
			it.SentWindow = r.Cursor.window()
		}
		v.Done[key] = it
	}
	for worker, in := range state.Inflight {
		v.Inflight[worker] = InflightView{Key: in.Key, Rows: in.Rows, Part: in.Kind == kindName(nativefmt.KindPart)}
	}
	for _, it := range state.Plan {
		v.Plan = append(v.Plan, PlannedItemView{Key: it.Key, Sources: it.Sources, TargetNodes: it.TargetNodes})
	}
	return v
}

func rowCountViews(counts []rowCount) []RowCountView {
	out := make([]RowCountView, 0, len(counts))
	for _, c := range counts {
		out = append(out, RowCountView{Catalog: c.Catalog, Group: c.Group, Stage: c.Stage, Kind: c.Kind, Rows: c.Rows})
	}
	return out
}
