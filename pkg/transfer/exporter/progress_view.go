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
	"os"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// ProgressView is a read-only view of an export progress file and its plan side file as a resume
// reads them, without checking them against an invocation: for tests and checks.
type ProgressView struct {
	// SessionID is the export session the progress file records, "" before it has one.
	SessionID string
	// Plan is the plan of the plan side file, none before it is written.
	Plan []*transferv1.UnitFrame
	// Done is every finished unit of every node.
	Done []DoneUnitView
}

// DoneUnitView is one finished unit of one node and the files it wrote, relative to the output
// directory, in the order they came.
type DoneUnitView struct {
	Node  string
	Unit  string
	Files []string
}

// ReadProgress reads the export progress file at path and its plan side file, checked against
// the digest the progress file records. A missing progress file is an error that wraps
// os.ErrNotExist.
func ReadProgress(path string) (*ProgressView, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	state := &exportProgress{}
	if err := resume.Read(path, progressKind, progressVersion, state); err != nil {
		return nil, err
	}
	rec, err := readPlan(path, state)
	if err != nil {
		return nil, err
	}
	v := &ProgressView{SessionID: state.Session.ID}
	if rec != nil {
		plan, resErr := rec.result()
		if resErr != nil {
			return nil, exitcode.Exit(exitcode.ExitResume, "plan side file of progress file %s is unusable: %w", path, resErr)
		}
		v.Plan = plan.Frames
	}
	for _, d := range state.Done {
		u := DoneUnitView{Node: d.Node, Unit: d.Unit}
		for _, f := range d.Files {
			u.Files = append(u.Files, f.Path)
		}
		v.Done = append(v.Done, u)
	}
	return v, nil
}
