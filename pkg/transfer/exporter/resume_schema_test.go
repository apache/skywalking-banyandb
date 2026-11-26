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
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// groupsLeft names the groups with units left in the progress file of opts and counts the
// units done.
func groupsLeft(t *testing.T, opts testOptions) ([]string, int) {
	t.Helper()
	state, _, jobs, err := loadProgress(ProgressPath(opts.Export), opts.Export)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, job := range jobs {
		for _, u := range job.units {
			if !u.done && !slices.Contains(left, u.group()) {
				left = append(left, u.group())
			}
		}
	}
	return left, len(state.Done)
}

// interruptCSVAfter interrupts a CSV export after n finished units and returns the groups with
// units left.
func interruptCSVAfter(t *testing.T, n int) (*fakeLiaison, testOptions, []string) {
	t.Helper()
	f, opts := interruptAfter(t, n)
	left, done := groupsLeft(t, opts)
	if done != n {
		t.Fatalf("%d unit(s) are done: %d", n, done)
	}
	return f, opts, left
}

// doneGroup is a selected group none of whose units is left.
func doneGroup(t *testing.T, left []string) string {
	t.Helper()
	for _, g := range []string{"sw_metric", "sw_record", "ui"} {
		if !slices.Contains(left, g) {
			return g
		}
	}
	t.Fatalf("every group has units left: %v", left)
	return ""
}

func metadataOf[T interface{ GetMetadata() *commonv1.Metadata }](items []T) []*commonv1.Metadata {
	out := make([]*commonv1.Metadata, 0, len(items))
	for _, it := range items {
		out = append(out, it.GetMetadata())
	}
	return out
}

// bumpDataResource changes the mod revision of the first stream, measure or property of group.
func bumpDataResource(s *fakeSchema, group string) {
	for _, list := range [][]*commonv1.Metadata{metadataOf(s.measures), metadataOf(s.streams), metadataOf(s.properties)} {
		for _, m := range list {
			if m.GetGroup() == group {
				m.ModRevision += 100
				return
			}
		}
	}
}

// catalogOf is the catalog of a group of exportFake.
func catalogOf(group string) commonv1.Catalog {
	switch group {
	case "sw_metric":
		return commonv1.Catalog_CATALOG_MEASURE
	case "sw_record":
		return commonv1.Catalog_CATALOG_STREAM
	}
	return commonv1.Catalog_CATALOG_PROPERTY
}

// addDataResource adds a resource named "added" of the group's data kind.
func addDataResource(s *fakeSchema, group string) {
	switch catalogOf(group) {
	case commonv1.Catalog_CATALOG_MEASURE:
		s.measures = append(s.measures, &databasev1.Measure{Metadata: md(group, "added", 1)})
	case commonv1.Catalog_CATALOG_STREAM:
		s.streams = append(s.streams, &databasev1.Stream{Metadata: md(group, "added", 1)})
	default:
		s.properties = append(s.properties, &databasev1.Property{Metadata: md(group, "added", 1)})
	}
}

// removeDataResource removes the first resource of the group's data kind.
func removeDataResource(s *fakeSchema, group string) {
	dropped := false
	drop := func(m *commonv1.Metadata) bool {
		if !dropped && m.GetGroup() == group {
			dropped = true
			return true
		}
		return false
	}
	s.measures = slices.DeleteFunc(s.measures, func(r *databasev1.Measure) bool { return drop(r.GetMetadata()) })
	s.streams = slices.DeleteFunc(s.streams, func(r *databasev1.Stream) bool { return drop(r.GetMetadata()) })
	s.properties = slices.DeleteFunc(s.properties, func(r *databasev1.Property) bool { return drop(r.GetMetadata()) })
}

// changedRevisionsAfter sends the units after the first n with schema revisions the exported
// schema does not have, as if the schema had changed meanwhile.
func changedRevisionsAfter(n int) func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
	var mu sync.Mutex
	sent := 0
	return scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		files := f.filesFor(req)
		mu.Lock()
		sent++
		change := sent > n
		mu.Unlock()
		if change {
			for _, file := range files {
				for name := range file.revs {
					file.revs[name] += 63
				}
			}
		}
		return sendUnit(stream, files, nil)
	})
}

// A schema that changed under finished CSV units cannot be resumed past: the hint says so
// instead of offering a resume that would fail the same way.
func TestRunExport_SchemaChangedAfterAFinishedUnitIsNotResumable(t *testing.T) {
	f := exportFake()
	f.exportScript = changedRevisionsAfter(1)
	opts, log := exportOptions(t, f)
	opts.Parallelism = 1
	requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "changed during export")
	if !strings.Contains(log.String(), "the schema changed after 1 unit(s) were exported with the first one, so this export cannot be resumed: "+
		"release the session with: bydbctl data release-session --id feedfeed") || strings.Contains(log.String(), "to resume:") {
		t.Fatalf("the hint:\n%s", log)
	}
}

// While no unit is done the resume writes the schema again, so a schema changed since the
// first run does not fail every resume.
func TestResume_RewritesTheSchemaWhileNoUnitIsDone(t *testing.T) {
	f := exportFake()
	f.exportScript = changedRevisionsAfter(0)
	opts, log := exportOptions(t, f)
	opts.Parallelism = 1
	requireExit(t, runExport(t, f, opts), exitcode.ExitRuntime, "changed during export")
	if !strings.Contains(log.String(), "to resume: run the same command again") {
		t.Fatalf("no unit is done, so a resume is offered:\n%s", log)
	}
	if done := readState(t, ProgressPath(opts.Export)).Done; len(done) != 0 {
		t.Fatalf("no unit is done, got %v", done)
	}
	newer := func(f *fakeLiaison, _ *testOptions) {
		for _, list := range [][]*commonv1.Metadata{metadataOf(f.schema.measures), metadataOf(f.schema.streams), metadataOf(f.schema.properties)} {
			for _, m := range list {
				m.ModRevision += 63
			}
		}
	}
	f.mu.Lock()
	newer(f, nil)
	f.mu.Unlock()
	rlog, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	requireSameArtifact(t, baseline(t, newer), opts.Output)
}

// What the CSV files of the units left do not bind to does not refuse a CSV resume: the index
// rules, bindings and Top-N aggregations, the groups whose units are all done and the groups
// not selected.
func TestResume_CSVSchemaChangeElsewhereDoesNotRefuse(t *testing.T) {
	for _, tc := range []struct {
		change func(*testing.T, *fakeSchema, []string)
		name   string
	}{
		{name: "index rule, binding and Top-N", change: func(_ *testing.T, s *fakeSchema, _ []string) {
			s.rules[0].Metadata.ModRevision += 100
			s.bindings[0].Metadata.ModRevision += 100
			s.topNs[0].Metadata.ModRevision += 100
			s.rules = append(s.rules, &databasev1.IndexRule{Metadata: md("sw_metric", "r2", 1)})
			s.bindings = append(s.bindings, &databasev1.IndexRuleBinding{Metadata: md("sw_record", "b2", 1)})
		}},
		{name: "a group whose units are all done", change: func(t *testing.T, s *fakeSchema, left []string) {
			g := doneGroup(t, left)
			bumpDataResource(s, g)
			addDataResource(s, g)
		}},
		{name: "a group not selected", change: func(_ *testing.T, s *fakeSchema, _ []string) {
			s.traces[0].Metadata.ModRevision += 100
			s.traces = append(s.traces, &databasev1.Trace{Metadata: md("unselected", "added", 1)})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, opts, left := interruptCSVAfter(t, 3)
			f.schema.mu.Lock()
			tc.change(t, &f.schema, left)
			f.schema.mu.Unlock()
			if log, err := resumeRun(t, f, opts); err != nil {
				t.Fatalf("%v\n%s", err, log)
			}
		})
	}
}

// Without finished units the resume writes the schema again and checks nothing against the
// old one; without --include-schema there is no exported schema to compare with.
func TestResume_CSVWithoutAnExportedSchemaToCompareGoesOn(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	opts.IncludeSchema = false
	f.exportScript = unitBudget(1, true, nil)
	opts.Parallelism = 1
	interrupted(context.Background(), t, f, opts, log)
	calls := len(f.schema.calls())
	if rlog, err := resumeRun(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, rlog)
	}
	if len(f.schema.calls()) != calls {
		t.Fatalf("no schema is read: %v", f.schema.calls()[calls:])
	}
}

// A native resume takes the units left from the session's snapshot of the first run: a
// resource added since cannot be in them, so it does not refuse; a NOTE names it.
func TestResume_NativeAddedResourceDoesNotRefuse(t *testing.T) {
	f, opts := interruptNativeAfterOne(t)
	f.schema.mu.Lock()
	f.schema.bindings = append(f.schema.bindings, &databasev1.IndexRuleBinding{Metadata: md("sw_record", "b2", 1)})
	f.schema.measures = append(f.schema.measures, &databasev1.Measure{Metadata: md("sw_metric", "new_measure", 1)})
	f.schema.mu.Unlock()
	log, err := resumeRun(t, f, opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if !strings.Contains(log.String(), "NOTE resource(s) added to the schema since the first run are not part of this export: "+
		"indexRuleBinding sw_record/b2, measure sw_metric/new_measure\n") {
		t.Fatalf("the NOTE names the added resources:\n%s", log)
	}
}

// A native resume that cannot list the schema is worth another try, and says so. A refusal the
// server makes there is no preflight rejection (exit 2 says nothing was exported): the earlier
// runs exported units, so it fails at run time.
func TestResume_LiveSchemaReadFailureOffersTheResume(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.PermissionDenied, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			f, opts := interruptNativeAfterOne(t)
			before := tree(t, opts.Output)
			f.schema.mu.Lock()
			f.schema.failList, f.schema.failCode = true, code
			f.schema.mu.Unlock()
			log, err := resumeRun(t, f, opts)
			requireExit(t, err, exitcode.ExitRuntime, "measure registry unavailable")
			if !strings.Contains(log.String(), keptText) {
				t.Fatalf("the resume is offered:\n%s", log)
			}
			requireUntouched(t, before, opts.Output)
		})
	}
}
