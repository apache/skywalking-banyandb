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
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// interruptWithCheckpointElsewhere is interruptAfterOne with the progress file outside the
// output directory; the give-up hint names it and its plan side file.
func interruptWithCheckpointElsewhere(t *testing.T) (*fakeLiaison, testOptions) {
	t.Helper()
	f := exportFake()
	opts, log := exportOptions(t, f)
	// Its directory does not exist yet: it is created before the cluster is touched.
	opts.Checkpoint = filepath.Join(t.TempDir(), "state", "p.json")
	opts.Parallelism = 1
	f.exportScript = unitBudget(1, true, nil)
	interrupted(context.Background(), t, f, opts, log)
	want := "then delete " + opts.Checkpoint + ", " + resume.SidePath(opts.Checkpoint, planSide) + " and what is in " + opts.Output + "; a released session"
	if !strings.Contains(log.String(), want) {
		t.Fatalf("the give-up hint does not name the progress file outside the output directory, want %q:\n%s", want, log)
	}
	return f, opts
}

// streamGroupLeft is the stream group sw_record, which interruptAfterOne leaves with units left:
// it fails unless the progress file of opts says so.
func streamGroupLeft(t *testing.T, opts testOptions) string {
	t.Helper()
	const group = "sw_record"
	if left, _ := groupsLeft(t, opts); !slices.Contains(left, group) {
		t.Fatalf("%s has no unit left: %v", group, left)
	}
	return group
}

// changeSchema applies change to the schema of f under its lock.
func changeSchema(f *fakeLiaison, change func(*fakeSchema)) {
	f.schema.mu.Lock()
	defer f.schema.mu.Unlock()
	change(&f.schema)
}

// changeLiaison applies change to f under its lock.
func changeLiaison(f *fakeLiaison, change func(*fakeLiaison)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f)
}

// removeDoneFile removes the first file of the first finished unit.
func removeDoneFile(t *testing.T, opts testOptions) {
	t.Helper()
	state := readState(t, ProgressPath(opts.Export))
	if err := os.Remove(filepath.Join(opts.Output, filepath.FromSlash(state.Done[0].Files[0].Path))); err != nil {
		t.Fatal(err)
	}
}

// appendToSchemaFile changes schema/measure.jsonl, which the finished units were checked against.
func appendToSchemaFile(t *testing.T, opts testOptions) {
	t.Helper()
	p := filepath.Join(opts.Output, "schema", "measure.jsonl")
	body, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(p, append(body, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeProgressBody replaces the progress file of opts with body.
func writeProgressBody(body string) func(*testing.T, *fakeLiaison, *testOptions) {
	return func(t *testing.T, _ *fakeLiaison, o *testOptions) {
		if err := os.WriteFile(ProgressPath(o.Export), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// A resume that is refused exits 6 (1 for a usage error), deletes nothing in the output
// directory, releases no session and does not offer itself again; only a refusal that needs the
// session's answer reads it, and no other renews its lease. The error says how to start over.
func TestResume_RefusalsExit6AndDeleteNothing(t *testing.T) {
	unusable := "nothing was deleted. To start over: if it records an export session, release it with: bydbctl data release-session --id <id>; then delete what is in "
	noHint := func(string, testOptions) string { return "" }
	schemaChanged := "the schema changed after 1 unit(s) were exported with the first one"
	for _, tc := range []struct {
		interrupt func(*testing.T) (*fakeLiaison, testOptions) // interruptAfterOne when nil
		change    func(*testing.T, *fakeLiaison, *testOptions)
		// hint is how the error says to start over, given the output directory of the
		// interrupted run; the release of session feedfeed and the cleanup of that directory when
		// nil, nothing to check when it returns "".
		hint func(output string, opts testOptions) string
		name string
		want []string
		code int  // exitcode.ExitResume when 0
		read bool // the refusal comes after Plan{read}
	}{
		{
			name: "options and selectors",
			want: []string{
				// maxFileSize may change (TestResume_AnotherMaxFileSizeIsKeptForTheUnitsLeft): it is not named.
				"options differs (format, compress, csv-annotations, include-schema):\n    compress: recorded none, now gzip (default)\nrun the first",
				"selectorsDigest differs (--selector / export.selectors):\n    recorded only: catalog=property,groups=\n",
			},
			change: func(_ *testing.T, _ *fakeLiaison, o *testOptions) {
				o.MaxFileSize = 64 << 20
				o.Compress = transferv1.Compress_COMPRESS_GZIP
				o.Selectors = o.Selectors[:2]
			},
		},
		{
			name: "session gone", want: []string{"cannot be resumed"}, read: true,
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) { f.readCode = codes.NotFound },
		},
		{
			name: "session expired", want: []string{"cannot be resumed"}, read: true,
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) { f.readCode = codes.FailedPrecondition },
		},
		{
			name: "unreachable node", want: []string{"could not be read on data node(s) [" + nodeB + "]: they no longer hold it or cannot be reached"}, read: true,
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{nodeA}, UnreachableNodes: []string{nodeB}}
			},
		},
		{
			name: "changed inventory", want: []string{"differs from the recorded plan"}, read: true,
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) { segmentOf(f.planFrames[2]).Shards[0].TotalCount++ },
		},
		{
			name: "deleted group", want: []string{"no longer in the session"}, read: true,
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) { f.planFrames = f.planFrames[:2] },
		},
		{
			// Not written by this bydbctl: the finished units would be exported again over their
			// own files, and every run would fail.
			name: "finished units without a plan", want: []string{"records finished units but no plan"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				editState(t, o, func(s *exportProgress) { s.PlanDigest = "" })
			},
		},
		{
			// Found before the question and before Plan{read}, not by the cleanup.
			name: "a link deep in an unfinished unit", want: []string{"is not a regular file or directory"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) { linkBesideATmp(t, *o) },
		},
		{
			// Nothing done yet: the schema would be written again without the group, and the
			// manifest would list units of a group it lacks.
			name: "group deleted before a unit was done", interrupt: func(t *testing.T) (*fakeLiaison, testOptions) { return interruptAfter(t, 0) },
			want: []string{"group(s) stream sw_record the first run recorded no longer exist"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				changeLiaison(f, func(f *fakeLiaison) {
					f.groups = slices.DeleteFunc(f.groups, func(cg *commonv1.Group) bool { return cg.GetMetadata().GetName() == "sw_record" })
				})
			},
		},
		{
			name: "source node gone", want: []string{"data node(s) data-b:17912 still have units of this export to export but are not registered in the cluster now"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) { f.dataNodes = f.dataNodes[:1] },
		},
		{
			name: "finished file missing", want: []string{"finished unit measure/sw_metric/"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) { removeDoneFile(t, *o) },
		},
		{
			name: "finished file short", want: []string{"has 1 bytes"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				state := readState(t, ProgressPath(o.Export))
				if len(state.Done) != 1 || len(state.Done[0].Files) != 1 {
					t.Fatalf("one finished unit with one file is recorded: %+v", state.Done)
				}
				if err := os.Truncate(filepath.Join(o.Output, filepath.FromSlash(state.Done[0].Files[0].Path)), 1); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "finished file outside nodes", want: []string{`the progress file lists "../x", which is not a file below nodes/`},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				editState(t, o, func(s *exportProgress) { s.Done[0].Files[0].Path = "../x" })
			},
		},
		{
			// The finished units were checked against the schema files: they must not change,
			// and the refusal comes before the cleanup.
			name: "schema file changed", want: []string{"schema/measure.jsonl: recorded "},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) { appendToSchemaFile(t, *o) },
		},
		{
			name: "plan side file changed", want: []string{"is not the one its progress file was written with"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				side := resume.SidePath(ProgressPath(o.Export), planSide)
				if err := os.WriteFile(side, []byte(`{"frames":[],"answeredNodes":[]}`+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "no session recorded", want: []string{"records no export session"},
			hint: func(output string, _ testOptions) string {
				return "to start over: delete what is in " + output + ", then export again"
			},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				editState(t, o, func(s *exportProgress) { s.Session.ID = "" })
			},
		},
		{
			name: "no session recorded, progress file elsewhere", interrupt: interruptWithCheckpointElsewhere, want: []string{"records no export session"},
			hint: func(output string, o testOptions) string {
				return "to start over: delete " + o.Checkpoint + ", " + resume.SidePath(o.Checkpoint, planSide) + " and what is in " + output + ", then export again"
			},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) {
				editState(t, o, func(s *exportProgress) { s.Session.ID = "" })
			},
		},
		{
			// A --checkpoint outside the output directory belongs to the export into that
			// directory; a new output directory with it is refused with the way out.
			name: "checkpoint of another output directory", interrupt: interruptWithCheckpointElsewhere, hint: noHint,
			want:   []string{"outputDir differs", "this --checkpoint records the export into that directory; a new export needs another --checkpoint"},
			change: func(t *testing.T, _ *fakeLiaison, o *testOptions) { o.Output = filepath.Join(t.TempDir(), "other") },
		},
		{name: "progress file not JSON", hint: noHint, want: []string{"progress file", unusable}, change: writeProgressBody("{")},
		{name: "progress file of an import", hint: noHint, want: []string{"progress file", unusable}, change: writeProgressBody(`{"kind":"import-progress","version":1}`)},
		{
			name: "progress file of the previous version", hint: noHint,
			want:   []string{"progress file", `"export-progress" version 1 file`, `resumes "export-progress" version 2`, unusable},
			change: writeProgressBody(`{"kind":"export-progress","version":1}`),
		},
		{
			name: "progress file of a later version", hint: noHint, want: []string{"progress file", `"export-progress" version 3 file`, unusable},
			change: writeProgressBody(`{"kind":"export-progress","version":3}`),
		},
		{
			name: "schema-only", code: exitcode.ExitUsage, hint: noHint, want: []string{"a schema-only export needs a new or empty directory"},
			change: func(_ *testing.T, _ *fakeLiaison, o *testOptions) { o.SchemaOnly = true },
		},
		// A CSV resume compares the live schema of what the CSV files of the units left bind to
		// before it reads the session, which renews its lease, and before it deletes anything.
		{
			name: "csv resource changed",
			want: []string{schemaChanged + ", so this export cannot be resumed; nothing was deleted", "stream sw_record/log: recorded revision "},
			change: func(t *testing.T, f *fakeLiaison, o *testOptions) {
				g := streamGroupLeft(t, *o)
				changeSchema(f, func(s *fakeSchema) { bumpDataResource(s, g) })
			},
		},
		{
			name: "csv resource added", want: []string{schemaChanged, "stream sw_record/added: added"},
			change: func(t *testing.T, f *fakeLiaison, o *testOptions) {
				g := streamGroupLeft(t, *o)
				changeSchema(f, func(s *fakeSchema) { addDataResource(s, g) })
			},
		},
		{
			name: "csv resource removed", want: []string{schemaChanged, "stream sw_record/log: removed"},
			change: func(t *testing.T, f *fakeLiaison, o *testOptions) {
				g := streamGroupLeft(t, *o)
				changeSchema(f, func(s *fakeSchema) { removeDataResource(s, g) })
			},
		},
		{
			// A group with units left that was deleted since the first run is a removed schema.
			name: "csv group deleted", want: []string{schemaChanged, "stream sw_record/log: removed"},
			change: func(t *testing.T, f *fakeLiaison, o *testOptions) {
				g := streamGroupLeft(t, *o)
				changeLiaison(f, func(f *fakeLiaison) {
					f.groups = slices.DeleteFunc(f.groups, func(cg *commonv1.Group) bool { return cg.GetMetadata().GetName() == g })
				})
				changeSchema(f, func(s *fakeSchema) { removeDataResource(s, g) })
			},
		},
		{
			name: "native measure revision", interrupt: interruptNativeAfterOne,
			want: []string{schemaChanged, "measure sw_metric/service_cpm: recorded revision 7, now revision 70"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				changeSchema(f, func(s *fakeSchema) { s.measures[0] = &databasev1.Measure{Metadata: md("sw_metric", "service_cpm", 70)} })
			},
		},
		{
			name: "native index rule", interrupt: interruptNativeAfterOne,
			want: []string{schemaChanged, "indexRule sw_metric/r1: recorded revision 1, now revision 2"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				changeSchema(f, func(s *fakeSchema) {
					s.rules[0] = &databasev1.IndexRule{Metadata: md("sw_metric", "r1", 2), Tags: []string{"x"}}
				})
			},
		},
		{
			name: "native removed binding", interrupt: interruptNativeAfterOne, want: []string{schemaChanged, "indexRuleBinding sw_record/b1: removed"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				changeSchema(f, func(s *fakeSchema) { s.bindings = nil })
			},
		},
		{
			name: "native removed measure", interrupt: interruptNativeAfterOne, want: []string{schemaChanged, "measure sw_metric/service_idx: removed"},
			change: func(_ *testing.T, f *fakeLiaison, _ *testOptions) {
				changeSchema(f, func(s *fakeSchema) { s.measures = slices.Delete(s.measures, 1, 2) })
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			interrupt := tc.interrupt
			if interrupt == nil {
				interrupt = interruptAfterOne
			}
			f, opts := interrupt(t)
			output := opts.Output
			plans, heartbeats := len(f.plans), f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT)
			tc.change(t, f, &opts)
			before := tree(t, output)
			log, err := resumeRun(t, f, opts)
			code := tc.code
			if code == 0 {
				code = exitcode.ExitResume
			}
			for _, w := range tc.want {
				requireExit(t, err, code, w)
			}
			hint := "to start over: bydbctl data release-session --id feedfeed, then delete what is in " + output + " and export again"
			if tc.hint != nil {
				hint = tc.hint(output, opts)
			}
			if !strings.Contains(err.Error(), hint) {
				t.Fatalf("a refusal says how to start over, want %q: %v", hint, err)
			}
			requireUntouched(t, before, output)
			if read := len(f.plans) > plans; read != tc.read {
				t.Fatalf("Plan{read} sent = %v, want %v", read, tc.read)
			}
			if !tc.read && f.countSessions(transferv1.SessionsRequest_ACTION_HEARTBEAT) != heartbeats {
				t.Fatal("the session is not read, so its lease is not renewed")
			}
			if got := f.releasedIDs(); len(got) != 0 {
				t.Fatalf("a refused resume releases nothing, released %v", got)
			}
			if strings.Contains(log.String(), "to resume:") {
				t.Fatalf("a refused resume does not offer itself again:\n%s", log)
			}
		})
	}
}

// A dry run checks what is on disk as the resume does, without the session.
func TestResume_DryRunRefusesWhatAResumeRefuses(t *testing.T) {
	for _, tc := range []struct {
		change func(*testing.T, testOptions)
		name   string
		want   string
	}{
		{name: "finished file missing", change: removeDoneFile, want: "finished unit "},
		{name: "schema file changed", change: appendToSchemaFile, want: "schema/measure.jsonl: recorded "},
		{name: "a link deep in an unfinished unit", change: linkBesideATmp, want: "is not a regular file or directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, opts := interruptAfterOne(t)
			log := &syncBuffer{}
			opts.Log, opts.Out = log, log
			tc.change(t, opts)
			_, err := DryRunResume(envFor(t, opts, f), opts.Export)
			requireExit(t, err, exitcode.ExitResume, tc.want)
		})
	}
}

// linkBesideATmp puts a symlink next to a .tmp file the interrupted unit left: in a directory no
// finished file's path checks.
func linkBesideATmp(t *testing.T, opts testOptions) {
	t.Helper()
	var tmp string
	err := filepath.WalkDir(filepath.Join(opts.Output, nodesDir), func(p string, _ fs.DirEntry, err error) error {
		if err == nil && tmp == "" && strings.HasSuffix(p, ".tmp") {
			tmp = p
		}
		return err
	})
	if err != nil || tmp == "" {
		t.Fatalf("no .tmp below nodes/: %v", err)
	}
	target := filepath.Join(t.TempDir(), "target")
	writeFile(t, target, "keep")
	if err = os.Symlink(target, filepath.Join(filepath.Dir(tmp), "deep-link")); err != nil {
		t.Fatal(err)
	}
}

// The cleanup refuses what it does not delete with how to start over, and removes the manifest's
// temporary files of its own directory only: the name of the directory is no pattern.
func TestCleanNodes(t *testing.T) {
	t.Run("refuses a link with how to start over", func(t *testing.T) {
		dir := t.TempDir()
		node := filepath.Join(dir, nodesDir, "n")
		if err := os.MkdirAll(node, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(node, "link")); err != nil {
			t.Fatal(err)
		}
		err := cleanNodes(dir, filepath.Join(dir, ProgressFileName), nil, "HOW TO START OVER")
		requireExit(t, err, exitcode.ExitResume, "nothing was deleted. HOW TO START OVER")
	})
	t.Run("a directory named like a pattern", func(t *testing.T) {
		parent := t.TempDir()
		dir, other := filepath.Join(parent, "exp[1]"), filepath.Join(parent, "exp1")
		tmpName := ".manifest.json.123.tmp"
		for _, d := range []string{dir, other} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(d, tmpName), "x")
		}
		writeFile(t, filepath.Join(dir, ".manifest.json.tmp"), "not one of manifest.Write's")
		if err := cleanNodes(dir, filepath.Join(dir, ProgressFileName), nil, ""); err != nil {
			t.Fatal(err)
		}
		if resume.Exists(filepath.Join(dir, tmpName)) {
			t.Fatal("the manifest's temporary file of the output directory is removed")
		}
		if !resume.Exists(filepath.Join(other, tmpName)) || !resume.Exists(filepath.Join(dir, ".manifest.json.tmp")) {
			t.Fatal("nothing else is removed")
		}
	})
}

// The progress file may list only files below nodes/.
func TestCheckDoneFiles_RefusesPathsOutsideNodes(t *testing.T) {
	for _, p := range []string{"../x", "nodes/../../x", `nodes\a\x.csv`, "schema/measure.jsonl", "/etc/passwd"} {
		err := checkDoneFiles(t.TempDir(), []doneUnit{{Node: nodeA, Unit: "u", Files: []doneFile{{File: manifest.File{Path: p}}}}}, "HOW")
		requireExit(t, err, exitcode.ExitResume, "which is not a file below nodes/; nothing was deleted. HOW")
	}
}

// listFiles names every regular file below dir, following no link.
func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, e os.DirEntry, err error) error {
		if err == nil && e.Type().IsRegular() {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

func requireSymlink(t *testing.T, p string) {
	t.Helper()
	info, err := os.Lstat(p)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s is still a symlink: %v %v", p, info, err)
	}
}

// A symlink below the output directory is never followed nor deleted by a resume: it is refused
// before anything is deleted, so no finished file drops out of the export.
func TestResume_SymlinksBelowNodesAreRefused(t *testing.T) {
	// moveAndLink moves rel below opts.Output elsewhere and puts a symlink to it in its place.
	moveAndLink := func(t *testing.T, opts testOptions, rel string) (string, string) {
		t.Helper()
		p := filepath.Join(opts.Output, filepath.FromSlash(rel))
		moved := filepath.Join(t.TempDir(), "moved")
		if err := os.Rename(p, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(moved, p); err != nil {
			t.Fatal(err)
		}
		return p, moved
	}
	for _, tc := range []struct {
		link func(*testing.T, testOptions) (string, string)
		name string
		want string
	}{
		{
			name: "nodes/", want: "is a symlink, which a resume does not follow",
			link: func(t *testing.T, opts testOptions) (string, string) { return moveAndLink(t, opts, nodesDir) },
		},
		{
			name: "a node directory", want: "is a symlink, which a resume does not follow",
			link: func(t *testing.T, opts testOptions) (string, string) {
				return moveAndLink(t, opts, nodesDir+"/"+manifest.NodeDir(nodeA))
			},
		},
		{
			name: "a finished file", want: "is a symlink, which a resume does not follow",
			link: func(t *testing.T, opts testOptions) (string, string) {
				return moveAndLink(t, opts, readState(t, ProgressPath(opts.Export)).Done[0].Files[0].Path)
			},
		},
		{
			name: "a stray link no unit lists", want: "is not a regular file or directory",
			link: func(t *testing.T, opts testOptions) (string, string) {
				target := filepath.Join(t.TempDir(), "keep")
				if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(opts.Output, nodesDir, manifest.NodeDir(nodeA), "stray")
				if err := os.Symlink(target, p); err != nil {
					t.Fatal(err)
				}
				return p, target
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, opts := interruptAfterOne(t)
			link, target := tc.link(t, opts)
			beforeOut, beforeTarget := listFiles(t, opts.Output), listFiles(t, target)
			_, err := resumeRun(t, f, opts)
			requireExit(t, err, exitcode.ExitResume, tc.want)
			requireExit(t, err, exitcode.ExitResume, "nothing was deleted")
			requireSymlink(t, link)
			if after := listFiles(t, opts.Output); !slices.Equal(beforeOut, after) {
				t.Fatalf("the output directory changed:\nbefore %v\nafter  %v", beforeOut, after)
			}
			if after := listFiles(t, target); !slices.Equal(beforeTarget, after) {
				t.Fatalf("what the link points to changed:\nbefore %v\nafter  %v", beforeTarget, after)
			}
			if got := f.releasedIDs(); len(got) != 0 {
				t.Fatalf("a refused resume releases nothing, released %v", got)
			}
		})
	}
}

// An unusable output directory or --checkpoint is refused (exit 1) before anything is created or
// asked of the cluster, the same way every time.
func TestRunExport_RefusedBeforeTheClusterIsAsked(t *testing.T) {
	for _, tc := range []struct {
		change func(*testing.T, *testOptions)
		want   func(testOptions) string
		name   string
	}{
		{
			name: "checkpoint in a subdirectory of the output",
			change: func(_ *testing.T, o *testOptions) {
				o.Checkpoint = filepath.Join(o.Output, "state", "p.json")
			},
			want: func(o testOptions) string { return "is in a subdirectory of the output directory " + o.Output },
		},
		{
			// Its directory cannot be created, whoever runs the test: a file is in the way.
			name: "checkpoint below a file",
			change: func(t *testing.T, o *testOptions) {
				notDir := filepath.Join(t.TempDir(), "file")
				writeFile(t, notDir, "")
				o.Checkpoint = filepath.Join(notDir, "state", "p.json")
			},
			want: func(o testOptions) string { return "progress file " + o.Checkpoint },
		},
		{
			// What the output directory is, not the lock, explains the refusal.
			name: "a file as the output directory",
			change: func(t *testing.T, o *testOptions) {
				o.Output = filepath.Join(t.TempDir(), "file")
				writeFile(t, o.Output, "x")
			},
			want: func(o testOptions) string { return "output directory " + o.Output + " is not a directory" },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := exportFake()
			opts, _ := exportOptions(t, f)
			tc.change(t, &opts)
			_, statErr := os.Lstat(opts.Output)
			existed := statErr == nil
			for range 2 {
				requireExit(t, runExport(t, f, opts), exitcode.ExitUsage, tc.want(opts))
				if _, err := os.Lstat(opts.Output); (err == nil) != existed {
					t.Fatalf("nothing is created: the output directory existed %v, now %v", existed, err)
				}
			}
			if len(f.plans) != 0 || len(f.schema.calls()) != 0 {
				t.Fatalf("nothing is asked of the cluster: plans %v, schema %v", f.plans, f.schema.calls())
			}
		})
	}
}
