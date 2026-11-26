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
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// requireNoLockFile checks the run that ended left no lock file.
func requireNoLockFile(t *testing.T, opts testOptions, when string) {
	t.Helper()
	if _, err := os.Stat(resume.LockPath(ProgressPath(opts.Export))); !os.IsNotExist(err) {
		t.Fatalf("the lock file is removed %s: %v", when, err)
	}
}

// Another run holding the lock refuses the export before the cluster is asked anything; a dry
// run reads without the lock.
func TestRunExport_AnotherHolderOfTheLockExits1(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	path := ProgressPath(opts.Export)
	unlock, err := resume.Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock() }()
	requireExit(t, runExport(t, f, opts), exitcode.ExitUsage, "another bydbctl is using "+path)
	if len(f.plans) != 0 || len(f.schema.calls()) != 0 {
		t.Fatalf("nothing is asked of the cluster: plans %v, schema %v", f.plans, f.schema.calls())
	}

	f2, opts2 := interruptAfterOne(t)
	unlock2, err := resume.Lock(ProgressPath(opts2.Export))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlock2() }()
	log := &syncBuffer{}
	opts2.Log, opts2.Out = log, log
	if found, dryErr := DryRunResume(envFor(t, opts2, f2), opts2.Export); !found || dryErr != nil {
		t.Fatalf("a dry run does not need the lock: %v %v\n%s", found, dryErr, log)
	}
}

// The lock file goes on every way out of a run: a failure and an interrupt as well as success,
// or the next run, the resume, would be refused by a lock nobody holds.
func TestRunExport_RemovesTheLockFileOnEveryWayOut(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := exportFake()
		opts, log := exportOptions(t, f)
		if err := runExport(t, f, opts); err != nil {
			t.Fatalf("%v\n%s", err, log)
		}
		requireNoLockFile(t, opts, "with the export done")
		if _, err := os.Stat(resume.SidePath(ProgressPath(opts.Export), planSide)); !os.IsNotExist(err) {
			t.Fatalf("the plan side file is removed with the export done: %v", err)
		}
	})
	t.Run("failure", func(t *testing.T) {
		f, opts := interruptAfterOne(t)
		requireNoLockFile(t, opts, "after a failed run")
		if log, err := resumeRun(t, f, opts); err != nil {
			t.Fatalf("%v\n%s", err, log)
		}
		requireNoLockFile(t, opts, "after the resume")
	})
	t.Run("interrupt", func(t *testing.T) {
		f := exportFake()
		opts, log := exportOptions(t, f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.exportScript = unitBudget(1, true, cancel)
		opts.Parallelism = 1
		interrupted(ctx, t, f, opts, log)
		requireNoLockFile(t, opts, "after an interrupted run")
	})
	t.Run("refusal", func(t *testing.T) {
		f, opts := interruptAfterOne(t)
		opts.Compress = transferv1.Compress_COMPRESS_GZIP
		_, err := resumeRun(t, f, opts)
		requireExit(t, err, exitcode.ExitResume, "")
		requireNoLockFile(t, opts, "after a refused resume")
	})
}

// A run killed with SIGKILL leaves its lock file but not its file lock: the next run locks it at
// once and resumes, and warns about the run that left it when the lock file names it.
func TestResume_TakesOverTheLockOfAKilledRun(t *testing.T) {
	for _, tc := range []struct {
		holder func(*testing.T) ([]byte, string) // the lock file and the warning it gives, if any
		name   string
	}{
		{name: "empty lock file", holder: func(*testing.T) ([]byte, string) { return nil, "" }},
		{name: "lock file of a gone pid", holder: func(t *testing.T) ([]byte, string) {
			// No process has this pid: the file lock is free like that of a killed run.
			const gone = 999999999
			host, _ := os.Hostname()
			body, err := json.Marshal(resume.Holder{PID: gone, Host: host, StartedAt: time.Now(), Command: "bydbctl data export"})
			if err != nil {
				t.Fatal(err)
			}
			return body, "WARN taking over a stale lock of pid " + strconv.Itoa(gone) + " on " + host
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, opts := interruptAfterOne(t)
			body, warning := tc.holder(t)
			if err := os.WriteFile(resume.LockPath(ProgressPath(opts.Export)), body, 0o600); err != nil {
				t.Fatal(err)
			}
			log, err := resumeRun(t, f, opts)
			if err != nil {
				t.Fatalf("%v\n%s", err, log)
			}
			if !strings.Contains(log.String(), "resuming export session feedfeed") {
				t.Fatalf("the export is resumed:\n%s", log)
			}
			// An empty lock file names no run: nothing is warned about.
			if warned := strings.Contains(log.String(), "WARN taking over a stale lock"); warned != (warning != "") {
				t.Fatalf("the stale lock warning is %q, log:\n%s", warning, log)
			}
			if !strings.Contains(log.String(), warning) {
				t.Fatalf("the stale lock is taken over with %q:\n%s", warning, log)
			}
			requireNoLockFile(t, opts, "after the resume")
		})
	}
}
