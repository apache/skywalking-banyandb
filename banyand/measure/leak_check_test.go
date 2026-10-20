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

package measure

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// Leak checking for the vectorized scan (measure scan path v2, step 4d).
//
// verifyNoScanLeaks compares what the scan holds at the end of a test with what it
// held at the start: a block cursor still referenced (by a scan or by a batch that
// borrows its buffers), a scan reservation still held, a decode task still queued
// or running, a decode worker still alive, an unreleased query result. The
// comparison is against the starting point, not zero, because the suite shares one
// decode pool and a Ginkgo suite may keep a service running across tests; TestMain
// checks the absolute zero once everything has finished.

// scanLeakSettle is how long a test end waits for the scan's goroutines to settle
// before an outstanding resource is reported: a canceled decode finishes on a worker.
var scanLeakSettle = 5 * time.Second

// verifyNoScanLeaks fails t when the scan still holds, at the end of t, anything it
// did not hold when verifyNoScanLeaks was called. The check runs in a t.Cleanup, so
// it sees the state after the test's own cleanups, which were registered later.
func verifyNoScanLeaks(t testing.TB) {
	t.Helper()
	// Start the shared pool first so its workers are part of the baseline.
	sharedFixturePool()
	before := ScanResourceSnapshot()
	t.Cleanup(func() {
		t.Helper()
		var now ScanResources
		deadline := time.Now().Add(scanLeakSettle)
		for {
			now = ScanResourceSnapshot()
			if now == before || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if now != before {
			t.Errorf("the scan leaked resources: before=%+v after=%+v%s", before, now, leakStacks())
		}
	})
}

// leakStacks lists the acquisition stacks of the cursors still checked out, when
// stack tracking is on.
func leakStacks() string {
	stacks := pool.AllStacks()[cursorPoolName]
	if len(stacks) == 0 {
		return ""
	}
	out := "\nblock cursors still checked out were acquired at:"
	for _, s := range stacks {
		out += "\n" + s
	}
	return out
}

// TestMain fails the package when, after every test and suite has run, the scan
// still holds a cursor, a reservation, a decode task or a query result. Decode
// workers are not counted: the shared fixture pool lives for the whole run.
func TestMain(m *testing.M) {
	configureInvariantChecks()
	if os.Getenv("BYDB_LEAK_STACKS") == "1" {
		pool.EnableStackTracking(true)
	}
	code := m.Run()
	if code == 0 {
		deadline := time.Now().Add(scanLeakSettle)
		var res ScanResources
		for {
			res = ScanResourceSnapshot()
			if res.Idle() || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !res.Idle() {
			fmt.Fprintf(os.Stderr, "measure scan leak at the end of the test binary: %+v%s\n", res, leakStacks())
			code = 1
		}
	}
	os.Exit(code)
}
