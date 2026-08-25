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

package inverted

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// A read-only count runs in its own process throughout this contract. Three of
// the guarantees NIDX-01A owes callers -- that inspecting a directory does not
// panic, does not abort the process, and does not hang -- are invisible to a
// caller sharing that process: a fault while decoding mapped bytes takes the
// whole test binary down with it, and a hang stalls it. Counting in a child
// makes each of those an ordinary assertion. It is also how the guarantee is
// used in practice: the process inspecting an index directory is generally not
// the process writing it.
const (
	countChildEnv     = "NIDX01A_COUNT_CHILD_PATH"
	countChildMarker  = "NIDX01A_COUNT_RESULT:"
	countChildTest    = "-test.run=^TestReadOnlyDocCountChildProcess$"
	countChildTimeout = "-test.timeout=60s"
)

// countObservation is what a child process reports after one call to the
// boundary: the value it returned, and how the returned error classifies.
type countObservation struct {
	Err         string `json:"err"`
	Count       int64  `json:"count"`
	AllocBytes  uint64 `json:"alloc_bytes"`
	Succeeded   bool   `json:"succeeded"`
	Corrupt     bool   `json:"corrupt"`
	NoCommitted bool   `json:"no_committed"`
}

// TestReadOnlyDocCountChildProcess counts the index directory named by
// countChildEnv and reports the outcome on stdout for the parent test that
// spawned it. It is inert in an ordinary run, where that variable is unset.
func TestReadOnlyDocCountChildProcess(t *testing.T) {
	path := os.Getenv(countChildEnv)
	if path == "" {
		t.Skipf("%s is set only by the parent contract tests", countChildEnv)
	}

	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	allocatedBefore := stats.TotalAlloc

	count, err := ReadOnlyDocCount(path)

	runtime.ReadMemStats(&stats)
	observed := countObservation{
		Count:       count,
		AllocBytes:  stats.TotalAlloc - allocatedBefore,
		Succeeded:   err == nil,
		Corrupt:     errors.Is(err, ErrCorruptIndex),
		NoCommitted: errors.Is(err, ErrNoCommittedIndex),
	}
	if err != nil {
		observed.Err = err.Error()
	}
	encoded, marshalErr := json.Marshal(observed)
	if marshalErr != nil {
		t.Fatalf("failed to encode the observation: %v", marshalErr)
	}
	fmt.Println(countChildMarker + string(encoded))
}

// countInChildProcess calls the boundary on path in a fresh process and returns
// what that process observed. It fails the test if the call brings its process
// down, exceeds the child's timeout, or produces no observation at all.
func countInChildProcess(t *testing.T, path string) countObservation {
	t.Helper()
	// #nosec G204 -- self-exec of the running test binary; arguments are constant.
	cmd := exec.Command(os.Args[0], countChildTest, countChildTimeout, "-test.count=1")
	cmd.Env = append(os.Environ(), countChildEnv+"="+path)
	output, runErr := cmd.CombinedOutput()

	var encoded string
	for _, line := range strings.Split(string(output), "\n") {
		if suffix, found := strings.CutPrefix(strings.TrimSpace(line), countChildMarker); found {
			encoded = suffix
		}
	}
	if encoded == "" {
		t.Fatalf("ReadOnlyDocCount(%q) reported nothing: it must return a value rather than panic, "+
			"abort its process, or hang. Child exited with %v and said:\n%s", path, runErr, output)
	}
	if runErr != nil {
		t.Fatalf("the process counting %q exited with %v; output:\n%s", path, runErr, output)
	}
	var observed countObservation
	if unmarshalErr := json.Unmarshal([]byte(encoded), &observed); unmarshalErr != nil {
		t.Fatalf("failed to decode the observation %q: %v", encoded, unmarshalErr)
	}
	return observed
}
