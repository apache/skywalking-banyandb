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

package querybench

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func qlbT17ValidOverloadEvidence() qlbT17OverloadEvidence {
	zero := uint64(0)
	e := qlbT17OverloadEvidence{
		Contract: qlbT17OverloadRevision, Overload: "pass", Recovery: "pass", Cardinality: 1024, QueryMemoryMiB: 256, Workers: 32, ProcessID: 1,
		FixtureBaseUnixNanos: time.Unix(1700000000, 0).UnixNano(), CPUQuota: 400000, CPUPeriod: 100000, MemoryLimit: 8 << 30,
		Probe: qlbT17CapacityProbe{
			ProtectorLimit: 6 << 30, PoolLimit: 1536 << 20, QueryLimit: 192 << 20, Admitted: 30,
			Rejected: "query memory pool exhausted", Rollback: "pass", HeldBytes: 1500 << 20, AfterLocalClose: 1500 << 20,
			AfterTransportClose: &zero, Recovery: "pass", FinalBytes: &zero,
		},
	}
	oracles := qlbT17Oracles(qlbT17Spec{composed: true}, 1024, time.Unix(0, e.FixtureBaseUnixNanos))
	cleanup := func() map[string]int64 {
		m := map[string]int64{}
		for _, key := range qlbT17CleanupKeys() {
			m[key] = 0
		}
		return m
	}
	for _, mode := range []string{"topk", "fullsort"} {
		c := qlbT17OverloadCohort{Mode: mode, PeakInFlight: 32, Attempts: 96, Successful: 95, Rejected: 1, Cleanup: cleanup(), RecoveryCleanup: cleanup()}
		for index := 0; index < 96; index++ {
			o := oracles[index%3]
			call := qlbT17OverloadCall{Index: index, Binding: index % 3, Status: "OK", Rows: len(o.expected.DataPoints), Hash: o.hash, Exact: true}
			if index < 3 {
				c.Recovery = append(c.Recovery, call)
			}
			if index == 95 {
				call.Status, call.Error, call.Rows, call.Hash, call.Exact = "ResourceExhausted", qlbT17BroadcastRejection, 0, 0, false
			}
			c.Calls = append(c.Calls, call)
		}
		e.Cohorts = append(e.Cohorts, c)
	}
	return e
}

func TestQLBT17OverloadEvidenceContract(t *testing.T) {
	e := qlbT17ValidOverloadEvidence()
	if err := validateQLBT17OverloadEvidence(e); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*qlbT17OverloadEvidence){
		"old_contract":              func(e *qlbT17OverloadEvidence) { e.Contract = "old" },
		"missing_verdict":           func(e *qlbT17OverloadEvidence) { e.Recovery = "" },
		"changed_budget":            func(e *qlbT17OverloadEvidence) { e.QueryMemoryMiB = 512 },
		"changed_cpu":               func(e *qlbT17OverloadEvidence) { e.CPUQuota++ },
		"swap":                      func(e *qlbT17OverloadEvidence) { e.SwapLimit = 1 },
		"missing_peak":              func(e *qlbT17OverloadEvidence) { e.Cohorts[0].PeakInFlight = 0 },
		"missing_mode":              func(e *qlbT17OverloadEvidence) { e.Cohorts = e.Cohorts[:1] },
		"duplicate_mode":            func(e *qlbT17OverloadEvidence) { e.Cohorts[1].Mode = "topk" },
		"skipped_attempt":           func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls = e.Cohorts[0].Calls[:95] },
		"invented_count":            func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Successful++ },
		"unknown_error":             func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[95].Error = "query memory pool exhausted" },
		"operator_rejection":        func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[95].Error = "sort memory exhausted" },
		"deadline":                  func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[95].Status = "DeadlineExceeded" },
		"partial_rejection":         func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[95].Rows = 1 },
		"wrong_hash":                func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[0].Hash++ },
		"unchecked_success":         func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[0].Exact = false },
		"duplicate_call":            func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Calls[1] = e.Cohorts[0].Calls[0] },
		"missing_cleanup":           func(e *qlbT17OverloadEvidence) { delete(e.Cohorts[0].Cleanup, "native_reserved") },
		"leak":                      func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Cleanup["sort"] = 1 },
		"missing_recovery":          func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Recovery = nil },
		"recovery_rejected":         func(e *qlbT17OverloadEvidence) { e.Cohorts[0].Recovery[0].Status = "ResourceExhausted" },
		"recovery_leak":             func(e *qlbT17OverloadEvidence) { e.Cohorts[0].RecoveryCleanup["owner_pins"] = 1 },
		"missing_probe_rejection":   func(e *qlbT17OverloadEvidence) { e.Probe.Rejected = "" },
		"missing_rollback":          func(e *qlbT17OverloadEvidence) { e.Probe.Rollback = "" },
		"early_release":             func(e *qlbT17OverloadEvidence) { e.Probe.AfterLocalClose = 0 },
		"missing_transport_cleanup": func(e *qlbT17OverloadEvidence) { e.Probe.AfterTransportClose = nil },
		"missing_final_cleanup":     func(e *qlbT17OverloadEvidence) { e.Probe.FinalBytes = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := qlbT17ValidOverloadEvidence()
			mutate(&changed)
			if validateQLBT17OverloadEvidence(changed) == nil {
				t.Fatal("accepted incomplete or incorrect overload evidence")
			}
		})
	}
	// RPC scheduling need not cause a rejection; the held-capacity probe proves it independently.
	allAdmitted := qlbT17ValidOverloadEvidence()
	for i := range allAdmitted.Cohorts {
		c := &allAdmitted.Cohorts[i]
		call := c.Recovery[2]
		call.Index = 95
		c.Calls[95] = call
		c.Successful, c.Rejected = 96, 0
	}
	if err := validateQLBT17OverloadEvidence(allAdmitted); err != nil {
		t.Fatal(err)
	}
}

func TestQLBT17OverloadArtifactRejectsTimingAndTrailingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overload.json")
	body, err := json.Marshal(qlbT17ValidOverloadEvidence())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	actual, digest, err := readQLBT17Overload(path)
	if err != nil || len(digest) != 64 || actual.Contract != qlbT17OverloadRevision {
		t.Fatalf("read evidence: %s %v", digest, err)
	}
	for _, changed := range [][]byte{
		bytes.Replace(body, []byte(`,"cgroup_swap_limit_bytes":0`), nil, 1),
		bytes.Replace(body, []byte(`"cgroup_swap_limit_bytes":0`), []byte(`"cgroup_swap_limit_bytes":null`), 1),
	} {
		if bytes.Equal(body, changed) {
			t.Fatal("missing-limit mutation did not change the artifact")
		}
		if err = os.WriteFile(path, changed, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err = readQLBT17Overload(path); err == nil {
			t.Fatal("accepted missing swap limit evidence")
		}
	}
	for _, suffix := range []string{`,"qps":100}`, `,"latency_ns":[1]}`, `} {}`, `} garbage`} {
		changed := append(append([]byte(nil), body[:len(body)-1]...), suffix...)
		if err = os.WriteFile(path, changed, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err = readQLBT17Overload(path); err == nil {
			t.Fatal("accepted timed or trailing evidence")
		}
	}
}
