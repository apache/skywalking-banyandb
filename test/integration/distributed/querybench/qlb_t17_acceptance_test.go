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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validateQLBT17Acceptance(result Result) error {
	verdicts := result.Acceptance
	if verdicts == nil || verdicts.Contract != qlbT17OverloadRevision || verdicts.Performance != "pass" ||
		verdicts.Overload != "pass" || verdicts.Recovery != "pass" || result.Correctness != "pass" || result.Error != "" {
		return fmt.Errorf("T17 revised acceptance requires separate passing performance, overload and recovery verdicts")
	}
	if len(result.EvidenceArtifacts) != 2 || result.EvidenceArtifacts["overload_report"] == "" {
		return fmt.Errorf("T17 revised acceptance requires an overload report and its digest")
	}
	_, digest, err := readQLBT17Overload(result.EvidenceArtifacts["overload_report"])
	if err != nil {
		return err
	}
	if digest != result.EvidenceArtifacts["overload_sha256"] {
		return fmt.Errorf("T17 overload/recovery evidence changed after verification")
	}
	return nil
}

func revisedQLBT17Fixture(t *testing.T, rows int) Result {
	t.Helper()
	path := filepath.Join(t.TempDir(), "overload.json")
	body, err := json.Marshal(qlbT17ValidOverloadEvidence())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	_, digest, err := readQLBT17Overload(path)
	if err != nil {
		t.Fatal(err)
	}
	result := qlbT17FixtureReport(rows, uint64(rows))
	result.Variants = result.Variants[:4]
	for index := range result.Variants {
		variant := &result.Variants[index]
		variant.BatchMetrics["checked_timed_samples"] = uint64(variant.TimedSamples)
		variant.BatchMetrics["checked_response_bytes"] = uint64(variant.TimedSamples) * 2
	}
	result.Acceptance = &AcceptanceVerdicts{Contract: qlbT17OverloadRevision, Performance: "pass", Overload: "pass", Recovery: "pass"}
	result.EvidenceArtifacts = map[string]string{"overload_report": path, "overload_sha256": digest}
	return result
}

func TestQLBT17RevisedAcceptance(t *testing.T) {
	result := revisedQLBT17Fixture(t, 1024)
	if err := validateQLBT17Result(result); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*Result){
		"no contract":           func(r *Result) { r.Acceptance = nil },
		"wrong revision":        func(r *Result) { r.Acceptance.Contract = "unknown" },
		"failed performance":    func(r *Result) { r.Acceptance.Performance = "fail" },
		"failed overload":       func(r *Result) { r.Acceptance.Overload = "fail" },
		"missing recovery":      func(r *Result) { r.Acceptance.Recovery = "" },
		"missing evidence":      func(r *Result) { r.EvidenceArtifacts = nil },
		"changed evidence":      func(r *Result) { r.EvidenceArtifacts["overload_sha256"] = "changed" },
		"missing timed cohort":  func(r *Result) { r.Variants = r.Variants[:3] },
		"timed overload":        func(r *Result) { r.Variants = append(r.Variants, qlbT17FixtureReport(1024, 1).Variants[4]) },
		"rejected timed cohort": func(r *Result) { r.Variants[0].Correctness = "blocked" },
		"unchecked responses":   func(r *Result) { r.Variants[0].BatchMetrics["checked_timed_samples"] = 6 },
		"lost samples":          func(r *Result) { r.Variants[0].RawLatenciesNS = r.Variants[0].RawLatenciesNS[1:] },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := revisedQLBT17Fixture(t, 1024)
			mutate(&changed)
			if validateQLBT17Result(changed) == nil {
				t.Fatal("accepted incomplete revised acceptance")
			}
		})
	}
	// A same-path file replacement must invalidate the report, even if it remains valid JSON.
	if err := os.WriteFile(result.EvidenceArtifacts["overload_report"], []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if validateQLBT17Result(result) == nil {
		t.Fatal("accepted replaced resource evidence")
	}
}

func TestQLBT17MatrixKeepsAcceptanceProtocolsSeparate(t *testing.T) {
	results := []Result{revisedQLBT17Fixture(t, 1024), revisedQLBT17Fixture(t, 32768)}
	if err := validateQLBT17Matrix(results); err != nil {
		t.Fatal(err)
	}
	results[1] = qlbT17FixtureReport(32768, 2)
	if validateQLBT17Matrix(results) == nil {
		t.Fatal("accepted mixed historical and revised performance contracts")
	}
}

func TestQLBT17MergedReportPreservesAcceptance(t *testing.T) {
	dir := t.TempDir()
	for _, rows := range []int{1024, 32768} {
		result := revisedQLBT17Fixture(t, rows)
		if _, err := writeShard(result, dir); err != nil {
			t.Fatal(err)
		}
	}
	runMergeShards(t, Config{ReportDir: dir, Engine: engineMeasure})
	body, err := os.ReadFile(filepath.Join(dir, "distributed-querybench.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err = json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 8 {
		t.Fatalf("expected8 timedvariants, got%d", len(report.Results))
	}
	for _, result := range report.Results {
		if err = validateQLBT17Acceptance(result); err != nil {
			t.Fatal(err)
		}
	}
	markdown, err := os.ReadFile(filepath.Join(dir, "distributed-querybench.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{qlbT17OverloadRevision, "performance=pass", "overload=pass", "recovery=pass", "overload_sha256"} {
		if !strings.Contains(string(markdown), text) {
			t.Fatalf("merged Markdown lost %s", text)
		}
	}
}
