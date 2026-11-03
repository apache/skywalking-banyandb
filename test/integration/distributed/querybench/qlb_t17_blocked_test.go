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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type qlbT17BlockEvidence struct {
	Timed              *bool             `json:"timed"`
	Metrics            map[string]uint64 `json:"metrics"`
	Scenario           string            `json:"scenario"`
	Correctness        string            `json:"correctness"`
	FailedVariant      string            `json:"failed_variant"`
	QueryError         string            `json:"query_error"`
	QueryStatus        string            `json:"query_status"`
	Cleanup            string            `json:"cleanup"`
	Mode               string            `json:"mode"`
	Cardinality        int               `json:"cardinality"`
	QueryMemoryMiB     int               `json:"query_memory_mib"`
	Workers            int               `json:"workers"`
	ClientPeakInFlight int               `json:"client_peak_inflight"`
}

func readQLBT17Blocked32(path string) (qlbT17BlockEvidence, string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return qlbT17BlockEvidence{}, "", fmt.Errorf("read retained T17 workers32 evidence: %w", err)
	}
	var evidence qlbT17BlockEvidence
	if err = json.Unmarshal(body, &evidence); err != nil {
		return evidence, "", err
	}
	if evidence.Cardinality != 1024 || evidence.QueryMemoryMiB != 256 || evidence.Correctness != "blocked" || evidence.Timed == nil || *evidence.Timed ||
		evidence.QueryStatus != "ResourceExhausted" || !strings.Contains(evidence.QueryError, "vec distributed plan: broadcast: query memory pool exhausted") {
		return evidence, "", fmt.Errorf("T17 workers32 reference must preserve the untimed1024-row native ResourceExhausted result")
	}
	if err = validateQLBT17BlockCleanup(body, evidence); err != nil {
		return evidence, "", err
	}
	digest := sha256.Sum256(body)
	return evidence, hex.EncodeToString(digest[:]), nil
}

func validateQLBT17BlockCleanup(body []byte, evidence qlbT17BlockEvidence) error {
	switch evidence.Scenario {
	case "qlb_t17_admission":
		// The original admission artifact recorded the result of the complete
		// native/operator cleanup check as an explicit JSON null on success.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			return err
		}
		cleanup, exists := fields["cleanup_error"]
		if evidence.FailedVariant != "composed_workers_32" || !exists || string(cleanup) != "null" {
			return fmt.Errorf("T17 workers32 admission reference lacks successful complete cleanup")
		}
	case "qlb_t17_concurrency_control":
		if evidence.Mode != "forced_fullsort" || evidence.Workers != 32 || evidence.ClientPeakInFlight != 32 || evidence.Cleanup != "pass" {
			return fmt.Errorf("T17 workers32 control lacks the same32-worker full-sort protocol and cleanup")
		}
		for _, key := range []string{
			"sort_live_bytes", "aggregate_live_bytes", "filter_live_bytes", "having_live_bytes", "post_close_live_results_delta",
			"post_close_reserved_bytes_delta", "post_close_owner_pins_delta", "post_close_flatten_pool_refs_delta",
		} {
			value, exists := evidence.Metrics[key]
			if !exists || value != 0 {
				return fmt.Errorf("T17 workers32 control lacks zero %s", key)
			}
		}
	default:
		return fmt.Errorf("T17 workers32 reference is not an owning admission/control artifact")
	}
	return nil
}

func newQLBT17Blocked32(cfg Config, path string) (Result, error) {
	evidence, digest, err := readQLBT17Blocked32(path)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Scenario: ScenarioQLBT17, Engine: engineMeasure, Mode: cfg.Mode, Cardinality: cfg.Cardinality, Variant: "composed_workers_32",
		QueryWorkers: 32, QueryMemoryMiB: 256, Correctness: "blocked", Error: evidence.QueryError,
		EvidenceArtifacts:  map[string]string{"blocked_32_report": path, "blocked_32_sha256": digest},
		BatchMetrics:       map[string]uint64{"blocked_reference_cardinality": 1024, "blocked_reference_query_memory_mib": 256, "blocked_reference_cleanup_pass": 1},
		UnavailableMetrics: []string{"rpc_latency", "cpu_profile", "allocation_bytes_per_query", "timed_sort_counters"},
	}, nil
}

func validateQLBT17Blocked32(v Result, rows int) error {
	if err := validateQLBT17BlockedHeader(v, rows); err != nil {
		return err
	}
	evidence, digest, err := readQLBT17Blocked32(v.EvidenceArtifacts["blocked_32_report"])
	if err != nil {
		return err
	}
	if v.EvidenceArtifacts["blocked_32_sha256"] != digest || v.Error != evidence.QueryError ||
		v.BatchMetrics["blocked_reference_cardinality"] != 1024 || v.BatchMetrics["blocked_reference_query_memory_mib"] != 256 ||
		v.BatchMetrics["blocked_reference_cleanup_pass"] != 1 ||
		len(v.BatchMetrics) != 3 {
		return fmt.Errorf("T17 workers32 blocked reference changed retained evidence or fabricated timed work")
	}
	return nil
}

func validateQLBT17BlockedHeader(v Result, rows int) error {
	if v.Scenario != ScenarioQLBT17 || v.Engine != engineMeasure || v.Mode != modeVec || v.Cardinality != rows || v.Variant != "composed_workers_32" ||
		v.QueryWorkers != 32 || v.QueryMemoryMiB != 256 || v.Correctness != "blocked" {
		return fmt.Errorf("T17 invalid blocked workers32 header")
	}
	if v.TimedSamples != 0 || v.QueryIterations != 0 || len(v.RawLatenciesNS) != 0 || v.Latency != (LatencyStats{}) ||
		v.Allocations != (AllocationStats{}) || v.Resources != (ResourceStats{}) || len(v.Profiles) != 0 || v.QPS != 0 ||
		v.ExpectedRows != 0 || v.ResponseRows != 0 || v.ExpectedHash != 0 || v.ApproxResultHash != 0 {
		return fmt.Errorf("T17 workers32 blocked reference must contain zero timing/throughput/allocation/result samples")
	}
	return nil
}
