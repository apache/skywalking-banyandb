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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	qlbT17OverloadRevision   = "2026-10-18-v1"
	qlbT17BroadcastRejection = "fail to execute the query plan for measure qlb_t17: vec distributed plan: broadcast: query memory pool exhausted"
)

type qlbT17OverloadCall struct {
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
	Index   int    `json:"index"`
	Binding int    `json:"binding"`
	Rows    int    `json:"rows"`
	Hash    uint64 `json:"ordered_hash"`
	Exact   bool   `json:"exact_result"`
}

type qlbT17OverloadCohort struct {
	Cleanup         map[string]int64     `json:"cleanup"`
	RecoveryCleanup map[string]int64     `json:"recovery_cleanup"`
	Mode            string               `json:"mode"`
	Calls           []qlbT17OverloadCall `json:"calls"`
	Recovery        []qlbT17OverloadCall `json:"recovery"`
	PeakInFlight    int                  `json:"peak_inflight"`
	Attempts        int                  `json:"attempts"`
	Successful      int                  `json:"successful"`
	Rejected        int                  `json:"rejected"`
}

type qlbT17CapacityProbe struct {
	AfterTransportClose *uint64 `json:"after_transport_close_bytes"`
	FinalBytes          *uint64 `json:"final_bytes"`
	Rejected            string  `json:"rejection"`
	Rollback            string  `json:"failed_charge_rollback"`
	Recovery            string  `json:"recovery"`
	ProtectorLimit      uint64  `json:"protector_limit_bytes"`
	PoolLimit           uint64  `json:"pool_limit_bytes"`
	QueryLimit          uint64  `json:"query_limit_bytes"`
	Admitted            int     `json:"admitted_fanouts"`
	HeldBytes           uint64  `json:"held_bytes"`
	AfterLocalClose     uint64  `json:"after_local_close_bytes"`
}

type qlbT17OverloadEvidence struct {
	Cohorts              []qlbT17OverloadCohort `json:"cohorts"`
	Contract             string                 `json:"contract"`
	Overload             string                 `json:"overload"`
	Recovery             string                 `json:"recovery"`
	Probe                qlbT17CapacityProbe    `json:"held_capacity_probe"`
	Cardinality          int                    `json:"cardinality"`
	QueryMemoryMiB       int                    `json:"query_memory_mib"`
	Workers              int                    `json:"workers"`
	ProcessID            int                    `json:"process_id"`
	FixtureBaseUnixNanos int64                  `json:"fixture_base_unix_nanos"`
	CPUQuota             int64                  `json:"cgroup_cpu_quota"`
	CPUPeriod            int64                  `json:"cgroup_cpu_period"`
	MemoryLimit          int64                  `json:"cgroup_memory_limit_bytes"`
	SwapLimit            int64                  `json:"cgroup_swap_limit_bytes"`
}

func qlbT17CleanupKeys() []string {
	return []string{
		"native_results", "native_reserved", "owner_pins", "flatten_refs", "sort", "aggregate", "native_aggregate",
		"filter", "having", "group_expression", "cache_inflight", "cache_compile", "cache_waiters", "cache_uncached",
	}
}

func validateQLBT17OverloadCleanup(values map[string]int64) error {
	if len(values) != len(qlbT17CleanupKeys()) {
		return fmt.Errorf("T17 incomplete cleanup counters")
	}
	for _, key := range qlbT17CleanupKeys() {
		if value, ok := values[key]; !ok || value != 0 {
			return fmt.Errorf("T17 missing/nonzero cleanup counter %s", key)
		}
	}
	return nil
}

func validateQLBT17OverloadEvidence(e qlbT17OverloadEvidence) error {
	if e.Contract != qlbT17OverloadRevision || e.Overload != "pass" || e.Recovery != "pass" || e.Cardinality != 1024 ||
		e.QueryMemoryMiB != 256 || e.Workers != 32 || e.ProcessID <= 0 || e.FixtureBaseUnixNanos <= 0 ||
		e.CPUPeriod <= 0 || e.CPUQuota != 4*e.CPUPeriod || e.MemoryLimit != 8<<30 || e.SwapLimit != 0 || len(e.Cohorts) != 2 {
		return fmt.Errorf("T17 invalid overload contract, verdicts or fixed limits")
	}
	if err := validateQLBT17OverloadCohorts(e); err != nil {
		return err
	}
	return validateQLBT17CapacityProbe(e.Probe)
}

func validateQLBT17OverloadCohorts(e qlbT17OverloadEvidence) error {
	for index, mode := range []string{"topk", "fullsort"} {
		c := e.Cohorts[index]
		if c.PeakInFlight <= 0 || c.PeakInFlight > 32 || c.Mode != mode || c.Attempts != 96 || len(c.Calls) != 96 || len(c.Recovery) != 3 {
			return fmt.Errorf("T17 missing overload attempts/recovery for %s", mode)
		}
		if err := validateQLBT17OverloadCleanup(c.Cleanup); err != nil {
			return err
		}
		if err := validateQLBT17OverloadCleanup(c.RecoveryCleanup); err != nil {
			return err
		}
		oracles := qlbT17Oracles(qlbT17Spec{composed: true}, e.Cardinality, time.Unix(0, e.FixtureBaseUnixNanos))
		successes, rejected := 0, 0
		for callIndex, call := range c.Calls {
			if err := validateQLBT17OverloadCall(call, callIndex, oracles, true); err != nil {
				return err
			}
			if call.Status == "OK" {
				successes++
			} else {
				rejected++
			}
		}
		if successes != c.Successful || rejected != c.Rejected {
			return fmt.Errorf("T17 overload counts do not match calls")
		}
		for callIndex, call := range c.Recovery {
			if err := validateQLBT17OverloadCall(call, callIndex, oracles, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateQLBT17CapacityProbe(p qlbT17CapacityProbe) error {
	if p.ProtectorLimit != 6<<30 || p.PoolLimit != 1536<<20 || p.QueryLimit != 192<<20 || p.Admitted <= 0 || p.Admitted >= 64 ||
		p.Rejected != "query memory pool exhausted" || p.Rollback != "pass" || p.HeldBytes == 0 || p.HeldBytes > p.PoolLimit ||
		p.AfterLocalClose != p.HeldBytes || p.AfterTransportClose == nil || *p.AfterTransportClose != 0 || p.Recovery != "pass" ||
		p.FinalBytes == nil || *p.FinalBytes != 0 {
		return fmt.Errorf("T17 missing held-capacity rejection, ownership cleanup or recovery proof")
	}
	return nil
}

func validateQLBT17OverloadCall(call qlbT17OverloadCall, index int, oracles []*qlbT17Oracle, allowRejection bool) error {
	if call.Index != index || call.Binding != index%3 {
		return fmt.Errorf("T17 call index/binding missing or duplicated")
	}
	switch call.Status {
	case "OK":
		oracle := oracles[call.Binding]
		if !call.Exact || call.Error != "" || call.Rows != len(oracle.expected.DataPoints) || call.Hash != oracle.hash {
			return fmt.Errorf("T17 successful call lacks exact result proof")
		}
	case "ResourceExhausted":
		if !allowRejection || call.Error != qlbT17BroadcastRejection || call.Exact || call.Rows != 0 || call.Hash != 0 {
			return fmt.Errorf("T17 unexpected rejection or partial result")
		}
	default:
		return fmt.Errorf("T17 unexpected call status %q", call.Status)
	}
	return nil
}

func readQLBT17Overload(path string) (qlbT17OverloadEvidence, string, error) {
	var evidence qlbT17OverloadEvidence
	body, err := os.ReadFile(path)
	if err != nil {
		return evidence, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&evidence); err != nil {
		return evidence, "", err
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return evidence, "", fmt.Errorf("T17 overload artifact contains trailing data")
	}
	var limits struct {
		Swap *int64 `json:"cgroup_swap_limit_bytes"`
	}
	if err = json.Unmarshal(body, &limits); err != nil {
		return evidence, "", err
	}
	if limits.Swap == nil {
		return evidence, "", fmt.Errorf("T17 overload artifact lacks observed swap limit")
	}
	if err = validateQLBT17OverloadEvidence(evidence); err != nil {
		return evidence, "", err
	}
	digest := sha256.Sum256(body)
	return evidence, hex.EncodeToString(digest[:]), nil
}
