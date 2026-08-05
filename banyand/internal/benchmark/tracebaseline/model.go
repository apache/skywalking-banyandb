// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package tracebaseline runs the constrained one-shard ordinary trace merge benchmark.
package tracebaseline

import (
	"time"

	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

// LedgerName values identify the three logical ledgers compared by the
// Phase 1 measurement foundation. Keeping the literals centralized prevents
// the suite, server, and readiness gate from drifting apart.
const (
	LedgerCore      = "core"
	LedgerLatency   = "latency"
	LedgerStartTime = "start_time"
)

// Mode values identify the measured data-node's workload shape. ModeSerial
// waits for merge-idle after every publication so the wall-time per write
// is observable; ModeThroughput backpressures against the logical schedule
// without waiting.
const (
	ModeSerial     = "serial"
	ModeThroughput = "throughput"
)

// ExecutionIdentity captures the harness-recorded portion of the execution
// envelope. The measured process detects filesystem and storage device only
// when the harness does not provide them. Fields remain optional for local
// debugging; the SAME TEST BOUNDARY gate enforces them in benchmark suites.
type ExecutionIdentity struct {
	ImageDigest   string `json:"imageDigest,omitempty"`
	Filesystem    string `json:"filesystem,omitempty"`
	StorageDevice string `json:"storageDevice,omitempty"`
	CloneMethod   string `json:"cloneMethod,omitempty"`
	BinarySHA256  string `json:"binarySHA256,omitempty"`
	PluginSHA256  string `json:"pluginSHA256,omitempty"`
}

// PublishRequest introduces one already-renamed fixture part at its logical publication time.
type PublishRequest struct {
	LogicalNow time.Time `json:"logicalNow"`
	PartID     uint64    `json:"partID"`
}

// StatusPoint captures publication lag and merge backlog after one write.
type StatusPoint struct {
	LogicalNow     time.Time `json:"logicalNow"`
	WallTime       time.Time `json:"wallTime"`
	BarrierNanos   int64     `json:"barrierNanos"`
	LagNanos       int64     `json:"lagNanos"`
	CoreBytes      uint64    `json:"coreBytes"`
	CoreParts      int       `json:"coreParts"`
	QueuedMerges   int       `json:"queuedMerges"`
	RunningMerges  int       `json:"runningMerges"`
	InFlightParts  int       `json:"inFlightParts"`
	OldestPartAge  int64     `json:"oldestPartAgeNanos"`
	PublishedParts int       `json:"publishedParts"`
}

// ResourceSnapshot captures process and runtime counters at a phase boundary.
type ResourceSnapshot struct {
	At            time.Time `json:"at"`
	CPUNanos      int64     `json:"cpuNanos"`
	Allocated     uint64    `json:"allocatedBytes"`
	Allocations   uint64    `json:"allocations"`
	HeapBytes     uint64    `json:"heapBytes"`
	RSSBytes      uint64    `json:"rssBytes"`
	ReadBytes     uint64    `json:"readBytes"`
	WriteBytes    uint64    `json:"writeBytes"`
	CgroupCPUUsec uint64    `json:"cgroupCPUUsec"`
	CgroupPeak    uint64    `json:"cgroupPeakBytes"`
}

// PhaseResult reports one independently measured benchmark phase.
type PhaseResult struct {
	Name          string           `json:"name"`
	StartedAt     time.Time        `json:"startedAt"`
	FinishedAt    time.Time        `json:"finishedAt"`
	Start         ResourceSnapshot `json:"start"`
	End           ResourceSnapshot `json:"end"`
	WallNanos     int64            `json:"wallNanos"`
	DrainNanos    int64            `json:"drainNanos"`
	InputBytes    uint64           `json:"inputBytes"`
	PublishedRows uint64           `json:"publishedRows"`
}

// Environment records the constrained data-node execution envelope.
//
// Runtime fields such as GoVersion, Kernel, cgroup limits, process IDs, and
// cgroup paths come from the measured process. Commit, image, clone method,
// checksums, filesystem, and storage device come from the harness. Filesystem
// and StorageDevice fall back to live mount detection when omitted. SAME TEST
// BOUNDARY enforces complete and comparable identities for managed suites.
type Environment struct {
	Filesystem       string `json:"filesystem,omitempty"`
	GoVersion        string `json:"goVersion"`
	Kernel           string `json:"kernel"`
	CgroupVersion    string `json:"cgroupVersion"`
	CPUSet           string `json:"cpuSet"`
	MemoryMax        string `json:"memoryMax"`
	MemorySwapMax    string `json:"memorySwapMax"`
	PIDsMax          string `json:"pidsMax"`
	PluginSHA256     string `json:"pluginSHA256,omitempty"`
	BinarySHA256     string `json:"binarySHA256,omitempty"`
	ControllerCgroup string `json:"controllerCgroup"`
	CloneMethod      string `json:"cloneMethod,omitempty"`
	StorageDevice    string `json:"storageDevice,omitempty"`
	DataNodeCgroup   string `json:"dataNodeCgroup"`
	ImageDigest      string `json:"imageDigest,omitempty"`
	Commit           string `json:"commit"`
	ControllerPID    int    `json:"controllerPID"`
	DataNodePID      int    `json:"dataNodePID"`
	GOMAXPROCS       int    `json:"gomaxprocs"`
	OneShardOnly     bool   `json:"oneShardOnly"`
}

// RunReport is the machine-readable result of one fresh data-node process.
type RunReport struct {
	ActualLedger              map[string]string                    `json:"actualLedgerSHA256"`
	ExpectedLedger            map[string]string                    `json:"expectedLedgerSHA256"`
	Environment               Environment                          `json:"environment"`
	Merges                    storagetrace.BenchmarkMergeReport    `json:"merges"`
	ScheduleSHA256            string                               `json:"scheduleSHA256"`
	FixtureSHA256             string                               `json:"fixtureSHA256"`
	Error                     string                               `json:"error,omitempty"`
	RunID                     string                               `json:"runID"`
	Mode                      string                               `json:"mode"`
	Status                    []StatusPoint                        `json:"status"`
	Inventory                 storagetrace.BenchmarkMergeInventory `json:"inventory"`
	Cooldown                  PhaseResult                          `json:"cooldown"`
	Primary                   PhaseResult                          `json:"primary"`
	Acceleration              float64                              `json:"acceleration"`
	Published                 int                                  `json:"published"`
	ExpectedRows              uint64                               `json:"expectedRows"`
	HotMerges                 int                                  `json:"hotMerges"`
	MatureMerges              int                                  `json:"matureMerges"`
	SamplingCalls             uint64                               `json:"samplingCalls"`
	LogicalWriteAmplification float64                              `json:"logicalWriteAmplification,omitempty"`
	Version                   uint32                               `json:"version"`
	LedgerVerified            bool                                 `json:"ledgerVerified"`
	Correct                   bool                                 `json:"correct"`
}

// SweepPoint records one acceleration calibration result.
type SweepPoint struct {
	Acceleration float64 `json:"acceleration"`
	WallNanos    int64   `json:"wallNanos"`
	DrainNanos   int64   `json:"drainNanos"`
	P95LagNanos  int64   `json:"p95LagNanos"`
	Sustainable  bool    `json:"sustainable"`
}

// SuiteReport combines calibration and comparable baseline repetitions.
type SuiteReport struct {
	GeneratedAt                time.Time                  `json:"generatedAt"`
	Readiness                  *BaselineReadiness         `json:"readiness,omitempty"`
	Commit                     string                     `json:"commit"`
	BinarySHA256               string                     `json:"binarySHA256"`
	SourcePatchSHA256          string                     `json:"sourcePatchSHA256,omitempty"`
	FixtureSHA256              string                     `json:"fixtureSHA256"`
	ScheduleSHA256             string                     `json:"scheduleSHA256"`
	ThroughputRuns             []RunReport                `json:"throughputRuns"`
	Sweep                      []SweepPoint               `json:"sweep"`
	SerialRuns                 []RunReport                `json:"serialRuns"`
	DisabledEnabledAlternating []ControlledMergeRunReport `json:"disabledEnabledAlternating,omitempty"`
	MaximumRate                float64                    `json:"maximumSustainableAcceleration"`
	FrozenRate                 float64                    `json:"frozenAcceleration"`
	WriteIntensity             int                        `json:"writeIntensity"`
	FixtureInputBytes          uint64                     `json:"fixtureInputBytes"`
	LogicalWriteAmplification  float64                    `json:"logicalWriteAmplification"`
	OneShardOnly               bool                       `json:"oneShardOnly"`
	PreRollDiscovered          bool                       `json:"preRollDiscovered"`
	LedgerVerified             bool                       `json:"ledgerVerified"`
}
