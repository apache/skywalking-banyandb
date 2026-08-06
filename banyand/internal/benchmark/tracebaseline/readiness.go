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

package tracebaseline

import (
	"math"
	"strconv"
)

const (
	minimumMatureMergeRounds = 10
	minimumSerialRepetitions = 5
	minimumAlternatingRuns   = 10
	minimumRunsPerVariant    = 5
	// minimumLogicalWAFloor and minimumLogicalWACeiling bound the per-run
	// median logical WA. The reference suite measures 1.0009× (see design
	// doc); the envelope is intentionally wide so legitimate variation does
	// not trip the gate while pathological values (missing output, runaway
	// amplification) are still rejected.
	minimumLogicalWAFloor   = 0.5
	minimumLogicalWACeiling = 2.0
	// maximumAlternatingWallNanosCV bounds the per-mode coefficient of
	// variation of the merge event duration. The Phase 1 design reference is
	// 9.87% for the disabled mode; 10% allows for legitimate variance while
	// still rejecting dispersion that would mask any real speedup signal.
	maximumAlternatingWallNanosCV = 0.10
)

// BaselineGate is one report readiness decision.
type BaselineGate struct {
	Name   string `json:"name"`
	Detail string `json:"detail"`
	Passed bool   `json:"passed"`
}

// BaselineReadiness is the canonical six-gate evaluation shared by validation and rendering.
type BaselineReadiness struct {
	Gates []BaselineGate `json:"gates"`
	Ready bool           `json:"ready"`
}

// EvaluateBaselineReadiness applies the frozen baseline gates to a suite.
func EvaluateBaselineReadiness(suite SuiteReport) BaselineReadiness {
	medianWA := serialRunMedianLogicalWriteAmplification(suite.SerialRuns)
	sameBoundary, correctOutput, referenceEnvironment := evaluateSerialRuns(suite)
	matureRounds, sustainable := evaluateSerialMergeLifecycle(suite.SerialRuns)
	logicalWAGate := evaluateLogicalWriteAmplification(suite, medianWA)
	stability := controlledSeriesStability(suite.DisabledEnabledAlternating)
	alternating := evaluateControlledSeries(suite.DisabledEnabledAlternating, referenceEnvironment, stability)
	readiness := BaselineReadiness{Gates: []BaselineGate{
		{
			Name: "SAME TEST BOUNDARY", Passed: sameBoundary,
			Detail: "Commit, GoVersion, Kernel, GOMAXPROCS=4, MemoryMax, PIDsMax, OneShardOnly, " +
				"distinct data/controller cgroups, CPUSet, Filesystem, StorageDevice, ImageDigest, " +
				"CloneMethod, BinarySHA256, fixture and schedule SHA, and serial mode agree across five serial runs",
		},
		{
			Name:   "CORRECT OUTPUT",
			Passed: correctOutput,
			Detail: "Core and secondary-index ledgers reconcile with expected sampling behavior",
		},
		{
			Name:   "MATURE MERGE ROUNDS",
			Passed: matureRounds,
			Detail: "At least ten mature merge rounds complete in every serialized run",
		},
		{
			Name:   "SUSTAINABLE EXECUTION",
			Passed: sustainable,
			Detail: "Every publication reaches epoch-aware merge-idle without unfinished work",
		},
		{
			Name: "LOGICAL WRITE AMPLIFICATION", Passed: logicalWAGate,
			Detail: "Per-run median logical WA across five serial runs lies in [0.5, 2.0] and is finite; " +
				"core + SIDX output bytes measured per run, /proc/self/io remains diagnostic",
		},
		{
			Name: "DISABLED/ENABLED SERIES STABILITY", Passed: alternating,
			Detail: "Five disabled and five retain-all runs alternate on one attributed frozen selection, " +
				"pass per-run correctness, and stay within 10% wall/CPU CV per pipeline mode " +
				"(disabled wall=" + formatPercent(stability.disabledWallCV) + ", CPU=" + formatPercent(stability.disabledCPUCV) +
				", retain-all wall=" + formatPercent(stability.retainAllWallCV) + ", CPU=" + formatPercent(stability.retainAllCPUCV) + ")",
		},
	}}
	readiness.Ready = true
	for gateIdx := range readiness.Gates {
		if !readiness.Gates[gateIdx].Passed {
			readiness.Ready = false
		}
	}
	return readiness
}

func evaluateSerialRuns(suite SuiteReport) (sameBoundary, correctOutput bool, referenceEnvironment Environment) {
	sameBoundary = suite.OneShardOnly && len(suite.SerialRuns) >= minimumSerialRepetitions &&
		suite.FixtureSHA256 != "" && suite.ScheduleSHA256 != ""
	correctOutput = len(suite.SerialRuns) >= minimumSerialRepetitions
	for runIdx := range suite.SerialRuns {
		run := &suite.SerialRuns[runIdx]
		if runIdx == 0 {
			referenceEnvironment = run.Environment
		}
		sameBoundary = sameBoundary && run.FixtureSHA256 == suite.FixtureSHA256 && run.ScheduleSHA256 == suite.ScheduleSHA256 &&
			run.Mode == ModeSerial &&
			serialEnvironmentComplete(run.Environment) && sameExecutionEnvironment(referenceEnvironment, run.Environment) &&
			run.Environment.Commit == suite.Commit && run.Environment.BinarySHA256 == suite.BinarySHA256
		correctOutput = correctOutput && run.Correct && run.LedgerVerified && run.SamplingCalls == 0 &&
			run.Inventory.CoreRows == run.ExpectedRows && run.Inventory.IndexRows[LedgerLatency] == run.ExpectedRows &&
			run.Inventory.IndexRows[LedgerStartTime] == run.ExpectedRows
	}
	return sameBoundary, correctOutput, referenceEnvironment
}

func evaluateSerialMergeLifecycle(runs []RunReport) (matureRounds, sustainable bool) {
	matureRounds = len(runs) >= minimumSerialRepetitions
	sustainable = len(runs) >= minimumSerialRepetitions
	for runIdx := range runs {
		run := &runs[runIdx]
		matureRounds = matureRounds && run.MatureMerges >= minimumMatureMergeRounds
		sustainable = sustainable && runIsSerialSustainable(run)
	}
	return matureRounds, sustainable
}

func evaluateLogicalWriteAmplification(suite SuiteReport, medianWA float64) bool {
	return serialLogicalWriteAmplificationValid(suite.SerialRuns) && medianWA >= minimumLogicalWAFloor &&
		medianWA <= minimumLogicalWACeiling && !math.IsInf(medianWA, 0) && !math.IsNaN(medianWA) &&
		math.Abs(suite.LogicalWriteAmplification-medianWA) <= 0.000001
}

func evaluateControlledSeries(runs []ControlledMergeRunReport, referenceEnvironment Environment, stability controlledStability) bool {
	return len(runs) >= minimumAlternatingRuns && alternatingOrderPreserved(runs) && allAlternatingRunsCorrect(runs) &&
		controlledRunsComparable(runs, referenceEnvironment) && stability.disabledSamples >= minimumRunsPerVariant &&
		stability.retainAllSamples >= minimumRunsPerVariant && stability.disabledWallCV <= maximumAlternatingWallNanosCV &&
		stability.retainAllWallCV <= maximumAlternatingWallNanosCV && stability.disabledCPUCV <= maximumAlternatingWallNanosCV &&
		stability.retainAllCPUCV <= maximumAlternatingWallNanosCV
}

func serialEnvironmentComplete(environment Environment) bool {
	return environment.GOMAXPROCS == 4 && environment.MemoryMax == "8589934592" && environment.MemorySwapMax == "0" &&
		environment.PIDsMax == "512" && environment.OneShardOnly && environment.DataNodeCgroup != "" &&
		environment.ControllerCgroup != "" && environment.DataNodeCgroup != environment.ControllerCgroup &&
		environment.CPUSet != "" && environment.GoVersion != "" && environment.Kernel != "" && environment.Commit != "" &&
		environment.Filesystem != "" && environment.StorageDevice != "" && environment.ImageDigest != "" &&
		environment.CloneMethod != "" && environment.BinarySHA256 != ""
}

func controlledEnvironmentComplete(environment Environment) bool {
	return environment.GOMAXPROCS == 4 && environment.MemoryMax == "8589934592" && environment.MemorySwapMax == "0" &&
		environment.PIDsMax == "512" && environment.OneShardOnly && environment.DataNodeCgroup != "" && environment.CPUSet != "" &&
		environment.GoVersion != "" && environment.Kernel != "" && environment.Commit != "" && environment.Filesystem != "" &&
		environment.StorageDevice != "" && environment.ImageDigest != "" && environment.CloneMethod != "" && environment.BinarySHA256 != ""
}

func sameExecutionEnvironment(left, right Environment) bool {
	return left.Commit == right.Commit && left.GoVersion == right.GoVersion && left.Kernel == right.Kernel &&
		left.CgroupVersion == right.CgroupVersion && left.CPUSet == right.CPUSet && left.MemoryMax == right.MemoryMax &&
		left.MemorySwapMax == right.MemorySwapMax && left.PIDsMax == right.PIDsMax && left.GOMAXPROCS == right.GOMAXPROCS &&
		left.OneShardOnly == right.OneShardOnly && left.ImageDigest == right.ImageDigest && left.Filesystem == right.Filesystem &&
		left.StorageDevice == right.StorageDevice && left.CloneMethod == right.CloneMethod && left.BinarySHA256 == right.BinarySHA256
}

func serialLogicalWriteAmplificationValid(runs []RunReport) bool {
	if len(runs) < minimumSerialRepetitions {
		return false
	}
	for runIdx := range runs {
		value := runs[runIdx].LogicalWriteAmplification
		if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

// serialRunMedianLogicalWriteAmplification computes the median per-run
// LogicalWriteAmplification across the serial runs. Returns 0 when no runs are
// present. The server computes LogicalWriteAmplification per run from the
// selected input and output bytes of that run's core and secondary-index merge
// events, so the median is the correct suite aggregation.
func serialRunMedianLogicalWriteAmplification(runs []RunReport) float64 {
	if len(runs) == 0 {
		return 0
	}
	values := make([]float64, 0, len(runs))
	for runIdx := range runs {
		run := &runs[runIdx]
		if math.IsNaN(run.LogicalWriteAmplification) || math.IsInf(run.LogicalWriteAmplification, 0) {
			continue
		}
		if run.LogicalWriteAmplification <= 0 {
			continue
		}
		values = append(values, run.LogicalWriteAmplification)
	}
	if len(values) == 0 {
		return 0
	}
	return medianFloat64(values)
}

func medianFloat64(values []float64) float64 {
	sortedValues := make([]float64, len(values))
	copy(sortedValues, values)
	for i := 1; i < len(sortedValues); i++ {
		for j := i; j > 0 && sortedValues[j-1] > sortedValues[j]; j-- {
			sortedValues[j-1], sortedValues[j] = sortedValues[j], sortedValues[j-1]
		}
	}
	return sortedValues[len(sortedValues)/2]
}

type controlledStability struct {
	disabledWallCV   float64
	disabledCPUCV    float64
	retainAllWallCV  float64
	retainAllCPUCV   float64
	disabledSamples  int
	retainAllSamples int
}

func controlledSeriesStability(runs []ControlledMergeRunReport) controlledStability {
	var disabledWall, disabledCPU, retainAllWall, retainAllCPU []float64
	for runIdx := range runs {
		run := &runs[runIdx]
		wallNanos := float64(run.Event.Resources.ElapsedNanos)
		cpuNanos := float64(run.Event.Resources.CPUNanos)
		switch run.PipelineMode {
		case ControlledMergePipelineDisabled:
			if wallNanos > 0 && cpuNanos > 0 {
				disabledWall = append(disabledWall, wallNanos)
				disabledCPU = append(disabledCPU, cpuNanos)
			}
		case ControlledMergePipelineRetainAll:
			if wallNanos > 0 && cpuNanos > 0 {
				retainAllWall = append(retainAllWall, wallNanos)
				retainAllCPU = append(retainAllCPU, cpuNanos)
			}
		}
	}
	return controlledStability{
		disabledWallCV: coefficientOfVariation(disabledWall), disabledCPUCV: coefficientOfVariation(disabledCPU),
		retainAllWallCV: coefficientOfVariation(retainAllWall), retainAllCPUCV: coefficientOfVariation(retainAllCPU),
		disabledSamples: len(disabledWall), retainAllSamples: len(retainAllWall),
	}
}

func coefficientOfVariation(samples []float64) float64 {
	if len(samples) < 2 {
		return math.Inf(1)
	}
	var sum, sumSquares float64
	for _, sample := range samples {
		sum += sample
		sumSquares += sample * sample
	}
	mean := sum / float64(len(samples))
	if mean <= 0 {
		return math.Inf(1)
	}
	variance := sumSquares/float64(len(samples)) - mean*mean
	if variance <= 0 {
		return 0
	}
	standardDeviation := math.Sqrt(variance)
	return standardDeviation / mean
}

func controlledRunsComparable(runs []ControlledMergeRunReport, serialEnvironment Environment) bool {
	if len(runs) < minimumAlternatingRuns {
		return false
	}
	referenceSeed := runs[0].SeedSnapshotSHA256
	referenceSelection := runs[0].SelectionSHA256
	referenceLogicalNow := runs[0].MatureLogicalNow
	referenceStagingLimits := runs[0].StagingLimits
	var retainAllPluginSHA256 string
	if referenceSeed == "" || referenceSelection == "" || referenceLogicalNow.IsZero() ||
		!controlledStagingLimitsValid(&runs[0]) {
		return false
	}
	for runIdx := range runs {
		run := &runs[runIdx]
		if run.SeedSnapshotSHA256 != referenceSeed || run.SelectionSHA256 != referenceSelection || run.MatureLogicalNow != referenceLogicalNow ||
			run.Event.SelectionSHA256 != referenceSelection || !run.Event.Resources.AttributionValid || run.Event.Resources.Overlapped ||
			run.Event.Resources.CrossedPhase ||
			!controlledEnvironmentComplete(run.Environment) || !sameExecutionEnvironment(serialEnvironment, run.Environment) ||
			run.StagingLimits != referenceStagingLimits || !controlledStagingLimitsValid(run) {
			return false
		}
		switch run.PipelineMode {
		case ControlledMergePipelineDisabled:
			if run.PluginSHA256 != "" || run.Environment.PluginSHA256 != "" {
				return false
			}
		case ControlledMergePipelineRetainAll:
			if run.PluginSHA256 == "" || run.Environment.PluginSHA256 != run.PluginSHA256 {
				return false
			}
			if retainAllPluginSHA256 == "" {
				retainAllPluginSHA256 = run.PluginSHA256
			} else if run.PluginSHA256 != retainAllPluginSHA256 {
				return false
			}
		default:
			return false
		}
	}
	return retainAllPluginSHA256 != ""
}

func controlledStagingLimitsValid(run *ControlledMergeRunReport) bool {
	memoryMax, parseErr := strconv.ParseUint(run.Environment.MemoryMax, 10, 64)
	if parseErr != nil || memoryMax == 0 {
		return false
	}
	limits := run.StagingLimits
	return limits.MemoryLimit == memoryMax && limits.StageBytes > 0 && limits.StageBytes <= memoryMax &&
		limits.TraceBytes > 0 && limits.TraceBytes <= memoryMax && limits.MaxTraceCount > 0
}

func formatPercent(ratio float64) string {
	return formatRatio(ratio*100.0) + "%"
}

func formatRatio(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "n/a"
	}
	return strconv.FormatFloat(value, 'f', 2, 64)
}

func runIsSerialSustainable(run *RunReport) bool {
	if run.Mode != ModeSerial || len(run.Status) == 0 || run.Error != "" {
		return false
	}
	for statusIdx := range run.Status {
		status := &run.Status[statusIdx]
		if status.QueuedMerges != 0 || status.RunningMerges != 0 || status.InFlightParts != 0 {
			return false
		}
	}
	return true
}

// alternatingOrderPreserved reports whether the captured controlled runs follow
// the canonical [disabled, enabled, disabled, enabled, ...] order. The
// alternating shape is the Phase 1 evidence that disabled and enabled variants
// were measured back-to-back, which is the only way to attribute differences
// to the pipeline rather than to environmental drift.
func alternatingOrderPreserved(runs []ControlledMergeRunReport) bool {
	if len(runs) < minimumAlternatingRuns {
		return false
	}
	for runIdx, run := range runs {
		expected := ControlledMergePipelineDisabled
		if runIdx%2 == 1 {
			expected = ControlledMergePipelineRetainAll
		}
		if run.PipelineMode != expected {
			return false
		}
	}
	return true
}

// allAlternatingRunsCorrect reports whether every captured controlled run
// passed its per-run correctness gate. The Phase 1 stability gate requires
// that no run regressed during the alternating sequence.
func allAlternatingRunsCorrect(runs []ControlledMergeRunReport) bool {
	for runIdx := range runs {
		if !runs[runIdx].Correct {
			return false
		}
	}
	return true
}
