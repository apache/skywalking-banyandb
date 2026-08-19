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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/trace"
)

func TestEvaluateBaselineReadinessUsesFrozenControlledMatureMerge(t *testing.T) {
	suite := readyTestSuite()
	readiness := EvaluateBaselineReadiness(suite)
	require.True(t, readiness.Ready)
	require.Len(t, readiness.Gates, 6)
	require.True(t, gateByName(t, readiness, "CONTROLLED MATURE MERGE").Passed)

	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].MatureMerges = 0
	}
	readiness = EvaluateBaselineReadiness(suite)
	require.True(t, readiness.Ready, "serialized maturity count is diagnostic after the frozen mature seed replaces it")

	suite.DisabledEnabledAlternating[2].Event.HotInputParts = 1
	suite.DisabledEnabledAlternating[2].Event.MatureInputParts = 1
	readiness = EvaluateBaselineReadiness(suite)
	require.False(t, readiness.Ready)
	require.False(t, gateByName(t, readiness, "CONTROLLED MATURE MERGE").Passed)
}

func TestEvaluateBaselineReadinessRequiresLogicalWriteAmplification(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)

	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].LogicalWriteAmplification = 0
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)

	suite = readyTestSuite()
	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].LogicalWriteAmplification = minimumLogicalWAFloor - 0.01
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)

	suite = readyTestSuite()
	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].LogicalWriteAmplification = minimumLogicalWACeiling + 0.01
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)
}

func TestEvaluateBaselineReadinessLogicalWAGateUsesMedian(t *testing.T) {
	suite := readyTestSuite()
	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].LogicalWriteAmplification = minimumLogicalWAFloor + 0.01
	}
	suite.LogicalWriteAmplification = minimumLogicalWAFloor + 0.01
	require.True(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)

	suite = readyTestSuite()
	for runIdx := range suite.SerialRuns {
		suite.SerialRuns[runIdx].LogicalWriteAmplification = minimumLogicalWACeiling + 0.5
	}
	suite.LogicalWriteAmplification = minimumLogicalWACeiling + 0.5
	require.False(t, gateByName(t, readinessFixture(t, suite), "LOGICAL WRITE AMPLIFICATION").Passed)
}

func TestEvaluateBaselineReadinessRequiresAlternatingDisabledEnabledRuns(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)

	short := suite.DisabledEnabledAlternating[:len(suite.DisabledEnabledAlternating)-1]
	suite.DisabledEnabledAlternating = short
	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)

	suite = readyTestSuite()
	suite.DisabledEnabledAlternating[4] = ControlledMergeRunReport{
		Correct: true, PipelineMode: ControlledMergePipelineRetainAll,
		Event: trace.BenchmarkMergeEvent{
			Resources: trace.BenchmarkMergeResources{ElapsedNanos: int64(time.Second)},
		},
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)

	suite.DisabledEnabledAlternating[4] = ControlledMergeRunReport{
		Correct: false, PipelineMode: ControlledMergePipelineDisabled,
		Event: trace.BenchmarkMergeEvent{
			Resources: trace.BenchmarkMergeResources{ElapsedNanos: int64(time.Second)},
		},
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresFiveRunsPerPipelineMode(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating = suite.DisabledEnabledAlternating[:9]

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresStableControlledCPU(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating[1].Event.Resources.CPUNanos = int64(4 * time.Second)

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRejectsMissingControlledTimings(t *testing.T) {
	suite := readyTestSuite()
	for runIdx := range suite.DisabledEnabledAlternating {
		if suite.DisabledEnabledAlternating[runIdx].PipelineMode == ControlledMergePipelineRetainAll {
			suite.DisabledEnabledAlternating[runIdx].Event.Resources.ElapsedNanos = 0
			suite.DisabledEnabledAlternating[runIdx].Event.Resources.CPUNanos = 0
		}
	}

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresCVWithinBound(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)

	suite = readyTestSuite()
	for runIdx := range suite.DisabledEnabledAlternating {
		var duration int64
		if runIdx%2 == 0 {
			duration = int64(float64(time.Second) * (1 + float64(runIdx)*maximumAlternatingWallNanosCV))
		} else {
			duration = int64(float64(time.Second) * (1 + 0.5*float64(runIdx)))
		}
		suite.DisabledEnabledAlternating[runIdx].Event.Resources.ElapsedNanos = duration
	}
	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresRecordedCPUSet(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite.SerialRuns[0].Environment.CPUSet = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)
}

func TestEvaluateBaselineReadinessRequiresHarnessIdentityFields(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite.SerialRuns[0].Environment.ImageDigest = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.CloneMethod = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.BinarySHA256 = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.Filesystem = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.StorageDevice = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)
}

func TestEvaluateBaselineReadinessRequiresRuntimeFields(t *testing.T) {
	suite := readyTestSuite()
	require.True(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite.SerialRuns[0].Environment.GoVersion = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.Kernel = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)

	suite = readyTestSuite()
	suite.SerialRuns[0].Environment.Commit = ""
	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)
}

func TestEvaluateBaselineReadinessRequiresComparableEnvironment(t *testing.T) {
	suite := readyTestSuite()
	suite.SerialRuns[1].Environment.ImageDigest = "sha256:different"

	require.False(t, gateByName(t, readinessFixture(t, suite), "SAME TEST BOUNDARY").Passed)
}

func TestEvaluateBaselineReadinessRequiresFrozenControlledSelection(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating[3].SelectionSHA256 = "different-selection"

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresControlledPluginIdentity(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating[1].PluginSHA256 = ""

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresControlledAttribution(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating[0].Event.Resources.AttributionValid = false

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessRequiresContainerDerivedStagingLimit(t *testing.T) {
	suite := readyTestSuite()
	suite.DisabledEnabledAlternating[0].StagingLimits.MemoryLimit = 8 << 30

	require.False(t, gateByName(t, readinessFixture(t, suite), "DISABLED/ENABLED SERIES STABILITY").Passed)
}

func TestEvaluateBaselineReadinessDoesNotRequirePreRoll(t *testing.T) {
	suite := readyTestSuite()
	suite.PreRollDiscovered = false
	readiness := EvaluateBaselineReadiness(suite)
	require.True(t, readiness.Ready)
	require.True(t, gateByName(t, readiness, "SAME TEST BOUNDARY").Passed)
}

func TestDetectFilesystemAndDevicePicksLongestMount(t *testing.T) {
	mountInfo := []byte("24 23 0:2 /run /run rw,relatime - ext4 /dev/sda1 rw\n" +
		"25 24 0:3 /run/data /run/data rw,relatime - btrfs /dev/sdb2 rw\n" +
		"26 25 0:4 /run/data/shard /run/data/shard rw,relatime - xfs /dev/sdc3 rw\n")
	probePath := "/run/data/shard/part"
	originalReader := mountInfoReader
	mountInfoReader = func() ([]byte, error) { return mountInfo, nil }
	t.Cleanup(func() { mountInfoReader = originalReader })

	fstype, device := detectFilesystemAndDeviceWithReader(probePath)
	require.Equal(t, "xfs", fstype)
	require.Equal(t, "/dev/sdc3", device)
}

func TestDetectFilesystemAndDeviceReturnsEmptyWhenNoMatch(t *testing.T) {
	mountInfo := []byte("24 23 0:2 /run /run rw,relatime - ext4 /dev/sda1 rw\n" +
		"25 24 0:3 /run/data /run/data rw,relatime - btrfs /dev/sdb2 rw\n")
	probePath := "/var/log/agent"
	originalReader := mountInfoReader
	mountInfoReader = func() ([]byte, error) { return mountInfo, nil }
	t.Cleanup(func() { mountInfoReader = originalReader })

	fstype, device := detectFilesystemAndDeviceWithReader(probePath)
	require.Equal(t, "", fstype)
	require.Equal(t, "", device)
}

func TestDetectFilesystemAndDeviceRequiresPathComponentBoundary(t *testing.T) {
	mountInfo := []byte("24 23 0:2 / / rw,relatime - overlay overlay rw\n" +
		"25 24 0:3 /run/data /run/data rw,relatime - btrfs /dev/sdb2 rw\n")
	probePath := "/run/database/part"
	originalReader := mountInfoReader
	mountInfoReader = func() ([]byte, error) { return mountInfo, nil }
	t.Cleanup(func() { mountInfoReader = originalReader })

	fstype, device := detectFilesystemAndDeviceWithReader(probePath)
	require.Equal(t, "overlay", fstype)
	require.Equal(t, "overlay", device)
}

func readyTestSuite() SuiteReport {
	const fixtureHash = "fixture"
	const scheduleHash = "schedule"
	runs := make([]RunReport, minimumSerialRepetitions)
	for runIdx := range runs {
		run := RunReport{
			RunID: fmt.Sprintf("run-%d", runIdx), Mode: ModeSerial, FixtureSHA256: fixtureHash, ScheduleSHA256: scheduleHash,
			ExpectedRows: 100, LedgerVerified: true, Correct: true,
			LogicalWriteAmplification: 1.0009,
			Primary:                   PhaseResult{InputBytes: 100, DrainNanos: int64(100 * time.Millisecond)},
			Environment: Environment{
				Commit: "abc123", GoVersion: "go1.25.13", Kernel: "5.15.0",
				GOMAXPROCS: 2, MemoryMax: "4294967296", MemorySwapMax: "0", PIDsMax: "512", OneShardOnly: true,
				DataNodeCgroup: "/data", ControllerCgroup: "/controller", CPUSet: "0-1",
				Filesystem: "ext4", StorageDevice: "/dev/sda1", ImageDigest: "sha256:image",
				CloneMethod: "os.CopyFS", BinarySHA256: "sha256:bin",
			},
			Status: []StatusPoint{{}},
		}
		run.Inventory.CoreRows = 100
		run.Inventory.IndexRows = map[string]uint64{LedgerLatency: 100, LedgerStartTime: 100}
		runs[runIdx] = run
	}
	alternating := make([]ControlledMergeRunReport, 0, 10)
	controlledEnvironment := runs[0].Environment
	controlledEnvironment.ControllerCgroup = ""
	controlledEnvironment.ControllerPID = 0
	for runIdx := 0; runIdx < 10; runIdx++ {
		mode := ControlledMergePipelineDisabled
		pluginSHA256 := ""
		runEnvironment := controlledEnvironment
		if runIdx%2 == 1 {
			mode = ControlledMergePipelineRetainAll
			pluginSHA256 = "sha256:plugin"
			runEnvironment.PluginSHA256 = pluginSHA256
		}
		alternating = append(alternating, ControlledMergeRunReport{
			Correct: true, PipelineMode: mode, PluginSHA256: pluginSHA256,
			SeedSnapshotSHA256: "seed", SelectionSHA256: "selection", MatureLogicalNow: time.Unix(100, 0), Environment: runEnvironment,
			StagingLimits: trace.BenchmarkMergeStagingLimits{
				MemoryLimit: 4 << 30, StageBytes: 64 << 20, TraceBytes: 64 << 20, MaxTraceCount: 64 * 1024,
			},
			Event: trace.BenchmarkMergeEvent{
				SelectionSHA256: "selection", InputPartIDs: []uint64{1, 2}, MatureInputParts: 2,
				Children: []trace.BenchmarkMergeChild{{}, {}},
				Resources: trace.BenchmarkMergeResources{
					AttributionValid: true, ElapsedNanos: int64(time.Second), CPUNanos: int64(500 * time.Millisecond),
				},
			},
		})
	}
	return SuiteReport{
		Commit: "abc123", BinarySHA256: "sha256:bin", FixtureSHA256: fixtureHash, ScheduleSHA256: scheduleHash,
		SerialRuns: runs, OneShardOnly: true,
		LogicalWriteAmplification: 1.0009, DisabledEnabledAlternating: alternating,
	}
}

func readinessFixture(t *testing.T, suite SuiteReport) BaselineReadiness {
	t.Helper()
	return EvaluateBaselineReadiness(suite)
}

func gateByName(t *testing.T, readiness BaselineReadiness, name string) BaselineGate {
	t.Helper()
	for gateIdx := range readiness.Gates {
		if readiness.Gates[gateIdx].Name == name {
			return readiness.Gates[gateIdx]
		}
	}
	t.Fatalf("gate %q not found", name)
	return BaselineGate{}
}
