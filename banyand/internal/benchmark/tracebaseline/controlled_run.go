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
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracefixture"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// ControlledMergePipelineMode identifies the pipeline configuration used by a controlled merge.
type ControlledMergePipelineMode string

const (
	// ControlledMergePipelineDisabled measures the production merge without pipeline framework work.
	ControlledMergePipelineDisabled ControlledMergePipelineMode = "disabled"
	// ControlledMergePipelineRetainAll measures the pipeline framework with a metadata-only sampler that retains every trace.
	ControlledMergePipelineRetainAll ControlledMergePipelineMode = "retain-all"
	// ControlledMergePipelineDeterministicDrop measures deterministic trace deletion and secondary-index pruning.
	ControlledMergePipelineDeterministicDrop ControlledMergePipelineMode = "deterministic-drop"
)

// ControlledMergeRunOptions configures one controlled merge process. The ExecutionIdentity field
// captures the same harness-recorded fields as the primary run; the readiness
// gate enforces the harness requirement (ImageDigest, CloneMethod,
// BinarySHA256, plus PluginSHA256 for retain-all runs) at the suite level.
type ControlledMergeRunOptions struct {
	SeedManifestPath  string
	DataRoot          string
	OutputPath        string
	RunID             string
	Mode              string
	Commit            string
	PluginPath        string
	ProfileDir        string
	ExecutionIdentity ExecutionIdentity
	SamplingOracle    *SamplingOracleArtifact
	PluginConfig      []byte
}

// ControlledMergeRunReport records one exact production-picker merge.
type ControlledMergeRunReport struct {
	MatureLogicalNow   time.Time                                `json:"matureLogicalNow"`
	AfterLedger        map[string]string                        `json:"afterLedgerSHA256"`
	BeforeLedger       map[string]string                        `json:"beforeLedgerSHA256"`
	PipelineMode       ControlledMergePipelineMode              `json:"pipelineMode"`
	SeedSnapshotSHA256 string                                   `json:"seedSnapshotSHA256"`
	SelectionSHA256    string                                   `json:"selectionSHA256"`
	RunID              string                                   `json:"runID"`
	PluginSHA256       string                                   `json:"pluginSHA256,omitempty"`
	PluginConfigSHA256 string                                   `json:"pluginConfigSHA256,omitempty"`
	SamplingOracle     *SamplingOracleArtifact                  `json:"samplingOracle,omitempty"`
	Inventory          storagetrace.BenchmarkMergeInventory     `json:"inventory"`
	Environment        Environment                              `json:"environment"`
	Event              storagetrace.BenchmarkMergeEvent         `json:"event"`
	StagingLimits      storagetrace.BenchmarkMergeStagingLimits `json:"stagingLimits"`
	Version            uint32                                   `json:"version"`
	RecursiveEligible  bool                                     `json:"recursiveEligible"`
	Correct            bool                                     `json:"correct"`
}

// RunControlledMerge validates a seed clone and executes exactly one mature production-picker merge.
func RunControlledMerge(ctx context.Context, options ControlledMergeRunOptions) (report ControlledMergeRunReport, runErr error) {
	pipelineMode, validateErr := validateControlledMergeOptions(options)
	if validateErr != nil {
		return ControlledMergeRunReport{}, validateErr
	}
	manifest, manifestErr := ReadControlledMergeSeedManifest(options.SeedManifestPath)
	if manifestErr != nil {
		return ControlledMergeRunReport{}, manifestErr
	}
	sampler, pluginSHA256, segmentTimeRange, samplerErr := loadControlledMergePlugin(pipelineMode, options, manifest)
	if samplerErr != nil {
		return ControlledMergeRunReport{}, samplerErr
	}
	if sampler != nil {
		defer func() {
			if closeErr := sampler.Close(); closeErr != nil {
				runErr = errors.Join(runErr, fmt.Errorf("cannot close controlled merge plugin: %w", closeErr))
			}
		}()
	}
	receiver, receiverErr := openControlledMergeReceiver( //nolint:contextcheck // The storage constructor has no context parameter.
		options.DataRoot, manifest, sampler, segmentTimeRange,
	)
	if receiverErr != nil {
		return ControlledMergeRunReport{}, receiverErr
	}
	defer func() {
		if closeErr := receiver.Close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("cannot close controlled merge receiver: %w", closeErr))
		}
	}()
	stagingLimits, stagingLimitsErr := receiver.MergeStagingLimits()
	if stagingLimitsErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot read controlled merge staging limits: %w", stagingLimitsErr)
	}
	selection, selectionErr := receiver.PreviewMergeSelection()
	if selectionErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot preview controlled merge seed: %w", selectionErr)
	}
	beforeLedger, ledgerErr := tracefixture.LogicalLedgerChecksums(ctx, receiver)
	if ledgerErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot checksum controlled merge input: %w", ledgerErr)
	}
	partDepths, depthErr := receiver.MergePartDepths()
	if depthErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot read controlled merge depths: %w", depthErr)
	}
	if validateErr := ValidateControlledMergeSeedManifest(options.DataRoot, manifest, selection, beforeLedger, partDepths); validateErr != nil {
		return ControlledMergeRunReport{}, validateErr
	}
	profileDir, profileMkdirErr := prepareControlledMergeProfileDir(options.ProfileDir)
	if profileMkdirErr != nil {
		return ControlledMergeRunReport{}, profileMkdirErr
	}
	if captureErr := captureControlledMergePreDispatchProfile(profileDir); captureErr != nil {
		return ControlledMergeRunReport{}, captureErr
	}
	profiler, profileStartErr := startControlledMergeProfiler(profileDir)
	if profileStartErr != nil {
		return ControlledMergeRunReport{}, profileStartErr
	}
	mergeEvent, mergeErr := receiver.RunOneMerge(ctx, storagetrace.BenchmarkOneMergeOptions{
		LogicalNow: manifest.MatureLogicalNow, ExpectedSelectionSHA256: manifest.Selection.SHA256, RequireAllMature: true,
	})
	profileStopErr := profiler.stop()
	var postIntroductionProfileErr error
	if profileDir != "" {
		postIntroductionProfileErr = writeRuntimeProfiles(profileDir, "post-introduction")
	}
	if mergeErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot run controlled mature merge: %w",
			errors.Join(mergeErr, profileStopErr, postIntroductionProfileErr))
	}
	if profileErr := errors.Join(profileStopErr, postIntroductionProfileErr); profileErr != nil {
		return ControlledMergeRunReport{}, fmt.Errorf("cannot capture controlled merge profiles: %w", profileErr)
	}
	afterLedger, inventory, recursiveEligible, assemblyErr := assembleControlledMergeOutput(ctx, receiver)
	if assemblyErr != nil {
		return ControlledMergeRunReport{}, assemblyErr
	}
	report, buildErr := buildControlledMergeReport(
		options, pipelineMode, manifest, mergeEvent, inventory, stagingLimits, pluginSHA256, beforeLedger, afterLedger, recursiveEligible,
	)
	if buildErr != nil {
		writeErr := writeControlledMergeReport(options.OutputPath, report)
		return report, errors.Join(buildErr, writeErr)
	}
	if writeErr := writeControlledMergeReport(options.OutputPath, report); writeErr != nil {
		return ControlledMergeRunReport{}, writeErr
	}
	return report, nil
}

// validateControlledMergeOptions enforces the options required by every
// pipeline mode before any further work runs.
func validateControlledMergeOptions(options ControlledMergeRunOptions) (ControlledMergePipelineMode, error) {
	pipelineMode, modeErr := controlledMergePipelineMode(options.Mode)
	if modeErr != nil {
		return "", modeErr
	}
	if pipelineMode == ControlledMergePipelineRetainAll && options.PluginPath == "" {
		return "", fmt.Errorf("retain-all plugin path is required")
	}
	if pipelineMode == ControlledMergePipelineDeterministicDrop && options.PluginPath == "" {
		return "", fmt.Errorf("deterministic-drop plugin path is required")
	}
	if pipelineMode == ControlledMergePipelineDeterministicDrop && options.SamplingOracle == nil {
		return "", fmt.Errorf("deterministic-drop sampling oracle is required")
	}
	if options.SamplingOracle != nil {
		serverOptions := ServerOptions{PluginPath: options.PluginPath, PluginConfig: options.PluginConfig, SamplingOracle: options.SamplingOracle}
		if oracleErr := validateSamplingOracle(serverOptions); oracleErr != nil {
			return "", oracleErr
		}
	}
	if options.SeedManifestPath == "" {
		return "", fmt.Errorf("controlled seed manifest path is required")
	}
	if options.DataRoot == "" {
		return "", fmt.Errorf("controlled seed data root is required")
	}
	return pipelineMode, nil
}

// loadControlledMergePlugin opens the configured sampler plugin, computes its
// SHA-256, and computes the segment time range used by the receiver. The
// returned sampler is nil for the pipeline-disabled mode and the caller is
// responsible for closing it otherwise.
func loadControlledMergePlugin(pipelineMode ControlledMergePipelineMode, options ControlledMergeRunOptions,
	manifest ControlledMergeSeedManifest,
) (sdk.Sampler, string, timestamp.TimeRange, error) {
	if pipelineMode == ControlledMergePipelineDisabled {
		return nil, "", timestamp.TimeRange{}, nil
	}
	if options.PluginPath == "" {
		if pipelineMode == ControlledMergePipelineRetainAll {
			return nil, "", timestamp.TimeRange{}, fmt.Errorf("retain-all plugin path is required")
		}
		return nil, "", timestamp.TimeRange{}, fmt.Errorf("deterministic-drop plugin path is required")
	}
	sampler, loadErr := sdk.OpenSampler(options.PluginPath, "NewSampler", options.PluginConfig)
	if loadErr != nil {
		return nil, "", timestamp.TimeRange{}, fmt.Errorf("cannot load controlled plugin: %w", loadErr)
	}
	pluginSHA256, shaErr := fileSHA256(options.PluginPath)
	if shaErr != nil {
		return nil, "", timestamp.TimeRange{}, errors.Join(shaErr, sampler.Close())
	}
	if options.ExecutionIdentity.PluginSHA256 != "" && options.ExecutionIdentity.PluginSHA256 != pluginSHA256 {
		return nil, "", timestamp.TimeRange{}, errors.Join(
			fmt.Errorf("controlled plugin checksum %s does not match expected %s", pluginSHA256, options.ExecutionIdentity.PluginSHA256),
			sampler.Close(),
		)
	}
	configDigest := sha256.Sum256(options.PluginConfig)
	configSHA256 := fmt.Sprintf("%x", configDigest)
	if options.ExecutionIdentity.PluginConfigSHA256 != "" && options.ExecutionIdentity.PluginConfigSHA256 != configSHA256 {
		return nil, "", timestamp.TimeRange{}, errors.Join(
			fmt.Errorf("controlled plugin configuration checksum %s does not match expected %s",
				configSHA256, options.ExecutionIdentity.PluginConfigSHA256),
			sampler.Close(),
		)
	}
	// The frozen integration seed spans one logical day. Include that entire day
	// plus grace so a full-snapshot finalize does not fail closed at an artificial
	// benchmark segment boundary.
	coverageMargin := manifest.MergeGrace + 24*time.Hour
	segmentTimeRange := timestamp.NewInclusiveTimeRange(
		time.Unix(0, manifest.Selection.MinTimestamp).Add(-coverageMargin),
		time.Unix(0, manifest.Selection.MaxTimestamp).Add(coverageMargin),
	)
	return sampler, pluginSHA256, segmentTimeRange, nil
}

// openControlledMergeReceiver constructs the receiver the controlled merge
// runs against. Resource cleanup remains the responsibility of the caller.
func openControlledMergeReceiver(dataRoot string, manifest ControlledMergeSeedManifest, sampler sdk.Sampler,
	segmentTimeRange timestamp.TimeRange,
) (*storagetrace.BenchmarkPartReceiver, error) {
	receiver, receiverErr := storagetrace.NewBenchmarkMergeReceiver(dataRoot, storagetrace.BenchmarkMergeReceiverOptions{
		LogicalNow: manifest.MatureLogicalNow, MergeGrace: manifest.MergeGrace, PartMergeDepths: manifest.PartMergeDepths,
		Attribution: true, BlockMerges: true, Sampler: sampler, SegmentTimeRange: segmentTimeRange,
	})
	if receiverErr != nil {
		return nil, fmt.Errorf("cannot open controlled merge seed clone: %w", receiverErr)
	}
	return receiver, nil
}

// captureControlledMergePreDispatchProfile writes the pre-dispatch allocation
// profile when a profile directory is configured. The post-introduction
// profile is captured later so it does not pollute the per-merge pprof base.
func captureControlledMergePreDispatchProfile(profileDir string) error {
	if profileDir == "" {
		return nil
	}
	return writeRuntimeProfiles(profileDir, "pre-dispatch")
}

// prepareControlledMergeProfileDir creates the profile directory when
// requested. Returns the resolved profile directory (empty when the caller did
// not request one).
func prepareControlledMergeProfileDir(profileDir string) (string, error) {
	if profileDir == "" {
		return "", nil
	}
	if mkdirErr := os.MkdirAll(profileDir, 0o755); mkdirErr != nil {
		return "", fmt.Errorf("cannot create controlled merge profile directory: %w", mkdirErr)
	}
	return profileDir, nil
}

// startControlledMergeProfiler starts the phase profiler when a profile
// directory is configured. It returns an inactive profiler when no directory
// is set so callers can stop it unconditionally.
func startControlledMergeProfiler(profileDir string) (*phaseProfiler, error) {
	if profileDir == "" {
		return &phaseProfiler{}, nil
	}
	profiler, err := startPhaseProfiler(filepath.Join(profileDir, "controlled-cpu.pprof"))
	if err != nil {
		return nil, err
	}
	return profiler, nil
}

// assembleControlledMergeOutput collects the post-merge ledgers, inventory,
// and recursive-eligibility signal so the report builder can assemble the
// final report.
func assembleControlledMergeOutput(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver,
) (map[string]string, storagetrace.BenchmarkMergeInventory, bool, error) {
	afterLedger, afterLedgerErr := tracefixture.LogicalLedgerChecksums(ctx, receiver)
	if afterLedgerErr != nil {
		return nil, storagetrace.BenchmarkMergeInventory{}, false, fmt.Errorf("cannot checksum controlled merge output: %w", afterLedgerErr)
	}
	inventory, inventoryErr := receiver.MergeInventory()
	if inventoryErr != nil {
		return nil, storagetrace.BenchmarkMergeInventory{}, false, fmt.Errorf("cannot inspect controlled merge output: %w", inventoryErr)
	}
	_, recursiveErr := receiver.PreviewMergeSelection()
	recursiveEligible := recursiveErr == nil
	if recursiveErr != nil && !errors.Is(recursiveErr, storagetrace.ErrBenchmarkNoMergeSelection) {
		return nil, storagetrace.BenchmarkMergeInventory{}, false, fmt.Errorf("cannot inspect controlled recursive work: %w", recursiveErr)
	}
	return afterLedger, inventory, recursiveEligible, nil
}

// buildControlledMergeReport assembles the final report and its per-run
// correctness decision.
func buildControlledMergeReport(options ControlledMergeRunOptions, pipelineMode ControlledMergePipelineMode,
	manifest ControlledMergeSeedManifest, event storagetrace.BenchmarkMergeEvent, inventory storagetrace.BenchmarkMergeInventory,
	stagingLimits storagetrace.BenchmarkMergeStagingLimits, pluginSHA256 string,
	beforeLedger, afterLedger map[string]string, recursiveEligible bool,
) (ControlledMergeRunReport, error) {
	environmentOptions := options
	environmentOptions.ExecutionIdentity.PluginSHA256 = pluginSHA256
	configSHA256 := ""
	if pipelineMode != ControlledMergePipelineDisabled {
		configDigest := sha256.Sum256(options.PluginConfig)
		configSHA256 = fmt.Sprintf("%x", configDigest)
		environmentOptions.ExecutionIdentity.PluginConfigSHA256 = configSHA256
	}
	report := ControlledMergeRunReport{
		Version: 5, RunID: options.RunID, PipelineMode: pipelineMode, PluginSHA256: pluginSHA256, SeedSnapshotSHA256: manifest.Snapshot.SHA256,
		PluginConfigSHA256: configSHA256, SamplingOracle: options.SamplingOracle,
		SelectionSHA256:  manifest.Selection.SHA256,
		MatureLogicalNow: manifest.MatureLogicalNow, Event: event, Inventory: inventory, StagingLimits: stagingLimits,
		BeforeLedger: beforeLedger, AfterLedger: afterLedger, RecursiveEligible: recursiveEligible,
		Environment: readEnvironmentForControlledMerge(options.DataRoot, environmentOptions),
	}
	outputCorrect := event.InputRows == event.OutputRows && maps.Equal(beforeLedger, afterLedger)
	if options.SamplingOracle != nil {
		outputCorrect = maps.Equal(options.SamplingOracle.ExpectedLedger, afterLedger) && samplingOracleRowsCorrect(inventory, *options.SamplingOracle)
	}
	report.Correct = event.SelectionSHA256 == manifest.Selection.SHA256 && event.HotInputParts == 0 &&
		int(event.MatureInputParts) == len(event.InputPartIDs) && len(event.Children) == 2 &&
		controlledMergePipelineCorrect(pipelineMode, event, options.SamplingOracle) && outputCorrect
	if !report.Correct {
		return report, fmt.Errorf("controlled merge correctness gate failed")
	}
	return report, nil
}

// writeControlledMergeReport encodes the controlled merge report to JSON and
// persists it to the configured output path. Parent directories are created
// on demand.
func writeControlledMergeReport(path string, report ControlledMergeRunReport) error {
	reportData, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("cannot encode controlled merge report: %w", marshalErr)
	}
	reportData = append(reportData, '\n')
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o755); mkdirErr != nil {
		return fmt.Errorf("cannot create controlled merge report directory: %w", mkdirErr)
	}
	if writeErr := os.WriteFile(path, reportData, 0o600); writeErr != nil {
		return fmt.Errorf("cannot write controlled merge report %q: %w", path, writeErr)
	}
	return nil
}

func controlledMergePipelineMode(mode string) (ControlledMergePipelineMode, error) {
	if mode == "" {
		return ControlledMergePipelineDisabled, nil
	}
	pipelineMode := ControlledMergePipelineMode(mode)
	switch pipelineMode {
	case ControlledMergePipelineDisabled, ControlledMergePipelineRetainAll, ControlledMergePipelineDeterministicDrop:
		return pipelineMode, nil
	default:
		return "", fmt.Errorf("unsupported controlled merge pipeline mode %q", mode)
	}
}

func controlledMergePipelineCorrect(mode ControlledMergePipelineMode, event storagetrace.BenchmarkMergeEvent,
	oracle *SamplingOracleArtifact,
) bool {
	switch mode {
	case ControlledMergePipelineDisabled:
		return event.Sampling == storagetrace.BenchmarkMergeSamplingNotExecuted &&
			event.Reason == storagetrace.BenchmarkMergeReasonPipelineDisabled && event.PluginCalls == 0 && event.TracesEvaluated == 0
	case ControlledMergePipelineRetainAll:
		return event.Sampling == storagetrace.BenchmarkMergeSamplingExecuted && event.PluginCalls > 0 && event.TracesEvaluated > 0 &&
			event.TracesRetained == event.TracesEvaluated && event.TracesDropped == 0
	case ControlledMergePipelineDeterministicDrop:
		return oracle != nil && event.Sampling == storagetrace.BenchmarkMergeSamplingExecuted && event.PluginCalls > 0 &&
			event.TracesEvaluated == oracle.Evaluated && event.TracesRetained == oracle.Retained && event.TracesDropped == oracle.Dropped
	default:
		return false
	}
}

func fileSHA256(path string) (string, error) {
	pluginFile, openErr := os.Open(path)
	if openErr != nil {
		return "", fmt.Errorf("cannot open controlled plugin for checksum: %w", openErr)
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, pluginFile)
	closeErr := pluginFile.Close()
	if copyErr != nil {
		return "", errors.Join(fmt.Errorf("cannot checksum controlled plugin: %w", copyErr), closeErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("cannot close controlled plugin after checksum: %w", closeErr)
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), nil
}

// readEnvironmentForControlledMerge captures the same runtime envelope as the
// primary server so the controlled run can be compared against the suite.
func readEnvironmentForControlledMerge(dataRoot string, options ControlledMergeRunOptions) Environment {
	serverOptions := ServerOptions{Root: dataRoot, ExecutionIdentity: options.ExecutionIdentity}
	return readEnvironment(options.Commit, serverOptions)
}

// ReadControlledMergeSeedManifest reads one controlled seed contract.
func ReadControlledMergeSeedManifest(path string) (ControlledMergeSeedManifest, error) {
	manifestData, readErr := os.ReadFile(path)
	if readErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot read controlled seed manifest %q: %w", path, readErr)
	}
	var manifest ControlledMergeSeedManifest
	if decodeErr := json.Unmarshal(manifestData, &manifest); decodeErr != nil {
		return ControlledMergeSeedManifest{}, fmt.Errorf("cannot decode controlled seed manifest %q: %w", path, decodeErr)
	}
	return manifest, nil
}
