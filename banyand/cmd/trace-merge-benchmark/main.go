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

// Command trace-merge-benchmark runs controlled trace merge performance experiments.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracebaseline"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: trace-merge-benchmark <serve|drive|capture-seed|run-controlled|render|validate> [flags]")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch os.Args[1] {
	case "serve":
		serve(ctx, os.Args[2:])
	case "drive":
		drive(ctx, os.Args[2:])
	case "capture-seed":
		captureSeed(ctx, os.Args[2:])
	case "run-controlled":
		runControlled(ctx, os.Args[2:])
	case "render":
		render(os.Args[2:])
	case "validate":
		validate(os.Args[2:])
	default:
		fatalf("unknown command %q", os.Args[1])
	}
}

func runControlled(ctx context.Context, arguments []string) {
	flags := flag.NewFlagSet("run-controlled", flag.ExitOnError)
	var options tracebaseline.ControlledMergeRunOptions
	flags.StringVar(&options.SeedManifestPath, "seed-manifest", "", "controlled seed manifest JSON")
	flags.StringVar(&options.DataRoot, "data", "", "fresh controlled seed shard clone")
	flags.StringVar(&options.OutputPath, "output", "", "controlled merge report JSON")
	flags.StringVar(&options.RunID, "run-id", "controlled", "run identifier")
	flags.StringVar(&options.Mode, "pipeline", string(tracebaseline.ControlledMergePipelineDisabled), "pipeline mode: disabled or retain-all")
	flags.StringVar(&options.Commit, "commit", "", "revision under test")
	flags.StringVar(&options.PluginPath, "plugin", "", "native retain-all sampler .so path")
	flags.StringVar(&options.ProfileDir, "profiles", "", "optional controlled merge profile directory")
	flags.StringVar(&options.ExecutionIdentity.ImageDigest, "image-digest", "", "Docker image digest recorded in the controlled run environment")
	flags.StringVar(&options.ExecutionIdentity.CloneMethod, "clone-method", "", "clone method recorded in the controlled run environment")
	flags.StringVar(&options.ExecutionIdentity.BinarySHA256, "binary-sha256", "", "controlled data-node binary checksum")
	flags.StringVar(&options.ExecutionIdentity.Filesystem, "filesystem", "", "data-root filesystem recorded in the controlled run environment")
	flags.StringVar(&options.ExecutionIdentity.StorageDevice, "storage-device", "", "data-root storage device recorded in the controlled run environment")
	flags.StringVar(&options.ExecutionIdentity.PluginSHA256, "plugin-sha256", "", "plugin .so checksum for retain-all pipeline mode")
	_ = flags.Parse(arguments)
	report, runErr := tracebaseline.RunControlledMerge(ctx, options)
	if runErr != nil {
		fatalf("controlled merge failed: %v", runErr)
	}
	encoded, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		fatalf("cannot encode controlled merge summary: %v", marshalErr)
	}
	_, _ = fmt.Fprintln(os.Stdout, string(encoded))
}

func captureSeed(ctx context.Context, arguments []string) {
	flags := flag.NewFlagSet("capture-seed", flag.ExitOnError)
	var options tracebaseline.ControlledSeedCaptureOptions
	var minInputDepth uint
	flags.StringVar(&options.SourceRoot, "source", "", "mutable fixture source root")
	flags.StringVar(&options.DataRoot, "data", "", "discovery data-node shard root")
	flags.StringVar(&options.SchedulePath, "schedule", "", "fixture schedule JSON")
	flags.StringVar(&options.OutputRoot, "output", "", "new controlled seed output directory")
	flags.Uint64Var(&options.MinInputRows, "min-input-rows", 100000, "minimum selected rows")
	flags.UintVar(&minInputDepth, "min-input-depth", 2, "minimum selected merge depth")
	flags.IntVar(&options.MinInputParts, "min-input-parts", 15, "minimum selected part count")
	_ = flags.Parse(arguments)
	options.MinInputDepth = uint32(minInputDepth)
	manifest, captureErr := tracebaseline.CaptureControlledMergeSeed(ctx, options)
	if captureErr != nil {
		fatalf("capture seed failed: %v", captureErr)
	}
	encoded, marshalErr := json.MarshalIndent(manifest, "", "  ")
	if marshalErr != nil {
		fatalf("cannot encode captured seed summary: %v", marshalErr)
	}
	_, _ = fmt.Fprintln(os.Stdout, string(encoded))
}

func validate(arguments []string) {
	flags := flag.NewFlagSet("validate", flag.ExitOnError)
	suitePath := flags.String("suite", "", "suite report JSON")
	_ = flags.Parse(arguments)
	suiteData, readErr := os.ReadFile(*suitePath)
	if readErr != nil {
		fatalf("cannot read suite report: %v", readErr)
	}
	var suite tracebaseline.SuiteReport
	if decodeErr := json.Unmarshal(suiteData, &suite); decodeErr != nil {
		fatalf("cannot decode suite report: %v", decodeErr)
	}
	readiness := tracebaseline.EvaluateBaselineReadiness(suite)
	if readiness.Ready {
		return
	}
	for gateIdx := range readiness.Gates {
		gate := &readiness.Gates[gateIdx]
		if !gate.Passed {
			_, _ = fmt.Fprintf(os.Stderr, "failed gate %s: %s\n", gate.Name, gate.Detail)
		}
	}
	os.Exit(1)
}

func render(arguments []string) {
	flags := flag.NewFlagSet("render", flag.ExitOnError)
	suitePath := flags.String("suite", "", "suite report JSON")
	outputPath := flags.String("output", "", "standalone HTML output")
	_ = flags.Parse(arguments)
	suiteData, readErr := os.ReadFile(*suitePath)
	if readErr != nil {
		fatalf("cannot read suite report: %v", readErr)
	}
	var suite tracebaseline.SuiteReport
	if decodeErr := json.Unmarshal(suiteData, &suite); decodeErr != nil {
		fatalf("cannot decode suite report: %v", decodeErr)
	}
	output, createErr := os.Create(*outputPath)
	if createErr != nil {
		fatalf("cannot create HTML report: %v", createErr)
	}
	if renderErr := tracebaseline.RenderHTML(output, suite); renderErr != nil {
		_ = output.Close()
		fatalf("cannot render HTML report: %v", renderErr)
	}
	if closeErr := output.Close(); closeErr != nil {
		fatalf("cannot close HTML report: %v", closeErr)
	}
}

func serve(ctx context.Context, arguments []string) {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	var options tracebaseline.ServerOptions
	var segmentMinTimeNanos, segmentMaxTimeNanos int64
	flags.StringVar(&options.Root, "root", "", "data-node shard root")
	flags.StringVar(&options.SocketPath, "socket", "", "Unix control socket")
	flags.StringVar(&options.OutputPath, "output", "", "run report JSON path")
	flags.StringVar(&options.ProfileDir, "profiles", "", "profile artifact directory")
	flags.StringVar(&options.Commit, "commit", "", "revision under test")
	flags.StringVar(&options.FixtureSHA256, "fixture-sha256", "", "fixture manifest checksum")
	flags.StringVar(&options.ScheduleSHA256, "schedule-sha256", "", "schedule checksum")
	flags.StringVar(&options.RunID, "run-id", "", "run identifier")
	flags.StringVar(&options.Mode, "mode", "throughput", "serial or throughput")
	flags.Float64Var(&options.Acceleration, "acceleration", 1, "logical-to-wall acceleration")
	flags.Uint64Var(&options.ExpectedRows, "expected-rows", 0, "expected ledger rows")
	var expectedCoreLedger, expectedLatencyLedger, expectedStartTimeLedger string
	flags.StringVar(&expectedCoreLedger, "expected-core-ledger", "", "expected logical core ledger checksum")
	flags.StringVar(&expectedLatencyLedger, "expected-latency-ledger", "", "expected logical latency-index ledger checksum")
	flags.StringVar(&expectedStartTimeLedger, "expected-start-time-ledger", "", "expected logical start-time-index ledger checksum")
	flags.Uint64Var(&options.MaxInputPartID, "max-input-part-id", 0, "highest scheduled raw part ID")
	flags.BoolVar(&options.Attribution, "attribution", false, "serialize active merges for per-merge resource attribution")
	flags.BoolVar(&options.RunFinalize, "finalize", false, "run one production finalization round after the two-hour cooldown")
	flags.StringVar(&options.ExecutionIdentity.ImageDigest, "image-digest", "", "Docker image digest recorded in the environment envelope")
	flags.StringVar(&options.ExecutionIdentity.Filesystem, "filesystem", "", "data-root filesystem recorded in the environment envelope")
	flags.StringVar(&options.ExecutionIdentity.StorageDevice, "storage-device", "", "data-root storage device recorded in the environment envelope")
	flags.StringVar(&options.ExecutionIdentity.CloneMethod, "clone-method", "", "clone method recorded in the environment envelope (e.g. os.CopyFS, hardlink)")
	flags.StringVar(&options.ExecutionIdentity.BinarySHA256, "binary-sha256", "", "measured data-node binary checksum")
	flags.StringVar(&options.ExecutionIdentity.PluginSHA256, "plugin-sha256", "", "plugin .so checksum for retain-all pipeline mode")
	flags.StringVar(&options.PluginPath, "plugin", "", "native sampler plugin .so path")
	flags.Int64Var(&segmentMinTimeNanos, "segment-min-time-nanos", 0, "inclusive minimum fixture timestamp for sampler coverage")
	flags.Int64Var(&segmentMaxTimeNanos, "segment-max-time-nanos", 0, "inclusive maximum fixture timestamp for sampler coverage")
	_ = flags.Parse(arguments)
	if segmentMinTimeNanos != 0 || segmentMaxTimeNanos != 0 {
		options.SegmentTimeRange = timestamp.NewInclusiveTimeRange(time.Unix(0, segmentMinTimeNanos), time.Unix(0, segmentMaxTimeNanos))
	}
	options.ExpectedLedger = map[string]string{
		tracebaseline.LedgerCore: expectedCoreLedger, tracebaseline.LedgerLatency: expectedLatencyLedger,
		tracebaseline.LedgerStartTime: expectedStartTimeLedger,
	}
	if serveErr := tracebaseline.Serve(ctx, options); serveErr != nil {
		fatalf("serve failed: %v", serveErr)
	}
}

func drive(ctx context.Context, arguments []string) {
	flags := flag.NewFlagSet("drive", flag.ExitOnError)
	var options tracebaseline.DriverOptions
	flags.StringVar(&options.SocketPath, "socket", "", "data-node Unix control socket")
	flags.StringVar(&options.SourceRoot, "source", "", "fresh fixture source root")
	flags.StringVar(&options.DataRoot, "data", "", "data-node shard root")
	flags.StringVar(&options.SchedulePath, "schedule", "", "fixture schedule JSON")
	flags.StringVar(&options.OutputPath, "output", "", "completed run report path")
	flags.StringVar(&options.Mode, "mode", "throughput", "serial or throughput")
	flags.Float64Var(&options.Acceleration, "acceleration", 1, "logical-to-wall acceleration")
	flags.IntVar(&options.ControllerCPU, "controller-cpu", -1, "dedicated controller CPU")
	_ = flags.Parse(arguments)
	if _, driveErr := tracebaseline.Drive(ctx, options); driveErr != nil {
		fatalf("drive failed: %v", driveErr)
	}
}

func fatalf(format string, arguments ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", arguments...)
	os.Exit(1)
}
