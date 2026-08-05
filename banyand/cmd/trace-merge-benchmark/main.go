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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracebaseline"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: trace-merge-benchmark <serve|drive> [flags]")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch os.Args[1] {
	case "serve":
		serve(ctx, os.Args[2:])
	case "drive":
		drive(ctx, os.Args[2:])
	case "render":
		render(os.Args[2:])
	default:
		fatalf("unknown command %q", os.Args[1])
	}
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
	flags.Uint64Var(&options.MaxInputPartID, "max-input-part-id", 0, "highest scheduled raw part ID")
	flags.BoolVar(&options.Attribution, "attribution", false, "serialize active merges for per-merge resource attribution")
	_ = flags.Parse(arguments)
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
