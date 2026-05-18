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
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	envRunBench       = "RUN_DISTRIBUTED_QUERY_BENCH"
	envInContainer    = "DQB_IN_CONTAINER"
	envQueryWorkers   = "DQB_QUERY_WORKERS"
	envQueryIters     = "DQB_QUERY_ITERATIONS"
	envWriters        = "DQB_WRITERS"
	envReportDir      = "DQB_REPORT_DIR"
	envProfile        = "DQB_PROFILE"
	envWarmupIters    = "DQB_WARMUP_ITERATIONS"
	envSmallExactRows = "DQB_SMALL_EXACT_ROWS"
	envDockerImage    = "DQB_DOCKER_IMAGE"
	envCPULimit       = "DQB_CPU_LIMIT"
	envMemoryLimit    = "DQB_MEMORY_LIMIT"
	envMode           = "DQB_MODE"
	envScenario       = "DQB_SCENARIO"
	envCardinality    = "DQB_CARDINALITY"
	envMerge          = "DQB_MERGE"

	defaultReportDir      = ".omx/bench-reports/distributed-query"
	defaultQueryWorkers   = 4
	defaultQueryIters     = 50
	defaultWarmupIters    = 3
	defaultWriters        = 4
	defaultSmallExactRows = 10000

	modeRow = "row"
	modeVec = "vec"
)

// Scenario identifies a distributed query benchmark shape.
type Scenario string

const (
	// ScenarioScanAll benchmarks the measure scan-all fixture.
	ScenarioScanAll Scenario = "scan_all"
	// ScenarioTopWithFilter benchmarks the measure Top-N-with-filter fixture.
	ScenarioTopWithFilter Scenario = "top_with_filter"
)

// Config drives a single test-binary invocation. The shell orchestrator
// (run-docker.sh -> orchestrate.sh) owns the (mode × scenario × cardinality)
// matrix; each invocation runs either:
//
//   - single-shot: one combo selected by Mode + Scenario + Cardinality,
//     writes a shard JSON under ReportDir/shards/;
//   - merge: reads every shard, computes correctness, writes the unified
//     report under ReportDir/.
//
// Direct go test invocations without the right env vars surface a hard
// configuration error.
type Config struct {
	ReportDir        string
	DockerImage      string
	CPULimit         string
	MemoryLimit      string
	Mode             string
	Scenario         Scenario
	Cardinality      int
	QueryWorkers     int
	QueryIterations  int
	WarmupIterations int
	Writers          int
	SmallExactRows   int
	RunBench         bool
	InContainer      bool
	Profile          bool
	Merge            bool
}

// LoadConfig reads benchmark settings from environment variables.
func LoadConfig() Config {
	return Config{
		RunBench:         getBool(envRunBench, false),
		InContainer:      getBool(envInContainer, false),
		Profile:          getBool(envProfile, false),
		Merge:            getBool(envMerge, false),
		ReportDir:        getString(envReportDir, defaultReportDir),
		DockerImage:      getString(envDockerImage, ""),
		CPULimit:         getString(envCPULimit, ""),
		MemoryLimit:      getString(envMemoryLimit, ""),
		Mode:             getString(envMode, ""),
		Scenario:         Scenario(getString(envScenario, "")),
		Cardinality:      getInt(envCardinality, 0),
		QueryWorkers:     getInt(envQueryWorkers, defaultQueryWorkers),
		QueryIterations:  getInt(envQueryIters, defaultQueryIters),
		WarmupIterations: getInt(envWarmupIters, defaultWarmupIters),
		Writers:          getInt(envWriters, defaultWriters),
		SmallExactRows:   getInt(envSmallExactRows, defaultSmallExactRows),
	}
}

// IsSingleShot reports whether the caller selected a single (mode, scenario,
// cardinality) combo. Returns true when any of the three singular env vars is
// set so Validate can flag a partially-set selection as a hard error.
func (c Config) IsSingleShot() bool {
	return c.Mode != "" || c.Scenario != "" || c.Cardinality > 0
}

// Validate enforces the single-shot OR merge contract.
func (c Config) Validate() error {
	if !c.RunBench {
		return nil
	}
	if !c.InContainer {
		return fmt.Errorf("%s=1 requires %s=1; invoke via test/integration/distributed/querybench/run-docker.sh", envRunBench, envInContainer)
	}
	if c.Merge {
		if c.IsSingleShot() {
			return fmt.Errorf("%s=1 is mutually exclusive with %s/%s/%s", envMerge, envMode, envScenario, envCardinality)
		}
		if c.ReportDir == "" {
			return fmt.Errorf("%s must be set for merge mode", envReportDir)
		}
		return nil
	}
	if !c.IsSingleShot() {
		return fmt.Errorf(
			"missing combo selection: set %s + %s + %s for single-shot or %s=1 for merge; invoke run-docker.sh to drive the matrix",
			envMode, envScenario, envCardinality, envMerge,
		)
	}
	switch c.Mode {
	case modeRow, modeVec:
	default:
		return fmt.Errorf("%s must be %q or %q, got %q", envMode, modeRow, modeVec, c.Mode)
	}
	switch c.Scenario {
	case ScenarioScanAll, ScenarioTopWithFilter:
	default:
		return fmt.Errorf("%s unsupported: %q", envScenario, c.Scenario)
	}
	if c.Cardinality <= 0 {
		return fmt.Errorf("%s must be > 0", envCardinality)
	}
	if c.QueryWorkers <= 0 {
		return fmt.Errorf("%s must be > 0", envQueryWorkers)
	}
	if c.QueryIterations <= 0 {
		return fmt.Errorf("%s must be > 0", envQueryIters)
	}
	if c.WarmupIterations < 0 {
		return fmt.Errorf("%s must be >= 0", envWarmupIters)
	}
	if c.Writers <= 0 {
		return fmt.Errorf("%s must be > 0", envWriters)
	}
	if c.SmallExactRows <= 0 {
		return fmt.Errorf("%s must be > 0", envSmallExactRows)
	}
	return nil
}

func getString(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func getBool(key string, def bool) bool {
	if value := os.Getenv(key); value != "" {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

func getInt(key string, def int) int {
	if value := os.Getenv(key); value != "" {
		parsed, parseErr := strconv.Atoi(strings.TrimSpace(value))
		if parseErr == nil {
			return parsed
		}
	}
	return def
}

func splitCardinality(totalRows int) (entities, pointsEach int) {
	if totalRows <= 1024 {
		return 32, max(1, totalRows/32)
	}
	if totalRows <= 10000 {
		return 100, max(1, totalRows/100)
	}
	if totalRows <= 100000 {
		return 1000, max(1, totalRows/1000)
	}
	return max(1, totalRows/1000), 1000
}

func queryTimeout(cardinality int) time.Duration {
	base := 30 * time.Second
	if cardinality > 1000000 {
		return 5 * time.Minute
	}
	if cardinality > 100000 {
		return 2 * time.Minute
	}
	return base
}
