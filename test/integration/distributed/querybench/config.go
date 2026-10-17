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

// Package querybench is the docker-gated distributed-query benchmark harness
// that drives row vs vec mode comparisons across scenarios and cardinalities.
package querybench

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	envRunBench          = "RUN_DISTRIBUTED_QUERY_BENCH"
	envInContainer       = "DQB_IN_CONTAINER"
	envQueryWorkers      = "DQB_QUERY_WORKERS"
	envQueryIters        = "DQB_QUERY_ITERATIONS"
	envByIDIters         = "DQB_TRACE_BY_ID_ITERATIONS"
	envWriters           = "DQB_WRITERS"
	envReportDir         = "DQB_REPORT_DIR"
	envProfile           = "DQB_PROFILE"
	envWarmupIters       = "DQB_WARMUP_ITERATIONS"
	envDockerImage       = "DQB_DOCKER_IMAGE"
	envCPULimit          = "DQB_CPU_LIMIT"
	envMemoryLimit       = "DQB_MEMORY_LIMIT"
	envEngine            = "DQB_ENGINE"
	envMatrix            = "DQB_MATRIX"
	envMode              = "DQB_MODE"
	envScenario          = "DQB_SCENARIO"
	envCardinality       = "DQB_CARDINALITY"
	envMerge             = "DQB_MERGE"
	envSpansPerTrace     = "DQB_SPANS_PER_TRACE"
	envSpanDist          = "DQB_SPAN_DIST"
	envSelectivity       = "DQB_FILTER_SELECTIVITY"
	envTraceIDBatch      = "DQB_TRACE_ID_BATCH"
	envShardNum          = "DQB_SHARD_NUM"
	envDataNodes         = "DQB_DATA_NODES"
	envSpanBytes         = "DQB_SPAN_BYTES"
	envQueryMemoryMiB    = "DQB_QUERY_MEMORY_MIB"
	envSoak              = "DQB_SOAK"
	envSoakHeapGrowthPct = "SOAK_HEAP_GROWTH_MAX_PCT"
	// Measure soak bounds (TestMeasureVecSoak): the soak stops at whichever of the
	// two is reached first.
	envSoakMeasureIterations = "DQB_MEASURE_SOAK_ITERATIONS"
	envSoakMeasureDuration   = "DQB_MEASURE_SOAK_DURATION"

	defaultReportDir    = ".omx/bench-reports/distributed-query"
	defaultQueryWorkers = 4
	defaultQueryIters   = 50
	// defaultByIDIters is the iteration count for the trace_by_id scenario. A
	// trace-id point lookup is sub-10ms, so the 50-iteration default yields
	// noisy p95/p99 tails (a few GC pauses dominate); it needs a far larger
	// sample to report a stable tail.
	defaultByIDIters         = 1000
	defaultWarmupIters       = 3
	defaultWriters           = 4
	defaultSpansPerTrace     = 20
	defaultSpanDist          = spanDistUniform
	defaultSelectivity       = 0.01
	defaultTraceIDBatch      = 1
	defaultShardNum          = 2
	defaultDataNodes         = 2
	defaultSpanBytes         = 1024
	defaultQueryMemoryMiB    = 256
	defaultSoakHeapGrowthPct = 10
	// The short preset of the measure soak: about two minutes of mixed queries.
	defaultSoakMeasureIterations = 6000
	defaultSoakMeasureDuration   = 3 * time.Minute

	engineMeasure = "measure"
	engineTrace   = "trace"

	matrixA    = "A"
	matrixB    = "B"
	matrixBoth = "both"

	modeVec = "vec"

	spanDistUniform   = "uniform"
	spanDistHeavytail = "heavytail"
)

// Scenario identifies a distributed query benchmark shape.
type Scenario string

const (
	// ScenarioScanAll benchmarks the measure scan-all fixture.
	ScenarioScanAll Scenario = "scan_all"
	// ScenarioTopWithFilter benchmarks the measure Top-N-with-filter fixture.
	ScenarioTopWithFilter Scenario = "top_with_filter"
	// ScenarioQLBT01 benchmarks native and legacy QL parameter rebinding.
	ScenarioQLBT01 Scenario = "qlb_t01"
	// ScenarioQLBT02 benchmarks the shared native and QL batch boundary.
	ScenarioQLBT02 Scenario = "qlb_t02"
	// ScenarioQLBT03 benchmarks typed raw QL projection and time pruning.
	ScenarioQLBT03 Scenario = "qlb_t03"
	// ScenarioQLBT05 compares compiled-plan reuse with uncached planning.
	ScenarioQLBT05 Scenario = "qlb_t05"
	// ScenarioQLBT07 benchmarks complete local SQL aggregation over raw batches.
	ScenarioQLBT07 Scenario = "qlb_t07"
	// ScenarioQLBT08 compares native aggregate branches with local SQL aggregation.
	ScenarioQLBT08 Scenario = "qlb_t08"
	// ScenarioQLBT10 benchmarks typed expression projection over native batches.
	ScenarioQLBT10 Scenario = "qlb_t10"
	// ScenarioQLBT12 benchmarks field filtering before projection and aggregation.
	ScenarioQLBT12 Scenario = "qlb_t12"
	// ScenarioQLBT14 benchmarks completed-group HAVING and collection membership.
	ScenarioQLBT14 Scenario = "qlb_t14"
	// ScenarioQLBT15 benchmarks complete visible-tuple DISTINCT.
	ScenarioQLBT15 Scenario = "qlb_t15"
	// ScenarioQLBT16 benchmarks computed and ordinal group keys.
	ScenarioQLBT16 Scenario = "qlb_t16"
	// ScenarioTraceByID benchmarks distributed trace-id lookup.
	ScenarioTraceByID Scenario = "trace_by_id"
	// ScenarioTraceTagFilter benchmarks distributed ordered trace tag filters.
	ScenarioTraceTagFilter Scenario = "trace_tag_filter"
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
	ReportDir            string
	DockerImage          string
	CPULimit             string
	MemoryLimit          string
	Engine               string
	Matrix               string
	Mode                 string
	Scenario             Scenario
	SpanDist             string
	Cardinality          int
	SpansPerTrace        int
	TraceIDBatch         int
	ShardNum             int
	DataNodes            int
	SpanBytes            int
	QueryMemoryMiB       int
	QueryWorkers         int
	QueryIterations      int
	ByIDIterations       int
	WarmupIterations     int
	Writers              int
	FilterSelectivity    float64
	RunBench             bool
	InContainer          bool
	Profile              bool
	Merge                bool
	Soak                 bool
	SoakHeapGrowthMaxPct int
	// SoakMeasureIterations and SoakMeasureDuration bound the measure soak.
	SoakMeasureIterations int
	SoakMeasureDuration   time.Duration
}

// LoadConfig reads benchmark settings from environment variables.
func LoadConfig() Config {
	return Config{
		RunBench:              getBool(envRunBench),
		InContainer:           getBool(envInContainer),
		Profile:               getBool(envProfile),
		Merge:                 getBool(envMerge),
		Soak:                  getBool(envSoak),
		SoakHeapGrowthMaxPct:  getInt(envSoakHeapGrowthPct, defaultSoakHeapGrowthPct),
		SoakMeasureIterations: getInt(envSoakMeasureIterations, defaultSoakMeasureIterations),
		SoakMeasureDuration:   getDuration(envSoakMeasureDuration, defaultSoakMeasureDuration),
		ReportDir:             getString(envReportDir, defaultReportDir),
		DockerImage:           getString(envDockerImage, ""),
		CPULimit:              getString(envCPULimit, ""),
		MemoryLimit:           getString(envMemoryLimit, ""),
		Engine:                getString(envEngine, engineMeasure),
		Matrix:                getString(envMatrix, matrixA),
		Mode:                  getString(envMode, ""),
		Scenario:              Scenario(getString(envScenario, "")),
		Cardinality:           getInt(envCardinality, 0),
		SpansPerTrace:         getInt(envSpansPerTrace, defaultSpansPerTrace),
		SpanDist:              getString(envSpanDist, defaultSpanDist),
		FilterSelectivity:     getFloat(envSelectivity, defaultSelectivity),
		TraceIDBatch:          getInt(envTraceIDBatch, defaultTraceIDBatch),
		ShardNum:              getInt(envShardNum, defaultShardNum),
		DataNodes:             getInt(envDataNodes, defaultDataNodes),
		SpanBytes:             getInt(envSpanBytes, defaultSpanBytes),
		QueryMemoryMiB:        getInt(envQueryMemoryMiB, defaultQueryMemoryMiB),
		QueryWorkers:          getInt(envQueryWorkers, defaultQueryWorkers),
		QueryIterations:       getInt(envQueryIters, defaultQueryIters),
		ByIDIterations:        getInt(envByIDIters, defaultByIDIters),
		WarmupIterations:      getInt(envWarmupIters, defaultWarmupIters),
		Writers:               getInt(envWriters, defaultWriters),
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
	if c.Engine == "" {
		return fmt.Errorf("%s must be set", envEngine)
	}
	switch c.Engine {
	case engineMeasure, engineTrace:
	default:
		return fmt.Errorf("%s must be %q or %q, got %q", envEngine, engineMeasure, engineTrace, c.Engine)
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
	if c.Mode != modeVec {
		return fmt.Errorf("%s must be %q, got %q: the row-based query path was removed in 0.12.0, see apache/skywalking#13998",
			envMode, modeVec, c.Mode)
	}
	switch c.Engine {
	case engineMeasure:
		switch c.Scenario {
		case ScenarioScanAll, ScenarioTopWithFilter, ScenarioQLBT01, ScenarioQLBT02, ScenarioQLBT03, ScenarioQLBT05,
			ScenarioQLBT07, ScenarioQLBT08, ScenarioQLBT10, ScenarioQLBT12, ScenarioQLBT14, ScenarioQLBT15, ScenarioQLBT16:
		default:
			return fmt.Errorf("%s unsupported for %s=%s: %q", envScenario, envEngine, c.Engine, c.Scenario)
		}
	case engineTrace:
		switch c.Scenario {
		case ScenarioTraceByID, ScenarioTraceTagFilter:
		default:
			return fmt.Errorf("%s unsupported for %s=%s: %q", envScenario, envEngine, c.Engine, c.Scenario)
		}
	}
	if c.Cardinality <= 0 {
		return fmt.Errorf("%s must be > 0", envCardinality)
	}
	if c.Engine == engineTrace {
		if validateErr := c.validateTrace(); validateErr != nil {
			return validateErr
		}
	}
	if err := c.validateCatalogB(); err != nil {
		return err
	}
	if c.QueryWorkers <= 0 {
		return fmt.Errorf("%s must be > 0", envQueryWorkers)
	}
	if c.QueryIterations <= 0 {
		return fmt.Errorf("%s must be > 0", envQueryIters)
	}
	if c.ByIDIterations <= 0 {
		return fmt.Errorf("%s must be > 0", envByIDIters)
	}
	if c.WarmupIterations < 0 {
		return fmt.Errorf("%s must be >= 0", envWarmupIters)
	}
	if c.Writers <= 0 {
		return fmt.Errorf("%s must be > 0", envWriters)
	}
	return nil
}

// ValidateSoak enforces the DQB_SOAK contract. The trace soak must run inside the
// container (DQB_IN_CONTAINER=1), whose cgroup limit makes its budget gate
// reproducible. The measure soak (DQB_ENGINE=measure, the default) gates on process
// heap and on counters, so it also runs directly under go test.
func (c Config) ValidateSoak() error {
	if !c.Soak {
		return nil
	}
	if !c.InContainer && c.Engine != engineMeasure {
		return fmt.Errorf("%s=1 requires %s=1; invoke via test/integration/distributed/querybench/run-docker.sh", envSoak, envInContainer)
	}
	if c.Engine == engineMeasure {
		if c.SoakMeasureIterations <= 0 || c.SoakMeasureDuration <= 0 {
			return fmt.Errorf("%s and %s must be > 0, got %d and %s", envSoakMeasureIterations, envSoakMeasureDuration,
				c.SoakMeasureIterations, c.SoakMeasureDuration)
		}
	}
	if c.SoakHeapGrowthMaxPct <= 0 {
		return fmt.Errorf("%s must be > 0, got %d", envSoakHeapGrowthPct, c.SoakHeapGrowthMaxPct)
	}
	if c.SoakHeapGrowthMaxPct > 100 {
		return fmt.Errorf("%s must be <= 100, got %d", envSoakHeapGrowthPct, c.SoakHeapGrowthMaxPct)
	}
	return nil
}

func (c Config) validateTrace() error {
	switch c.Matrix {
	case matrixA, matrixB, matrixBoth:
	default:
		return fmt.Errorf("%s must be %q, %q, or %q, got %q", envMatrix, matrixA, matrixB, matrixBoth, c.Matrix)
	}
	switch c.SpanDist {
	case spanDistUniform, spanDistHeavytail:
	default:
		return fmt.Errorf("%s must be %q or %q, got %q", envSpanDist, spanDistUniform, spanDistHeavytail, c.SpanDist)
	}
	if c.SpansPerTrace <= 0 {
		return fmt.Errorf("%s must be > 0", envSpansPerTrace)
	}
	if c.FilterSelectivity <= 0 || c.FilterSelectivity > 1 {
		return fmt.Errorf("%s must be > 0 and <= 1", envSelectivity)
	}
	if c.TraceIDBatch <= 0 {
		return fmt.Errorf("%s must be > 0", envTraceIDBatch)
	}
	if c.ShardNum <= 0 {
		return fmt.Errorf("%s must be > 0", envShardNum)
	}
	if c.DataNodes <= 0 {
		return fmt.Errorf("%s must be > 0", envDataNodes)
	}
	if c.SpanBytes <= 0 {
		return fmt.Errorf("%s must be > 0", envSpanBytes)
	}
	if c.QueryMemoryMiB <= 0 {
		return fmt.Errorf("%s must be > 0", envQueryMemoryMiB)
	}
	return nil
}

func getString(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func getBool(key string) bool {
	value := os.Getenv(key)
	if value == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
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

func getDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getFloat(key string, def float64) float64 {
	if value := os.Getenv(key); value != "" {
		parsed, parseErr := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if parseErr == nil {
			return parsed
		}
	}
	return def
}

type traceShape struct {
	SpansByTrace []int
	Offsets      []int
	TraceCount   int
	TotalSpans   int
	MeanSpans    float64
}

func deriveTraceShape(totalSpans, spansPerTrace int, spanDist string) traceShape {
	if totalSpans <= 0 {
		totalSpans = 1
	}
	if spansPerTrace <= 0 {
		spansPerTrace = defaultSpansPerTrace
	}
	meanSpans := float64(spansPerTrace)
	if spanDist == spanDistHeavytail {
		meanSpans = 0.95*10 + 0.05*500
	}
	traceCount := int(math.Round(float64(totalSpans) / meanSpans))
	if traceCount < 1 {
		traceCount = 1
	}
	spansByTrace := make([]int, traceCount)
	offsets := make([]int, traceCount+1)
	actualSpans := 0
	for traceIdx := 0; traceIdx < traceCount; traceIdx++ {
		spans := spansPerTrace
		if spanDist == spanDistHeavytail {
			spans = 10
			if traceIdx%20 == 0 {
				spans = 500
			}
		}
		spansByTrace[traceIdx] = spans
		offsets[traceIdx] = actualSpans
		actualSpans += spans
	}
	offsets[traceCount] = actualSpans
	return traceShape{
		SpansByTrace: spansByTrace,
		Offsets:      offsets,
		TraceCount:   traceCount,
		TotalSpans:   actualSpans,
		MeanSpans:    float64(actualSpans) / float64(traceCount),
	}
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

func (c Config) validateCatalogB() error {
	if c.Scenario == ScenarioQLBT16 {
		return c.validateQLBT16()
	}
	if c.Scenario == ScenarioQLBT15 {
		return c.validateQLBT15()
	}
	if c.Scenario == ScenarioQLBT14 {
		return c.validateQLBT14()
	}
	if c.Scenario == ScenarioQLBT12 {
		return c.validateQLBT12()
	}
	if c.Scenario == ScenarioQLBT07 || c.Scenario == ScenarioQLBT08 || c.Scenario == ScenarioQLBT10 {
		iterations, warmup, err := qlbT07Schedule(c.Cardinality)
		if c.Scenario == ScenarioQLBT08 {
			iterations, warmup, err = qlbT08Schedule(c.Cardinality)
		}
		if c.Scenario == ScenarioQLBT10 {
			iterations, warmup, err = qlbT10Schedule(c.Cardinality)
			if c.QueryMemoryMiB != 256 {
				return fmt.Errorf("qlb_t10 requires DQB_QUERY_MEMORY_MIB=256")
			}
		}
		if err != nil {
			return err
		}
		if c.QueryWorkers != 1 || c.QueryIterations != iterations || c.WarmupIterations != warmup {
			return fmt.Errorf("%s at %d rows requires DQB_QUERY_WORKERS=1, DQB_QUERY_ITERATIONS=%d and DQB_WARMUP_ITERATIONS=%d",
				c.Scenario, c.Cardinality, iterations, warmup)
		}
		return nil
	}
	if c.Scenario == ScenarioQLBT05 {
		if c.Cardinality != 1024 || c.QueryWorkers != 8 || c.QueryIterations != 1000 || c.WarmupIterations != 20 {
			return fmt.Errorf("qlb_t05 requires cardinality 1024, DQB_QUERY_WORKERS=8, DQB_QUERY_ITERATIONS=1000 and DQB_WARMUP_ITERATIONS=20")
		}
		return nil
	}
	switch c.Scenario {
	case ScenarioQLBT01, ScenarioQLBT02, ScenarioQLBT03:
	default:
		return nil
	}
	if c.QueryIterations != 1000 {
		return fmt.Errorf("%s requires exactly DQB_QUERY_ITERATIONS=1000 per variant", c.Scenario)
	}
	if (c.Scenario == ScenarioQLBT02 || c.Scenario == ScenarioQLBT03) && c.WarmupIterations != 20 {
		return fmt.Errorf("%s requires DQB_WARMUP_ITERATIONS=20", c.Scenario)
	}
	if c.QueryWorkers != 1 {
		return fmt.Errorf("%s requires DQB_QUERY_WORKERS=1", c.Scenario)
	}
	if c.Scenario == ScenarioQLBT03 && c.Cardinality != 1024 && c.Cardinality != 100000 {
		return fmt.Errorf("qlb_t03 requires cardinality 1024 or 100000")
	}
	return nil
}

func (c Config) validateQLBT12() error {
	iterations, warmup, err := qlbT12Schedule(c.Cardinality)
	if err != nil {
		return err
	}
	if c.QueryWorkers != 1 || c.QueryIterations != iterations || c.WarmupIterations != warmup || c.QueryMemoryMiB != 256 {
		return fmt.Errorf("qlb_t12 at %d rows requires one worker, %d timed calls, %d warmups and 256 MiB", c.Cardinality, iterations, warmup)
	}
	return nil
}

func qlbT12Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 16384:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t12 requires input cardinality 1024 or 16384; larger inputs belong to the resource probe")
	}
}

func qlbT14Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 32768:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t14 requires input cardinality 1024 or 32768; large inputs belong to the owning resource probe")
	}
}

func qlbT07Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 32768:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t07 successful queries require cardinality 1024 or 32768; larger inputs belong to the resource probe")
	}
}

func qlbT08Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 32768:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t08 requires input cardinality 1024 or 32768")
	}
}

func qlbT10Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 32768:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t10 requires input cardinality 1024 or 32768")
	}
}

func (c Config) validateQLBT14() error {
	iterations, warmup, err := qlbT14Schedule(c.Cardinality)
	if err != nil {
		return err
	}
	if c.QueryWorkers != 1 || c.QueryIterations != iterations || c.WarmupIterations != warmup || c.QueryMemoryMiB != 256 {
		return fmt.Errorf("qlb_t14 at %d rows requires one worker, %d timed calls, %d warmups and 256 MiB", c.Cardinality, iterations, warmup)
	}
	return nil
}

func qlbT15Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 8192:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t15 successful queries require cardinality 1024 or 8192; larger inputs belong to bounded resource probes")
	}
}

func (c Config) validateQLBT15() error {
	iterations, warmup, err := qlbT15Schedule(c.Cardinality)
	if err != nil {
		return err
	}
	if c.QueryWorkers != 1 || c.QueryIterations != iterations || c.WarmupIterations != warmup || c.QueryMemoryMiB != 256 || !c.Profile {
		return fmt.Errorf("qlb_t15 requires one worker, %d calls, %d warmups, 256 MiB and profiles", iterations, warmup)
	}
	return nil
}

func qlbT16Schedule(cardinality int) (iterations, warmup int, err error) {
	switch cardinality {
	case 1024:
		return 100, 5, nil
	case 32768:
		return 20, 3, nil
	default:
		return 0, 0, fmt.Errorf("qlb_t16 successful queries require cardinality 1024 or 32768; larger inputs belong to bounded resource probes")
	}
}

func (c Config) validateQLBT16() error {
	iterations, warmup, err := qlbT16Schedule(c.Cardinality)
	if err != nil {
		return err
	}
	if c.QueryWorkers != 1 || c.QueryIterations != iterations || c.WarmupIterations != warmup || c.QueryMemoryMiB != 256 || !c.Profile {
		return fmt.Errorf("qlb_t16 requires one worker, %d calls, %d warmups, 256 MiB and profiles", iterations, warmup)
	}
	return nil
}
