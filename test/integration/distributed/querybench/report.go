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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Report is the machine-readable benchmark output.
type Report struct {
	GeneratedAt time.Time   `json:"generated_at"`
	Environment Environment `json:"environment"`
	Config      ConfigView  `json:"config"`
	Results     []Result    `json:"results"`
}

// ConfigView is a JSON-friendly copy of Config.
type ConfigView struct {
	ReportDir        string     `json:"report_dir"`
	Cardinalities    []int      `json:"cardinalities"`
	Scenarios        []Scenario `json:"scenarios"`
	QueryWorkers     int        `json:"query_workers"`
	QueryIterations  int        `json:"query_iterations"`
	WarmupIterations int        `json:"warmup_iterations"`
	Writers          int        `json:"writers"`
	Profile          bool       `json:"profile"`
}

// Environment records reproducibility metadata.
type Environment struct {
	GoVersion    string `json:"go_version"`
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	NumCPU       int    `json:"num_cpu"`
	DockerImage  string `json:"docker_image,omitempty"`
	CPULimit     string `json:"cpu_limit,omitempty"`
	MemoryLimit  string `json:"memory_limit,omitempty"`
	Cgroup       string `json:"cgroup,omitempty"`
	ContainerID  string `json:"container_id,omitempty"`
	ResourceNote string `json:"resource_note,omitempty"`
}

// Result records one mode/scenario/cardinality benchmark outcome.
type Result struct {
	Mode             string            `json:"mode"`
	Scenario         Scenario          `json:"scenario"`
	Cardinality      int               `json:"cardinality"`
	Entities         int               `json:"entities"`
	PointsEach       int               `json:"points_each"`
	ResponseRows     int               `json:"response_rows"`
	Correctness      string            `json:"correctness"`
	QueryIterations  int               `json:"query_iterations"`
	QueryWorkers     int               `json:"query_workers"`
	Latency          LatencyStats      `json:"latency"`
	QPS              float64           `json:"qps"`
	Resources        ResourceStats     `json:"resources"`
	Allocations      AllocationStats   `json:"allocations"`
	Profiles         map[string]string `json:"profiles,omitempty"`
	Error            string            `json:"error,omitempty"`
	ApproxResultHash uint64            `json:"approx_result_hash,omitempty"`
	// SampleDataPointText is the prototext of the first DataPoint of the
	// first response. Captured so the merge pass can dump row vs vec
	// shapes side by side when the correctness gate fires on a hash
	// mismatch — the row counts can match while the proto byte layout
	// diverges (TagFamily order, oneof variant choice, etc.).
	SampleDataPointText string `json:"sample_data_point_text,omitempty"`
}

// LatencyStats contains latency percentiles in milliseconds.
type LatencyStats struct {
	P50Ms float64 `json:"p50_ms"`
	P90Ms float64 `json:"p90_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
	MeanMs float64 `json:"mean_ms"`
}

// ResourceStats records process-level resource deltas for the in-process cluster harness.
type ResourceStats struct {
	CPUSecondsDelta float64 `json:"cpu_seconds_delta,omitempty"`
	RSSBytes        uint64  `json:"rss_bytes,omitempty"`
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes,omitempty"`
	HeapSysBytes    uint64  `json:"heap_sys_bytes,omitempty"`
	NumGC           uint32  `json:"num_gc,omitempty"`
	MetricSource    string  `json:"metric_source"`
}

// AllocationStats records allocation counters for the timed read phase.
type AllocationStats struct {
	MallocsDelta      uint64  `json:"mallocs_delta"`
	TotalAllocDelta   uint64  `json:"total_alloc_delta"`
	MallocsPerQuery   float64 `json:"mallocs_per_query"`
	AllocBytesPerQuery float64 `json:"alloc_bytes_per_query"`
	MetricSource      string  `json:"metric_source"`
}

// newReportFromShards builds the unified Report from the shard set the
// orchestrator produced. The cardinality and scenario lists come from the
// shards themselves so the report's Config view reflects exactly what ran.
func newReportFromShards(cfg Config, results []Result) Report {
	cardSet := make(map[int]struct{})
	scenSet := make(map[Scenario]struct{})
	queryWorkers := cfg.QueryWorkers
	queryIters := cfg.QueryIterations
	profile := false
	for _, r := range results {
		cardSet[r.Cardinality] = struct{}{}
		scenSet[r.Scenario] = struct{}{}
		if len(r.Profiles) > 0 {
			profile = true
		}
		if r.QueryWorkers > 0 {
			queryWorkers = r.QueryWorkers
		}
		if r.QueryIterations > 0 {
			queryIters = r.QueryIterations
		}
	}
	cards := make([]int, 0, len(cardSet))
	for c := range cardSet {
		cards = append(cards, c)
	}
	sort.Ints(cards)
	scens := make([]Scenario, 0, len(scenSet))
	for s := range scenSet {
		scens = append(scens, s)
	}
	sort.Slice(scens, func(i, j int) bool { return scens[i] < scens[j] })
	return Report{
		GeneratedAt: time.Now().UTC(),
		Environment: Environment{
			GoVersion:    runtime.Version(),
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			NumCPU:       runtime.NumCPU(),
			DockerImage:  cfg.DockerImage,
			CPULimit:     cfg.CPULimit,
			MemoryLimit:  cfg.MemoryLimit,
			Cgroup:       detectCgroupVersion(),
			ContainerID:  detectContainerID(),
			ResourceNote: "single-shot harness: each (mode, scenario, cardinality) combo runs in its own Go process so heap and CPU profiles describe only that combo",
		},
		Config: ConfigView{
			ReportDir:        cfg.ReportDir,
			Cardinalities:    cards,
			Scenarios:        scens,
			QueryWorkers:     queryWorkers,
			QueryIterations:  queryIters,
			WarmupIterations: cfg.WarmupIterations,
			Writers:          cfg.Writers,
			Profile:          profile,
		},
		Results: results,
	}
}

// writeShard serialises a single single-shot Result to ReportDir/shards/.
// The filename encodes mode_scenario_cardinality so merge can pair vec
// shards with their row counterparts without parsing the JSON.
func writeShard(result Result, reportDir string) (string, error) {
	shardDir := filepath.Join(reportDir, "shards")
	if mkErr := os.MkdirAll(shardDir, 0o755); mkErr != nil {
		return "", fmt.Errorf("create shard directory: %w", mkErr)
	}
	name := fmt.Sprintf("%s_%s_%d.json", result.Mode, result.Scenario, result.Cardinality)
	shardPath := filepath.Join(shardDir, name)
	body, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		return "", fmt.Errorf("marshal shard: %w", marshalErr)
	}
	if writeErr := os.WriteFile(shardPath, append(body, '\n'), 0o644); writeErr != nil {
		return "", fmt.Errorf("write shard: %w", writeErr)
	}
	return shardPath, nil
}

// readShards loads every *.json shard under ReportDir/shards/. The order of
// results is filesystem-dependent; callers that care about order must sort.
func readShards(reportDir string) ([]Result, error) {
	shardDir := filepath.Join(reportDir, "shards")
	entries, readErr := os.ReadDir(shardDir)
	if readErr != nil {
		return nil, fmt.Errorf("read shard directory %s: %w", shardDir, readErr)
	}
	results := make([]Result, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		body, fileErr := os.ReadFile(filepath.Join(shardDir, entry.Name()))
		if fileErr != nil {
			return nil, fmt.Errorf("read shard %s: %w", entry.Name(), fileErr)
		}
		var result Result
		if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr != nil {
			return nil, fmt.Errorf("unmarshal shard %s: %w", entry.Name(), unmarshalErr)
		}
		results = append(results, result)
	}
	return results, nil
}

func summarizeLatencies(latencies []time.Duration, elapsed time.Duration) (LatencyStats, float64) {
	if len(latencies) == 0 {
		return LatencyStats{}, 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, latency := range sorted {
		total += latency
	}
	stats := LatencyStats{
		P50Ms: percentile(sorted, 0.50),
		P90Ms: percentile(sorted, 0.90),
		P95Ms: percentile(sorted, 0.95),
		P99Ms: percentile(sorted, 0.99),
		MaxMs: float64(sorted[len(sorted)-1].Microseconds()) / 1000,
		MeanMs: float64(total.Microseconds()) / 1000 / float64(len(sorted)),
	}
	qps := 0.0
	if elapsed > 0 {
		qps = float64(len(latencies)) / elapsed.Seconds()
	}
	return stats, qps
}

func percentile(sorted []time.Duration, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Microseconds()) / 1000
}

func writeReport(report Report, dir string) (string, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	jsonPath := filepath.Join(dir, "distributed-querybench.json")
	jsonBody, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return "", "", marshalErr
	}
	if writeErr := os.WriteFile(jsonPath, append(jsonBody, '\n'), 0o644); writeErr != nil {
		return "", "", writeErr
	}
	mdPath := filepath.Join(dir, "distributed-querybench.md")
	if writeErr := os.WriteFile(mdPath, []byte(renderMarkdown(report)), 0o644); writeErr != nil {
		return "", "", writeErr
	}
	return jsonPath, mdPath, nil
}

func renderMarkdown(report Report) string {
	var b strings.Builder
	b.WriteString("# Distributed Query Benchmark\n\n")
	b.WriteString(fmt.Sprintf("Generated: %s\n\n", report.GeneratedAt.Format(time.RFC3339)))
	b.WriteString("## Per-mode results\n\n")
	b.WriteString("| Scenario | Cardinality | Mode | p50 ms | p95 ms | p99 ms | QPS | Rows | Mallocs/query | Bytes/query |\n")
	b.WriteString("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, result := range report.Results {
		b.WriteString(fmt.Sprintf("| %s | %d | %s | %.2f | %.2f | %.2f | %.2f | %d | %.2f | %.2f |\n",
			result.Scenario, result.Cardinality, result.Mode, result.Latency.P50Ms, result.Latency.P95Ms,
			result.Latency.P99Ms, result.QPS, result.ResponseRows, result.Allocations.MallocsPerQuery, result.Allocations.AllocBytesPerQuery))
	}
	renderRatioTable(&b, report)
	b.WriteString("\n## Profile artifacts\n\n")
	for _, result := range report.Results {
		if len(result.Profiles) == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf("- %s/%d/%s:\n", result.Scenario, result.Cardinality, result.Mode))
		keys := make([]string, 0, len(result.Profiles))
		for key := range result.Profiles {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			b.WriteString(fmt.Sprintf("  - %s: `%s`\n", key, result.Profiles[key]))
		}
	}
	return b.String()
}

func renderRatioTable(b *strings.Builder, report Report) {
	type modeKey struct {
		scenario    Scenario
		cardinality int
	}
	byMode := make(map[modeKey]map[string]Result)
	for _, result := range report.Results {
		key := modeKey{result.Scenario, result.Cardinality}
		if byMode[key] == nil {
			byMode[key] = make(map[string]Result)
		}
		byMode[key][result.Mode] = result
	}
	keys := make([]modeKey, 0, len(byMode))
	for key := range byMode {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].scenario != keys[right].scenario {
			return keys[left].scenario < keys[right].scenario
		}
		return keys[left].cardinality < keys[right].cardinality
	})
	b.WriteString("\n## Vec/Row Ratios\n\n")
	b.WriteString("Vec divided by row; < 1.00x means vec is faster or lighter.\n\n")
	b.WriteString("| Scenario | Cardinality | p50 | p95 | p99 | QPS | CPU sec | RSS | Mallocs/query | Bytes/query |\n")
	b.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, key := range keys {
		rowResult, rowOk := byMode[key][modeRow]
		vecResult, vecOk := byMode[key][modeVec]
		if !rowOk || !vecOk {
			continue
		}
		b.WriteString(fmt.Sprintf("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			key.scenario,
			key.cardinality,
			ratioString(vecResult.Latency.P50Ms, rowResult.Latency.P50Ms),
			ratioString(vecResult.Latency.P95Ms, rowResult.Latency.P95Ms),
			ratioString(vecResult.Latency.P99Ms, rowResult.Latency.P99Ms),
			ratioString(vecResult.QPS, rowResult.QPS),
			ratioString(vecResult.Resources.CPUSecondsDelta, rowResult.Resources.CPUSecondsDelta),
			ratioString(float64(vecResult.Resources.RSSBytes), float64(rowResult.Resources.RSSBytes)),
			ratioString(vecResult.Allocations.MallocsPerQuery, rowResult.Allocations.MallocsPerQuery),
			ratioString(vecResult.Allocations.AllocBytesPerQuery, rowResult.Allocations.AllocBytesPerQuery),
		))
	}
}

func ratioString(numerator, denominator float64) string {
	if denominator == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", numerator/denominator)
}

func detectCgroupVersion() string {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		return "v2"
	}
	if _, err := os.Stat("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		return "v1"
	}
	return "unknown"
}

func detectContainerID() string {
	body, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) < 3 {
			continue
		}
		path := fields[2]
		parts := strings.Split(path, "/")
		for idx := len(parts) - 1; idx >= 0; idx-- {
			part := strings.TrimPrefix(parts[idx], "docker-")
			part = strings.TrimSuffix(part, ".scope")
			if len(part) >= 12 {
				return part
			}
		}
	}
	return ""
}
