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

package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BenchReport is the top-level JSON report artifact for a benchmark run.
type BenchReport struct {
	GeneratedAt time.Time  `json:"generated_at"`
	Results     []RFResult `json:"results"`
	Config      Config     `json:"config"`
}

// RFResult stores one replication-factor benchmark result.
type RFResult struct {
	ReplicationFactor int            `json:"replication_factor"`
	Write             WriteResult    `json:"write"`
	Read              ReadResult     `json:"read"`
	Resources         ResourceReport `json:"resources"`
}

// WriteResult describes write phase throughput and duration.
type WriteResult struct {
	TotalPoints   int     `json:"total_points"`
	DurationSec   float64 `json:"duration_sec"`
	ThroughputPps float64 `json:"throughput_points_per_sec"`
}

// ReadResult summarizes read latency percentiles.
type ReadResult struct {
	Samples  int     `json:"samples"`
	MinMs    float64 `json:"min_ms"`
	MedianMs float64 `json:"median_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
	MaxMs    float64 `json:"max_ms"`
}

// ResourceReport contains write/read phase resource usage.
type ResourceReport struct {
	WritePhase ResourcePhase `json:"write_phase"`
	ReadPhase  ResourcePhase `json:"read_phase"`
}

// ResourcePhase stores resource summaries for liaison and data roles.
type ResourcePhase struct {
	Liaison ResourceStats `json:"liaison"`
	Data    ResourceStats `json:"data"`
}

// ResourceStats contains CPU and RSS summary metrics.
type ResourceStats struct {
	PeakRSSBytes   int64   `json:"peak_rss_bytes"`
	PeakRSSPercent float64 `json:"peak_rss_percent"`
	MeanCPUPercent float64 `json:"mean_cpu_percent"`
	PeakCPUPercent float64 `json:"peak_cpu_percent"`
}

func writeReport(report BenchReport, dir string) (string, error) {
	if dir == "" {
		resolved, err := resolveDefaultReportDir()
		if err != nil {
			return "", err
		}
		dir = resolved
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	filename := fmt.Sprintf("banyandb-benchmark-%s.json", time.Now().Format("20060102-150405"))
	path := filepath.Join(dir, filename)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func resolveDefaultReportDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	root := findRepoRoot(wd)
	if root == "" {
		return os.TempDir(), nil
	}
	return filepath.Dir(root), nil
}

func findRepoRoot(start string) string {
	dir := start
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func formatRFResultSummary(result RFResult) string {
	template := strings.Join([]string{
		"RF=%d",
		"write: total=%d points duration=%.2fs throughput=%.2f points/s",
		"read: samples=%d min=%.2fms median=%.2fms p95=%.2fms p99=%.2fms max=%.2fms",
		"resources(write): liaison cpu(mean/peak)=%.2f%%/%.2f%% " +
			"rss(peak)=%.2f%% (%d bytes), data cpu(mean/peak)=%.2f%%/%.2f%% " +
			"rss(peak)=%.2f%% (%d bytes)",
		"resources(read): liaison cpu(mean/peak)=%.2f%%/%.2f%% " +
			"rss(peak)=%.2f%% (%d bytes), data cpu(mean/peak)=%.2f%%/%.2f%% " +
			"rss(peak)=%.2f%% (%d bytes)",
	}, "\n")

	return fmt.Sprintf(
		template,
		result.ReplicationFactor,
		result.Write.TotalPoints,
		result.Write.DurationSec,
		result.Write.ThroughputPps,
		result.Read.Samples,
		result.Read.MinMs,
		result.Read.MedianMs,
		result.Read.P95Ms,
		result.Read.P99Ms,
		result.Read.MaxMs,
		result.Resources.WritePhase.Liaison.MeanCPUPercent,
		result.Resources.WritePhase.Liaison.PeakCPUPercent,
		result.Resources.WritePhase.Liaison.PeakRSSPercent,
		result.Resources.WritePhase.Liaison.PeakRSSBytes,
		result.Resources.WritePhase.Data.MeanCPUPercent,
		result.Resources.WritePhase.Data.PeakCPUPercent,
		result.Resources.WritePhase.Data.PeakRSSPercent,
		result.Resources.WritePhase.Data.PeakRSSBytes,
		result.Resources.ReadPhase.Liaison.MeanCPUPercent,
		result.Resources.ReadPhase.Liaison.PeakCPUPercent,
		result.Resources.ReadPhase.Liaison.PeakRSSPercent,
		result.Resources.ReadPhase.Liaison.PeakRSSBytes,
		result.Resources.ReadPhase.Data.MeanCPUPercent,
		result.Resources.ReadPhase.Data.PeakCPUPercent,
		result.Resources.ReadPhase.Data.PeakRSSPercent,
		result.Resources.ReadPhase.Data.PeakRSSBytes,
	)
}
