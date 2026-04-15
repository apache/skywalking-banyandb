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
	"strings"
	"testing"
	"time"
)

func TestParsePromMetrics(t *testing.T) {
	data := strings.NewReader(`process_cpu_seconds_total 12
process_resident_memory_bytes 2048
banyandb_system_cpu_num 4
banyandb_system_memory_state{kind="total"} 8192
`)
	metrics, err := parsePromMetrics(data)
	if err != nil {
		t.Fatalf("parsePromMetrics: %v", err)
	}
	if metrics.CPUTotalSeconds != 12 {
		t.Fatalf("cpu seconds expected 12 got %v", metrics.CPUTotalSeconds)
	}
	if metrics.RSSBytes != 2048 {
		t.Fatalf("rss expected 2048 got %v", metrics.RSSBytes)
	}
	if metrics.CPUNum != 4 {
		t.Fatalf("cpu num expected 4 got %v", metrics.CPUNum)
	}
	if metrics.TotalMemBytes != 8192 {
		t.Fatalf("total mem expected 8192 got %v", metrics.TotalMemBytes)
	}
}

func TestSummarizeSeries(t *testing.T) {
	series := AggregatedSeries{
		Samples: []AggregatedSample{
			{Timestamp: time.Unix(0, 0), CPUSeconds: 0, RSSBytes: 1024, CPUNum: 2, TotalMemory: 4096},
			{Timestamp: time.Unix(2, 0), CPUSeconds: 2, RSSBytes: 2048, CPUNum: 2, TotalMemory: 4096},
		},
	}
	stats := summarizeSeries(series)
	if stats.PeakRSSBytes != 2048 {
		t.Fatalf("peak rss expected 2048 got %d", stats.PeakRSSBytes)
	}
	if stats.PeakRSSPercent == 0 {
		t.Fatalf("expected rss percent > 0")
	}
	if stats.PeakCPUPercent == 0 {
		t.Fatalf("expected peak cpu percent > 0")
	}
}
