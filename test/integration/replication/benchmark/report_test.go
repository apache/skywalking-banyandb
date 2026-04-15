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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteReport(t *testing.T) {
	dir := t.TempDir()
	report := BenchReport{
		GeneratedAt: time.Now(),
		Config:      LoadConfig(),
		Results: []RFResult{{
			ReplicationFactor: 1,
			Write:             WriteResult{TotalPoints: 10, DurationSec: 1, ThroughputPps: 10},
			Read:              ReadResult{Samples: 1, MinMs: 1, MedianMs: 1, P95Ms: 1, P99Ms: 1, MaxMs: 1},
		}},
	}
	path, err := writeReport(report, dir)
	if err != nil {
		t.Fatalf("writeReport: %v", err)
	}
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var parsed BenchReport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if len(parsed.Results) != 1 {
		t.Fatalf("expected 1 result got %d", len(parsed.Results))
	}
}
