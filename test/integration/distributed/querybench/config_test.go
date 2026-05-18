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

import "testing"

func TestLoadConfigSingleShotFromEnv(t *testing.T) {
	t.Setenv(envRunBench, "true")
	t.Setenv(envInContainer, "1")
	t.Setenv(envMode, "vec")
	t.Setenv(envScenario, "scan_all")
	t.Setenv(envCardinality, "1024")
	t.Setenv(envQueryWorkers, "2")
	t.Setenv(envQueryIters, "7")
	t.Setenv(envWriters, "3")
	t.Setenv(envWarmupIters, "1")
	cfg := LoadConfig()
	if validateErr := cfg.Validate(); validateErr != nil {
		t.Fatalf("Validate() failed: %v", validateErr)
	}
	if !cfg.IsSingleShot() {
		t.Fatalf("expected IsSingleShot, got cfg=%+v", cfg)
	}
	if cfg.Mode != modeVec || cfg.Scenario != ScenarioScanAll || cfg.Cardinality != 1024 {
		t.Fatalf("unexpected single-shot selection: mode=%s scenario=%s cardinality=%d", cfg.Mode, cfg.Scenario, cfg.Cardinality)
	}
	if cfg.QueryWorkers != 2 || cfg.QueryIterations != 7 || cfg.Writers != 3 || cfg.WarmupIterations != 1 {
		t.Fatalf("unexpected worker/iteration settings: %+v", cfg)
	}
}

func TestLoadConfigMergeFromEnv(t *testing.T) {
	t.Setenv(envRunBench, "true")
	t.Setenv(envInContainer, "1")
	t.Setenv(envMerge, "1")
	cfg := LoadConfig()
	if validateErr := cfg.Validate(); validateErr != nil {
		t.Fatalf("Validate() failed: %v", validateErr)
	}
	if !cfg.Merge {
		t.Fatalf("expected Merge, got cfg=%+v", cfg)
	}
	if cfg.IsSingleShot() {
		t.Fatalf("merge mode should not be single-shot: %+v", cfg)
	}
}

func TestValidateRejectsDirectVMBenchmark(t *testing.T) {
	cfg := Config{
		RunBench:        true,
		InContainer:     false,
		Mode:            modeRow,
		Scenario:        ScenarioScanAll,
		Cardinality:     1024,
		QueryWorkers:    1,
		QueryIterations: 1,
		Writers:         1,
		SmallExactRows:  1,
	}
	if validateErr := cfg.Validate(); validateErr == nil {
		t.Fatalf("Validate() succeeded for direct VM benchmark")
	}
}

func TestValidateRejectsMissingComboSelection(t *testing.T) {
	cfg := Config{
		RunBench:        true,
		InContainer:     true,
		QueryWorkers:    1,
		QueryIterations: 1,
		Writers:         1,
		SmallExactRows:  1,
	}
	if validateErr := cfg.Validate(); validateErr == nil {
		t.Fatalf("Validate() succeeded with no Merge and no single-shot selection")
	}
}

func TestValidateRejectsPartialSingleShot(t *testing.T) {
	cfg := Config{
		RunBench:        true,
		InContainer:     true,
		Mode:            modeRow,
		QueryWorkers:    1,
		QueryIterations: 1,
		Writers:         1,
		SmallExactRows:  1,
	}
	if validateErr := cfg.Validate(); validateErr == nil {
		t.Fatalf("Validate() succeeded with only Mode set; Scenario and Cardinality are required too")
	}
}

func TestValidateRejectsMergePlusSingleShot(t *testing.T) {
	cfg := Config{
		RunBench:    true,
		InContainer: true,
		Merge:       true,
		Mode:        modeVec,
		Scenario:    ScenarioScanAll,
		Cardinality: 1024,
		ReportDir:   "/tmp/dqb",
	}
	if validateErr := cfg.Validate(); validateErr == nil {
		t.Fatalf("Validate() accepted Merge combined with single-shot fields")
	}
}

func TestValidateRejectsUnknownMode(t *testing.T) {
	cfg := Config{
		RunBench:        true,
		InContainer:     true,
		Mode:            "weird",
		Scenario:        ScenarioScanAll,
		Cardinality:     1024,
		QueryWorkers:    1,
		QueryIterations: 1,
		Writers:         1,
		SmallExactRows:  1,
	}
	if validateErr := cfg.Validate(); validateErr == nil {
		t.Fatalf("Validate() accepted unknown mode")
	}
}

func TestSplitCardinality(t *testing.T) {
	tests := []struct {
		name       string
		rows       int
		entities   int
		pointsEach int
	}{
		{name: "small", rows: 1024, entities: 32, pointsEach: 32},
		{name: "medium", rows: 10000, entities: 100, pointsEach: 100},
		{name: "large", rows: 100000, entities: 1000, pointsEach: 100},
		{name: "million", rows: 1000000, entities: 1000, pointsEach: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entities, pointsEach := splitCardinality(tt.rows)
			if entities != tt.entities || pointsEach != tt.pointsEach {
				t.Fatalf("splitCardinality(%d) = (%d,%d), want (%d,%d)", tt.rows, entities, pointsEach, tt.entities, tt.pointsEach)
			}
		})
	}
}
