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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT10QueriesAndFrozenSchedule(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, rows := range []int{1024, 32768} {
		iterations, warmup, err := qlbT10Schedule(rows)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT10,
			Cardinality: rows, QueryWorkers: 1, QueryIterations: iterations, WarmupIterations: warmup, Writers: 4, ByIDIterations: 1000, QueryMemoryMiB: 256,
		}
		if err = cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, name := range qlbT10Variants {
			request, queryErr := qlbT10Query(name, rows, base)
			if queryErr != nil {
				t.Fatal(queryErr)
			}
			prepared, prepareErr := bydbql.Prepare(request.Query)
			if prepareErr != nil {
				t.Fatal(prepareErr)
			}
			if _, bindErr := prepared.Bind(request.Params); bindErr != nil {
				t.Fatal(bindErr)
			}
		}
		cfg.QueryIterations++
		if cfg.Validate() == nil {
			t.Fatal("changed T10 protocol must be rejected")
		}
	}
	shifted, err := qlbT10ExpectedValue("arithmetic", 1)
	if err != nil || shifted.GetInt().Value != -45 {
		t.Fatalf("row1 shifted literal: %v %v", shifted, err)
	}
	cast, err := qlbT10ExpectedValue("parameter_cast", 1)
	if err != nil || cast.GetFloat().Value != -45.75 {
		t.Fatalf("row1 promoted literal: %v %v", cast, err)
	}
	nullFallback, err := qlbT10ExpectedValue("masked_case_coalesce", 0)
	if err != nil || nullFallback.GetInt().Value != -1 {
		t.Fatalf("row0 null default: %v %v", nullFallback, err)
	}
}
