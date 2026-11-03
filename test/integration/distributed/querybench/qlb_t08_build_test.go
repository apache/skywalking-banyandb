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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT08ConfigAndOwningQueries(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, cardinality := range []int{1024, 32768} {
		iterations, warmup, err := qlbT08Schedule(cardinality)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			RunBench: true, InContainer: true, Engine: engineMeasure, Mode: modeVec, Scenario: ScenarioQLBT08,
			Cardinality: cardinality, QueryWorkers: 1, QueryIterations: iterations, WarmupIterations: warmup, ByIDIterations: 1000, Writers: 4,
		}
		if err = cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*Config){
			func(c *Config) { c.QueryWorkers = 2 }, func(c *Config) { c.QueryIterations++ },
			func(c *Config) { c.WarmupIterations = 0 }, func(c *Config) { c.Cardinality = 100000 },
		} {
			changed := cfg
			mutate(&changed)
			if changed.Validate() == nil {
				t.Fatal("T08 accepted an unfrozen protocol")
			}
		}
		for _, shape := range qlbT08Shapes {
			request, queryErr := qlbT08Query(shape.name, cardinality, base)
			if queryErr != nil {
				t.Fatal(queryErr)
			}
			statement, prepareErr := bydbql.Prepare(request.Query)
			if prepareErr != nil {
				t.Fatal(prepareErr)
			}
			if _, bindErr := statement.Bind(request.Params); bindErr != nil {
				t.Fatal(bindErr)
			}
			oracle, oracleErr := newQLBT08Oracle(shape.name, cardinality)
			if oracleErr != nil {
				t.Fatal(oracleErr)
			}
			if _, checkErr := oracle.check(qlbT08Sample{ql: oracle.expected}); checkErr != nil {
				t.Fatal(checkErr)
			}
			corrupted := proto.Clone(oracle.expected).(*measurev1.QueryResponse)
			corrupted.DataPoints[0].Fields[0].Value = intFieldValue(999999)
			if _, checkErr := oracle.check(qlbT08Sample{ql: corrupted}); checkErr == nil {
				t.Fatal("independent T08 oracle accepted a wrong value/type")
			}
			branches, branchErr := qlbT08NativeQueries(shape.name, cardinality, base)
			if branchErr != nil {
				t.Fatal(branchErr)
			}
			if len(branches) != shape.branches {
				t.Fatalf("%s has %d branches, expected %d", shape.name, len(branches), shape.branches)
			}
			for _, branch := range branches {
				if branch.GetLimit() != ^uint32(0) || branch.GetOffset() != 0 || len(branch.GetGroups()) != 1 || branch.GetName() != qlbT07Measure ||
					branch.GetGroupBy().GetTagProjection().GetTagFamilies()[0].GetTags()[0] != "host" {
					t.Fatal("native control changed complete scan/group contract")
				}
			}
		}
	}
	oracle, err := newQLBT08Oracle("float_shared", 1024)
	if err != nil {
		t.Fatal(err)
	}
	first := oracle.expected.DataPoints[0].Fields
	// Host 0 has 64 rows; exactly ten are NULL for f. Its non-null sum is -4.25.
	if first[0].Value.GetFloat().Value != -4.25 || first[1].Value.GetInt().Value != 54 || first[2].Value.GetFloat().Value != -4.25/54 {
		t.Fatalf("1024-row host0 literal oracle changed: %v", first)
	}
}
