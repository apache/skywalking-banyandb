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
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestQLBT16MergeRetainsEveryShardVariant(t *testing.T) {
	dir := t.TempDir()
	expected := map[string]Result{}
	for process, card := range []int{1024, 32768} {
		shard := qlbT16FixtureReport(card, uint64(process+1))
		if _, err := writeShard(shard, dir); err != nil {
			t.Fatal(err)
		}
		for _, variant := range shard.Variants {
			expected[fmt.Sprintf("%d/%s", card, variant.Variant)] = variant
		}
	}
	// Exercise the actual orchestrator merge boundary, including reading shards,
	// validation, variant flattening and writing the final report.
	runMergeShards(t, Config{Engine: engineMeasure, ReportDir: dir, QueryWorkers: 1, Profile: true})
	body, err := os.ReadFile(filepath.Join(dir, "distributed-querybench.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err = json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != len(expected) {
		t.Fatalf("merged %d variants, expected %d", len(report.Results), len(expected))
	}
	for _, variant := range report.Results {
		key := fmt.Sprintf("%d/%s", variant.Cardinality, variant.Variant)
		original, exists := expected[key]
		if !exists {
			t.Fatalf("unexpected or duplicated merged variant %s", key)
		}
		if !reflect.DeepEqual(variant, original) {
			t.Fatalf("merged variant %s changed raw samples, metrics or result evidence", key)
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		t.Fatalf("merge dropped %d variants", len(expected))
	}
}
