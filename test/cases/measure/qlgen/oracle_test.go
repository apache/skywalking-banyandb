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

package qlgen

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestBuildCasePredicateTruthTables(t *testing.T) {
	tests := []struct {
		name, column, op string
		values           []any
		ids              []string
	}{
		{"eq", "rid", "=", []any{3}, []string{"3"}},
		{"ne", "rid", "!=", []any{3}, []string{"1", "2", "4", "5", "6", "7", "8"}},
		{"lt", "rid", "<", []any{3}, []string{"1", "2"}},
		{"le", "rid", "<=", []any{3}, []string{"1", "2", "3"}},
		{"gt", "rid", ">", []any{6}, []string{"7", "8"}},
		{"ge", "rid", ">=", []any{6}, []string{"6", "7", "8"}},
		{"in", "host", "IN", []any{"a", "d"}, []string{"1", "2", "7", "8"}},
		{"empty-in", "host", "IN", []any{}, nil},
		{"empty-not-in", "host", "NOT IN", []any{}, []string{"1", "2", "3", "4", "5", "6", "7", "8"}},
		{"not-in", "host", "NOT IN", []any{"a", "d"}, []string{"3", "4", "5", "6"}},
		{"having-string", "labels", "HAVING", []any{"red", "blue"}, []string{"1", "8"}},
		{"not-having-string", "labels", "NOT HAVING", []any{"red", "blue"}, []string{"2", "3", "4", "5", "6", "7"}},
		{"having-int", "nums", "HAVING", []any{1, 2}, []string{"1", "8"}},
		{"match", "host", "MATCH", []any{"c"}, []string{"5", "6"}},
		{"empty-string", "zone", "=", []any{""}, []string{"6"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generated, err := BuildCase(test.name, Query{
				Columns: []string{"rid"}, BeginMillis: -1, EndMillis: 8000,
				Where: &Predicate{Column: test.column, Op: test.op, Values: test.values},
			})
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				DataPoints []struct {
					TagFamilies []struct {
						Tags []struct {
							Value struct{ Int struct{ Value string } }
						}
					}
				}
			}
			if err = json.Unmarshal(generated.WantJSON, &decoded); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, point := range decoded.DataPoints {
				got = append(got, point.TagFamilies[0].Tags[0].Value.Int.Value)
			}
			if !reflect.DeepEqual(got, test.ids) {
				t.Fatalf("got %v, want %v", got, test.ids)
			}
		})
	}
}

func TestBuildCaseAggregatesComputeFixtureValues(t *testing.T) {
	tests := []struct {
		function string
		want     []string
	}{
		{"SUM", []string{"2", "4", "6", "6"}},
		{"MIN", []string{"0", "4", "6", "-2"}},
		{"MAX", []string{"2", "4", "6", "8"}},
		{"COUNT", []string{"2", "1", "1", "2"}},
		{"AVG", []string{"1", "4", "6", "3"}},
		{"MEAN", []string{"1", "4", "6", "3"}},
		{"COUNT_DISTINCT", []string{"2", "1", "1", "2"}},
	}
	for _, test := range tests {
		t.Run(test.function, func(t *testing.T) {
			query := Query{
				Columns: []string{"host", "v::field"}, Aggregate: test.function, Target: "v",
				GroupBy: []string{"host"}, BeginMillis: -1, EndMillis: 8000,
			}
			generated, err := BuildCase("aggregate", query)
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				DataPoints []struct {
					Fields []struct {
						Value struct{ Int struct{ Value string } }
					}
				}
			}
			if err = json.Unmarshal(generated.WantJSON, &decoded); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, point := range decoded.DataPoints {
				got = append(got, point.Fields[0].Value.Int.Value)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got %v want %v", got, test.want)
			}
		})
	}
}

func TestBuildCaseTopRequiresUntiedValues(t *testing.T) {
	count := uint32(2)
	generated, err := BuildCase("top", Query{
		Columns: []string{"rid", "v::field"}, Target: "v", Top: &count, Direction: "DESC", BeginMillis: -1, EndMillis: 8000,
		Where: &Predicate{Column: "rid", Op: "IN", Values: []any{1, 2, 3, 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		DataPoints []struct {
			Fields []struct {
				Value struct{ Int struct{ Value string } }
			}
		}
	}
	if err = json.Unmarshal(generated.WantJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.DataPoints) != 2 || decoded.DataPoints[0].Fields[0].Value.Int.Value != "6" || decoded.DataPoints[1].Fields[0].Value.Int.Value != "4" {
		t.Fatal(string(generated.WantJSON))
	}
	if !generated.Ordered {
		t.Fatal("TOP must check rank order")
	}
	_, err = BuildCase("null-top", Query{Columns: []string{"v::field"}, Target: "v", Top: &count, BeginMillis: -1, EndMillis: 8000})
	if err == nil {
		t.Fatal("null ranking has no oracle contract")
	}
}
