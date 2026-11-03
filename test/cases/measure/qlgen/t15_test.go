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

package qlgen

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestT15GeneratedBoundedDistinctInputContracts(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	contracts := map[string][]uint64{
		"t15_limit_constant":       {1, 1, 0},
		"t15_limit_filtered":       {1, 0, 0},
		"t15_offset_past_constant": {2201, 8, 0},
	}
	for name, inputs := range contracts {
		t.Run(name, func(t *testing.T) {
			var cases []Case
			for _, scenario := range suite.DistinctScenarios {
				if scenario.Name == name {
					cases = scenario.Cases
				}
			}
			require.Len(t, cases, len(inputs), "bounded DISTINCT witness is required")
			for ordinal, c := range cases {
				encoded, encodeErr := json.Marshal(c)
				require.NoError(t, encodeErr)
				var contract map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(encoded, &contract))
				var examined uint64
				require.NoError(t, json.Unmarshal(contract["distinct_inputs"], &examined))
				require.Equal(t, inputs[ordinal], examined)
				require.Contains(t, c.Query, "LIMIT 1")
				require.NotContains(t, c.Query, "ORDER BY")
			}
		})
	}
}

func TestT15GeneratedDistinctDomain(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	require.NotEmpty(t, suite.DistinctScenarios)
	for _, scenario := range suite.DistinctScenarios {
		require.Len(t, scenario.Cases, 3)
		if scenario.Name == "t15_having_hidden_count" {
			for _, c := range scenario.Cases {
				require.Contains(t, c.Query, "SUM(v::FIELD)")
				require.Contains(t, c.Query, "COUNT(v::FIELD)")
				require.Contains(t, c.Features, "t15.aggregate_argument_qualifier")
			}
		}
		require.Equal(t, scenario.Cases[0].Query[:len("SELECT DISTINCT")], "SELECT DISTINCT")
	}
}

func TestT15IndependentTupleOracle(t *testing.T) {
	columns := []t10Column{t10Field("left", t10Source("host")), t10Field("right", t10Source("zone"))}
	q := t15Query{Columns: columns, Begin: -1, End: 300000}
	c, err := buildT15Case("tuple", q)
	require.NoError(t, err)
	require.Len(t, t03ExpectedPoints(t, c), 135)
	q.Columns = []t10Column{t10Field("zone", t10Source("zone"))}
	c, err = buildT15Case("zone", q)
	require.NoError(t, err)
	require.Len(t, t03ExpectedPoints(t, c), 6)
	q.Columns = []t10Column{t10Field("number", t10Source("f"))}
	q.End = 800
	c, err = buildT15Case("number", q)
	require.NoError(t, err)
	require.Len(t, t03ExpectedPoints(t, c), 3, "NULL differs from both signed zero representations")
}

func TestT15FixtureWritesIndependentNumericWitnesses(t *testing.T) {
	writes := T15FixtureWrites(time.Unix(0, 0))
	require.Len(t, writes, 2201)
	require.Equal(t, uint64(0), math.Float64bits(writes[0].DataPoint.Fields[1].GetFloat().Value))
	require.Equal(t, uint64(1)<<63, math.Float64bits(writes[1].DataPoint.Fields[1].GetFloat().Value))
	require.Equal(t, uint64(0x7ff8000000000001), math.Float64bits(writes[6].DataPoint.Fields[1].GetFloat().Value))
	require.Equal(t, uint64(0x7ff80000000000ff), math.Float64bits(writes[7].DataPoint.Fields[1].GetFloat().Value))
}

func TestT15InventoryRejectsLostWitnesses(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	suite.DistinctScenarios = suite.DistinctScenarios[1:]
	require.ErrorContains(t, suite.Validate(), "T15 bounded capability witnesses missing")
}

func TestT15ZoneFixtureUsesTypedResidualSelection(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	for _, scenario := range suite.DistinctScenarios {
		if scenario.Name != "t15_zone_F" {
			continue
		}
		for _, c := range scenario.Cases[:2] {
			require.Contains(t, c.Query, "CAST(rid AS INT)")
			points := t03ExpectedPoints(t, c)
			require.Len(t, points, 4)
			var values []any
			for _, point := range points {
				family := point["tagFamilies"].([]any)[0].(map[string]any)
				tag := family["tags"].([]any)[0].(map[string]any)
				values = append(values, tag["value"])
			}
			require.ElementsMatch(t, []any{map[string]any{"null": float64(0)}, jsonValue(""), jsonValue("x"), jsonValue("y")}, values)
		}
		return
	}
	t.Fatal("zone fixture scenario missing")
}
