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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP2CoverageRejectsMissingAndChangedOracle(t *testing.T) {
	s, err := Generate()
	require.NoError(t, err)
	require.Len(t, s.ShardAggregateScenarios, 3)
	require.Len(t, s.ShardAggregateScenarios[0].Cases, 9)
	require.Len(t, s.ShardAggregateScenarios[1].Cases, 9)
	require.Len(t, s.ShardAggregateScenarios[2].Cases, 6)
	s.ShardAggregateScenarios[0].Cases[0].WantJSON = []byte(`{"dataPoints":[]}`)
	require.ErrorContains(t, s.Validate(), "P2")
}

func TestP2FixtureOracleLiteralTotals(t *testing.T) {
	points := p2Oracle(false, 3, "all")
	fields := points[0].(map[string]any)["fields"].([]any)
	for index, value := range []int64{8192, 4096, 1024} {
		require.Equal(t, jsonValue(value), fields[index].(map[string]any)["value"])
	}
	points = p2Oracle(false, 3, "selected")
	fields = points[0].(map[string]any)["fields"].([]any)
	for index, value := range []int64{16, 8, 2} {
		require.Equal(t, jsonValue(value), fields[index].(map[string]any)["value"])
	}
	require.Empty(t, p2Oracle(true, 3, "empty"))
	c, err := P2ContributionReadiness()
	require.NoError(t, err)
	require.Contains(t, string(c.WantJSON), `"services"`)
}
