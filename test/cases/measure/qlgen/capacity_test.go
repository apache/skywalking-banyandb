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

func TestP1FixtureAndLiteralOracles(t *testing.T) {
	rows := P1FixtureWrites(P1BaseTime())
	require.Len(t, rows, 10240)
	schema := P1FixtureSchema()
	require.Equal(t, "storage-only", schema.TagFamilies[0].Name)
	require.Equal(t, "searchable", schema.TagFamilies[1].Name)
	require.Equal(t, []string{"entity_id"}, schema.Entity.TagNames)
	require.Len(t, P1IndexRules(), 1)
	require.Equal(t, []string{"service_id"}, P1IndexRules()[0].Tags)
	for _, size := range []int{10, 1000, 10000} {
		require.Equal(t, int64(32), p1Count(P1List(size, 8), false))
		require.Equal(t, int64(4064), p1Count(P1List(size, 8), true))
	}
	require.Equal(t, int64(28), p1Count(P1List(10, 7), false))
	require.Zero(t, p1Count(P1List(10, 0), false))
	require.Equal(t, int64(32), p1Count(append(P1List(10, 8), P1List(10, 8)...), false))
	for _, branches := range []int{2, 16} {
		require.Equal(t, []int64{1024, 3072}, p1Categories(P1Memberships(branches, "normal")))
		require.Equal(t, []int64{3072, 1024}, p1Categories(P1Memberships(branches, "swap")))
		require.Equal(t, []int64{1028, 3068}, p1Categories(P1Memberships(branches, "overlap")))
	}
}

func TestP1CoverageRejectsMissingAndChangedOracle(t *testing.T) {
	s, err := Generate()
	require.NoError(t, err)
	require.Len(t, s.CapacityScenarios, 4)
	s.CapacityScenarios[0].Cases[0].WantJSON = []byte(`{"dataPoints":[]}`)
	require.ErrorContains(t, s.Validate(), "P1")
}
