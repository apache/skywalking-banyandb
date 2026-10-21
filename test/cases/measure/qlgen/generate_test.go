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

package qlgen_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func TestGenerateReproducibleQLSuite(t *testing.T) {
	first, err := qlgen.Generate()
	require.NoError(t, err)
	second, err := qlgen.Generate()
	require.NoError(t, err)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.GreaterOrEqual(t, len(first.Cases), 50)
	require.Equal(t, "generated_scan_all", first.Cases[0].Name)
	require.Equal(t, qlgen.FixtureDigest(), first.FixtureDigest)
	require.NotEmpty(t, first.Features)
	for _, feature := range first.Features {
		if feature.Status == "pending" {
			require.NotEmpty(t, feature.Reason, feature.ID)
		}
	}
}
