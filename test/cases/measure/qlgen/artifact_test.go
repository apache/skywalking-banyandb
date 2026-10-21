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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/test/cases/measure/qlgen"
)

func TestGeneratedArtifact(t *testing.T) {
	require.NoError(t, qlgen.Check("generated"))
	directory := t.TempDir()
	require.NoError(t, qlgen.Write(directory))
	require.NoError(t, qlgen.Check(directory))
	content, err := os.ReadFile(filepath.Join(directory, "suite.json"))
	require.NoError(t, err)
	suite, err := qlgen.Load(content)
	require.NoError(t, err)
	require.NotEmpty(t, suite.Cases)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "suite.json"), []byte("{}"), 0o600))
	require.ErrorContains(t, qlgen.Check(directory), "stale")
}

func TestGeneratedSuiteRejectsMissingCoverage(t *testing.T) {
	for _, name := range []string{"fixture", "feature", "case", "pair", "dimension", "count"} {
		t.Run(name, func(t *testing.T) {
			suite, err := qlgen.Generate()
			require.NoError(t, err)
			switch name {
			case "fixture":
				suite.FixtureDigest = "changed"
			case "feature":
				suite.Features = suite.Features[1:]
			case "case":
				suite.Cases = append(suite.Cases, suite.Cases[0])
			case "pair":
				for index := range suite.Cases {
					suite.Cases[index].Dimensions = nil
				}
			case "dimension":
				delete(suite.Pairs.Dimensions, "filter")
			case "count":
				suite.Pairs.Covered++
			}
			content, err := json.Marshal(suite)
			require.NoError(t, err)
			_, err = qlgen.Load(content)
			require.Error(t, err)
		})
	}
}

func TestGeneratedSuccessQueriesParseAndBind(t *testing.T) {
	suite, err := qlgen.Generate()
	require.NoError(t, err)
	for _, c := range suite.Cases {
		if c.WantCode != codes.OK {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			rendered, renderErr := qlgen.Render(c.Query, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
			require.NoError(t, renderErr)
			query, params, extractErr := helpers.ExtractQL(rendered)
			require.NoError(t, extractErr)
			grammar, parseErr := bydbql.ParseQuery(query)
			require.NoError(t, parseErr)
			require.NoError(t, bydbql.BindParams(grammar, params))
		})
	}
}
