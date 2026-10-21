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

func TestLegacyScalarCountLiteralExpectations(t *testing.T) {
	suite, err := Generate()
	require.NoError(t, err)
	var found int
	for _, c := range suite.Cases {
		if c.Ticket != "LegacyScalar" {
			continue
		}
		found++
		switch c.Name {
		case "generated_legacy_scalar_count_nonempty":
			require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"rid","value":{"int":{"value":"8"}}}]}]}`, string(c.WantJSON))
		case "generated_legacy_scalar_count_idle", "generated_legacy_scalar_count_no_match":
			require.JSONEq(t, `{"dataPoints":[{"fields":[{"name":"rid","value":{"int":{"value":"0"}}}]}]}`, string(c.WantJSON))
		default:
			require.JSONEq(t, `{}`, string(c.WantJSON))
		}
	}
	require.Equal(t, 7, found)
}
