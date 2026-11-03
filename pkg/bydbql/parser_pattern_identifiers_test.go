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

package bydbql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestPatternWordsRemainSourceIdentifiers(t *testing.T) {
	for _, name := range []string{"like", "ilike", "escape", "like-rate", "ilike-rate", "escape-group"} {
		for _, catalog := range []string{"STREAM", "MEASURE", "TRACE", "PROPERTY"} {
			for _, position := range []string{"resource", "group", "stage"} {
				t.Run(catalog+"/"+position+"/"+name, func(t *testing.T) {
					resource, group, stage := "records", "metrics", "warm"
					switch position {
					case "resource":
						resource = name
					case "group":
						group = name
					case "stage":
						stage = name
					}
					grammar, err := bydbql.ParseQuery(fmt.Sprintf("SELECT * FROM %s %s IN %s ON %s STAGES", catalog, resource, group, stage))
					require.NoError(t, err)
					require.Equal(t, resource, grammar.Select.From.ResourceName)
					require.Equal(t, []string{group}, grammar.Select.From.In.Groups)
					require.Equal(t, []string{stage}, grammar.Select.From.Stage.Stages)
				})
			}
		}
		t.Run("topn/"+name, func(t *testing.T) {
			grammar, err := bydbql.ParseQuery(fmt.Sprintf("SHOW TOP 3 FROM MEASURE %s IN %s ON %s STAGES ORDER BY DESC", name, name, name))
			require.NoError(t, err)
			require.Equal(t, name, grammar.TopN.From.ResourceName)
			require.Equal(t, []string{name}, grammar.TopN.From.In.Groups)
			require.Equal(t, []string{name}, grammar.TopN.From.Stage.Stages)
		})
		for _, op := range []string{"LIKE", "ILIKE"} {
			for _, source := range []string{"MEASURE " + name + " IN " + name, name + "." + name} {
				t.Run(op+"/"+source, func(t *testing.T) {
					query := "SELECT host FROM " + source + " ON " + name + " STAGES WHERE host " + op + " ? ESCAPE '!'"
					execution := typedExecution(t, query, strParam("a!_b"))
					require.Equal(t, name, execution.Request.Name)
					require.Equal(t, []string{name}, execution.Request.Groups)
					require.Equal(t, []string{name}, execution.Request.Stages)
					require.Len(t, execution.Plan.Patterns, 1)
					require.Equal(t, "a!_b", execution.Plan.Patterns[0].Pattern)
					require.Equal(t, "!", execution.Plan.Patterns[0].Escape)
					require.Equal(t, op == "ILIKE", execution.Plan.Patterns[0].IgnoreCase)
				})
			}
		}
	}
}
