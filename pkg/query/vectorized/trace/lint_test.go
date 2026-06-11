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

package trace

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoConcurrencyInVectorizedTrace is a static-analysis guard that ensures no
// non-test source file in pkg/query/vectorized/trace/ spawns goroutines or
// references sidx.StreamingQuery (both forbidden by the plan's H1 constraint).
func TestNoConcurrencyInVectorizedTrace(t *testing.T) {
	entries, readErr := os.ReadDir(".")
	require.NoError(t, readErr)

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		content, fileErr := os.ReadFile(name)
		require.NoError(t, fileErr)
		src := string(content)

		require.False(
			t,
			containsGoroutineSpawn(src),
			"%s must not spawn goroutines — vectorized trace operators use the synchronous pull path only", name,
		)
		require.NotContains(
			t,
			src,
			"StreamingQuery",
			"%s must not reference sidx.StreamingQuery — use the synchronous Query/QuerySync path instead", name,
		)
	}
}

// containsGoroutineSpawn returns true if src contains a goroutine spawn statement
// on any non-comment line: "go func(", "go someIdent(", "go obj.Method(", or "run.Go(".
func containsGoroutineSpawn(src string) bool {
	for line := range strings.SplitSeq(src, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		// "go <identifier>" covers "go func(", "go someFunc(", "go obj.Method(" etc.
		if strings.HasPrefix(trimmed, "go ") {
			rest := strings.TrimLeft(trimmed[3:], " ")
			if len(rest) > 0 {
				ch := rest[0]
				if ch == '_' || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
					return true
				}
			}
		}
		// "run.Go(" is the project's goroutine helper.
		if strings.Contains(trimmed, "run.Go(") {
			return true
		}
	}
	return false
}
