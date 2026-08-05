// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRenderHTMLUsesDiagramsAsPrimaryReportSurface(t *testing.T) {
	suite := SuiteReport{
		GeneratedAt: time.Date(2026, time.August, 5, 0, 0, 0, 0, time.UTC), Commit: "abc123", OneShardOnly: true,
		MaximumRate: 1000, FrozenRate: 700,
		Sweep:          []SweepPoint{{Acceleration: 1000, Sustainable: true}},
		ThroughputRuns: []RunReport{{RunID: "run-1", Correct: true, HotMerges: 12, MatureMerges: 14}},
	}
	var output bytes.Buffer
	require.NoError(t, RenderHTML(&output, suite))
	html := output.String()
	require.Contains(t, html, "ONE SHARD /")
	require.Contains(t, html, "ORDINARY MERGE")
	require.Contains(t, html, "id=\"sweep-diagram\"")
	require.Contains(t, html, "id=\"variance-diagram\"")
	require.Contains(t, html, "id=\"backlog-diagram\"")
	require.Contains(t, html, "id=\"flow-diagram\"")
	require.Contains(t, html, "LOGICAL LEDGER")
	require.Contains(t, html, "overallReady=gates.every")
	require.Contains(t, html, "application/json")
	require.GreaterOrEqual(t, strings.Count(html, "<svg"), 4)
}
