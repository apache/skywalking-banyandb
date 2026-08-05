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
		GeneratedAt: time.Date(2026, time.August, 5, 0, 0, 0, 0, time.UTC), Commit: "abc123", OneShardOnly: true, WriteIntensity: 2,
		SerialRuns: []RunReport{{
			RunID: "run-1", Mode: ModeSerial, Correct: true, HotMerges: 12, MatureMerges: 14,
			Status: []StatusPoint{{BarrierNanos: int64(time.Millisecond)}},
		}},
		DisabledEnabledAlternating: []ControlledMergeRunReport{{
			RunID: "disabled-1", PipelineMode: ControlledMergePipelineDisabled,
		}},
	}
	var output bytes.Buffer
	require.NoError(t, RenderHTML(&output, suite))
	html := output.String()
	require.Contains(t, html, "ONE SHARD /")
	require.Contains(t, html, "ORDINARY MERGE")
	require.Contains(t, html, "id=\"barrier-diagram\"")
	require.Contains(t, html, "id=\"variance-diagram\"")
	require.Contains(t, html, "id=\"backlog-diagram\"")
	require.Contains(t, html, "id=\"flow-diagram\"")
	require.Contains(t, html, "id=\"controlled-variance-diagram\"")
	require.Contains(t, html, "id=\"controlled-run-table\"")
	require.Contains(t, html, "STAGE CAP")
	require.Contains(t, html, "MAX TRACES")
	require.Contains(t, html, "disabledEnabledAlternating")
	require.Contains(t, html, "SAME TEST BOUNDARY")
	require.Contains(t, html, "CORRECT OUTPUT")
	require.Contains(t, html, "MATURE MERGE ROUNDS")
	require.Contains(t, html, "SUSTAINABLE EXECUTION")
	require.Contains(t, html, "production write intensity")
	require.Contains(t, html, "Mixed selections may appear in both hot and mature counts")
	require.Contains(t, html, "event.hotInputParts")
	require.Contains(t, html, "overallReady=readiness.ready===true")
	require.Contains(t, html, "application/json")
	require.GreaterOrEqual(t, strings.Count(html, "<svg"), 4)
}
