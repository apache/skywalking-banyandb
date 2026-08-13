// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a copy
// of the License at
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRenderComparisonHTMLShowsPluginBenefitAndSystemCost(t *testing.T) {
	fixture := "fixture-sha"
	schedule := "schedule-sha"
	comparison := ComparisonReport{
		GeneratedAt: time.Date(2026, time.August, 11, 0, 0, 0, 0, time.UTC),
		Baseline:    SuiteReport{FixtureSHA256: fixture, ScheduleSHA256: schedule, SerialRuns: []RunReport{{RunID: "baseline-1", Correct: true}}},
		SkyWalking:  SuiteReport{FixtureSHA256: fixture, ScheduleSHA256: schedule, SerialRuns: []RunReport{{RunID: "skywalking-1", Correct: true}}},
	}
	var output bytes.Buffer
	require.NoError(t, RenderComparisonHTML(&output, comparison))
	html := output.String()
	require.Contains(t, html, "PLUGIN COST /")
	require.Contains(t, html, "Data removed")
	require.Contains(t, html, "Plugin call latency histogram")
	require.Contains(t, html, "System cost comparison")
	require.Contains(t, html, "Merge execution")
	require.Contains(t, html, "On-disk result")
	require.Contains(t, html, "READ I/O MiB")
	require.Contains(t, html, "Logical write amplification")
	require.Contains(t, html, "SAME FIXTURE")
	require.Contains(t, html, "durationBuckets")
	require.Contains(t, html, "comparison-data")
}
