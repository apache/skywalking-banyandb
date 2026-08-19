// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStandaloneDefaultTracePipelineMergeGrace(t *testing.T) {
	service := &standalone{}

	flags := service.FlagSet()

	require.Equal(t, 2*time.Hour, service.option.mergeGraceDefault)
	require.Equal(t, 5*time.Minute, service.option.finalizeGraceDefault)
	for _, flagName := range []string{
		"trace-pipeline-native-plugin-enabled",
		"trace-pipeline-trusted-plugin-dir",
		"trace-pipeline-decide-timeout",
		"trace-pipeline-decide-timeout-circuit-break",
	} {
		require.NotNil(t, flags.Lookup(flagName), "expected pipeline flag %q", flagName)
	}
	for _, removedFlagName := range []string{
		"trace-pipeline-merge-grace-default",
		"trace-pipeline-max-fragment-gap",
		"trace-pipeline-finalize-grace-default",
	} {
		require.Nil(t, flags.Lookup(removedFlagName), "removed pipeline flag %q must not be registered", removedFlagName)
	}
}

func TestDefaultMergeGraceMaturityBoundary(t *testing.T) {
	const now = int64(10 * time.Hour)
	frontier := now - int64(defaultTracePipelineMergeGrace)
	testCases := []struct {
		name       string
		timestamp  int64
		wantMature bool
	}{
		{name: "one nanosecond inside grace", timestamp: frontier + 1},
		{name: "exactly at grace", timestamp: frontier, wantMature: true},
		{name: "one nanosecond beyond grace", timestamp: frontier - 1, wantMature: true},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			parts := []*partWrapper{
				{
					p: &part{
						partMetadata: partMetadata{MinTimestamp: testCase.timestamp, MaxTimestamp: testCase.timestamp},
					},
				},
			}

			require.Equal(t, testCase.wantMature, mergeMayContainMatureTrace(parts, frontier))
		})
	}
}
