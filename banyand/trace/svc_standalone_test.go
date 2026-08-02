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

	service.FlagSet()

	require.Equal(t, 2*time.Hour, service.option.mergeGraceDefault)
	require.Zero(t, service.option.maxTraceFragmentGap)
}

func TestDefaultMergeGraceMaturityBoundary(t *testing.T) {
	const now = int64(10 * time.Hour)
	frontier := now - int64(defaultTracePipelineMergeGrace)
	testCases := []struct {
		name         string
		maxTimestamp int64
		wantHot      bool
	}{
		{name: "one nanosecond inside grace", maxTimestamp: frontier + 1, wantHot: true},
		{name: "exactly at grace", maxTimestamp: frontier},
		{name: "one nanosecond beyond grace", maxTimestamp: frontier - 1},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			parts := []*partWrapper{
				{
					p: &part{
						partMetadata: partMetadata{MaxTimestamp: testCase.maxTimestamp},
					},
				},
			}

			require.Equal(t, testCase.wantHot, isMergeHot(parts, int64(defaultTracePipelineMergeGrace), now))
		})
	}
}

func TestStandaloneRejectsNegativeTraceFragmentDurations(t *testing.T) {
	testCases := []struct {
		mutate  func(*standalone)
		name    string
		message string
	}{
		{
			name: "negative merge grace",
			mutate: func(service *standalone) {
				service.option.mergeGraceDefault = -time.Nanosecond
			},
			message: "trace-pipeline-merge-grace-default must not be negative",
		},
		{
			name: "negative maximum fragment gap",
			mutate: func(service *standalone) {
				service.option.maxTraceFragmentGap = -time.Nanosecond
			},
			message: "trace-pipeline-max-fragment-gap must not be negative",
		},
	}

	for testCaseIdx := range testCases {
		testCase := testCases[testCaseIdx]
		t.Run(testCase.name, func(t *testing.T) {
			service := &standalone{root: t.TempDir()}
			service.FlagSet()
			testCase.mutate(service)

			require.EqualError(t, service.Validate(), testCase.message)
		})
	}
}
