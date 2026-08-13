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

package main

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveSegmentTimeRange(t *testing.T) {
	tests := []struct {
		expectedStart time.Time
		expectedEnd   time.Time
		name          string
		errorContains string
		arguments     []string
	}{
		{name: "omitted"},
		{
			name: "both boundaries", arguments: []string{"--segment-min-time-nanos=1", "--segment-max-time-nanos=2"},
			expectedStart: time.Unix(0, 1), expectedEnd: time.Unix(0, 2),
		},
		{
			name: "epoch boundary remains explicit", arguments: []string{"--segment-min-time-nanos=0", "--segment-max-time-nanos=2"},
			expectedStart: time.Unix(0, 0), expectedEnd: time.Unix(0, 2),
		},
		{name: "minimum only", arguments: []string{"--segment-min-time-nanos=1"}, errorContains: "must be provided together"},
		{name: "maximum only", arguments: []string{"--segment-max-time-nanos=2"}, errorContains: "must be provided together"},
		{
			name: "equal boundaries", arguments: []string{"--segment-min-time-nanos=2", "--segment-max-time-nanos=2"},
			errorContains: "must be less than",
		},
		{
			name: "inverted boundaries", arguments: []string{"--segment-min-time-nanos=3", "--segment-max-time-nanos=2"},
			errorContains: "must be less than",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet(test.name, flag.ContinueOnError)
			var minTimeNanos, maxTimeNanos int64
			flags.Int64Var(&minTimeNanos, "segment-min-time-nanos", 0, "")
			flags.Int64Var(&maxTimeNanos, "segment-max-time-nanos", 0, "")
			require.NoError(t, flags.Parse(test.arguments))

			segmentTimeRange, resolveErr := resolveSegmentTimeRange(flags, minTimeNanos, maxTimeNanos)
			if test.errorContains != "" {
				require.ErrorContains(t, resolveErr, test.errorContains)
				return
			}
			require.NoError(t, resolveErr)
			if len(test.arguments) == 0 {
				require.True(t, segmentTimeRange.Start.IsZero())
				require.True(t, segmentTimeRange.End.IsZero())
				return
			}
			require.Equal(t, test.expectedStart, segmentTimeRange.Start)
			require.Equal(t, test.expectedEnd, segmentTimeRange.End)
			require.True(t, segmentTimeRange.IncludeStart)
			require.True(t, segmentTimeRange.IncludeEnd)
		})
	}
}
