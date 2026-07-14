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

package pub

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func day(n uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
}

// stagedOpts mirrors the sw_metricsHour shape: group default 5d/ttl 1d, warm 10d/ttl 7d,
// cold 20d/ttl 30d.
func stagedOpts() *commonv1.ResourceOpts {
	return &commonv1.ResourceOpts{
		ShardNum:        1,
		SegmentInterval: day(5),
		Ttl:             day(1),
		Stages: []*commonv1.LifecycleStage{
			{Name: "warm", ShardNum: 2, SegmentInterval: day(10), Ttl: day(7), NodeSelector: "type=warm"},
			{Name: "cold", ShardNum: 3, SegmentInterval: day(20), Ttl: day(30), NodeSelector: "type=cold", Close: true},
		},
	}
}

func TestResolveStageResourceOpts_ColdNodeGetsColdStage(t *testing.T) {
	resolved, matched, idx, err := ResolveStageResourceOpts(stagedOpts(), map[string]string{"type": "cold"})
	require.NoError(t, err)
	require.NotNil(t, matched)
	require.Equal(t, "cold", matched.GetName())
	require.Equal(t, 1, idx)
	require.Equal(t, uint32(20), resolved.GetSegmentInterval().GetNum(), "cold segment interval")
	require.Equal(t, uint32(3), resolved.GetShardNum(), "cold shard num")
	// Cumulative ttl through cold: group 1 + warm 7 + cold 30 = 38.
	require.Equal(t, uint32(38), resolved.GetTtl().GetNum(), "cumulative cold ttl")
}

func TestResolveStageResourceOpts_WarmNodeGetsWarmStage(t *testing.T) {
	resolved, matched, idx, err := ResolveStageResourceOpts(stagedOpts(), map[string]string{"type": "warm"})
	require.NoError(t, err)
	require.NotNil(t, matched)
	require.Equal(t, "warm", matched.GetName())
	require.Equal(t, 0, idx)
	require.Equal(t, uint32(10), resolved.GetSegmentInterval().GetNum())
	require.Equal(t, uint32(2), resolved.GetShardNum())
	require.Equal(t, uint32(8), resolved.GetTtl().GetNum(), "group 1 + warm 7")
}

func TestResolveStageResourceOpts_NoMatchFallsBackToGroupDefault(t *testing.T) {
	// A node label matching no stage selector must fall back to the group default, and
	// signal matched=nil so the caller can warn.
	resolved, matched, idx, err := ResolveStageResourceOpts(stagedOpts(), map[string]string{"type": "hot"})
	require.NoError(t, err)
	require.Nil(t, matched)
	require.Equal(t, -1, idx)
	require.Equal(t, uint32(5), resolved.GetSegmentInterval().GetNum())
	require.Equal(t, uint32(1), resolved.GetShardNum())
	require.Equal(t, uint32(1), resolved.GetTtl().GetNum())
}

func TestResolveStageResourceOpts_NoLabelsOrNoStages(t *testing.T) {
	resolved, matched, _, err := ResolveStageResourceOpts(stagedOpts(), nil)
	require.NoError(t, err)
	require.Nil(t, matched)
	require.Equal(t, uint32(5), resolved.GetSegmentInterval().GetNum())

	noStages := &commonv1.ResourceOpts{ShardNum: 1, SegmentInterval: day(5), Ttl: day(1)}
	resolved, matched, _, err = ResolveStageResourceOpts(noStages, map[string]string{"type": "cold"})
	require.NoError(t, err)
	require.Nil(t, matched)
	require.Equal(t, uint32(5), resolved.GetSegmentInterval().GetNum())
}

func TestResolveStageResourceOpts_DoesNotMutateInput(t *testing.T) {
	// Regression for the pointer-aliasing bug: resolving must not mutate the shared
	// group schema's Ttl/SegmentInterval.
	ro := stagedOpts()
	resolved, _, _, err := ResolveStageResourceOpts(ro, map[string]string{"type": "cold"})
	require.NoError(t, err)
	require.Equal(t, uint32(20), resolved.GetSegmentInterval().GetNum(), "resolved copy carries the cold interval")
	require.Equal(t, uint32(1), ro.GetTtl().GetNum(), "group base ttl untouched")
	require.Equal(t, uint32(5), ro.GetSegmentInterval().GetNum(), "group default interval untouched")
	require.Equal(t, uint32(1), ro.GetShardNum(), "group shard num untouched")
}

func TestResolveStageResourceOpts_TTLUnitMismatchErrors(t *testing.T) {
	ro := stagedOpts()
	ro.Stages[1].Ttl = &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_HOUR, Num: 30}
	_, matched, _, err := ResolveStageResourceOpts(ro, map[string]string{"type": "cold"})
	require.Error(t, err)
	require.Nil(t, matched)
}
