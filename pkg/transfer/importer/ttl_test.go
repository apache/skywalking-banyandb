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

package importer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

func dayRule(n uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
}

func hourRule(n uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_HOUR, Num: n}
}

func stagedOpts() *commonv1.ResourceOpts {
	return &commonv1.ResourceOpts{
		ShardNum: 2, SegmentInterval: dayRule(1), Ttl: dayRule(1),
		Stages: []*commonv1.LifecycleStage{
			{Name: "warm", ShardNum: 1, SegmentInterval: dayRule(3), Ttl: dayRule(7), NodeSelector: "type=warm"},
			{Name: "Cold", ShardNum: 1, SegmentInterval: dayRule(7), Ttl: dayRule(30), NodeSelector: "type=cold"},
		},
	}
}

func TestCumulativeTTLMatchesStageResolution(t *testing.T) {
	ro := stagedOpts()
	for _, tc := range []struct {
		labels map[string]string
		stage  string
	}{
		{stage: "hot", labels: map[string]string{"type": "hot"}},
		{stage: "", labels: nil},
		{stage: "warm", labels: map[string]string{"type": "warm"}},
		{stage: "cold", labels: map[string]string{"type": "cold"}},
	} {
		resolved, _, _, err := pub.ResolveStageResourceOpts(ro, tc.labels)
		require.NoError(t, err)
		want, err := intervalDuration(resolved.GetTtl().GetUnit(), resolved.GetTtl().GetNum())
		require.NoError(t, err)
		got, err := CumulativeTTL(ro, tc.stage)
		require.NoError(t, err)
		assert.Equal(t, want, got, "stage %q", tc.stage)
	}
}

func TestCumulativeTTLCases(t *testing.T) {
	ro := stagedOpts()
	got, err := CumulativeTTL(ro, "hot")
	require.NoError(t, err)
	assert.Equal(t, 24*time.Hour, got, "the default stage has the group ttl only")
	got, err = CumulativeTTL(ro, "WARM")
	require.NoError(t, err)
	assert.Equal(t, 8*24*time.Hour, got, "stage names match case-insensitively")
	got, err = CumulativeTTL(ro, "cold")
	require.NoError(t, err)
	assert.Equal(t, 38*24*time.Hour, got)
	_, err = CumulativeTTL(ro, "frozen")
	assert.Error(t, err)
	_, err = CumulativeTTL(&commonv1.ResourceOpts{ShardNum: 1}, "hot")
	assert.Error(t, err, "a property group has no ttl")

	explicitHot := &commonv1.ResourceOpts{Ttl: hourRule(6), Stages: []*commonv1.LifecycleStage{{Name: "hot", Ttl: hourRule(2)}}}
	got, err = CumulativeTTL(explicitHot, "hot")
	require.NoError(t, err)
	assert.Equal(t, 8*time.Hour, got, "a stage explicitly named hot is a lifecycle stage")
	assert.Equal(t, "group 1d + warm 7d + Cold 30d", ttlBreakdown(ro, "cold"))
}

func TestSegmentBounds(t *testing.T) {
	_, end, err := segmentBounds("20261016", "Asia/Shanghai", "1d")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 16, 16, 0, 0, 0, time.UTC), end.UTC())

	_, end, err = segmentBounds("2026101605", "", "3h")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC), end.UTC())

	_, _, err = segmentBounds("20261016", "Mars/Base", "1d")
	assert.Error(t, err)
	_, _, err = segmentBounds("20261016", "", "1w")
	assert.Error(t, err)
	_, _, err = segmentBounds("2026101605", "", "1d")
	assert.Error(t, err, "a day segment has an eight-digit suffix")
}

func TestTTLDetailHasNoNowDerivedValue(t *testing.T) {
	item := &WorkItem{Catalog: catMeasure, Group: "sw_metricsHour", Stage: "hot", Segment: "2026101600", Kind: ShardItem}
	newest := time.Date(2026, 10, 16, 5, 0, 0, 0, time.UTC).UnixNano()
	line := ttlDetail(item, "hot", newest, 24*time.Hour, "group 1d", false)
	assert.Equal(t, "measure/sw_metricsHour/hot seg=2026101600 shard=0: every row is older than the target retention "+
		"(newest 2026-10-16T05:00:00Z, retention 1d = group 1d); skipped; --force-ttl imports it", line)
}

func TestSourceStagePrefersLifecycleHot(t *testing.T) {
	m := &manifest.Manifest{Groups: []manifest.Group{{Name: "g", Stages: []manifest.Stage{
		{Name: "hot", SegmentInterval: "1d"}, {Name: "hot", SegmentInterval: "3d"}, {Name: "cold", SegmentInterval: "7d"},
	}}}}
	st, ok := sourceStage(m, "g", "HOT")
	require.True(t, ok)
	assert.Equal(t, "3d", st.SegmentInterval, "a lifecycle stage named hot wins over the group default")
	st, ok = sourceStage(m, "g", "cold")
	require.True(t, ok)
	assert.Equal(t, "7d", st.SegmentInterval)
	_, ok = sourceStage(m, "g", "warm")
	assert.False(t, ok)
}
