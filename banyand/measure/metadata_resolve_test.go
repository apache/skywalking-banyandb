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

package measure

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func resolveDay(n uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
}

func metricsHourGroup() *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "sw_metricsHour"},
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: resolveDay(5),
			Ttl:             resolveDay(1),
			Stages: []*commonv1.LifecycleStage{
				{Name: "warm", ShardNum: 2, SegmentInterval: resolveDay(10), Ttl: resolveDay(7), NodeSelector: "type=warm"},
				{Name: "cold", ShardNum: 3, SegmentInterval: resolveDay(20), Ttl: resolveDay(30), NodeSelector: "type=cold", Close: true},
			},
		},
	}
}

// TestSupplierResolveResourceOpts_ColdNode exercises the real measure data supplier's
// ResolveResourceOpts -- the value storeGroup feeds into DB.UpdateOptions -- and asserts
// a type=cold node resolves the cold stage's interval/ttl/shardNum, not the group default.
func TestSupplierResolveResourceOpts_ColdNode(t *testing.T) {
	s := &supplier{l: logger.GetLogger("test"), nodeLabels: map[string]string{"type": "cold"}}
	g := metricsHourGroup()

	got := s.ResolveResourceOpts(g)
	require.Equal(t, uint32(20), got.GetSegmentInterval().GetNum(), "cold segment interval, not group default 5")
	require.Equal(t, uint32(3), got.GetShardNum(), "cold shard num")
	require.Equal(t, uint32(38), got.GetTtl().GetNum(), "cumulative ttl 1+7+30")

	// The shared group schema must not be mutated by resolution.
	require.Equal(t, uint32(5), g.ResourceOpts.GetSegmentInterval().GetNum())
	require.Equal(t, uint32(1), g.ResourceOpts.GetTtl().GetNum())
	require.Equal(t, uint32(1), g.ResourceOpts.GetShardNum())
}

// TestSupplierResolveResourceOpts_UnlabeledNodeKeepsGroupDefault asserts a node with no
// labels (e.g. the hot/initial tier) keeps the group default.
func TestSupplierResolveResourceOpts_UnlabeledNodeKeepsGroupDefault(t *testing.T) {
	s := &supplier{l: logger.GetLogger("test")}
	got := s.ResolveResourceOpts(metricsHourGroup())
	require.Equal(t, uint32(5), got.GetSegmentInterval().GetNum())
	require.Equal(t, uint32(1), got.GetTtl().GetNum())
}

// TestSupplierResolveResourceOpts_ResolveErrorKeepsGroupDefault asserts that when stage
// resolution errors (here a stage ttl unit inconsistent with the group), the update path
// keeps the group default instead of failing.
func TestSupplierResolveResourceOpts_ResolveErrorKeepsGroupDefault(t *testing.T) {
	s := &supplier{l: logger.GetLogger("test"), nodeLabels: map[string]string{"type": "cold"}}
	g := metricsHourGroup()
	g.ResourceOpts.Stages[1].Ttl = &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_HOUR, Num: 30}
	got := s.ResolveResourceOpts(g)
	require.Equal(t, uint32(5), got.GetSegmentInterval().GetNum(), "keeps group default on resolve error")
	require.Equal(t, uint32(1), got.GetTtl().GetNum())
}
