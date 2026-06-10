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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestParseGroup_RejectsMissingIntervals verifies that parseGroup fails fast
// with a descriptive error when required IntervalRule fields are nil, rather
// than panicking later inside the migration log. parseGroup validates these
// fields before touching metadata.Repo or the cluster state manager, so
// passing nil for those parameters is safe in this test - if a future change
// reorders parseGroup so the deps run first, the tests will panic clearly
// and that contract should be revisited.
func TestParseGroup_RejectsMissingIntervals(t *testing.T) {
	makeGroup := func(mutate func(ro *commonv1.ResourceOpts)) *commonv1.Group {
		ro := &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: dayInterval(1),
			Ttl:             dayInterval(7),
			Stages: []*commonv1.LifecycleStage{
				stage(stageWarm, selectorWarm, 7, 7),
				stage(stageCold, selectorCold, 15, 30),
			},
		}
		mutate(ro)
		return &commonv1.Group{
			Metadata:     &commonv1.Metadata{Name: "test-group"},
			ResourceOpts: ro,
		}
	}

	cases := []struct {
		name    string
		mutate  func(*commonv1.ResourceOpts)
		errFrag string
	}{
		{
			name:    "top-level ttl missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Ttl = nil },
			errFrag: "group test-group: missing ttl",
		},
		{
			name:    "top-level segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.SegmentInterval = nil },
			errFrag: "group test-group: missing segment_interval",
		},
		{
			name:    "stage segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[0].SegmentInterval = nil },
			errFrag: "group test-group stage warm: missing segment_interval",
		},
		{
			name:    "stage ttl missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[0].Ttl = nil },
			errFrag: "group test-group stage warm: missing ttl",
		},
		{
			name:    "next stage segment_interval missing",
			mutate:  func(ro *commonv1.ResourceOpts) { ro.Stages[1].SegmentInterval = nil },
			errFrag: "group test-group stage cold: missing segment_interval",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := makeGroup(c.mutate)
			_, err := parseGroup(g, map[string]string{"type": "warm"}, nil, nil, nil, nil, nil, "")
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.errFrag)
		})
	}
}

// TestDeriveSelfIdentity verifies the sender-identity resolution order: the
// co-located data node's address match (including loopback host aliases, since
// test clusters dial localhost:PORT while nodes register 127.0.0.1:PORT), the
// label fallbacks, and the guard that keeps an empty label set from
// wildcard-matching an arbitrary registry node.
func TestDeriveSelfIdentity(t *testing.T) {
	nodes := []*databasev1.Node{
		{Metadata: &commonv1.Metadata{Name: "warm-node"}, GrpcAddress: "127.0.0.1:2", Labels: map[string]string{"type": "warm"}},
		{Metadata: &commonv1.Metadata{Name: "hot-node"}, GrpcAddress: "127.0.0.1:1", Labels: map[string]string{"type": "hot"}},
	}

	node, tier := deriveSelfIdentity("127.0.0.1:1", nil, nodes)
	assert.Equal(t, "hot-node", node, "exact address match")
	assert.Equal(t, "hot", tier)

	node, tier = deriveSelfIdentity("localhost:1", nil, nodes)
	assert.Equal(t, "hot-node", node, "loopback alias must match the registered 127.0.0.1 form")
	assert.Equal(t, "hot", tier)

	node, tier = deriveSelfIdentity("localhost:9", nil, nodes)
	assert.Empty(t, node, "unmatched address with no labels must not wildcard-match an arbitrary node")
	assert.Empty(t, tier)

	node, tier = deriveSelfIdentity("", map[string]string{"type": "hot"}, nodes)
	assert.Equal(t, "hot-node", node, "label fallback")
	assert.Equal(t, "hot", tier)
}
