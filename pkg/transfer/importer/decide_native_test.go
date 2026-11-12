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
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// nativeInputs are the decide fixture as a native artifact whose topology matches the target
// the fixture creates: every group of shard number 2, the measure's warm stage of 1, and a
// warm route of 1 shard.
func nativeInputs(t *testing.T, mutate func(in *Inputs)) Inputs {
	t.Helper()
	in := decideInputs(t, Flags{ForceTTL: true})
	m := in.Artifact.Manifest
	m.Format = manifest.FormatNative
	for i := range m.Groups {
		for j := range m.Groups[i].Stages {
			st := &m.Groups[i].Stages[j]
			if st.ShardNum == 0 {
				st.ShardNum = 2
			}
			if st.SegmentInterval == "" && m.Groups[i].Catalog != "PROPERTY" {
				st.SegmentInterval = "1d"
			}
		}
	}
	for i := range m.Units {
		m.Units[i].SegmentVersion = "1.5.0"
	}
	in.Target.Routes[NewRouteKey("sw_metric", "warm")].ShardNum = 1
	if mutate != nil {
		mutate(&in)
	}
	return in
}

func gateRejections(p *Plan, gate string) []Rejection {
	var out []Rejection
	for _, r := range p.Rejected {
		if r.Gate == gate {
			out = append(out, r)
		}
	}
	return out
}

func TestDecideNative_MatchingTopologyPasses(t *testing.T) {
	p := Decide(nativeInputs(t, nil))
	assert.Empty(t, gateRejections(p, GateTopology), "%v", p.GateDetails)
	assert.Empty(t, gateRejections(p, GateTimezone))
	assert.Empty(t, gateRejections(p, GateVersion))
	assert.False(t, p.Refused(), "%v", rejectedGates(p))
	assert.Contains(t, entryKeys(p), "measure/sw_metric/warm/20261010/shard-0")
}

func TestDecideNative_TopologyGate(t *testing.T) {
	cases := []struct {
		mutate func(in *Inputs)
		name   string
		want   string
		key    string
	}{
		{
			name: "source stage shard number", key: "measure/sw_metric/hot/20261018/shard-0", want: "shard number 3→2",
			mutate: func(in *Inputs) {
				in.Artifact.Manifest.Groups[0].Stages[0].ShardNum = 3
			},
		},
		{
			name: "routed stage shard number", key: "measure/sw_metric/warm/20261010/shard-0", want: "stage warm shard number 1→4",
			mutate: func(in *Inputs) { in.Target.Routes[NewRouteKey("sw_metric", "warm")].ShardNum = 4 },
		},
		{
			name: "segment interval", key: "measure/sw_metric/warm/20261010/shard-0", want: "segment interval 2d→3d",
			mutate: func(in *Inputs) { in.Artifact.Manifest.Groups[0].Stages[1].SegmentInterval = "2d" },
		},
		{
			name: "property shard number", key: "property/sw_prop/shard-0", want: "group shard number 4→2",
			mutate: func(in *Inputs) { in.Artifact.Manifest.Groups[2].Stages[0].ShardNum = 4 },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := Decide(nativeInputs(t, c.mutate))
			var found bool
			for _, r := range gateRejections(p, GateTopology) {
				if r.Item.Key() == c.key {
					found = true
					assert.Contains(t, r.Reason, c.want)
				}
			}
			assert.True(t, found, "%s is not refused by the topology gate: %v", c.key, rejectedGates(p))
			assert.True(t, p.Refused(), "a topology refusal refuses the import")
			assert.NotContains(t, entryKeys(p), c.key)
		})
	}
}

func TestDecideNative_TopologyGateIgnoresCSV(t *testing.T) {
	in := nativeInputs(t, func(in *Inputs) { in.Artifact.Manifest.Groups[0].Stages[0].ShardNum = 3 })
	in.Artifact.Manifest.Format = manifest.FormatCSV
	p := Decide(in)
	assert.Empty(t, gateRejections(p, GateTopology))
}

func TestDecideNative_TimezoneGate(t *testing.T) {
	p := Decide(nativeInputs(t, func(in *Inputs) {
		in.Target.DataNodes[0].TzName = "Asia/Shanghai"
	}))
	keys := map[string]bool{}
	for _, r := range gateRejections(p, GateTimezone) {
		keys[r.Item.Key()] = true
		assert.Contains(t, r.Reason, "runs in UTC, target node dst-hot1 in Asia/Shanghai")
	}
	assert.True(t, keys["measure/sw_metric/hot/20261018/shard-0"], "an item routed to dst-hot1 is refused: %v", keys)
	assert.True(t, keys["stream/sw_stream/cold/20261018/shard-0"], "an unrouted group is checked against every data node: %v", keys)
	assert.False(t, keys["measure/sw_metric/warm/20261010/shard-0"], "an item routed to dst-warm1 passes")
	assert.False(t, keys["property/sw_prop/shard-0"], "a property shard has no segments to cut")
	assert.True(t, p.Refused())
}

func TestDecideNative_VersionGate(t *testing.T) {
	p := Decide(nativeInputs(t, func(in *Inputs) {
		in.Target.DataNodes[1].Version = &clusterv1.VersionInfo{FileFormatVersion: "1.6.0", CompatibleFileFormatVersion: []string{"1.6.0"}}
	}))
	var keys []string
	for _, r := range gateRejections(p, GateVersion) {
		keys = append(keys, r.Item.Key())
		assert.Contains(t, r.Reason, "dst-warm1 cannot read segment file format 1.5.0 (it reads 1.6.0)")
	}
	// The warm item is routed to dst-warm1; the unrouted stream group is checked against every node.
	assert.Equal(t, []string{"measure/sw_metric/warm/20261010/shard-0", "stream/sw_stream/cold/20261018/shard-0"}, keys)
	assert.True(t, p.Refused())

	p = Decide(nativeInputs(t, func(in *Inputs) {
		in.Target.DataNodes[1].Version = &clusterv1.VersionInfo{FileFormatVersion: "1.6.0", CompatibleFileFormatVersion: []string{"1.5.0", "1.6.0"}}
	}))
	assert.Empty(t, gateRejections(p, GateVersion), "a compatible version passes")
}

func TestDecideNative_SegmentSourcesFollowTheChosenShards(t *testing.T) {
	// data-a and data-b hold the same shard 0, so one of them is skipped as a replica; with
	// shard 1 gone from data-a, the series index of the skipped source is skipped too.
	p := Decide(nativeInputs(t, func(in *Inputs) {
		in.Artifact.Manifest.Groups[0].IndexMode = false
		src := &in.Artifact.Manifest.Units[0].Sources[0]
		require.Equal(t, "data-a", src.Node)
		src.Shards = src.Shards[:1]
	}))
	entries := entriesByKey(p)
	shard := entries["measure/sw_metric/hot/20261018/shard-0"]
	require.Len(t, shard.Item.Sources, 1)
	chosen := shard.Item.Sources[0].Node
	seg, ok := entries["measure/sw_metric/hot/20261018/segment"]
	require.True(t, ok, "%v", entryKeys(p))
	require.Len(t, seg.Item.Sources, 1)
	assert.Equal(t, chosen, seg.Item.Sources[0].Node)
	assert.Equal(t, []uint32{0}, seg.Item.Shards)
	assert.Equal(t, "1.5.0", seg.Item.SegmentVersion)

	// CSV keeps every source of a segment item.
	in := nativeInputs(t, func(in *Inputs) {
		in.Artifact.Manifest.Groups[0].IndexMode = false
		in.Artifact.Manifest.Units[0].Sources[0].Shards = in.Artifact.Manifest.Units[0].Sources[0].Shards[:1]
	})
	in.Artifact.Manifest.Format = manifest.FormatCSV
	assert.Len(t, entriesByKey(Decide(in))["measure/sw_metric/hot/20261018/segment"].Item.Sources, 2)
}

func TestDecideNative_IndexModeGroupKeepsEverySegmentSource(t *testing.T) {
	// The series index of a group with an index-mode measure also holds the index-mode rows, so
	// the source skipped as a replica of shard 0 keeps its segment item, whether the manifest,
	// the artifact's schema or the target's names the index mode.
	cases := map[string]func(in *Inputs){
		"manifest": func(*Inputs) {},
		"artifact schema": func(in *Inputs) {
			in.Artifact.Manifest.Groups[0].IndexMode = false
			in.Artifact.Measures[ResourceKey{"sw_metric", "cpm"}].IndexMode = true
		},
		"target schema": func(in *Inputs) {
			in.Artifact.Manifest.Groups[0].IndexMode = false
			m := testMeasure("sw_metric", "cpm")
			m.IndexMode = true
			in.Target.Measures[ResourceKey{"sw_metric", "cpm"}] = m
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := Decide(nativeInputs(t, func(in *Inputs) {
				mutate(in)
				src := &in.Artifact.Manifest.Units[0].Sources[0]
				src.Shards = src.Shards[:1]
			}))
			entries := entriesByKey(p)
			require.Len(t, entries["measure/sw_metric/hot/20261018/shard-0"].Item.Sources, 1)
			seg, ok := entries["measure/sw_metric/hot/20261018/segment"]
			require.True(t, ok, "%v", entryKeys(p))
			var nodes []string
			for _, s := range seg.Item.Sources {
				nodes = append(nodes, s.Node)
			}
			assert.Equal(t, []string{"data-a", "data-b"}, nodes)
		})
	}
}

func TestRestrictSegmentSources(t *testing.T) {
	in := Inputs{Artifact: &Artifact{Manifest: &manifest.Manifest{}, Schemas: NewSchemas()}, Target: NewTargetState()}
	item := WorkItem{Group: "g", Stage: "hot", Segment: "20261018", Kind: SegmentItem, Sources: []WorkSource{{Node: "a"}, {Node: "b"}}}
	p := &Plan{}
	p.native().shardSources[unitKey(&WorkItem{Group: "other", Stage: "hot", Segment: "20261018"})] = []string{"a"}
	got := restrictSegmentSources(in, p, &item)
	assert.Len(t, got.Sources, 2, "a unit no shard item chose from keeps every source")

	p.native().shardSources[unitKey(&item)] = []string{"b", "b"}
	got = restrictSegmentSources(in, p, &item)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, "b", got.Sources[0].Node, "the sources the unit's shard items push are kept")

	in.Artifact.Manifest.Groups = []manifest.Group{{Name: "g", IndexMode: true}}
	// A plan caches what it derived per group during Decide, so a changed artifact needs a new plan.
	fresh := &Plan{}
	fresh.native().shardSources = p.native().shardSources
	got = restrictSegmentSources(in, fresh, &item)
	assert.Len(t, got.Sources, 2, "an index-mode group keeps every source")
}

func TestDecideNative_RejectedShardItemNamesTheSegmentSources(t *testing.T) {
	// A shard item the TZ gate refuses still names the sources of its unit's series index.
	p := Decide(nativeInputs(t, func(in *Inputs) {
		in.Artifact.Manifest.Groups[0].IndexMode = false
		src := &in.Artifact.Manifest.Units[0].Sources[0]
		src.Shards = src.Shards[:1]
		in.Target.DataNodes[0].TzName = "Asia/Shanghai"
	}))
	var shardNode string
	var seg *Rejection
	for i, r := range p.Rejected {
		switch r.Item.Key() {
		case "measure/sw_metric/hot/20261018/shard-0":
			require.Len(t, r.Item.Sources, 1)
			shardNode = r.Item.Sources[0].Node
		case "measure/sw_metric/hot/20261018/segment":
			seg = &p.Rejected[i]
		}
	}
	require.NotEmpty(t, shardNode, "%v", rejectedGates(p))
	require.NotNil(t, seg, "%v", rejectedGates(p))
	require.Len(t, seg.Item.Sources, 1)
	assert.Equal(t, shardNode, seg.Item.Sources[0].Node)
}

func TestNativePayloadNodes(t *testing.T) {
	v := routeView{r: testRoute("g", "hot", 1, "n0", "n1", "n0")}
	assert.Equal(t, []string{"n1"}, nativePayloadNodes(v, &WorkItem{Kind: ShardItem, Shard: 1}))
	assert.Equal(t, []string{"n0"}, nativePayloadNodes(v, &WorkItem{Kind: PropertyItem, Shard: 2}))
	assert.Equal(t, []string{"n0", "n1"}, nativePayloadNodes(v, &WorkItem{Kind: SegmentItem, Shards: []uint32{2, 1}}))
	assert.Equal(t, []string{"n0"}, nativePayloadNodes(v, &WorkItem{Kind: SegmentItem, Shards: []uint32{0, 2}}))
	assert.Equal(t, []string{"n0", "n1"}, nativePayloadNodes(v, &WorkItem{Kind: SegmentItem}), "a series index naming no shard goes to every shard")
	assert.Empty(t, nativePayloadNodes(v, &WorkItem{Kind: ShardItem, Shard: 7}))
}

func TestDecideNative_GatesCheckOnlyTheShardNodes(t *testing.T) {
	// Shard 0 of the hot route lives on dst-hot1 and shard 1 on dst-warm1, which runs in another
	// time zone: the liaison relays a shard-0 item only to dst-hot1, so it passes.
	p := Decide(nativeInputs(t, func(in *Inputs) {
		in.Target.Routes[NewRouteKey("sw_metric", "hot")] = testRoute("sw_metric", "hot", 1, "dst-hot1", "dst-warm1")
		in.Target.DataNodes[1].TzName = "Asia/Shanghai"
	}))
	entries := entriesByKey(p)
	shard0, ok := entries["measure/sw_metric/hot/20261018/shard-0"]
	require.True(t, ok, "%v", rejectedGates(p))
	assert.Equal(t, []string{"dst-hot1"}, shard0.TargetNodes)
	for _, r := range p.Rejected {
		if r.Item.Key() == "measure/sw_metric/hot/20261018/segment" {
			want := []string{"dst-hot1"}
			if slices.Contains(r.Item.Shards, 1) {
				want = []string{"dst-hot1", "dst-warm1"}
			}
			assert.Equal(t, want, r.TargetNodes, "a series index goes to the nodes of every shard it covers")
		}
	}
	if seg, found := entries["measure/sw_metric/hot/20261018/segment"]; found {
		assert.False(t, slices.Contains(seg.Item.Shards, 1), "a series index covering shard 1 must be refused by the TZ gate")
		assert.Equal(t, []string{"dst-hot1"}, seg.TargetNodes)
	}
}

func TestDecideNative_IndexRuleGate(t *testing.T) {
	withRules := func(targetIDs map[ResourceKey]uint32, artifactID uint32) Inputs {
		return nativeInputs(t, func(in *Inputs) {
			rule := testIndexRule("sw_metric", "tier")
			rule.Metadata.Id = artifactID
			in.Artifact.IndexRules[ResourceKey{"sw_metric", "tier"}] = rule
			in.TargetIndexRuleIDs = targetIDs
		})
	}
	p := Decide(withRules(map[ResourceKey]uint32{{"sw_metric", "tier"}: 9}, 7))
	rejected := gateRejections(p, GateIndexRule)
	require.NotEmpty(t, rejected, "%v", rejectedGates(p))
	for _, r := range rejected {
		assert.Equal(t, "sw_metric", r.Item.Group)
		assert.Contains(t, r.Reason, "tier (id 7 in the artifact, 9 on the target)")
	}
	assert.True(t, p.Refused())

	cases := map[string]Inputs{
		"same id":               withRules(map[ResourceKey]uint32{{"sw_metric", "tier"}: 7}, 7),
		"rule the target lacks": withRules(map[ResourceKey]uint32{}, 7),
		"artifact rule id 0":    withRules(map[ResourceKey]uint32{{"sw_metric", "tier"}: 9}, 0),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			p := Decide(in)
			assert.Empty(t, gateRejections(p, GateIndexRule))
			assert.False(t, p.Refused(), "%v", rejectedGates(p))
		})
	}
}

func TestDecideNative_WarnsWithoutSchema(t *testing.T) {
	const want = "the INDEX_RULE gate cannot check"
	hasWarning := func(p *Plan) bool {
		return slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.Contains(w, want) })
	}
	assert.True(t, hasWarning(Decide(nativeInputs(t, nil))), "a native artifact without schema files is warned about")

	in := nativeInputs(t, func(in *Inputs) {
		in.Artifact.Manifest.SchemaFiles = []manifest.File{{Path: "schema/index_rule.jsonl"}}
	})
	assert.False(t, hasWarning(Decide(in)), "a native artifact with its schema is checked")

	in = nativeInputs(t, nil)
	in.Artifact.Manifest.Format = manifest.FormatCSV
	assert.False(t, hasWarning(Decide(in)), "a CSV artifact has no index files")
}

func TestOpenArtifactAcceptsNative(t *testing.T) {
	dir := t.TempDir()
	m := &manifest.Manifest{Format: manifest.FormatNative, ArtifactVersion: manifest.ArtifactVersion, SchemaFiles: []manifest.File{}}
	require.NoError(t, manifest.Write(filepath.Join(dir, manifest.FileName), m))
	art, err := OpenArtifact(t.Context(), dir, true)
	require.NoError(t, err)
	assert.True(t, art.Native())
}

// disjointStreamShards makes the stream unit hold shard 0 on data-a and shard 1 on data-b, each
// with its series index, as a cluster without replicas exports it.
func disjointStreamShards(in *Inputs) {
	const prefix = "stream/sw_stream/seg-20261018/"
	u := &in.Artifact.Manifest.Units[3]
	u.Sources = []manifest.UnitSource{
		{Node: "data-a", Shards: []manifest.Shard{
			testShard(0, "data-a", prefix, 10, "2026-10-18T00:00:00Z", "2026-10-18T01:00:00Z", "shard-0-00001.bnc"),
		}, SegmentFiles: []manifest.File{dataFile("nodes/data-a/" + prefix + "sidx-00001.bnc")}},
		{Node: "data-b", Shards: []manifest.Shard{
			testShard(1, "data-b", prefix, 10, "2026-10-18T00:00:00Z", "2026-10-18T01:00:00Z", "shard-1-00001.bnc"),
		}, SegmentFiles: []manifest.File{dataFile("nodes/data-b/" + prefix + "sidx-00001.bnc")}},
	}
}

func TestDecideNative_StrictCoverageExemptsTheSeriesIndex(t *testing.T) {
	p := Decide(nativeInputs(t, func(in *Inputs) {
		require.Equal(t, "sw_stream", in.Artifact.Manifest.Units[3].Group)
		disjointStreamShards(in)
		in.Flags.StrictCoverage = true
	}))
	assert.Empty(t, gateRejections(p, GateCoverage), "%v", p.GateDetails)
	assert.False(t, p.Refused(), "%v", rejectedGates(p))
	seg, ok := entriesByKey(p)["stream/sw_stream/cold/20261018/segment"]
	require.True(t, ok, "%v", entryKeys(p))
	assert.Len(t, seg.Item.Sources, 2, "the series index is pushed from both shard owners")

	// A shard item pushed from two sources is still refused.
	p = Decide(nativeInputs(t, func(in *Inputs) {
		in.Flags.StrictCoverage, in.Flags.AllSources = true, true
	}))
	var keys []string
	for _, r := range gateRejections(p, GateCoverage) {
		keys = append(keys, r.Item.Key())
	}
	assert.Contains(t, keys, "measure/sw_metric/hot/20261018/shard-0")
	assert.True(t, p.Refused())
}

func TestDecideNative_GroupShardNumberIgnoresALifecycleStageNamedHot(t *testing.T) {
	in := nativeInputs(t, nil)
	m := in.Artifact.Manifest
	m.Groups[0].Stages = append(m.Groups[0].Stages, manifest.Stage{Name: "hot", SegmentInterval: "1d", ShardNum: 4})
	m.Groups[2].Stages = append(m.Groups[2].Stages, manifest.Stage{Name: "hot", ShardNum: 4})
	for _, item := range []*WorkItem{
		{Catalog: catMeasure, Group: "sw_metric", Stage: "warm", Kind: ShardItem},
		{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "sw_prop", Kind: PropertyItem},
	} {
		reason := topologyMismatch(in, item, item.Stage)
		assert.NotContains(t, reason, "group shard number", "%s: the group default is the first stage, not the lifecycle hot", item.Group)
	}
	m.Groups[2].Stages[0].ShardNum = 3
	assert.Contains(t, topologyMismatch(in, &WorkItem{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "sw_prop", Kind: PropertyItem}, ""),
		"group shard number 3→2", "a differing group default is still refused")
}

func TestDecideNative_LifecycleStageNamedHot(t *testing.T) {
	hotItem := &WorkItem{Catalog: catMeasure, Group: "sw_metric", Stage: "hot", Kind: ShardItem}
	t.Run("same layout as the group default passes", func(t *testing.T) {
		in := nativeInputs(t, nil)
		g := &in.Artifact.Manifest.Groups[0]
		g.Stages = append(g.Stages, manifest.Stage{Name: "hot", SegmentInterval: g.Stages[0].SegmentInterval, ShardNum: g.Stages[0].ShardNum})
		assert.Empty(t, topologyMismatch(in, hotItem, "hot"))
		p := Decide(in)
		assert.Empty(t, gateRejections(p, GateTopology), "%v", p.GateDetails)
		assert.False(t, p.Refused(), "%v", rejectedGates(p))
	})
	t.Run("a differing shard number is ambiguous", func(t *testing.T) {
		in := nativeInputs(t, nil)
		g := &in.Artifact.Manifest.Groups[0]
		g.Stages = append(g.Stages, manifest.Stage{Name: "hot", SegmentInterval: g.Stages[0].SegmentInterval, ShardNum: 4})
		assert.Contains(t, topologyMismatch(in, hotItem, "hot"), "cannot tell which layout a hot unit has")
		assert.True(t, Decide(in).Refused())
	})
	t.Run("a lifecycle hot matching the target interval is still refused", func(t *testing.T) {
		in := nativeInputs(t, nil)
		g := &in.Artifact.Manifest.Groups[0]
		g.Stages[0].SegmentInterval = "1d"
		g.Stages = append(g.Stages, manifest.Stage{Name: "hot", SegmentInterval: "2d", ShardNum: g.Stages[0].ShardNum})
		in.Target.Groups["sw_metric"].GetResourceOpts().SegmentInterval = dayRule(2)
		assert.Contains(t, topologyMismatch(in, hotItem, "hot"), "cannot tell which layout a hot unit has")
		assert.True(t, Decide(in).Refused())
	})
}

func TestDecideNative_UnresolvableSegmentTimeRangeRefuses(t *testing.T) {
	p := Decide(nativeInputs(t, func(in *Inputs) {
		u := &in.Artifact.Manifest.Units[3]
		require.Equal(t, "sw_stream", u.Group)
		u.Segment = "notadate"
		u.Sources[0].SegmentFiles = []manifest.File{dataFile("nodes/data-c/stream/sw_stream/seg-notadate/sidx-00001.bnc")}
	}))
	const key = "stream/sw_stream/cold/notadate/segment"
	assert.True(t, p.Refused())
	assert.True(t, slices.ContainsFunc(p.Errors, func(e string) bool { return strings.HasPrefix(e, key+":") }), "%v", p.Errors)
	assert.NotContains(t, entryKeys(p), key)
}
