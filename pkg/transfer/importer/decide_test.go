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
	"bytes"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

var decideNow = time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC)

func ts(s string) int64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UnixNano()
}

func dataFile(path string) manifest.File {
	return manifest.File{Path: path, Bytes: 10, CRC32: "0x00000001"}
}

// testShard is one source's shard holding rows between two RFC 3339 times in one part.
func testShard(id uint32, node, prefix string, rows uint64, from, to string, files ...string) manifest.Shard {
	s := manifest.Shard{
		Shard: id, TotalCount: rows, MinTimestamp: ts(from), MaxTimestamp: ts(to),
		Parts: []manifest.Part{{ID: 1, MinTimestamp: ts(from), MaxTimestamp: ts(to), TotalCount: rows}},
	}
	for _, f := range files {
		s.Files = append(s.Files, dataFile("nodes/"+node+"/"+prefix+f))
	}
	return s
}

// decideManifest is an artifact of three groups: sw_metric (measure, hot and warm units, a
// replicated shard, an expired unit and index-mode segment files), sw_stream (stream, a stage
// the target lacks) and sw_prop (property from two nodes).
func decideManifest() *manifest.Manifest {
	const mp = "measure/sw_metric/"
	return &manifest.Manifest{
		Format: "csv", ArtifactVersion: 1, ExportedAt: "2026-10-19T08:00:00Z",
		Source: manifest.Source{Nodes: []manifest.Node{
			{ID: "data-a", Dir: "data-a", Timezone: "UTC"}, {ID: "data-b", Dir: "data-b", Timezone: "UTC"}, {ID: "data-c", Dir: "data-c", Timezone: "UTC"},
		}},
		Groups: []manifest.Group{
			{Name: "sw_metric", Catalog: "MEASURE", IndexMode: true, Stages: []manifest.Stage{
				{Name: "hot", SegmentInterval: "1d", TTL: "3d", ShardNum: 2}, {Name: "warm", SegmentInterval: "3d", TTL: "7d", ShardNum: 1},
			}},
			{Name: "sw_stream", Catalog: "STREAM", Stages: []manifest.Stage{{Name: "hot", SegmentInterval: "1d", TTL: "3d"}, {Name: "cold", SegmentInterval: "1d", TTL: "9d"}}},
			{Name: "sw_prop", Catalog: "PROPERTY", Stages: []manifest.Stage{{Name: "hot"}}},
		},
		Units: []manifest.Unit{
			{
				Catalog: "MEASURE", Group: "sw_metric", Stage: "hot", Segment: "20261018",
				Sources: []manifest.UnitSource{
					{
						Node: "data-a", Shards: []manifest.Shard{
							testShard(0, "data-a", mp+"seg-20261018/", 100, "2026-10-18T00:00:00Z", "2026-10-18T05:00:00Z", "shard-0-00002.csv.gz", "shard-0-00001.csv.gz"),
							testShard(1, "data-a", mp+"seg-20261018/", 50, "2026-10-18T01:00:00Z", "2026-10-18T03:00:00Z", "shard-1-00001.csv.gz"),
						},
						SegmentFiles: []manifest.File{dataFile("nodes/data-a/" + mp + "seg-20261018/segment-00001.csv.gz")},
						RowCounts:    map[string]uint64{"idx": 7}, TotalCount: 7,
					},
					{
						Node: "data-b", Shards: []manifest.Shard{
							testShard(0, "data-b", mp+"seg-20261018/", 100, "2026-10-18T00:00:00Z", "2026-10-18T05:00:00Z", "shard-0-00001.csv.gz"),
						},
						SegmentFiles: []manifest.File{dataFile("nodes/data-b/" + mp + "seg-20261018/segment-00001.csv.gz")},
						RowCounts:    map[string]uint64{"idx": 7}, TotalCount: 7,
					},
				},
			},
			{
				Catalog: "MEASURE", Group: "sw_metric", Stage: "hot", Segment: "20261012",
				Sources: []manifest.UnitSource{{Node: "data-a", Shards: []manifest.Shard{
					testShard(0, "data-a", mp+"seg-20261012/", 30, "2026-10-12T00:00:00Z", "2026-10-12T10:00:00Z", "shard-0-00001.csv.gz"),
				}}},
			},
			{
				Catalog: "MEASURE", Group: "sw_metric", Stage: "warm", Segment: "20261010",
				Sources: []manifest.UnitSource{{Node: "data-c", Shards: []manifest.Shard{
					testShard(0, "data-c", mp+"seg-20261010/", 80, "2026-10-10T00:00:00Z", "2026-10-12T00:00:00Z", "shard-0-00001.csv.gz"),
				}}},
			},
			{
				Catalog: "STREAM", Group: "sw_stream", Stage: "cold", Segment: "20261018",
				Sources: []manifest.UnitSource{{Node: "data-c", Shards: []manifest.Shard{
					testShard(0, "data-c", "stream/sw_stream/seg-20261018/", 2000, "2026-10-18T00:00:00Z", "2026-10-18T23:00:00Z", "shard-0-00001.csv.gz"),
				}}},
			},
			{
				Catalog: "PROPERTY", Group: "sw_prop", Stage: "hot",
				Sources: []manifest.UnitSource{
					{Node: "data-a", Shards: []manifest.Shard{
						{Shard: 0, TotalCount: 3, Files: []manifest.File{dataFile("nodes/data-a/property/sw_prop/shard-0-00001.csv.gz")}},
						{Shard: 1, TotalCount: 4, Files: []manifest.File{dataFile("nodes/data-a/property/sw_prop/shard-1-00001.csv.gz")}},
					}},
					{Node: "data-b", Shards: []manifest.Shard{
						{Shard: 0, TotalCount: 3, Files: []manifest.File{dataFile("nodes/data-b/property/sw_prop/shard-0-00001.csv.gz")}},
					}},
				},
			},
		},
	}
}

func testRoute(group, stage string, copies uint32, nodes ...string) *transferv1.TargetsResponse {
	r := &transferv1.TargetsResponse{Group: group, Stage: stage, ShardNum: 2, Copies: copies}
	for i, n := range nodes {
		r.Shards = append(r.Shards, &transferv1.TargetsResponse_Shard{ShardId: uint32(i), NodeIds: []string{n}})
	}
	return r
}

// decideInputs returns the inputs of the decide fixture: the artifact schema is created on an
// empty target whose routes are known.
func decideInputs(t *testing.T, flags Flags) Inputs {
	t.Helper()
	art := &Artifact{Dir: "./artifact", Manifest: decideManifest(), Schemas: NewSchemas()}
	art.Groups["sw_metric"] = testGroup("sw_metric", commonv1.Catalog_CATALOG_MEASURE)
	art.Groups["sw_stream"] = testGroup("sw_stream", commonv1.Catalog_CATALOG_STREAM)
	art.Groups["sw_prop"] = testGroup("sw_prop", commonv1.Catalog_CATALOG_PROPERTY)
	art.Measures[ResourceKey{"sw_metric", "cpm"}] = testMeasure("sw_metric", "cpm")
	art.Streams[ResourceKey{"sw_stream", "log"}] = testStream("sw_stream", "log")
	art.Properties[ResourceKey{"sw_prop", "ui"}] = testProperty("sw_prop", "ui")
	target := NewTargetState()
	target.DataNodes = []*databasev1.Node{{Metadata: &commonv1.Metadata{Name: "dst-hot1"}}, {Metadata: &commonv1.Metadata{Name: "dst-warm1"}}}
	target.Routes[NewRouteKey("sw_metric", "hot")] = testRoute("sw_metric", "hot", 1, "dst-hot1", "dst-hot1")
	target.Routes[NewRouteKey("sw_metric", "warm")] = testRoute("sw_metric", "warm", 2, "dst-warm1")
	target.Routes[NewRouteKey("sw_prop", "hot")] = testRoute("sw_prop", "hot", 1, "dst-hot1", "dst-hot1")
	d := DiffSchemas(&art.Schemas, target, defaultOpts())
	return Inputs{
		Artifact: art, Target: PredictState(target, d.Actions), Actions: d.Actions, Flags: flags,
		Now: decideNow, Endpoint: "liaison-dst:17912 (ROLE_LIAISON)",
	}
}

func entryKeys(p *Plan) []string {
	keys := make([]string, 0, len(p.Entries))
	for _, e := range p.Entries {
		keys = append(keys, e.Item.Key())
	}
	return keys
}

func entriesByKey(p *Plan) map[string]Entry {
	m := make(map[string]Entry, len(p.Entries))
	for _, e := range p.Entries {
		m[e.Item.Key()] = e
	}
	return m
}

func rejectedGates(p *Plan) []string {
	var gates []string
	for _, r := range p.Rejected {
		gates = append(gates, r.Gate+" "+r.Item.Key())
	}
	return gates
}

func assertGroupSchemaRefused(t *testing.T, p *Plan, reason string) {
	t.Helper()
	var schemaRejected int
	for _, r := range p.Rejected {
		if r.Gate == GateSchema {
			schemaRejected++
			assert.Equal(t, "sw_metric", r.Item.Group)
			assert.Equal(t, reason, r.Reason)
		}
	}
	assert.Equal(t, 5, schemaRejected, "every work item of the group is refused")
	assert.True(t, p.Refused())
}

func TestBuildWorkItems(t *testing.T) {
	items, err := BuildWorkItems(decideManifest())
	require.NoError(t, err)
	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, it.Key())
	}
	assert.Equal(t, []string{
		"measure/sw_metric/hot/20261012/shard-0",
		"measure/sw_metric/hot/20261018/shard-0",
		"measure/sw_metric/hot/20261018/shard-1",
		"measure/sw_metric/hot/20261018/segment",
		"measure/sw_metric/warm/20261010/shard-0",
		"property/sw_prop/shard-0",
		"property/sw_prop/shard-1",
		"stream/sw_stream/cold/20261018/shard-0",
	}, keys)
	shard0 := items[1]
	require.Len(t, shard0.Sources, 2)
	assert.Equal(t, "data-a", shard0.Sources[0].Node)
	assert.Equal(t, []string{
		"nodes/data-a/measure/sw_metric/seg-20261018/shard-0-00001.csv.gz",
		"nodes/data-a/measure/sw_metric/seg-20261018/shard-0-00002.csv.gz",
	}, []string{shard0.Sources[0].Files[0].Path, shard0.Sources[0].Files[1].Path}, "files in sequence order")
	seg := items[3]
	assert.Equal(t, SegmentItem, seg.Kind, "a mixed group splits its segment files into their own item")
	assert.Len(t, seg.Sources, 2)
	prop0 := items[5]
	assert.Equal(t, PropertyItem, prop0.Kind)
	assert.Equal(t, "", prop0.Stage)
	assert.Len(t, prop0.Sources, 2, "property shards are grouped by source shard id across nodes")
	assert.Len(t, items[6].Sources, 1)
	assert.Equal(t, "UTC", prop0.Sources[0].Timezone)
}

func TestDecide(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	assert.Equal(t, []string{
		"measure/sw_metric/hot/20261018/shard-0",
		"measure/sw_metric/hot/20261018/shard-1",
		"measure/sw_metric/hot/20261018/segment",
		"measure/sw_metric/warm/20261010/shard-0",
		"property/sw_prop/shard-0",
		"property/sw_prop/shard-1",
		"stream/sw_stream/cold/20261018/shard-0",
	}, entryKeys(p))
	require.Len(t, p.Rejected, 1)
	rej := p.Rejected[0]
	assert.Equal(t, GateTTL, rej.Gate)
	assert.Equal(t, "measure/sw_metric/hot/20261012/shard-0", rej.Item.Key())
	assert.Equal(t, []string{"measure/sw_metric/hot seg=20261012 shard=0: every row is older than the target retention " +
		"(newest 2026-10-12T10:00:00Z, retention 3d = group 3d); skipped; --force-ttl imports it"},
		p.GateDetails)

	byKey := entriesByKey(p)
	replicated := byKey["measure/sw_metric/hot/20261018/shard-0"]
	require.Len(t, replicated.Item.Sources, 1, "a true replica is pushed once")
	assert.Equal(t, "data-b", replicated.Item.Sources[0].Node)
	assert.Equal(t, uint64(100), replicated.Item.Rows())
	assert.Equal(t, []string{"dst-hot1"}, replicated.TargetNodes)
	assert.Equal(t, uint32(1), replicated.Copies)
	assert.Len(t, byKey["measure/sw_metric/hot/20261018/segment"].Item.Sources, 2, "segment files are pushed from every source")
	assert.Len(t, byKey["property/sw_prop/shard-0"].Item.Sources, 2)
	assert.Equal(t, "", byKey["property/sw_prop/shard-0"].TargetStage)

	warm := byKey["measure/sw_metric/warm/20261010/shard-0"]
	assert.Equal(t, "warm", warm.TargetStage, "within group 3d + warm 7d")
	assert.Equal(t, uint32(2), warm.Copies)
	assert.Contains(t, p.Warnings, "group sw_metric stage warm: 2 copies wanted but only 1 node(s) match; each row is written to every one of them")

	stream := byKey["stream/sw_stream/cold/20261018/shard-0"]
	assert.Equal(t, "hot", stream.TargetStage, "a stage the target lacks falls back to hot")
	assert.Equal(t, uint32(1), stream.Copies, "no route yet: copies come from the predicted group")
	assert.Empty(t, stream.TargetNodes)
	assert.Contains(t, p.Warnings, `group sw_stream: the target has no stage "cold"; its units go to hot`)

	require.Len(t, p.MultiSource, 1)
	assert.Equal(t, "measure/sw_metric/hot/20261018/shard-0", p.MultiSource[0].Key)
	assert.False(t, p.Refused())
}

func TestDecideForceTTL(t *testing.T) {
	p := Decide(decideInputs(t, Flags{ForceTTL: true}))
	assert.Empty(t, p.Rejected)
	var expired []string
	for _, e := range p.Entries {
		if e.TTLExpired {
			expired = append(expired, e.Item.Key())
		}
	}
	assert.Equal(t, []string{"measure/sw_metric/hot/20261012/shard-0"}, expired)
	require.Len(t, p.GateDetails, 1)
	assert.True(t, strings.HasSuffix(p.GateDetails[0], "; imported anyway because of --force-ttl"))
	assert.True(t, slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.Contains(w, "imported although expired") }))
}

func TestDecideAllSourcesAndStrictCoverage(t *testing.T) {
	p := Decide(decideInputs(t, Flags{AllSources: true}))
	byKey := entriesByKey(p)
	e, ok := byKey["measure/sw_metric/hot/20261018/shard-0"]
	require.True(t, ok, "the --all-sources item must be accepted")
	assert.Len(t, e.Item.Sources, 2)
	require.Len(t, p.MultiSource, 1)
	for _, d := range p.MultiSource[0].Decisions {
		assert.True(t, d.Chosen, d.Node)
	}
	assert.True(t, slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.Contains(w, "--all-sources pushes 2 sources") }))

	p = Decide(decideInputs(t, Flags{StrictCoverage: true}))
	assert.Equal(t, []string{"TTL measure/sw_metric/hot/20261012/shard-0"}, rejectedGates(p))
	assert.Contains(t, entryKeys(p), "measure/sw_metric/hot/20261018/segment", "index-mode segment items deduplicate by version")
	assert.False(t, p.Refused())

	p = Decide(decideInputs(t, Flags{AllSources: true, StrictCoverage: true}))
	assert.Equal(t, []string{"TTL measure/sw_metric/hot/20261012/shard-0", "COVERAGE measure/sw_metric/hot/20261018/shard-0"}, rejectedGates(p))
	assert.True(t, p.Refused())
}

func TestDecideForceTTLDoesNotClaimImportOfACoverageRejection(t *testing.T) {
	in := decideInputs(t, Flags{ForceTTL: true, AllSources: true, StrictCoverage: true})
	in.Now = decideNow.Add(30 * 24 * time.Hour)
	p := Decide(in)
	const key = "measure/sw_metric/hot/20261018/shard-0"
	var gates []string
	for _, r := range p.Rejected {
		if r.Item.Key() == key {
			gates = append(gates, r.Gate)
		}
	}
	assert.Equal(t, []string{GateCoverage}, gates)
	assert.False(t, slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.HasPrefix(w, key+": imported although expired") }))
	assert.False(t, slices.ContainsFunc(p.GateDetails, func(d string) bool {
		return strings.Contains(d, "measure/sw_metric/hot seg=20261018 shard=0") && strings.Contains(d, "imported because of --force-ttl")
	}), p.GateDetails)
	assert.True(t, slices.ContainsFunc(p.Warnings, func(w string) bool {
		return strings.HasPrefix(w, "measure/sw_metric/hot/20261018/shard-1: imported although expired")
	}), "an accepted expired item still warns")
}

func TestDecideSchemaGate(t *testing.T) {
	in := decideInputs(t, Flags{})
	for i := range in.Actions {
		if in.Actions[i].ID() == "measure/sw_metric/cpm" {
			in.Actions[i].Disposition, in.Actions[i].Reason = DispositionConflict, "policy=error (default)"
		}
	}
	assertGroupSchemaRefused(t, Decide(in), "CONFLICT measure/sw_metric/cpm")
}

func TestDecideTopNResultConflictRefusesImport(t *testing.T) {
	in := decideInputs(t, Flags{})
	in.Actions = append(in.Actions, SchemaAction{Kind: "measure", Group: "sw_metric", Name: "_top_n_result", Disposition: DispositionConflict})
	assertGroupSchemaRefused(t, Decide(in), "CONFLICT measure/sw_metric/_top_n_result")
}

func TestDecideIsDeterministic(t *testing.T) {
	want := Decide(decideInputs(t, Flags{}))
	var wantOut bytes.Buffer
	require.NoError(t, Render(&wantOut, "table", want))
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // a seeded shuffle keeps the test reproducible
	for i := 0; i < 10; i++ {
		in := decideInputs(t, Flags{})
		m := in.Artifact.Manifest
		rng.Shuffle(len(m.Units), func(a, b int) { m.Units[a], m.Units[b] = m.Units[b], m.Units[a] })
		for u := range m.Units {
			srcs := m.Units[u].Sources
			rng.Shuffle(len(srcs), func(a, b int) { srcs[a], srcs[b] = srcs[b], srcs[a] })
		}
		in.Now = decideNow.Add(time.Duration(i) * time.Minute)
		got := Decide(in)
		assert.Equal(t, want, got, "shuffle %d", i)
		var out bytes.Buffer
		require.NoError(t, Render(&out, "table", got))
		assert.Equal(t, wantOut.String(), out.String(), "shuffle %d", i)
	}
}

func TestComparePlans(t *testing.T) {
	a := Decide(decideInputs(t, Flags{}))
	b := Decide(decideInputs(t, Flags{}))
	for i := range b.Schema {
		b.Schema[i].Disposition = DispositionOK
	}
	assert.Empty(t, ComparePlans(a, b), "schema dispositions are not compared")

	in := decideInputs(t, Flags{})
	in.Target.Routes[NewRouteKey("sw_metric", "hot")] = testRoute("sw_metric", "hot", 1, "dst-hot2")
	diff := ComparePlans(a, Decide(in))
	require.NotEmpty(t, diff)
	assert.True(t, strings.HasPrefix(diff[0], "- entry measure/sw_metric/hot/20261018/shard-0"))
	assert.True(t, slices.ContainsFunc(diff, func(l string) bool { return strings.Contains(l, "nodes=[dst-hot2]") }))

	c := Decide(decideInputs(t, Flags{ForceTTL: true}))
	assert.Contains(t, ComparePlans(a, c), "- rejected measure/sw_metric/hot/20261012/shard-0 gate=TTL: every row is older than the target stage's retention")
}

func TestNonEmptyFromPlan(t *testing.T) {
	frames := []*transferv1.UnitFrame{{NodeId: "n1", Units: []*transferv1.UnitInventory{
		{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{Group: "full"}, Shards: []*transferv1.ShardStat{{PartsCount: 1}},
		}}},
		{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{Group: "idx"}, SegmentLevel: &transferv1.SidxStat{DocCount: 3},
		}}},
		{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{Group: "empty"}, Shards: []*transferv1.ShardStat{{}},
		}}},
		{Kind: &transferv1.UnitInventory_Property{Property: &transferv1.PropertyInventory{
			Group: "prop", Shards: []*transferv1.PropertyShardStat{{DocCount: 1}},
		}}},
	}}}
	got := NonEmptyFromPlan([]string{"full", "idx", "empty", "prop"}, frames, nil)
	assert.Equal(t, map[string]NonEmptyState{
		"full": {Known: true, NonEmpty: true}, "idx": {Known: true, NonEmpty: true},
		"empty": {Known: true}, "prop": {Known: true, NonEmpty: true},
	}, got)
	got = NonEmptyFromPlan([]string{"full"}, frames, []string{"n2"})
	assert.False(t, got["full"].Known)
	assert.Contains(t, got["full"].Reason, "n2")
}

// standaloneInputs is the decide fixture on a standalone target that keeps the groups' stages
// in its schema; routes holds the Targets answers already known.
func standaloneInputs(t *testing.T, flags Flags, routes ...*transferv1.TargetsResponse) Inputs {
	t.Helper()
	in := decideInputs(t, flags)
	in.Target.Standalone = true
	in.Target.DataNodes = nil
	in.Target.Routes = map[RouteKey]*transferv1.TargetsResponse{}
	for _, r := range routes {
		in.Target.Routes[NewRouteKey(r.GetGroup(), r.GetStage())] = r
	}
	return in
}

// standaloneNode is the node id a standalone target reports, in Targets and Plan alike.
const standaloneNode = "standalone-1"

// standaloneRoute is a standalone Targets answer to a non-default stage: hot, one copy, fallen back.
func standaloneRoute(group string) *transferv1.TargetsResponse {
	return testRoute(group, "hot", 1, standaloneNode, standaloneNode)
}

func TestDecideStandaloneTargetPutsEveryStageInHot(t *testing.T) {
	routes := []*transferv1.TargetsResponse{standaloneRoute("sw_metric"), standaloneRoute("sw_stream")}
	p := Decide(standaloneInputs(t, Flags{}, routes...))
	var warm *Rejection
	for i := range p.Rejected {
		if p.Rejected[i].Item.Key() == "measure/sw_metric/warm/20261010/shard-0" {
			warm = &p.Rejected[i]
		}
	}
	require.NotNil(t, warm, "the standalone target keeps the group ttl only, so the warm unit is expired")
	assert.Equal(t, GateTTL, warm.Gate)
	assert.Equal(t, "hot", warm.TargetStage)
	assert.Equal(t, []string{standaloneNode}, warm.TargetNodes)
	assert.Contains(t, p.GateDetails,
		"measure/sw_metric/hot seg=20261010 shard=0: every row is older than the target retention "+
			"(newest 2026-10-12T00:00:00Z, retention 3d = group 3d); skipped; --force-ttl imports it")
	assert.Contains(t, p.Warnings, `group sw_metric: the standalone target routes stage "warm" to hot`)
	assert.Contains(t, p.Warnings, `group sw_stream: the standalone target routes stage "cold" to hot`)
	assert.False(t, slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.Contains(w, "routes stage hot to hot") }))

	p = Decide(standaloneInputs(t, Flags{ForceTTL: true}, routes...))
	byKey := entriesByKey(p)
	e, ok := byKey["measure/sw_metric/warm/20261010/shard-0"]
	require.True(t, ok)
	assert.Equal(t, "hot", e.TargetStage)
	assert.Equal(t, uint32(1), e.Copies)
	assert.Equal(t, []string{standaloneNode}, e.TargetNodes)
}

func TestDecideStandaloneTargetVerifiesTheHotCounts(t *testing.T) {
	routes := []*transferv1.TargetsResponse{standaloneRoute("sw_metric"), standaloneRoute("sw_stream")}
	p := Decide(standaloneInputs(t, Flags{}, routes...))
	pushed := map[PushKey]uint64{}
	for _, e := range p.Entries {
		if e.Item.Group == "sw_stream" {
			pushed[pushKey(e.Item.Catalog, e.Item.Group, e.TargetStage, e.Item.Kind)] += e.Item.Rows()
		}
	}
	require.Equal(t, map[PushKey]uint64{pushKey(stream, "sw_stream", "hot", ShardItem): 2000}, pushed)
	post := emptyCounts().set(stream, "sw_stream", "hot", standaloneNode, 0, "20261018", 2000)
	routeMap := map[RouteKey]*transferv1.TargetsResponse{NewRouteKey("sw_stream", "hot"): routes[1]}
	r := evaluate(emptyCounts(), post, routeMap, pushed)
	assert.False(t, r.Failed(), r.Mismatches)
}

func TestDecideStandaloneCreatedGroupKeepsOneCopy(t *testing.T) {
	first := Decide(standaloneInputs(t, Flags{ForceTTL: true}))
	for _, e := range first.Entries {
		if e.Item.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
			assert.Equal(t, uint32(1), e.Copies, e.Item.Key())
		}
	}
	routes := []*transferv1.TargetsResponse{
		standaloneRoute("sw_metric"), standaloneRoute("sw_stream"), testRoute("sw_prop", "hot", 1, standaloneNode, standaloneNode),
	}
	second := Decide(standaloneInputs(t, Flags{ForceTTL: true}, routes...))
	assert.Empty(t, ComparePlans(first, second))
}

func TestDecideFallbackRouteAddsNoHotToHotWarning(t *testing.T) {
	in := decideInputs(t, Flags{})
	in.Target.Routes[NewRouteKey("sw_stream", "hot")] = testRoute("sw_stream", "hot", 1, "dst-hot1")
	hot := in.Artifact.Manifest.Units[3]
	hot.Stage, hot.Segment = "hot", "20261017"
	in.Artifact.Manifest.Units = append(in.Artifact.Manifest.Units, hot)
	p := Decide(in)
	assert.False(t, slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.Contains(w, "routes stage") }))
	var noStage []string
	for _, w := range p.Warnings {
		if strings.Contains(w, "has no stage") {
			noStage = append(noStage, w)
		}
	}
	assert.Equal(t, []string{`group sw_stream: the target has no stage "cold"; its units go to hot`}, noStage)
}

func TestDecideUnknownCatalogRefusesTheImport(t *testing.T) {
	in := decideInputs(t, Flags{})
	in.Artifact.Manifest.Units = append(in.Artifact.Manifest.Units, manifest.Unit{Catalog: "BOGUS", Group: "sw_x", Stage: "hot", Segment: "20261018"})
	p := Decide(in)
	assert.Empty(t, p.Entries)
	assert.Equal(t, []string{`unit sw_x/20261018 has unknown catalog "BOGUS"`}, p.Errors)
	assert.True(t, p.Refused())
	assert.Contains(t, refusalReasons(p), `unknown catalog "BOGUS"`)
	p.SchemaOnly = true
	assert.True(t, p.Refused(), "an artifact error refuses a schema-only import too")
}

func TestDecideSchemaOnlyIgnoresTheDataGates(t *testing.T) {
	expiredInputs := func(flags Flags) Inputs {
		in := decideInputs(t, flags)
		in.Now = decideNow.Add(365 * 24 * time.Hour)
		m := in.Artifact.Manifest
		m.Units = slices.DeleteFunc(m.Units, func(u manifest.Unit) bool { return u.Catalog == "PROPERTY" })
		return in
	}
	p := Decide(expiredInputs(Flags{}))
	require.Empty(t, p.Entries)
	require.True(t, p.Refused(), "every work item is expired")

	p = Decide(expiredInputs(Flags{SchemaOnly: true}))
	assert.False(t, p.Refused())
	assert.NotContains(t, result(p), "refused")

	p = Decide(decideInputs(t, Flags{SchemaOnly: true, AllSources: true, StrictCoverage: true}))
	require.True(t, slices.ContainsFunc(p.Rejected, func(r Rejection) bool { return r.Gate == GateCoverage }))
	assert.False(t, p.Refused())

	in := decideInputs(t, Flags{SchemaOnly: true})
	in.Actions[0].Disposition = DispositionConflict
	assert.True(t, Decide(in).Refused(), "a blocking schema action still refuses")
}

func TestRefusalReasonsOmitTheDryRunWording(t *testing.T) {
	p := Decide(decideInputs(t, Flags{AllSources: true, StrictCoverage: true}))
	require.True(t, p.Refused())
	reasons := refusalReasons(p)
	assert.Equal(t, "--strict-coverage refuses multi-source units", reasons)
	assert.NotContains(t, reasons, "exit 2")
	assert.NotContains(t, reasons, "nothing is written")
	assert.Equal(t, "refused, nothing written (exit 2)", result(p))
}

func TestSegmentItemsSpanTheirSegment(t *testing.T) {
	items, err := BuildWorkItems(decideManifest())
	require.NoError(t, err)
	seg := items[3]
	require.Equal(t, SegmentItem, seg.Kind)
	lo, hi := seg.TimeRange()
	assert.Equal(t, ts("2026-10-18T00:00:00Z"), lo)
	assert.Equal(t, ts("2026-10-19T00:00:00Z")-1, hi)
}

// A target without the item's group has no stage of it to miss: the schema refuses the item,
// and no stage fallback is reported.
func TestMapStage_NoWarningForAGroupTheTargetLacks(t *testing.T) {
	p := &Plan{}
	item := &WorkItem{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "absent", Stage: "warm"}
	assert.Equal(t, "hot", mapStage(Inputs{Target: NewTargetState()}, p, item))
	assert.Empty(t, p.Warnings)
}
