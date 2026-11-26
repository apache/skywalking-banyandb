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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// presenceFixture is a done shard item of shard 1 of catalog group g (hot, source shard number 2)
// imported in format into a target stage of dstShards shards, with 6 rows landed.
func presenceFixture(format string, catalog commonv1.Catalog, dstShards uint32) (*Artifact, *TargetState, []doneItem) {
	art := &Artifact{Manifest: &manifest.Manifest{Format: format, Groups: []manifest.Group{{
		Name: "g", Stages: []manifest.Stage{{Name: "hot", ShardNum: 2, SegmentInterval: "1d"}},
	}}}}
	target := NewTargetState()
	target.Groups["g"] = &commonv1.Group{ResourceOpts: &commonv1.ResourceOpts{ShardNum: dstShards, SegmentInterval: dayRule(1), Ttl: dayRule(3)}}
	newest := dataTime.UnixNano()
	item := WorkItem{
		Catalog: catalog, Group: "g", Stage: "hot", Segment: "20261016", Shard: 1, Kind: ShardItem,
		Sources: []WorkSource{{Node: "s", MinTimestamp: newest, MaxTimestamp: newest, Rows: 6}},
	}
	return art, target, []doneItem{{entry: &Entry{Item: item, TargetStage: "hot"}, rows: 6}}
}

// shardCounts counts, per node and shard, the rows of group g stage hot of catalog in segments.
func shardCounts(catalog commonv1.Catalog, rows map[[2]any]map[string]int64) *Counts {
	c := &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}, Answered: []string{"d1", "d2", "d3"}}
	for k, segs := range rows {
		c.Shards[CountKey{Catalog: catalog, Group: "g", Stage: "hot", Node: k[0].(string), Shard: k[1].(uint32)}] = segs
	}
	return c
}

// The rows of a CSV item, and of any measure item, keep their shard when the target stage has the
// source's shard number: they are checked per shard and segment, the replicas summed only to tell
// that the shard holds rows.
func TestMissingData_ShardItemPerShardWhenTheShardNumberIsTheSame(t *testing.T) {
	for _, tc := range []struct {
		format  string
		catalog commonv1.Catalog
	}{
		{manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM},
		{manifest.FormatCSV, commonv1.Catalog_CATALOG_MEASURE},
		{manifest.FormatCSV, commonv1.Catalog_CATALOG_TRACE},
		{manifest.FormatNative, commonv1.Catalog_CATALOG_MEASURE},
	} {
		t.Run(tc.format+"/"+commonv1.Catalog_name[int32(tc.catalog)], func(t *testing.T) {
			art, target, done := presenceFixture(tc.format, tc.catalog, 2)
			check := func(rows map[[2]any]map[string]int64) presenceCheck {
				got, err := missingData(art, target, shardCounts(tc.catalog, rows), done, dataNow)
				require.NoError(t, err)
				assert.Empty(t, got.coarse, "checked per shard")
				return got
			}
			// One replica holds the shard: it exists, whatever the other replica counts.
			assert.Empty(t, check(map[[2]any]map[string]int64{{"d1", uint32(1)}: {"20261016": 2}, {"d2", uint32(1)}: {"20261016": 0}}).missing)
			// Fewer rows than imported is not this check's business: the shard holds some.
			assert.Empty(t, check(map[[2]any]map[string]int64{{"d2", uint32(1)}: {"20261016": 1}}).missing)

			got := check(map[[2]any]map[string]int64{{"d1", uint32(0)}: {"20261016": 9}, {"d2", uint32(0)}: {"20261016": 9}})
			require.Len(t, got.missing, 1, "rows in another shard only")
			assert.Contains(t, got.missing[0], "g stage hot shard 1: 6 row(s) imported")
			assert.Contains(t, got.missing[0], "no node of the target holds any in the segments of")

			got = check(map[[2]any]map[string]int64{{"d1", uint32(1)}: {"20261010": 9}})
			require.Len(t, got.missing, 1, "rows of the shard in another segment only")

			unreachable := shardCounts(tc.catalog, map[[2]any]map[string]int64{{"d1", uint32(1)}: {"20261016": 4}})
			unreachable.Unreachable = []string{"d1"}
			res, err := missingData(art, target, unreachable, done, dataNow)
			require.NoError(t, err)
			assert.Empty(t, res.missing, "a node the Plan did not count may hold the shard")
			require.Len(t, res.unchecked, 1, "the shard cannot be checked")
			assert.Contains(t, res.unchecked[0], "g stage hot shard 1: 6 row(s) imported")
			assert.Contains(t, res.unchecked[0], "not counted: d1")
			assert.Equal(t, []string{"d1"}, res.blind())

			// The route names the nodes of the shard: one that answered and holds none is missing
			// data, whatever node did not answer.
			target.Routes[NewRouteKey("g", "hot")] = &transferv1.TargetsResponse{Shards: []*transferv1.TargetsResponse_Shard{{ShardId: 1, NodeIds: []string{"d2"}}}}
			defer delete(target.Routes, NewRouteKey("g", "hot"))
			res, err = missingData(art, target, unreachable, done, dataNow)
			require.NoError(t, err)
			assert.Len(t, res.missing, 1)
			assert.Empty(t, res.unchecked)
			target.Routes[NewRouteKey("g", "hot")].Shards[0].NodeIds = []string{"d1", "d2"}
			res, err = missingData(art, target, unreachable, done, dataNow)
			require.NoError(t, err)
			assert.Empty(t, res.missing)
			assert.Len(t, res.unchecked, 1, "a route node of the shard did not answer")
		})
	}
}

// Another shard number routes the rows to other shards: the item is checked per (group, stage),
// and the check says so once per (catalog, group, stage).
func TestMissingData_ShardItemPerGroupStageWhenTheShardNumberDiffers(t *testing.T) {
	art, target, done := presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM, 3)
	second := *done[0].entry
	second.Item.Shard = 0
	done = append(done, doneItem{entry: &second, rows: 3})
	got, err := missingData(art, target, shardCounts(commonv1.Catalog_CATALOG_STREAM,
		map[[2]any]map[string]int64{{"d1", uint32(2)}: {"20261016": 9}}), done, dataNow)
	require.NoError(t, err)
	assert.Empty(t, got.missing, "any shard of the stage holding rows will do")
	assert.Equal(t, []string{"the data check of stream g stage hot is per (group, stage), not per shard: " +
		"its target stage has 3 shard(s), the source stage 2, so the rows were routed to other shards"}, got.coarse)

	got, err = missingData(art, target, shardCounts(commonv1.Catalog_CATALOG_STREAM, nil), done, dataNow)
	require.NoError(t, err)
	require.Len(t, got.missing, 2)
	assert.Contains(t, got.missing[0], "stream g stage hot: 6 row(s) imported")

	// A node the Plan did not count may hold the rows of the stage: they are not checked.
	unreachable := shardCounts(commonv1.Catalog_CATALOG_STREAM, nil)
	unreachable.Unreachable = []string{"d3"}
	got, err = missingData(art, target, unreachable, done, dataNow)
	require.NoError(t, err)
	assert.Empty(t, got.missing)
	require.Len(t, got.unchecked, 2)
	assert.Contains(t, got.unchecked[0], "stream g stage hot: 6 row(s) imported")
	assert.Contains(t, got.unchecked[0], "not counted: d3")

	// A source stage the manifest does not describe: the shard numbers are not known.
	art.Manifest.Groups = nil
	got, err = missingData(art, target, shardCounts(commonv1.Catalog_CATALOG_STREAM, nil), done, dataNow)
	require.NoError(t, err)
	assert.Equal(t, []string{"the data check of stream g stage hot is per (group, stage), not per shard: " +
		"the shard number of its source or target stage is not known"}, got.coarse)
}

// A target segment whose suffix is not a time of the stage's interval is refused instead of being
// matched loosely, naming the catalog, group, stage, node and segment.
func TestMissingData_UnparseableSegmentSuffixIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		art    func() (*Artifact, *TargetState, []doneItem)
		suffix string
		want   string
	}{
		{
			name: "csv per shard", suffix: "2026101600",
			art: func() (*Artifact, *TargetState, []doneItem) {
				return presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM, 2)
			},
			want: `segment "2026101600" of stream group g stage hot on node d2, whose suffix is not a time of the stage's 1d segment interval`,
		},
		{
			name: "csv per group and stage", suffix: "garbage",
			art: func() (*Artifact, *TargetState, []doneItem) {
				return presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM, 4)
			},
			want: `segment "garbage" of stream group g stage hot on node d2`,
		},
		{
			name: "native per node", suffix: "20261399",
			art: func() (*Artifact, *TargetState, []doneItem) {
				art, target, done := presenceFixture(manifest.FormatNative, commonv1.Catalog_CATALOG_STREAM, 2)
				done[0].landed = map[string]uint64{"d2": 6}
				return art, target, done
			},
			want: `segment "20261399" of stream group g stage hot on node d2`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			art, target, done := tc.art()
			counts := shardCounts(commonv1.Catalog_CATALOG_STREAM, map[[2]any]map[string]int64{
				{"d1", uint32(1)}: {"20261016": 6}, {"d2", uint32(1)}: {tc.suffix: 1},
			})
			_, err := missingData(art, target, counts, done, dataNow)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A native stream item's node that the Plan did not count is checked per shard over the nodes it
// counted: rows of the shard elsewhere pass; none is unchecked while the node is unreachable, and
// missing once it is gone from the cluster (neither answered nor reported unreachable).
func TestMissingData_NativeNodeNotCountedFallsBackToTheShard(t *testing.T) {
	art, target, done := presenceFixture(manifest.FormatNative, commonv1.Catalog_CATALOG_STREAM, 2)
	done[0].landed = map[string]uint64{"d1": 6, "gone": 6}
	counts := func(rows map[[2]any]map[string]int64, unreachable ...string) *Counts {
		c := shardCounts(commonv1.Catalog_CATALOG_STREAM, rows)
		c.Answered = []string{"d1", "d2"}
		c.Unreachable = unreachable
		return c
	}
	check := func(c *Counts) presenceCheck {
		got, err := missingData(art, target, c, done, dataNow)
		require.NoError(t, err)
		return got
	}
	got := check(counts(map[[2]any]map[string]int64{{"d1", uint32(1)}: {"20261016": 6}}))
	assert.Empty(t, got.missing, "the shard holds rows on a node the Plan counted")
	assert.Empty(t, got.unchecked)

	done[0].landed = map[string]uint64{"gone": 6}
	got = check(counts(map[[2]any]map[string]int64{{"d2", uint32(0)}: {"20261016": 6}}))
	require.Len(t, got.missing, 1, "no node holds the shard of a node gone from the cluster")
	assert.Contains(t, got.missing[0], "stream g stage hot shard 1: 6 row(s) imported (stream/g/hot/20261016/shard-1) on gone, which the count did not reach")
	assert.Empty(t, got.unchecked)

	done[0].landed = map[string]uint64{"d3": 6}
	got = check(counts(nil, "d3"))
	assert.Empty(t, got.missing)
	require.Len(t, got.unchecked, 1, "the node is unreachable: run again")
	assert.Contains(t, got.unchecked[0], "not counted: d3")
}

func TestMissingData_NativeStreamPerNodeShardAndSegment(t *testing.T) {
	art := &Artifact{Manifest: &manifest.Manifest{Format: manifest.FormatNative}}
	target := NewTargetState()
	target.Groups["g"] = &commonv1.Group{ResourceOpts: &commonv1.ResourceOpts{SegmentInterval: dayRule(1), Ttl: dayRule(3)}}
	newest := dataTime.UnixNano()
	item := WorkItem{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "20261016", Shard: 1, Kind: ShardItem,
		Sources: []WorkSource{{Node: "s", MinTimestamp: newest, MaxTimestamp: newest}},
	}
	done := []doneItem{{entry: &Entry{Item: item, TargetStage: "hot"}, landed: map[string]uint64{"d1": 5, "d2": 5}, rows: 10}}
	key := func(node string) CountKey {
		return CountKey{Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Node: node, Shard: 1}
	}
	counts := func(segs map[string]map[string]int64) *Counts {
		c := &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}, Answered: []string{"d1", "d2"}}
		for node, s := range segs {
			c.Shards[key(node)] = s
		}
		return c
	}

	missing := func(c *Counts, now time.Time) []string {
		got, err := missingData(art, target, c, done, now)
		require.NoError(t, err)
		return got.missing
	}
	assert.Empty(t, missing(counts(map[string]map[string]int64{"d1": {"20261016": 5}, "d2": {"20261016": 3}}), dataNow))

	got := missing(counts(map[string]map[string]int64{"d1": {"20261016": 5}, "d2": {"20261016": 0}}), dataNow)
	require.Len(t, got, 1, "a node with the segment counted empty")
	assert.Contains(t, got[0], "stream g stage hot shard 1 on d2: 5 row(s) imported")

	got = missing(counts(map[string]map[string]int64{"d1": {"20261016": 5}, "d2": {"20261010": 9}}), dataNow)
	require.Len(t, got, 1, "a node holding another segment only")
	assert.Contains(t, got[0], "on d2")

	got = missing(counts(map[string]map[string]int64{"d1": {"20261016": 5}}), dataNow)
	require.Len(t, got, 1, "a node holding nothing")

	unreachable := counts(map[string]map[string]int64{"d1": {"20261016": 5}})
	unreachable.Unreachable = []string{"d2"}
	assert.Empty(t, missing(unreachable, dataNow), "a node not counted is not checked")

	assert.Empty(t, missing(counts(nil), dataNow.AddDate(0, 0, 10)), "a segment past the ttl is not checked")
}

// A property item's group must hold documents on the target; a node the Plan did not count may
// hold them.
func TestMissingData_PropertyGroupMustHoldDocuments(t *testing.T) {
	art, target, _ := presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM, 2)
	item := WorkItem{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Group: "p", Kind: PropertyItem, Sources: []WorkSource{{Node: "s", Rows: 3}}}
	done := []doneItem{{entry: &Entry{Item: item}, rows: 3}}
	c := shardCounts(commonv1.Catalog_CATALOG_STREAM, nil)
	c.Properties = map[string]int64{"p": 1}
	got, err := missingData(art, target, c, done, dataNow)
	require.NoError(t, err)
	assert.Empty(t, got.missing)

	c.Properties = map[string]int64{}
	got, err = missingData(art, target, c, done, dataNow)
	require.NoError(t, err)
	require.Len(t, got.missing, 1)
	assert.Contains(t, got.missing[0], "property p: 3 row(s) imported")
	assert.Contains(t, got.missing[0], "the target holds no documents of the group")

	c.Unreachable = []string{"d3"}
	got, err = missingData(art, target, c, done, dataNow)
	require.NoError(t, err)
	assert.Empty(t, got.missing)
	assert.Len(t, got.unchecked, 1)
}

// A trace group that samples spans in merges may have dropped every span imported into a shard:
// that is a note, not a refusal.
func TestMissingData_SampledTraceGroupIsNotRefused(t *testing.T) {
	art, target, done := presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_TRACE, 2)
	target.Groups["g"].Catalog = commonv1.Catalog_CATALOG_TRACE
	target.Groups["g"].Pipeline = &commonv1.TracePipelineConfig{
		Enabled: true, EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE},
		Plugins: []*commonv1.Plugin{{Name: "s", Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{Path: "s.so"}}}},
	}
	got, err := missingData(art, target, shardCounts(commonv1.Catalog_CATALOG_TRACE, nil), done, dataNow)
	require.NoError(t, err)
	assert.Empty(t, got.missing)
	require.Len(t, got.sampled, 1)
	assert.Contains(t, got.sampled[0], "trace g stage hot shard 1: 6 row(s) imported")
	assert.Contains(t, got.sampled[0], "not refused: the group samples spans in merges")
}

// presenceRun runs checkPresence on the done CSV item of presenceFixture against counts.
func presenceRun(t *testing.T, catalog commonv1.Catalog, counts *Counts) (string, error) {
	t.Helper()
	art, target, done := presenceFixture(manifest.FormatCSV, catalog, 2)
	saved := countTarget
	t.Cleanup(func() { countTarget = saved })
	countTarget = func(context.Context, *grpc.ClientConn, []*transferv1.Selector) (*Counts, error) { return counts, nil }
	item := done[0].entry.Item
	state := &progressState{Done: map[string]*itemRecord{item.Key(): {Complete: true}}}
	var log bytes.Buffer
	err := checkPresence(context.Background(), &log, nil, art, "p.json", state, &decision{state: target, plan: &Plan{Entries: []Entry{*done[0].entry}}},
		dataNow)
	return log.String(), err
}

// A shard no node that answered holds, while a node that may hold it did not answer, cannot be
// checked: exit 3 asks to run again, instead of taking the target for a reset one (exit 6).
func TestCheckPresence_UnreachableNodesExitRuntime(t *testing.T) {
	unreachable := shardCounts(commonv1.Catalog_CATALOG_STREAM, nil)
	unreachable.Unreachable = []string{"d1"}
	_, err := presenceRun(t, commonv1.Catalog_CATALOG_STREAM, unreachable)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(err))
	assert.Contains(t, err.Error(), "could not check the data the progress file p.json records as imported: node(s) d1 did not answer the count")
	assert.Contains(t, err.Error(), "run again once the node(s) are reachable")
	assert.NotContains(t, err.Error(), "looks reset")

	_, err = presenceRun(t, commonv1.Catalog_CATALOG_STREAM, shardCounts(commonv1.Catalog_CATALOG_STREAM, nil))
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitResume, exitcode.For(err))
	assert.Contains(t, err.Error(), "the target looks reset or is another cluster")

	log, err := presenceRun(t, commonv1.Catalog_CATALOG_STREAM, shardCounts(commonv1.Catalog_CATALOG_STREAM,
		map[[2]any]map[string]int64{{"d2", uint32(1)}: {"20261016": 1}}))
	require.NoError(t, err)
	assert.Empty(t, log)
}

// A native item with no rows, such as a segment-level series index, is recorded complete without
// any row landed: the target must still hold the schema of its group, or the target is a reset
// one, and nothing is pushed on it.
func TestCheckPresence_ItemWithoutRowsStillNeedsTheGroupSchema(t *testing.T) {
	art, target, done := presenceFixture(manifest.FormatNative, commonv1.Catalog_CATALOG_MEASURE, 2)
	item := done[0].entry.Item
	item.Kind, item.Shard = SegmentItem, 0
	item.Sources = []WorkSource{{Node: "s", MinTimestamp: dataTime.UnixNano(), MaxTimestamp: dataTime.UnixNano()}}
	entry := Entry{Item: item, TargetStage: "hot"}
	saved := countTarget
	t.Cleanup(func() { countTarget = saved })
	countTarget = func(context.Context, *grpc.ClientConn, []*transferv1.Selector) (*Counts, error) {
		return &Counts{Answered: []string{"d1"}}, nil
	}
	state := &progressState{Done: map[string]*itemRecord{item.Key(): {Complete: true}}}
	diff := &SchemaDiff{Actions: []SchemaAction{{Kind: "group", Group: "g", Disposition: DispositionCreate}}}
	var log bytes.Buffer
	err := checkPresence(context.Background(), &log, nil, art, "p.json", state,
		&decision{state: target, diff: diff, plan: &Plan{Entries: []Entry{entry}}}, dataNow)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitResume, exitcode.For(err))
	assert.Contains(t, err.Error(), "the target lacks group/g")
	assert.Contains(t, err.Error(), "the target looks reset or is another cluster")

	// With the schema in place the item has no row to count: nothing is refused.
	diff.Actions[0].Disposition = DispositionOK
	require.NoError(t, checkPresence(context.Background(), &log, nil, art, "p.json", state,
		&decision{state: target, diff: diff, plan: &Plan{Entries: []Entry{entry}}}, dataNow))
}

// The rows of an index-mode group live in its segment-level series index, which no part counts as
// pushed: a complete native segment item with rows is checked against the segment-level documents
// of the target, so a group that is there without them is refused, while an item without rows (a
// series index of a regular measure) has nothing to count.
func TestCheckPresence_IndexModeSegmentItemNeedsItsRows(t *testing.T) {
	art, target, done := presenceFixture(manifest.FormatNative, commonv1.Catalog_CATALOG_MEASURE, 2)
	segment := func(rows uint64) Entry {
		item := done[0].entry.Item
		item.Kind, item.Shard = SegmentItem, 0
		item.Sources = []WorkSource{{Node: "s", Rows: rows, MinTimestamp: dataTime.UnixNano(), MaxTimestamp: dataTime.UnixNano()}}
		return Entry{Item: item, TargetStage: "hot"}
	}
	saved := countTarget
	t.Cleanup(func() { countTarget = saved })
	run := func(entry Entry, counts *Counts) error {
		countTarget = func(context.Context, *grpc.ClientConn, []*transferv1.Selector) (*Counts, error) { return counts, nil }
		state := &progressState{Done: map[string]*itemRecord{entry.Item.Key(): {Complete: true}}}
		diff := &SchemaDiff{Actions: []SchemaAction{{Kind: "group", Group: "g", Disposition: DispositionOK}}}
		return checkPresence(context.Background(), &bytes.Buffer{}, nil, art, "p.json", state,
			&decision{state: target, diff: diff, plan: &Plan{Entries: []Entry{entry}}}, dataNow)
	}
	empty := &Counts{Shards: map[CountKey]map[string]int64{}, Segments: map[CountKey]map[string]int64{}, Answered: []string{"d1"}}
	held := &Counts{Shards: map[CountKey]map[string]int64{}, Answered: []string{"d1"}, Segments: map[CountKey]map[string]int64{
		{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "g", Stage: "hot", Node: "d1"}: {"20261016": 16},
	}}

	err := run(segment(16), empty)
	require.Error(t, err)
	assert.Equal(t, exitcode.ExitResume, exitcode.For(err))
	assert.Contains(t, err.Error(), "does not hold")
	assert.Contains(t, err.Error(), "16 row(s) imported")
	assert.Contains(t, err.Error(), "the target looks reset or is another cluster")

	require.NoError(t, run(segment(16), held), "the target holds the rows")
	require.NoError(t, run(segment(0), empty), "an item without rows has nothing to count")
}

// doneItems places an item at the stage the progress file records for it, as knownItems does
// for reconciliation, whether it is planned or refused now.
func TestDoneItems_UseTheRecordedStage(t *testing.T) {
	_, _, done := presenceFixture(manifest.FormatCSV, commonv1.Catalog_CATALOG_STREAM, 2)
	planned := *done[0].entry
	refused := planned.Item
	refused.Shard = 0
	state := &progressState{
		Done: map[string]*itemRecord{planned.Item.Key(): {Complete: true}, refused.Key(): {Complete: true}},
		Plan: []plannedItem{{Key: planned.Item.Key(), TargetStage: "warm"}, {Key: refused.Key(), TargetStage: "warm"}},
	}
	got := doneItems(state, &Plan{Entries: []Entry{planned}, Rejected: []Rejection{{Item: refused, TargetStage: "hot"}}}, false)
	require.Len(t, got, 2)
	for _, d := range got {
		assert.Equal(t, "warm", d.entry.TargetStage, d.entry.Item.Key())
		assert.Equal(t, uint64(6), d.rows)
	}
	assert.Equal(t, "hot", planned.TargetStage, "the plan is not changed")
}

// A complete native item that records no landed rows, every part of it having been left out by
// skip, holds nothing to check: it is not taken for a complete CSV item whose rows all landed.
func TestDoneItems_NativeItemWithNothingLandedHoldsNothing(t *testing.T) {
	_, _, done := presenceFixture(manifest.FormatNative, commonv1.Catalog_CATALOG_STREAM, 2)
	e := *done[0].entry
	state := &progressState{
		Done: map[string]*itemRecord{e.Item.Key(): {Complete: true, Tainted: []string{"d1"}}},
		Plan: []plannedItem{{Key: e.Item.Key(), TargetStage: "hot"}},
	}
	assert.Empty(t, doneItems(state, &Plan{Entries: []Entry{e}}, true))
	got := doneItems(state, &Plan{Entries: []Entry{e}}, false)
	require.Len(t, got, 1, "a complete CSV item keeps no cursor: all its rows landed")
	assert.Equal(t, uint64(6), got[0].rows)
}

// A reset target lacks every resource of the groups: the refusal names a few and counts the rest.
func TestLackingList_NamesAFewAndCountsTheRest(t *testing.T) {
	assert.Equal(t, "group/a, measure/a/m", lackingList([]string{"group/a", "measure/a/m"}))
	assert.Equal(t, "r0, r1, r2, r3, r4 and 2 more", lackingList([]string{"r0", "r1", "r2", "r3", "r4", "r5", "r6"}))
}
