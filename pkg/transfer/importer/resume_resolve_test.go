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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

func csvArtifact() *Artifact {
	return &Artifact{Manifest: &manifest.Manifest{Format: manifest.FormatCSV}}
}

// nativeShardEntry is nativeArtifact and its stream shard entry.
func nativeShardEntry(t *testing.T) (*Artifact, []Entry, *Entry) {
	t.Helper()
	art, entries := nativeArtifact(t)
	for i := range entries {
		if entries[i].Item.Key() == streamShardKey {
			return art, entries, &entries[i]
		}
	}
	t.Fatal("no shard entry")
	return nil, nil, nil
}

// skip is refused where verify cannot check what it leaves out: for an element index, without a
// baseline, and for a trace group sampling spans in merges. The refusal halts the resume and names
// the policies left.
func TestClassify_SkipIsRefusedWhereVerifyCannotCheck(t *testing.T) {
	_, _, native := nativeShardEntry(t)
	csvEntry := func(catalog commonv1.Catalog, group string) Entry {
		return Entry{Item: WorkItem{
			Catalog: catalog, Group: group, Stage: "hot", Segment: "s", Kind: ShardItem,
			Sources: []WorkSource{{Node: "n1", Rows: 10}},
		}, TargetStage: "hot"}
	}
	window := func() *itemRecord {
		return &itemRecord{Cursor: &csvCursor{File: "f", Record: 2, Rows: 2, Sent: &csvMark{File: "f", Record: 4, Rows: 4}}}
	}
	for name, tc := range map[string]struct {
		record     *itemRecord
		sampled    map[string]bool
		want       string
		entry      Entry
		noBaseline bool
	}{
		"element index": {
			entry:  *native,
			record: &itemRecord{Blobs: map[string]*payloadRecord{"n1/ELEMENT_INDEX": {CompletionSent: []string{"d2"}}}},
			want:   "--verify does not count element index documents",
		},
		"no baseline": {entry: csvEntry(commonv1.Catalog_CATALOG_STREAM, "g"), record: window(), noBaseline: true, want: "the first run recorded no baseline"},
		"sampled trace group": {
			entry: csvEntry(commonv1.Catalog_CATALOG_TRACE, "tg"), record: window(), sampled: map[string]bool{"tg": true},
			want: "skip is refused: trace group tg samples spans in merges",
		},
		"trace group keeping its spans": {entry: csvEntry(commonv1.Catalog_CATALOG_TRACE, "tg"), record: window()},
	} {
		t.Run(name, func(t *testing.T) {
			state := newProgress(csvArtifact(), testBindings())
			state.NoBaseline = tc.noBaseline
			state.Done[tc.entry.Item.Key()] = tc.record
			opts := config.Import{DuplicationPolicy: config.DuplicationSkip}
			amb := classify(state, []Entry{tc.entry}, &opts, tc.sampled, "")
			require.Len(t, amb, 1)
			if tc.want == "" {
				assert.Equal(t, policyAction(config.DuplicationSkip), amb[0].action, "a group that keeps its spans may be skipped")
				return
			}
			assert.Equal(t, policyAction(config.DuplicationBlock), amb[0].action)
			assert.Contains(t, amb[0].source, tc.want)
			err := checkBlocked(&bytes.Buffer{}, amb)
			require.Error(t, err)
			assert.Equal(t, exitcode.ExitHalted, exitcode.For(err))
			assert.Contains(t, err.Error(), "--resume-duplication-policy warn|push (skip stays refused where a line says so)")
		})
	}
}

// A payload recorded in flight to no node may have gone to any target node of its item.
func TestClassify_InflightWithoutNodes(t *testing.T) {
	_, _, e := nativeShardEntry(t)
	state := newProgress(csvArtifact(), testBindings())
	state.Inflight["0"] = inflightPayload{Key: e.Item.Key(), Source: "n1", Kind: "PART", PartID: 1, Rows: 4}
	amb := classify(state, []Entry{*e}, &config.Import{}, nil, "")
	require.Len(t, amb, 1)
	assert.Equal(t, e.TargetNodes, amb[0].nodes)

	p := &partPusher{opts: nativePushOptions()}
	_, err := p.sendRecorded(context.Background(), 0, 0, e, partRef("n1", 1, 4), &payloadRecord{}, &transferv1.ImportPartsRequest_Init{Kind: nativefmt.KindPart}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no target node")
}

func TestResolveResume_SkipOfARetryInFlightIsDoneInOneWrite(t *testing.T) {
	art, entries, _ := nativeShardEntry(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	state := newProgress(art, testBindings())
	// d2 failed the part before its completion; the retry to d2 was in flight at the crash.
	state.Done[streamShardKey] = &itemRecord{Parts: map[string]*payloadRecord{"n1/1": {Nodes: []string{"d1"}, Missing: []string{"d2"}, Rows: 4}}}
	state.Inflight["0"] = inflightPayload{Key: streamShardKey, Source: "n1", Kind: "PART", PartID: 1, Rows: 4, Nodes: []string{"d2"}}
	// An item a gate refuses this time keeps its payload in flight for a later run.
	other := inflightPayload{Key: "stream/sg/hot/20261017/shard-0", Source: "n1", Kind: "PART", PartID: 9, Rows: 2, Nodes: []string{"d1"}}
	state.Inflight["1"] = other
	var err error
	state.PlanDigest, err = writePlan(path, nil, nil)
	require.NoError(t, err)
	writes := countingFoldWrites(t)
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	defer func() { _ = track.close() }()

	opts := config.Import{DuplicationPolicy: config.DuplicationSkip}
	amb := classify(state, entries, &opts, nil, "")
	require.Len(t, amb, 1)
	var log bytes.Buffer
	rp, err := resolveResume(context.Background(), &log, track, state, entries, nil, amb, nil)
	require.NoError(t, err, log.String())
	assert.Equal(t, 1, writes(), "the decisions and the pushed rows are one write")
	assert.NotEmpty(t, rp.skipped, "skip forces verify")

	disk := readState(t, path)
	part := disk.Done[streamShardKey].Parts["n1/1"]
	assert.Empty(t, part.Missing, "a skipped node is not pushed again")
	assert.Equal(t, []string{"d2"}, part.Skipped)
	assert.True(t, rp.payload(streamShardKey, partRef("n1", 1, 4)).done(), "the push does not send it again")
	assert.Equal(t, []rowCount{{Catalog: "stream", Group: "sg", Stage: "hot", Kind: ShardItem.String(), Rows: 4}}, disk.Pushed)
	assert.Equal(t, map[string]inflightPayload{keptInflightKey(other): other}, disk.Inflight, "only the in-flight payloads of planned items are resolved")
	require.Len(t, disk.SkippedPayloads, 1)
	assert.Contains(t, disk.SkippedPayloads[0], streamShardKey+" part 1 of n1 on d2")
}

func TestResolveResume_CursorCoveringEveryRowCompletesTheItem(t *testing.T) {
	e := Entry{Item: WorkItem{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Rows: 3}},
	}, TargetStage: "hot"}
	state := newProgress(csvArtifact(), testBindings())
	state.Done[e.Item.Key()] = &itemRecord{Cursor: &csvCursor{File: "f", Record: 3, Rows: 3}}
	opts := config.Import{DuplicationPolicy: config.DuplicationBlock}
	amb := classify(state, []Entry{e}, &opts, nil, "")
	assert.Empty(t, amb, "every row acknowledged: nothing is ambiguous")
	track, err := startTracker(context.Background(), filepath.Join(t.TempDir(), "p.json"), state, 1, nil)
	require.NoError(t, err)
	defer func() { _ = track.close() }()
	_, err = resolveResume(context.Background(), &bytes.Buffer{}, track, state, []Entry{e}, nil, amb, nil)
	require.NoError(t, err)
	assert.True(t, state.Done[e.Item.Key()].Complete)
}

// A resume that pushes the window of a CSV item again counts its duplicates and clears the window:
// a later resume only counts what this one sends again, so every resume adds its own duplicates.
func TestResolveResume_RePushedWindowIsCountedOnce(t *testing.T) {
	e := Entry{Item: WorkItem{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Rows: 10}},
	}, TargetStage: "hot"}
	state := newProgress(csvArtifact(), testBindings())
	state.Duplicates = []rowCount{{Catalog: "stream", Group: "g", Stage: "hot", Kind: ShardItem.String(), Rows: 2, Payloads: []string{"earlier"}}}
	state.Done[e.Item.Key()] = &itemRecord{Cursor: &csvCursor{File: "f", Record: 2, Rows: 2, Sent: &csvMark{File: "f", Record: 5, Rows: 5}}}
	opts := config.Import{DuplicationPolicy: config.DuplicationWarn}
	amb := classify(state, []Entry{e}, &opts, nil, "")
	require.Len(t, amb, 1)
	path := filepath.Join(t.TempDir(), "p.json")
	track, err := startTracker(context.Background(), path, state, 1, nil)
	require.NoError(t, err)
	defer func() { _ = track.close() }()
	rp, err := resolveResume(context.Background(), &bytes.Buffer{}, track, state, []Entry{e}, nil, amb, nil)
	require.NoError(t, err)
	disk := readState(t, path)
	require.Len(t, disk.Duplicates, 2, "each resume adds the window it pushes again")
	assert.Equal(t, uint64(3), disk.Duplicates[1].Rows)
	c := disk.Done[e.Item.Key()].Cursor
	require.NotNil(t, c)
	assert.Nil(t, c.Sent, "the window is decided: a later resume does not count it again")
	assert.Equal(t, uint64(2), c.Rows)
	assert.Nil(t, rp.cursor(e.Item.Key()).Sent)
	assert.Empty(t, classify(disk, []Entry{e}, &opts, nil, ""), "with nothing sent since, nothing is ambiguous")
	tol := toleranceOf(disk.Duplicates)
	assert.Equal(t, uint64(5), tol[streamKey{catalog: commonv1.Catalog_CATALOG_STREAM, group: "g", stage: "hot"}].rows)
}

func TestResume_MeasureRePushIsAVerifyTolerance(t *testing.T) {
	res := &resolution{state: &progressState{Done: map[string]*itemRecord{}}, added: map[PushKey]uint64{}, records: map[string]*itemRecord{}}
	for _, tc := range []struct {
		entry *Entry
		want  uint64
	}{
		{entry: &Entry{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "m", Kind: ShardItem}, TargetStage: "hot"}, want: 5},
		// An index-mode measure is not counted by rows.
		{entry: &Entry{Item: WorkItem{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "im", Kind: SegmentItem}, TargetStage: "hot"}},
	} {
		a := &ambiguousPayload{entry: tc.entry, action: actionRepush, ref: partRef("data-1", 7, 5), nodes: []string{"data-a"}}
		pk := PushKey{streamKey: entryStreamKey(tc.entry), Kind: tc.entry.Item.Kind}
		var log bytes.Buffer
		res.applyPolicy(&log, a, pk, actionRepush, "")
		tol := toleranceOf(res.duplicates)
		if tc.want == 0 {
			assert.Nil(t, tol[pk.streamKey])
			continue
		}
		require.NotNil(t, tol[pk.streamKey])
		assert.Equal(t, tc.want, tol[pk.streamKey].rows)
		assert.Equal(t, []string{a.name()}, tol[pk.streamKey].payloads)
	}
	require.Len(t, res.duplicates, 1, "recorded, so a later run of verify tolerates them too")
	assert.Equal(t, uint64(5), res.duplicates[0].Rows)
}

// What the progress file records landed is where the item was pushed: at the stage it records.
func TestKnownItems_RecordedStage(t *testing.T) {
	_, entries, e := nativeShardEntry(t)
	state := newProgress(csvArtifact(), testBindings())
	state.Plan = []plannedItem{{Key: e.Item.Key(), TargetStage: "warm"}}
	items := knownItems(state, entries, nil)
	assert.Equal(t, "warm", items[e.Item.Key()].TargetStage)
	assert.Equal(t, "hot", e.TargetStage, "the planned entry is left as it is")
}

// The parts of an item not planned this time that may have landed taint their keys.
func TestSeedLanded_UnplannedAmbiguousPartsTaint(t *testing.T) {
	_, entries, e := nativeShardEntry(t)
	refused := e.Item
	refused.Shard = 1
	refused2 := e.Item
	refused2.Shard = 2
	state := newProgress(csvArtifact(), testBindings())
	state.Done[refused.Key()] = &itemRecord{Parts: map[string]*payloadRecord{"n1/1": {CompletionSent: []string{"d1"}, Rows: 5}}}
	state.Inflight["kept:x"] = inflightPayload{Key: refused2.Key(), Source: "n1", Kind: "PART", PartID: 3, Rows: 2, Nodes: []string{"d2"}}
	rec := newReconciler(emptyCounts(), nil)
	rejected := []Rejection{{Item: refused, TargetStage: "hot"}, {Item: refused2, TargetStage: "hot"}}
	seedLanded(rec, state, knownItems(state, entries, rejected), plannedByKey(entries))
	at := func(shard uint32, node string) targetKey {
		return partTargetKey(e.Item.Catalog, e.Item.Group, e.TargetStage, e.Item.Segment, shard, node)
	}
	assert.True(t, rec.isTainted(at(1, "d1")))
	assert.True(t, rec.isTainted(at(2, "d2")))
	assert.False(t, rec.isTainted(at(0, "d1")))
}

func TestSeedLanded_PolicyDecisionsTaintTheirKeys(t *testing.T) {
	art, entries, e := nativeShardEntry(t)
	state := newProgress(art, testBindings())
	state.Done[streamShardKey] = &itemRecord{Parts: map[string]*payloadRecord{
		"n1/1": {Nodes: []string{"d1", "d2"}, Repushed: []string{"d2"}, Rows: 4},
	}}
	// An item refused by the TTL gate this time still counts at its key.
	refused := e.Item
	refused.Shard = 1
	state.Done[refused.Key()] = &itemRecord{Complete: true, Parts: map[string]*payloadRecord{"n1/1": {Nodes: []string{"d1"}, Rows: 5}}}
	rec := newReconciler(emptyCounts(), nil)
	seedLanded(rec, state, knownItems(state, entries, []Rejection{{Item: refused, TargetStage: "hot"}}), plannedByKey(entries))
	at := func(shard uint32, node string) targetKey {
		return partTargetKey(e.Item.Catalog, e.Item.Group, e.TargetStage, e.Item.Segment, shard, node)
	}
	assert.True(t, rec.isTainted(at(0, "d2")))
	assert.False(t, rec.isTainted(at(0, "d1")))
	assert.Equal(t, int64(4), rec.expected(at(0, "d1")))
	assert.Equal(t, int64(5), rec.expected(at(1, "d1")), "the refused item's landed part")
}

func TestAmbiguousLine_NamesTheFallbackOfAReconciledPart(t *testing.T) {
	_, _, e := nativeShardEntry(t)
	a := &ambiguousPayload{entry: e, ref: partRef("n1", 1, 4), nodes: []string{"d2"}, action: actionReconcile, fallback: "block", fallbackSource: "default"}
	assert.Equal(t, "  "+streamShardKey+" part 1 of n1 on d2: reconcile, else block (default)", a.line())
	var log bytes.Buffer
	a.undecided = a.nodes
	err := checkBlocked(&log, []*ambiguousPayload{a})
	require.Error(t, err)
	assert.Contains(t, log.String(), "part 1 of n1 on d2 (default)")
	assert.Contains(t, err.Error(), "--resume-duplication-policy warn|push|skip")
}
