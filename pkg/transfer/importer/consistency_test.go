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
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// A progress file with a baseline or pushed rows but no plan side file was not written by a run,
// which writes the side file before the file that names it: a resume that took it would count the
// baseline again after the push.
func TestReadPlan_ProgressWithoutAPlanSideFileIsRefused(t *testing.T) {
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		edit    func(*progressState)
		name    string
		refused bool
	}{
		{name: "nothing recorded", edit: func(*progressState) {}},
		{name: "a baseline", edit: func(s *progressState) { s.BaselineAt = &at }, refused: true},
		{name: "pushed rows", refused: true, edit: func(s *progressState) {
			s.Pushed = []rowCount{{Catalog: "stream", Group: "g", Stage: "hot", Kind: ShardItem.String(), Rows: 2}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "p.json")
			state := newProgress(csvArtifact(), testBindings())
			tc.edit(state)
			err := readPlan(path, state)
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, exitcode.ExitResume, exitcode.For(err))
			assert.ErrorContains(t, err, "records progress but names no plan side file")
			assert.ErrorContains(t, err, "--restart")
		})
	}
}

// A baseline recorded without the nodes it answered keeps them unknown when a node is added: a list
// of only the new nodes would leave every other node uncounted.
func TestMergeNodes_KeepsUnknownAnsweredNodesUnknown(t *testing.T) {
	pre := emptyCounts().set(stream, "g", "hot", "a", 0, "s", 5)
	extra := emptyCounts().set(stream, "g", "hot", "b", 0, "s", 7)
	merged := mergeNodes(pre, extra, []string{"b"})
	assert.Nil(t, merged.Answered)
	assert.True(t, merged.counted("a"), "the node the baseline counted still is")
	assert.True(t, merged.counted("b"))

	pre.Answered = []string{"a"}
	merged = mergeNodes(pre, extra, []string{"b"})
	assert.Equal(t, []string{"a", "b"}, merged.Answered)
	assert.Equal(t, []string{"a"}, pre.Answered, "the recorded baseline is not changed")
}

// The rows left of a CSV item are the ones its cursor does not acknowledge yet.
func TestSummarize_LeftRowsAreThoseNoCursorAcknowledges(t *testing.T) {
	entry := func(group string, rows uint64) Entry {
		return Entry{Item: WorkItem{
			Catalog: commonv1.Catalog_CATALOG_STREAM, Group: group, Stage: "hot", Segment: "s", Kind: ShardItem,
			Sources: []WorkSource{{Node: "n1", Rows: rows}},
		}, TargetStage: "hot"}
	}
	fresh, started, deduplicated := entry("a", 10), entry("b", 10), entry("c", 10)
	state := newProgress(csvArtifact(), testBindings())
	state.Done[started.Item.Key()] = &itemRecord{Cursor: &csvCursor{File: "f", Record: 4, Rows: 4}}
	// A cursor past the rows of the item (the winners of a deduplicated item are fewer) leaves none.
	state.Done[deduplicated.Item.Key()] = &itemRecord{Cursor: &csvCursor{File: "f", Record: 12, Rows: 12}}
	sum := summarize("p.json", state, []Entry{fresh, started, deduplicated}, nil)
	assert.Equal(t, uint64(10+6+0), sum.LeftRows)
}

// A failure to count the target before the first push says to run the command again, unless it was
// an interrupt, which the progress file's resume hint follows.
func TestNoDataSent_InterruptSaysNoMore(t *testing.T) {
	err := errors.New("count the target: unavailable")
	got := noDataSent(context.Background(), err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(got))
	assert.ErrorContains(t, got, "no data was sent: run the same command again once the target answers")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got = noDataSent(ctx, err)
	assert.Equal(t, exitcode.ExitRuntime, exitcode.For(got))
	assert.ErrorContains(t, got, "count the target: unavailable; no data was sent")
	assert.NotContains(t, got.Error(), "run the same command again")
}

// The time a message shows is in the local time zone, whatever offset the progress file recorded:
// run under a zone other than UTC (TZ=Asia/Tokyo), the times below are not written with a Z.
func TestMessages_ShowTimesInTheLocalZone(t *testing.T) {
	at := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	want := at.Local().Format(time.RFC3339)
	var buf bytes.Buffer
	alreadyImported(&buf, at, "liaison-0")
	finishedWithAcceptedItems(&buf, at, 2)
	assert.Contains(t, buf.String(), "already imported into this cluster at "+want)
	assert.Contains(t, buf.String(), "the import was finished at "+want)
	if _, offset := at.Local().Zone(); offset != 0 {
		assert.NotContains(t, buf.String(), "Z")
	}

	// The baseline a plan side file lacks is named by its time too.
	path := filepath.Join(t.TempDir(), "p.json")
	state := newProgress(csvArtifact(), testBindings())
	var err error
	state.PlanDigest, err = writePlan(path, nil, nil)
	require.NoError(t, err)
	state.BaselineAt = &at
	err = readPlan(path, state)
	require.Error(t, err)
	assert.ErrorContains(t, err, "records a baseline taken at "+want)
}

// A duplication policy set by the plan file or the variable says so in the messages.
func TestClassify_NamesWhereThePolicyWasSet(t *testing.T) {
	e := Entry{Item: WorkItem{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Rows: 10}},
	}, TargetStage: "hot"}
	state := newProgress(csvArtifact(), testBindings())
	state.Done[e.Item.Key()] = &itemRecord{Cursor: &csvCursor{File: "f", Record: 2, Rows: 2, Sent: &csvMark{File: "f", Record: 5, Rows: 5}}}
	opts := config.Import{DuplicationPolicy: config.DuplicationWarn}
	for source, want := range map[string]string{
		"": "--resume-duplication-policy",
		"BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY": "BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY",
		"plan.yaml import.resumeDuplicationPolicy": "plan.yaml import.resumeDuplicationPolicy",
	} {
		amb := classify(state, []Entry{e}, &opts, nil, source)
		require.Len(t, amb, 1)
		assert.Equal(t, want, amb[0].source)
	}
}

// sendRefusing is an import stream whose Send fails as gRPC does for a batch it never writes.
type sendRefusing struct {
	transferv1.ImportService_ImportRowsClient
	err error
}

func (s sendRefusing) Send(*transferv1.ImportRowsRequest) error { return s.err }

// A batch gRPC refuses to send (over the client's limit, say) never reached the wire: its rows
// leave the window a resume treats as ambiguous, as for a stream the liaison ended.
func TestFlush_ALocalSendFailureLeavesTheRowsOutOfTheWindow(t *testing.T) {
	e := &Entry{Item: WorkItem{
		Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "g", Stage: "hot", Segment: "s", Kind: ShardItem,
		Sources: []WorkSource{{Node: "n1", Rows: 1}},
	}, TargetStage: "hot"}
	ctx := context.Background()
	state := newProgress(csvArtifact(), testBindings())
	track, err := startTracker(ctx, filepath.Join(t.TempDir(), "p.json"), state, 64, nil)
	require.NoError(t, err)
	defer func() { _ = track.close() }()
	w := &worker{
		p:     &pusher{opts: PushOptions{Log: &bytes.Buffer{}, track: track}, rows: map[PushKey]uint64{}, done: map[string]bool{}},
		items: map[string]*itemProgress{}, window: 2,
	}
	k := entryStreamKey(e)
	st := &importStream{
		key: k, cancel: func() {}, pending: []row{{msg: &transferv1.ImportRowsRequest_StreamRow{}, file: "f", record: 1}},
		client: sendRefusing{err: status.Error(codes.ResourceExhausted, "grpc: trying to send message larger than max")},
	}
	err = w.flush(ctx, st, e)
	require.Error(t, err)
	assert.ErrorContains(t, err, "trying to send message larger than max")
	assert.Empty(t, st.inflight, "the batch is not in flight")
	assert.Zero(t, w.progress(e).sentRows)
	track.mu.Lock()
	cursor := track.cursors[e.Item.Key()]
	track.mu.Unlock()
	require.NotNil(t, cursor)
	assert.Nil(t, cursor.Sent, "nothing of the item was sent")
}
