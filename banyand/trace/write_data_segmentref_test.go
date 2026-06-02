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

package trace

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	metadataschema "github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestSyncReceiver_SegmentRefOwnership is the regression test for the
// cold-tier lifecycle sync crash. It drives the real tsdb / segment
// machinery and the real syncPartContext.Close method, and asserts both the
// refcount-level invariant (no mid-stream re-initialization) and the
// filesystem-level invariant (the in-flight part directory survives while
// another sync session starts).
//
// The bug: syncChunkCallback.CreatePartHandler released its segment reference
// via `defer segment.DecRef()`, which drops refCount to 0 on an idle-closed
// segment. The next CreatePartHandler's incRef hits the `initialize` branch,
// which re-runs loadShards -> initTSTable and deletes any in-progress part
// (no metadata.json yet). Fix: hold the segment in syncPartContext.segment and
// DecRef only in Close / FinishSync, matching write_standalone.go's pattern.
//
// What this test catches (any of these regressions makes it fail):
//  1. syncPartContext struct loses the `segment` field            -> compile error
//  2. CreatePartHandler stops storing segment in partCtx          -> on-disk partA dir gets deleted
//  3. Close stops calling s.segment.DecRef()                       -> final probe assertion fails
//  4. Close stops nil'ing s.segment                                -> require.Nil(partCtxA.segment) fails
//  5. defer segment.DecRef() is reintroduced in CreatePartHandler -> on-disk partA dir gets deleted
func TestSyncReceiver_SegmentRefOwnership(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	// TSTableCreator hook so we can count how many times loadShards opens a
	// shard. Each invocation means segment.refCount was 0 immediately before
	// the triggering CreateSegmentIfNotExist call.
	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, &openShardCount)
	defer db.Close()

	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)

	// Drop the segment to refCount 0. Under the dormant-refcount model it stays
	// open (loaded), so re-acquiring it does NOT reload shards: the open count
	// stays flat across the sibling sync sessions below.
	seg, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	seg.DecRef()
	seg.DecRef() // refCount = 0 (dormant: open, no active reference)
	baselineOpens := openShardCount.Load()

	// ---------- Part A: simulate syncChunkCallback.CreatePartHandler (fix) ----------
	// Sequence matches the production CreatePartHandler under the fix:
	// CreateSegmentIfNotExist -> CreateTSTableIfNotExist -> NewPartType (we
	// stand in for NewPartType by creating the on-disk part directory without
	// writing metadata.json, mirroring mid-stream state) -> store segment in
	// syncPartContext. Do NOT DecRef here.
	segA, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	tsA, err := segA.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	tsA.curPartID++
	partPathA := partPath(tsA.root, tsA.curPartID)
	require.NoError(t, os.MkdirAll(partPathA, storage.DirPerm),
		"simulate NewPartType creating the part directory")
	partCtxA := &syncPartContext{
		tsTable: tsA,
		segment: segA, // fix: store segment for the whole session
		l:       logger.GetLogger("test"),
	}

	opensAfterA := openShardCount.Load()
	require.Equal(t, int32(1), opensAfterA-baselineOpens,
		"Part A creates shard 0 for the first time (the dormant segment is reused "+
			"without a reload, but its shard table is opened on demand)")

	// ---------- Part B: another sync session starts while A is still in flight ----------
	// With the fix, segA keeps refCount>=1, so Part B's CreateSegmentIfNotExist
	// takes the AddInt32 branch -- no initialize, no loadShards, no initTSTable
	// rescan -> partPathA is not touched.
	segB, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	_, err = segB.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	partCtxB := &syncPartContext{
		tsTable: tsA,
		segment: segB,
		l:       logger.GetLogger("test"),
	}

	// Refcount-level invariant: no re-initialization between A and B.
	opensAfterB := openShardCount.Load()
	require.Equal(t, opensAfterA, opensAfterB,
		"REGRESSION: Part B re-opened the shard, meaning segment.refCount dropped to 0 "+
			"between Part A and Part B. Check that syncPartContext stores segment and "+
			"that CreatePartHandler no longer runs `defer segment.DecRef()`.")

	// Filesystem-level invariant: partA's in-flight directory survives.
	// If initTSTable re-ran (it would, under the bug), it would have taken the
	// "cannot validate part metadata. skip and delete it" branch because
	// partPathA has no metadata.json yet.
	_, err = os.Stat(partPathA)
	require.NoError(t, err,
		"REGRESSION: partA's in-flight directory was deleted while another sync "+
			"session started. initTSTable must not re-run mid-stream.")

	// ---------- Exercise syncPartContext.Close (fix path) ----------
	require.NoError(t, partCtxA.Close())
	require.Nil(t, partCtxA.segment,
		"REGRESSION: Close must clear syncPartContext.segment (`s.segment = nil`)")

	require.NoError(t, partCtxB.Close())
	require.Nil(t, partCtxB.segment,
		"REGRESSION: Close must clear syncPartContext.segment (`s.segment = nil`)")

	// ---------- After Close, the dormant segment is reused without reload ----------
	// Both Closes drop refCount back to 0; the segment stays open (dormant), so
	// the next CreateSegmentIfNotExist re-acquires it without reloading shards.
	segC, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	defer segC.DecRef()
	_, err = segC.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	opensAfterC := openShardCount.Load()
	require.Equal(t, int32(0), opensAfterC-opensAfterB,
		"the dormant segment is reused without reload after Close")
}

func openTestTSDBForRefTest(t *testing.T, tmpPath string, openShardCount *atomic.Int32) storage.TSDB[*tsTable, option] {
	t.Helper()
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum: 1,
		Location: filepath.Join(tmpPath, "tab"),
		// Wrap newTSTable so we can count how many times loadShards opens a
		// shard. Each call implies initTSTable ran, which means segment
		// refCount was 0 before and went through the `initialize` branch of
		// incRef. Counting these calls is how we observe whether the fix
		// holds refCount >0 across sibling CreatePartHandler invocations.
		TSTableCreator: func(fileSystem fs.FileSystem, root string, p common.Position,
			l *logger.Logger, tr timestamp.TimeRange, opt option, m any,
		) (*tsTable, error) {
			if openShardCount != nil {
				openShardCount.Add(1)
			}
			return newTSTable(fileSystem, root, p, l, tr, opt, m)
		},
		SegmentInterval: ir,
		TTL:             ir,
		Option:          option{protector: protector.Nop{}, mergePolicy: newDefaultMergePolicyForTesting()},
	}
	require.NoError(t, os.MkdirAll(opts.Location, storage.DirPerm))
	ctx := common.SetPosition(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")),
		func(p common.Position) common.Position {
			p.Database = "test"
			return p
		},
	)
	db, err := storage.OpenTSDB[*tsTable, option](ctx, opts, nil, "test-group")
	require.NoError(t, err)
	return db
}

// TestSyncChunkCallback_CreatePartHandler_StoresSegment drives the real
// syncChunkCallback.CreatePartHandler through a minimal schemaRepo shim and
// asserts that:
//
//  1. The returned PartHandler is a *syncPartContext whose `segment` field is
//     non-nil (i.e. CreatePartHandler transferred segment ownership to the
//     context — catches someone reverting `segment: segment` in the struct
//     literal or re-introducing `defer segment.DecRef()`).
//  2. syncPartContext.Close releases the segment (field cleared AND DecRef
//     actually happened — observable via the TSTableCreator counter).
//
// This complements TestSyncReceiver_SegmentRefOwnership, which builds
// syncPartContext manually. Together they cover every line of the fix.
//
// segTime must use time.Local: the callback path runs time.Unix(0,ns)
// -> Standard -> CreateSegmentIfNotExist in Local tz. If the warmup
// segment were created in UTC, the two calls could represent the same
// instant but format to different segment-suffix strings, causing a
// directory-name mismatch instead of reusing the same segment.
func TestSyncChunkCallback_CreatePartHandler_StoresSegment(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, &openShardCount)
	defer db.Close()

	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.Local)

	// Drop the segment to refCount 0. Under the dormant-refcount model it stays
	// open (loaded), so subsequent CreateSegmentIfNotExist calls re-acquire it
	// without reloading shards.
	warmup, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	warmup.DecRef()
	warmup.DecRef() // refCount = 0 (dormant: open, no active reference)
	baselineOpens := openShardCount.Load()

	const groupName = "test-group"
	callback := &syncChunkCallback{
		l:          logger.GetLogger("test"),
		schemaRepo: newTestSchemaRepo(db, groupName),
	}

	ctx := &queue.ChunkedSyncPartContext{
		ID:           1,
		Group:        groupName,
		ShardID:      0,
		MinTimestamp: segTime.UnixNano(),
		MaxTimestamp: segTime.UnixNano(),
		PartType:     PartTypeCore,
	}

	handler, err := callback.CreatePartHandler(ctx)
	require.NoError(t, err)
	require.NotNil(t, handler)

	partCtx, ok := handler.(*syncPartContext)
	require.True(t, ok, "CreatePartHandler must return *syncPartContext")
	require.NotNil(t, partCtx.segment,
		"REGRESSION: CreatePartHandler did not store segment in syncPartContext. "+
			"Check the `segment: segment` field assignment is still present in the "+
			"partCtx struct literal and that `defer segment.DecRef()` has not been "+
			"re-introduced at the top of CreatePartHandler.")

	// CreatePartHandler acquired the dormant segment (refCount 0 -> 1) and
	// opened shard 0 on demand for the first time.
	afterCreate := openShardCount.Load()
	require.Equal(t, int32(1), afterCreate-baselineOpens,
		"CreatePartHandler opens shard 0 for the first time on the dormant segment")

	// CRITICAL mid-stream probe (between CreatePartHandler and Close).
	// Under the fix, partCtx holds segment.refCount >= 1, so a concurrent
	// CreateSegmentIfNotExist takes the AddInt32 branch and does NOT re-init.
	// Under the bug (defer segment.DecRef() in CreatePartHandler), refCount
	// already dropped to 0 at CreatePartHandler return, performCleanup ran,
	// and this concurrent call re-initializes -> TSTableCreator fires again.
	// This is the assertion that catches someone re-introducing the defer.
	segMid, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	require.Equal(t, afterCreate, openShardCount.Load(),
		"REGRESSION: a second CreateSegmentIfNotExist between CreatePartHandler and "+
			"Close re-initialized the segment. That means segment.refCount dropped to 0 "+
			"when CreatePartHandler returned -- i.e. `defer segment.DecRef()` was "+
			"re-introduced or `segment: segment` was removed from the partCtx struct "+
			"literal.")
	segMid.DecRef() // release the mid-probe's own incRef

	// Close. This should:
	//   - call s.segment.DecRef()                   -> refCount 1 -> 0
	//   - set s.segment = nil
	// Neither is observable directly from outside the package, so we probe
	// with a follow-up CreateSegmentIfNotExist. If refCount is back to 0 as
	// intended, that probe will re-initialize and increment openShardCount.
	// If Close forgot to DecRef, refCount stays at 1 and the probe takes the
	// cheap AddInt32 path with no TSTableCreator invocation.
	require.NoError(t, partCtx.Close())
	require.Nil(t, partCtx.segment,
		"REGRESSION: Close must clear syncPartContext.segment (`s.segment = nil`)")

	beforeProbe := openShardCount.Load()
	segProbe, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	defer segProbe.DecRef()
	_, err = segProbe.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	require.Equal(t, int32(0), openShardCount.Load()-beforeProbe,
		"after Close the dormant segment is reused without reload")
}

// newTestSchemaRepo builds a minimal *schemaRepo whose loadTSDB returns the
// given db for the given group. All other schemaRepo methods that
// syncChunkCallback doesn't exercise are left at their zero values.
func newTestSchemaRepo(db storage.TSDB[*tsTable, option], groupName string) *schemaRepo {
	return &schemaRepo{
		Repository: &fakeRepository{
			groups: map[string]resourceSchema.Group{
				groupName: &fakeGroup{tsdb: db},
			},
		},
		l:    logger.GetLogger("test"),
		path: "",
	}
}

// fakeRepository is a minimal implementation of resourceSchema.Repository that
// only supports LoadGroup. Other methods are no-ops / zero-value returns so
// the type satisfies the interface.
type fakeRepository struct {
	groups map[string]resourceSchema.Group
}

func (f *fakeRepository) Watcher()                                         {}
func (f *fakeRepository) Init(_ metadataschema.Kind) ([]string, []int64)   { return nil, nil }
func (f *fakeRepository) SendMetadataEvent(_ resourceSchema.MetadataEvent) {}
func (f *fakeRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	g, ok := f.groups[name]
	return g, ok
}
func (f *fakeRepository) LoadAllGroups() []resourceSchema.Group { return nil }
func (f *fakeRepository) LoadResource(_ *commonv1.Metadata) (resourceSchema.Resource, bool) {
	return nil, false
}
func (f *fakeRepository) Close()                   {}
func (f *fakeRepository) StopCh() <-chan struct{}  { return nil }
func (f *fakeRepository) DropGroup(_ string) error { return nil }

// fakeGroup is a minimal implementation of resourceSchema.Group that returns
// a pre-built tsdb from SupplyTSDB.
type fakeGroup struct {
	tsdb storage.TSDB[*tsTable, option]
}

func (f *fakeGroup) GetSchema() *commonv1.Group { return nil }
func (f *fakeGroup) SupplyTSDB() io.Closer      { return f.tsdb }

// TestSyncChunkCallback_CreatePartHandler_AlignsOffGridMinTimestamp covers
// the regression where the trace part chunk-sync receiver passed the raw
// MinTimestamp straight into CreateSegmentIfNotExist. See the analogous
// measure test for full rationale.
func TestSyncChunkCallback_CreatePartHandler_AlignsOffGridMinTimestamp(t *testing.T) {
	t.Run("DAY(1)", func(t *testing.T) {
		runTraceOffGridCase(t, storage.IntervalRule{Unit: storage.DAY, Num: 1})
	})
	t.Run("DAY(5)", func(t *testing.T) {
		runTraceOffGridCase(t, storage.IntervalRule{Unit: storage.DAY, Num: 5})
	})
}

func runTraceOffGridCase(t *testing.T, ir storage.IntervalRule) {
	t.Helper()
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	const groupName = "off-grid-trace"
	db := openTestTSDBWithInterval(t, tmpPath, groupName, ir)
	defer db.Close()

	rawTS := time.Date(2026, 5, 15, 6, 0, 0, 0, time.Local)
	wantStart := ir.Standard(rawTS)

	callback := &syncChunkCallback{
		l:          logger.GetLogger("test-offgrid-trace"),
		schemaRepo: newTestSchemaRepo(db, groupName),
	}
	handler, err := callback.CreatePartHandler(&queue.ChunkedSyncPartContext{
		ID:           1,
		Group:        groupName,
		ShardID:      0,
		MinTimestamp: rawTS.UnixNano(),
		MaxTimestamp: rawTS.UnixNano(),
	})
	require.NoError(t, err)
	defer func() { _ = handler.(*syncPartContext).Close() }()
	gotStart := handler.(*syncPartContext).segment.GetTimeRange().Start
	require.True(t, gotStart.Equal(wantStart),
		"REGRESSION: raw MinTimestamp=%s landed in segment start=%s; expected aligned %s (ir=%+v)",
		rawTS, gotStart, wantStart, ir)
	require.False(t, rawTS.Equal(wantStart),
		"test setup degenerate: rawTS already aligned")
}

// TestSegmentCreateTS_ConsistencyAcrossPaths is the trace variant of the
// Path-A vs Path-B convergence demo. A pre-existing aligned historical
// segment must NOT be touched; both new writes must land in the same
// new bucket.
func TestSegmentCreateTS_ConsistencyAcrossPaths(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	const groupName = "consistency-trace"
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 5}
	db := openTestTSDBWithInterval(t, tmpPath, groupName, ir)
	defer db.Close()

	historicalStart := ir.Standard(time.Date(2026, 4, 10, 0, 0, 0, 0, time.Local))
	histSeg, err := db.CreateSegmentIfNotExist(historicalStart)
	require.NoError(t, err)
	histSeg.DecRef()
	histSeg.DecRef()

	rawTS := time.Date(2026, 5, 15, 6, 0, 0, 0, time.Local)
	wantNewStart := ir.Standard(rawTS)
	require.False(t, wantNewStart.Equal(historicalStart))
	require.False(t, wantNewStart.Equal(rawTS))

	repo := newTestSchemaRepo(db, groupName)

	chunkCallback := &syncChunkCallback{l: logger.GetLogger("test-trace-pathA"), schemaRepo: repo}
	handlerA, err := chunkCallback.CreatePartHandler(&queue.ChunkedSyncPartContext{
		ID:           1,
		Group:        groupName,
		ShardID:      0,
		MinTimestamp: rawTS.UnixNano(),
		MaxTimestamp: rawTS.UnixNano(),
	})
	require.NoError(t, err)
	defer func() { _ = handlerA.(*syncPartContext).Close() }()
	gotStartA := handlerA.(*syncPartContext).segment.GetTimeRange().Start

	seriesCallback := &syncSeriesCallback{l: logger.GetLogger("test-trace-pathB"), schemaRepo: repo}
	handlerB, err := seriesCallback.CreatePartHandler(&queue.ChunkedSyncPartContext{
		ID:           2,
		Group:        groupName,
		ShardID:      0,
		MinTimestamp: wantNewStart.UnixNano(),
		MaxTimestamp: wantNewStart.UnixNano(),
	})
	require.NoError(t, err)
	defer func() { _ = handlerB.(*syncSeriesContext).Close() }()
	gotStartB := handlerB.(*syncSeriesContext).segment.GetTimeRange().Start

	require.True(t, gotStartA.Equal(gotStartB),
		"REGRESSION: trace Path A (raw %s -> %s) and Path B (aligned %s -> %s) diverged",
		rawTS, gotStartA, wantNewStart, gotStartB)
	require.True(t, gotStartA.Equal(wantNewStart),
		"path A start %s != expected aligned %s", gotStartA, wantNewStart)
	require.False(t, gotStartA.Equal(historicalStart),
		"new write must NOT land in historical segment %s", historicalStart)
}

func openTestTSDBWithInterval(t *testing.T, tmpPath, groupName string, ir storage.IntervalRule) storage.TSDB[*tsTable, option] {
	t.Helper()
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:        1,
		Location:        filepath.Join(tmpPath, "tab"),
		TSTableCreator:  newTSTable,
		SegmentInterval: ir,
		TTL:             storage.IntervalRule{Unit: ir.Unit, Num: 60},
		Option:          option{protector: protector.Nop{}, mergePolicy: newDefaultMergePolicyForTesting()},
	}
	require.NoError(t, os.MkdirAll(opts.Location, storage.DirPerm))
	ctx := common.SetPosition(
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test-offgrid-trace")),
		func(p common.Position) common.Position {
			p.Database = "test-offgrid-trace"
			return p
		},
	)
	db, err := storage.OpenTSDB[*tsTable, option](ctx, opts, nil, groupName)
	require.NoError(t, err)
	return db
}
