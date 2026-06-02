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

package stream

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
// cold-tier lifecycle sync crash (stream equivalent of the trace test of the
// same name). See the trace package for the full design rationale. Summary:
// syncCallback.CreatePartHandler must hold the segment reference for the
// entire sync session so that initTSTable does not re-run mid-stream and
// delete the partial part directory.
func TestSyncReceiver_SegmentRefOwnership(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, &openShardCount)
	defer db.Close()

	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)

	seg, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	seg.DecRef()
	seg.DecRef() // refCount -> 0 (idle-closed)
	baselineOpens := openShardCount.Load()

	segA, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	tsA, err := segA.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	tsA.curPartID++
	partPathA := partPath(tsA.root, tsA.curPartID)
	require.NoError(t, os.MkdirAll(partPathA, storage.DirPerm))
	partCtxA := &syncPartContext{
		tsTable: tsA,
		segment: segA,
		partID:  tsA.curPartID,
	}

	opensAfterA := openShardCount.Load()
	require.Equal(t, int32(1), opensAfterA-baselineOpens,
		"Part A should re-initialize the idle-closed segment once")

	segB, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	_, err = segB.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	partCtxB := &syncPartContext{
		tsTable: tsA,
		segment: segB,
	}

	require.Equal(t, opensAfterA, openShardCount.Load(),
		"REGRESSION: Part B re-opened the shard -- refCount dropped to 0 between A and B")
	_, err = os.Stat(partPathA)
	require.NoError(t, err,
		"REGRESSION: partA's in-flight directory was deleted while another sync session started")

	require.NoError(t, partCtxA.Close())
	require.Nil(t, partCtxA.segment, "REGRESSION: Close must clear syncPartContext.segment")
	require.NoError(t, partCtxB.Close())
	require.Nil(t, partCtxB.segment, "REGRESSION: Close must clear syncPartContext.segment")

	beforeProbe := openShardCount.Load()
	segC, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	defer segC.DecRef()
	_, err = segC.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	require.Equal(t, int32(0), openShardCount.Load()-beforeProbe,
		"after Close the dormant segment and its already-open shard are reused, no new shard")
}

// TestSyncChunkCallback_CreatePartHandler_StoresSegment drives the real
// syncCallback.CreatePartHandler through a minimal schemaRepo shim and
// asserts that segment ownership transfers to partCtx and that Close
// actually DecRefs. See trace package for rationale.
// In each StoresSegment test, segTime must use time.Local because the
// callback path runs time.Unix(0,ns) -> Standard -> CreateSegmentIfNotExist
// in Local tz. If the warmup segment were created in UTC, the two calls
// could represent the same instant but format to different segment-suffix
// strings, causing a directory-name mismatch instead of reusing the same
// segment.
func TestSyncChunkCallback_CreatePartHandler_StoresSegment(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, &openShardCount)
	defer db.Close()

	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.Local)

	warmup, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	warmup.DecRef()
	warmup.DecRef()
	baselineOpens := openShardCount.Load()

	const groupName = "test-group"
	callback := &syncCallback{
		l:          logger.GetLogger("test"),
		schemaRepo: newTestSchemaRepo(db, groupName),
	}

	ctx := &queue.ChunkedSyncPartContext{
		ID:           1,
		Group:        groupName,
		ShardID:      0,
		MinTimestamp: segTime.UnixNano(),
		MaxTimestamp: segTime.UnixNano(),
	}

	handler, err := callback.CreatePartHandler(ctx)
	require.NoError(t, err)
	require.NotNil(t, handler)

	partCtx, ok := handler.(*syncPartContext)
	require.True(t, ok, "CreatePartHandler must return *syncPartContext")
	require.NotNil(t, partCtx.segment,
		"REGRESSION: CreatePartHandler did not store segment in syncPartContext")

	afterCreate := openShardCount.Load()
	require.Equal(t, int32(1), afterCreate-baselineOpens,
		"CreatePartHandler should have re-initialized the idle-closed segment once")

	segMid, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	require.Equal(t, afterCreate, openShardCount.Load(),
		"REGRESSION: a second CreateSegmentIfNotExist between CreatePartHandler and "+
			"Close re-initialized -- `defer segment.DecRef()` has been re-introduced.")
	segMid.DecRef()

	require.NoError(t, partCtx.Close())
	require.Nil(t, partCtx.segment,
		"REGRESSION: Close must clear syncPartContext.segment")

	beforeProbe := openShardCount.Load()
	segProbe, err := db.CreateSegmentIfNotExist(segTime)
	require.NoError(t, err)
	defer segProbe.DecRef()
	_, err = segProbe.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	require.Equal(t, int32(0), openShardCount.Load()-beforeProbe,
		"after Close the dormant segment and its already-open shard are reused, no new shard")
}

func openTestTSDBForRefTest(t *testing.T, tmpPath string, openShardCount *atomic.Int32) storage.TSDB[*tsTable, option] {
	t.Helper()
	ir := storage.IntervalRule{Unit: storage.DAY, Num: 1}
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum: 1,
		Location: filepath.Join(tmpPath, "tab"),
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

type fakeGroup struct {
	tsdb storage.TSDB[*tsTable, option]
}

func (f *fakeGroup) GetSchema() *commonv1.Group { return nil }
func (f *fakeGroup) SupplyTSDB() io.Closer      { return f.tsdb }

// TestSyncChunkCallback_CreatePartHandler_AlignsOffGridMinTimestamp covers
// the regression where the stream part chunk-sync receiver passed the raw
// MinTimestamp straight into CreateSegmentIfNotExist. See the analogous
// measure test for full rationale.
func TestSyncChunkCallback_CreatePartHandler_AlignsOffGridMinTimestamp(t *testing.T) {
	t.Run("DAY(1)", func(t *testing.T) {
		runStreamOffGridCase(t, storage.IntervalRule{Unit: storage.DAY, Num: 1})
	})
	t.Run("DAY(5)", func(t *testing.T) {
		runStreamOffGridCase(t, storage.IntervalRule{Unit: storage.DAY, Num: 5})
	})
}

func runStreamOffGridCase(t *testing.T, ir storage.IntervalRule) {
	t.Helper()
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	const groupName = "off-grid-stream"
	db := openTestTSDBWithInterval(t, tmpPath, groupName, ir)
	defer db.Close()

	rawTS := time.Date(2026, 5, 15, 6, 0, 0, 0, time.Local)
	wantStart := ir.Standard(rawTS)

	callback := &syncCallback{
		l:          logger.GetLogger("test-offgrid-stream"),
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

// TestSegmentCreateTS_ConsistencyAcrossPaths is the stream variant of the
// Path-A vs Path-B convergence demo. A pre-existing aligned historical
// segment must NOT be touched; both new writes must land in the same
// new bucket.
func TestSegmentCreateTS_ConsistencyAcrossPaths(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	const groupName = "consistency-stream"
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

	chunkCallback := &syncCallback{l: logger.GetLogger("test-stream-pathA"), schemaRepo: repo}
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

	seriesCallback := &syncSeriesCallback{l: logger.GetLogger("test-stream-pathB"), schemaRepo: repo}
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
		"REGRESSION: stream Path A (raw %s -> %s) and Path B (aligned %s -> %s) diverged",
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
		context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test-offgrid-stream")),
		func(p common.Position) common.Position {
			p.Database = "test-offgrid-stream"
			return p
		},
	)
	db, err := storage.OpenTSDB[*tsTable, option](ctx, opts, nil, groupName)
	require.NoError(t, err)
	return db
}
