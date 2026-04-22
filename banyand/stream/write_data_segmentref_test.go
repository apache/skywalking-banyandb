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
	require.Equal(t, int32(1), openShardCount.Load()-beforeProbe,
		"REGRESSION: Close did not call segment.DecRef()")
}

// TestSyncChunkCallback_CreatePartHandler_StoresSegment drives the real
// syncCallback.CreatePartHandler through a minimal schemaRepo shim and
// asserts that segment ownership transfers to partCtx and that Close
// actually DecRefs. See trace package for rationale.
func TestSyncChunkCallback_CreatePartHandler_StoresSegment(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	var openShardCount atomic.Int32
	db := openTestTSDBForRefTest(t, tmpPath, &openShardCount)
	defer db.Close()

	segTime := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)

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
	require.Equal(t, int32(1), openShardCount.Load()-beforeProbe,
		"REGRESSION: Close did not call segment.DecRef()")
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
