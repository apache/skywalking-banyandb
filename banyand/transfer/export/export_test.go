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

package export

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	testPropGroup = "sw_prop"
	testPropName  = "ui_template"
)

// newTestService wires a Service over backends without the run lifecycle.
func newTestService(repo metadata.Repo, backends Backends, clock *fakeClock) *Service {
	s := NewService(repo, nil, backends)
	s.l = logger.GetLogger("export-test")
	s.planner = NewPlanner(repo, backends, nil, s.l)
	if clock != nil {
		s.planner.sessions.now = clock.now
	}
	return s
}

func propertySchema() *databasev1.Property {
	return &databasev1.Property{
		Metadata: &commonv1.Metadata{Name: testPropName, Group: testPropGroup, ModRevision: 42},
		Tags: []*databasev1.TagSpec{
			{Name: "name", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "value", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}
}

// propertyRepo mocks a registry that knows the property schemas of testPropGroup.
func propertyRepo(t *testing.T, props ...*databasev1.Property) metadata.Repo {
	ctrl := gomock.NewController(t)
	repo := metadata.NewMockRepo(ctrl)
	propReg := schema.NewMockProperty(ctrl)
	repo.EXPECT().PropertyRegistry().Return(propReg).AnyTimes()
	propReg.EXPECT().ListProperty(gomock.Any(), gomock.Any()).Return(props, nil).AnyTimes()
	return repo
}

// writeProperties writes count live properties with valueSize-byte values, plus one
// tombstone, into the property data directory with the real property store.
func writeProperties(t *testing.T, dataDir string, count, valueSize int) {
	t.Helper()
	database, err := db.OpenDB(context.Background(), db.Config{
		Location:         dataDir,
		MetricsScopeName: "export_property_test",
		FlushInterval:    time.Second,
		Index:            db.IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, pkgfs.NewLocalFileSystem())
	require.NoError(t, err)
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// A deterministic xorshift stream: values that gzip cannot shrink much.
	state := uint64(88172645463325252)
	value := func() string {
		b := make([]byte, valueSize)
		for i := range b {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			b[i] = letters[state%uint64(len(letters))]
		}
		return string(b)
	}
	newProp := func(id string, rev int64, v string) *propertyv1.Property {
		return &propertyv1.Property{
			Metadata:  &commonv1.Metadata{Group: testPropGroup, Name: testPropName, ModRevision: rev, CreateRevision: rev - 1},
			Id:        id,
			UpdatedAt: timestamppb.New(time.Unix(0, rev)),
			Tags: []*modelv1.Tag{
				{Key: "name", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: id}}}},
				{Key: "value", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}},
			},
		}
	}
	for i := 0; i < count; i++ {
		p := newProp(fmt.Sprintf("p-%03d", i), int64(1000+i), value())
		require.NoError(t, database.Update(context.Background(), 0, db.GetPropertyID(p), p))
	}
	gone := newProp("gone", 5000, "bye")
	require.NoError(t, database.Update(context.Background(), 0, db.GetPropertyID(gone), gone))
	require.NoError(t, database.Delete(context.Background(), [][]byte{db.GetPropertyID(gone)}, time.Now()))
	require.NoError(t, database.Close())
}

// propertyFixture builds a service over fake backends whose property catalog holds real
// data, and a session snapshotting it.
func propertyFixture(t *testing.T, count, valueSize int) (*Service, string, *fakeClock) {
	t.Helper()
	backends, clock := newTestBackends(t)
	writeProperties(t, backends[commonv1.Catalog_CATALOG_PROPERTY].GetDataPath(), count, valueSize)
	s := newTestService(propertyRepo(t, propertySchema()), backends, clock)
	created, err := s.planner.sessions.create(context.Background(), testSessionID,
		catalogSet(commonv1.Catalog_CATALOG_PROPERTY), false)
	require.NoError(t, err)
	return s, created.roots[commonv1.Catalog_CATALOG_PROPERTY], clock
}

func TestExport_RequiresInitFirst(t *testing.T) {
	backends, clock := newTestBackends(t)
	s := newTestService(propertyRepo(t), backends, clock)

	err := s.Export(newFakeExportStream())
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)

	err = s.Export(newFakeExportStream(propertyFrame(testPropGroup, 0)))
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)

	// An init frame that fails validation is InvalidArgument.
	err = s.Export(newFakeExportStream(initFrame("not-hex", transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)))
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)
}

func TestExport_UnspecifiedCompressIsInvalid(t *testing.T) {
	s, _, _ := propertyFixture(t, 1, 8)
	stream := newFakeExportStream(initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_UNSPECIFIED))
	err := s.Export(stream)
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)
	require.Contains(t, err.Error(), "Compress")
	require.Empty(t, stream.sent)
}

func TestExport_NativeIsUnimplemented(t *testing.T) {
	s, _, _ := propertyFixture(t, 1, 8)
	stream := newFakeExportStream(initFrame(testSessionID, transferv1.Format_FORMAT_NATIVE, transferv1.Compress_COMPRESS_NONE))
	err := s.Export(stream)
	require.Equal(t, codes.Unimplemented, status.Code(err), err)
	require.Contains(t, err.Error(), "native export is not implemented yet")
	require.Empty(t, stream.sent)
}

func TestExport_UnknownAndExpiredSession(t *testing.T) {
	s, _, clock := propertyFixture(t, 1, 8)
	err := s.Export(newFakeExportStream(initFrame(strings.Repeat("b", 32), transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	require.Contains(t, err.Error(), "not held on this node")

	clock.advance(defaultLease + time.Minute)
	stream := newFakeExportStream(initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE))
	err = s.Export(stream)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	require.Empty(t, stream.sent)
}

func TestExport_MissingUnitStatusCodes(t *testing.T) {
	s, _, _ := propertyFixture(t, 1, 8)
	for name, c := range map[string]struct {
		unit *transferv1.ExportRequest
		want codes.Code
	}{
		"missing group": {propertyFrame("no_such_group", 0), codes.Internal},
		"missing shard": {propertyFrame(testPropGroup, 0, 7), codes.Internal},
		"catalog not in the session": {segmentFrame(&transferv1.SegmentUnit{
			Catalog: commonv1.Catalog_CATALOG_STREAM, Group: "sw_record", SegmentSuffix: "20990101", ShardIds: []uint32{0},
		}), codes.InvalidArgument},
		"group outside the snapshot": {propertyFrame("..", 0), codes.InvalidArgument},
		"group with a separator":     {propertyFrame("a/b", 0), codes.InvalidArgument},
		"duplicate shard":            {propertyFrame(testPropGroup, 0, 0), codes.InvalidArgument},
		"second init":                {initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE), codes.InvalidArgument},
	} {
		stream := newFakeExportStream(initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE), c.unit)
		err := s.Export(stream)
		require.Equal(t, c.want, status.Code(err), "%s: %v", name, err)
		require.Len(t, stream.sent, 1, "%s: only ready precedes the failure", name)
	}
}

// A release while a unit is being read fails the unit instead of reporting it complete: the
// readers can finish from the files they already opened, or take vanished files for absent
// ones, so the session is checked again before unit_finished.
func TestExport_ReleaseWhileReadingFailsTheUnit(t *testing.T) {
	s, _, _ := propertyFixture(t, 3, 8)
	init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)
	init.GetInit().RateLimit = &transferv1.RateLimit{RowsPerSecond: 2}
	stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0))
	time.AfterFunc(300*time.Millisecond, func() {
		_, _ = s.planner.sessions.release(testSessionID)
	})
	err := s.Export(stream)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	for _, f := range stream.sent {
		require.Nil(t, f.GetUnitFinished(), "a unit read across a release must not finish")
	}
}

func TestExport_LocateAfterConcurrentReleaseIsFailedPrecondition(t *testing.T) {
	s, _, _ := propertyFixture(t, 1, 8)
	roots, err := s.planner.sessions.locateAndHeartbeat(testSessionID)
	require.NoError(t, err)

	// While the session lives, a group the snapshot lacks is a node-side fault.
	_, err = s.locateInSession(testSessionID, roots, commonv1.Catalog_CATALOG_PROPERTY, "no_such_group", "", nil)
	require.Equal(t, codes.Internal, status.Code(err), err)

	// Released between the per-unit renewal and the lookup: the session is gone.
	_, err = s.planner.sessions.release(testSessionID)
	require.NoError(t, err)
	_, err = s.locateInSession(testSessionID, roots, commonv1.Catalog_CATALOG_PROPERTY, testPropGroup, "", []uint32{0})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	require.Contains(t, err.Error(), "export session "+testSessionID+" is gone (released or preempted)")
}

// The rates of Init reach the producer: at one row per second, a unit of several rows does
// not finish before a short deadline.
func TestExport_InitRatesThrottle(t *testing.T) {
	for name, limit := range map[string]*transferv1.RateLimit{
		"rows":  {RowsPerSecond: 1},
		"bytes": {BytesPerSecond: 1},
	} {
		t.Run(name, func(t *testing.T) {
			// 40 rows of 64 KiB outgrow the byte bucket's 1 MiB burst.
			s, _, _ := propertyFixture(t, 40, 64<<10)
			init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)
			init.GetInit().RateLimit = limit
			stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0))
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			stream.ctx = ctx
			start := time.Now()
			err := s.Export(stream)
			require.Equal(t, codes.DeadlineExceeded, status.Code(err), err)
			require.Less(t, time.Since(start), 5*time.Second, "the wait ends with the stream")
		})
	}
}

// A stream waiting for its row bucket sends the rows it holds once they are a second old, so
// a slow row rate still shows progress and keeps the stream alive.
func TestExport_SlowRowRateSendsTheRowsWritten(t *testing.T) {
	s, _, _ := propertyFixture(t, 3, 8)
	init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_GZIP)
	init.GetInit().RateLimit = &transferv1.RateLimit{RowsPerSecond: 1}
	stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0))
	require.NoError(t, s.Export(stream))

	units := decodeFrames(t, stream.sent)
	require.Len(t, units, 1)
	require.Len(t, units[0].files, 1)
	f := units[0].files[0]
	// Every wait at 1 row/s is about a second, so each row is sent before the next wait: the
	// chunks count 1, 2 and 3 rows, and the end of the file carries the last one.
	require.Len(t, csvRows(t, f), 4, "the early chunks keep the file one gzip stream")
	var counted []uint64
	for _, r := range f.chunkRows {
		if len(counted) == 0 || r != counted[len(counted)-1] {
			counted = append(counted, r)
		}
	}
	require.Equal(t, []uint64{1, 2, 3}, counted[:3], "chunk rows %v", f.chunkRows)
}

// A byte rate below 1 MiB/s sends chunks of one second's worth, so its first chunk is not a
// burst of 1 MiB and the next one comes a second later, not after 1 MiB at that rate.
func TestExport_LowByteRateSendsSmallChunks(t *testing.T) {
	const rate = 64 << 10
	s, _, _ := propertyFixture(t, 3, 64<<10)
	init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)
	init.GetInit().RateLimit = &transferv1.RateLimit{BytesPerSecond: rate}
	stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0))
	start := time.Now()
	require.NoError(t, s.Export(stream))
	elapsed := time.Since(start)

	units := decodeFrames(t, stream.sent)
	f := units[0].files[0]
	var largest int
	for _, frame := range stream.sent {
		largest = max(largest, len(frame.GetChunk().GetContent()))
	}
	require.LessOrEqual(t, largest, rate)
	// About 256 KiB at 64 KiB/s after a burst of one second's worth.
	require.Greater(t, elapsed, time.Duration(float64(len(f.content)-rate)/rate*float64(time.Second))*9/10)
}

func TestExport_PropertyUnitFramesRowsAndRates(t *testing.T) {
	const count, valueSize = 40, 64 << 10
	s, root, _ := propertyFixture(t, count, valueSize)
	before := hashTree(t, root)

	init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)
	init.GetInit().RateLimit = &transferv1.RateLimit{BytesPerSecond: 16 << 20, RowsPerSecond: 10000}
	stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0))
	require.NoError(t, s.Export(stream))

	units := decodeFrames(t, stream.sent)
	require.Len(t, units, 1)
	require.Equal(t, uint32(1), units[0].started.GetTotalShards())
	require.Zero(t, units[0].started.GetTotalParts(), "a property shard has no parts")
	require.Len(t, units[0].files, 1)
	f := units[0].files[0]
	require.Equal(t, "property/sw_prop/shard-0-00001.csv", f.started.GetRelativePath())
	require.Equal(t, uint32(1), f.started.GetSeq())
	require.Greater(t, f.chunks, 1, "a file over 1 MiB spans several chunks")
	require.Positive(t, f.chunkRows[0], "the first chunk reports the rows written before it")
	require.Less(t, f.chunkRows[0], uint64(count+1), "the first chunk is sent before the last row")
	require.Equal(t, map[string]uint64{testPropName: count + 1}, f.finished.GetRowCounts())
	require.Equal(t, map[string]int64{testPropName: 42}, f.finished.GetSchemaModRevisions())

	rows := csvRows(t, f)
	require.Len(t, rows, count+1)
	var tombstones int
	for _, row := range rows {
		require.Equal(t, testPropName, row["_name"])
		require.Equal(t, row["_id"], row["name"])
		require.NotEqual(t, `\N`, row["_mod_revision"])
		require.NotEqual(t, `\N`, row["_create_revision"])
		require.NotEqual(t, `\N`, row["_updated_at"])
		if row["_id"] == "gone" {
			tombstones++
			require.NotEqual(t, "0", row["_delete_time"], "a tombstone keeps its delete time")
			continue
		}
		require.Equal(t, "0", row["_delete_time"])
		require.Len(t, row["value"], valueSize)
	}
	require.Equal(t, 1, tombstones, "the tombstone must be exported")
	require.Empty(t, units[0].warnings)
	require.Equal(t, before, hashTree(t, root), "export must not change the snapshot")
}

func TestExport_GzipAndRolling(t *testing.T) {
	const count, valueSize = 20, 64 << 10
	s, _, _ := propertyFixture(t, count, valueSize)
	init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_GZIP)
	init.GetInit().MaxFileSize = 256 << 10
	init.GetInit().CsvAnnotations = true
	stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0), propertyFrame(testPropGroup))
	require.NoError(t, s.Export(stream))

	units := decodeFrames(t, stream.sent)
	require.Len(t, units, 2)
	require.Greater(t, len(units[0].files), 1, "files roll at max_file_size")
	var total uint64
	for i, f := range units[0].files {
		require.Equal(t, fmt.Sprintf("property/sw_prop/shard-0-%05d.csv.gz", i+1), f.started.GetRelativePath())
		require.Equal(t, uint32(i+1), f.started.GetSeq())
		total += f.finished.GetRowCounts()[testPropName]
		require.NotEmpty(t, csvRows(t, f))
	}
	require.Equal(t, uint64(count+1), total)
	// A unit without shards has nothing to export and no reason to warn.
	require.Empty(t, units[1].files)
	require.Empty(t, units[1].warnings)
	require.True(t, strings.HasSuffix(units[0].files[0].started.GetRelativePath(), ".gz"))
	require.Equal(t, filepath.Base(units[0].files[0].started.GetRelativePath()), "shard-0-00001.csv.gz")
}
