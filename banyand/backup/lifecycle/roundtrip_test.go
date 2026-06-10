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
	"context"
	"encoding/base64"
	"fmt"
	gofs "io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	roundtripMeasureGroup = "lc_rt_measure_group"
	roundtripMeasureName  = "lc_rt_measure"
	roundtripStreamGroup  = "lc_rt_stream_group"
	roundtripStreamName   = "lc_rt_stream"
	roundtripTraceGroup   = "lc_rt_trace_group"
	roundtripTraceName    = "lc_rt_trace"

	streamIdxGroup = "lc_rt_stream_idx_group"
	streamIdxName  = "lc_rt_stream_idx"
	streamIdxRule  = "endpoint_inverted"
)

// TestRoundtrip_Measure writes a measure row, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Measure(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	measureSvc, err := measure.NewStandalone(metaSvc, pipeline, nil, metricSvc, pm)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--measure-root-path=" + rootPath,
		"--measure-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, measureSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripMeasureSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := measureSvc.LoadGroup(roundtripMeasureGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "measure group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two rows with fully distinct entity/tags/fields/version/timestamp, routed
	// to different shards under shardNum=2 (series=ent-1 -> shard 0,
	// ent-4 -> shard 1), so a parser that swaps or conflates rows fails.
	type measureExpect struct {
		wr        *measurev1.WriteRequest
		series    string
		wantShard uint32
	}
	entries := []measureExpect{
		{buildMeasureWR(tsA, "ent-1", "alpha", 7, 111, 1.5, 42), "ent-1", 0},
		{buildMeasureWR(tsB, "ent-4", "bravo", 8888, 222222, 2.71828, 99), "ent-4", 1},
	}
	expect := make(map[string]measureExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.series] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i+1), &measurev1.InternalWriteRequest{
			ShardId:      e.wantShard,
			EntityValues: []*modelv1.TagValue{stringTagValue(e.series)},
			Request:      e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "measure parts for both shards not flushed")

	// Construct the replayer while the metadata service is still running so it
	// can enumerate measures + index rules; the IndexResolver below needs
	// exclusive access to the bluge index dir, so the service must be stopped
	// before any reader opens the segment.
	replayer, err := newMeasureRowReplayer(context.TODO(), roundtripMeasureGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	req.NoError(err)
	defer replayer.Close()
	// Warm the schema cache so buildWriteRequest does not need the metadata
	// service after we stop it below.
	_, err = replayer.loadSchema(roundtripMeasureName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		segPath := filepath.Dir(shardPath)
		reader, openErr := dumpmeasure.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		ir, irErr := replayer.loadIndexResolver(segPath)
		req.NoError(irErr)
		reader.SetIndexResolver(ir)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr, buildErr := replayer.buildWriteRequest(ir, row)
			req.NoError(buildErr)
			series := wr.GetDataPoint().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()
			e, ok := expect[series]
			require.Truef(t, ok, "replay produced an unexpected series %q", series)
			assertMeasureWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "series %q must route to shard %d under shardNum=2", series, e.wantShard)
			require.Equal(t, pbv1.EntityValues{stringTagValue(series)}.Encode(), iwr.EntityValues,
				"iwr.EntityValues must encode the entity tag (series)")
			require.Falsef(t, seen[series], "series %q reconstructed twice", series)
			seen[series] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both rows (shard 0 and shard 1) must roundtrip exactly once")
}

// TestRoundtrip_Stream writes a stream element, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Stream(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	streamSvc, err := stream.NewService(metaSvc, pipeline, metricSvc, pm, nil)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--stream-root-path=" + rootPath,
		"--stream-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, streamSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripStreamSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := streamSvc.LoadGroup(roundtripStreamGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "stream group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two rows routed to different shards under shardNum=2 (series=ent-3 -> shard 0,
	// ent-1 -> shard 1), with distinct tags, element ids and timestamps.
	type streamExpect struct {
		wr        *streamv1.WriteRequest
		series    string
		wantShard uint32
	}
	entries := []streamExpect{
		{buildStreamWR(tsA, "ent-3", "alpha", 7, "elem-a"), "ent-3", 0},
		{buildStreamWR(tsB, "ent-1", "bravo", 8888, "elem-b"), "ent-1", 1},
	}
	expect := make(map[string]streamExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.series] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(i+1), &streamv1.InternalWriteRequest{
			ShardId:      e.wantShard,
			EntityValues: []*modelv1.TagValue{stringTagValue(e.series)},
			Request:      e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "stream parts for both shards not flushed")

	replayer := newStreamRowReplayer(roundtripStreamGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	defer replayer.Close()
	// Warm the schema cache so buildWriteRequest does not need the metadata
	// service after we stop it below.
	_, err = replayer.loadSchema(context.TODO(), roundtripStreamName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		segPath := filepath.Dir(shardPath)
		reader, openErr := dumpstream.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		ir, irErr := replayer.loadIndexResolver(segPath)
		req.NoError(irErr)
		reader.SetIndexResolver(ir)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr, buildErr := replayer.buildWriteRequest(context.TODO(), row)
			req.NoError(buildErr)
			series := wr.GetElement().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()
			e, ok := expect[series]
			require.Truef(t, ok, "replay produced an unexpected series %q", series)
			assertStreamWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "series %q must route to shard %d under shardNum=2", series, e.wantShard)
			require.Equal(t, pbv1.EntityValues{stringTagValue(series)}.Encode(), iwr.EntityValues,
				"iwr.EntityValues must encode the entity tag (series)")
			require.Equal(t, row.ElementID, iwr.RawElementId, "raw_element_id must equal source row.ElementID")
			decodedID, decErr := base64.StdEncoding.DecodeString(wr.Element.ElementId)
			require.NoError(t, decErr, "wr.Element.ElementId must be valid base64")
			require.Equal(t, row.ElementID, convert.BytesToUint64(decodedID),
				"wr.Element.ElementId must encode the same eID carried by raw_element_id")
			require.Falsef(t, seen[series], "series %q reconstructed twice", series)
			seen[series] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both rows (shard 0 and shard 1) must roundtrip exactly once")
}

// TestRoundtrip_Trace writes a trace span, flushes to disk, then verifies
// buildWriteRequest reconstructs a proto-equal WriteRequest.
func TestRoundtrip_Trace(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	traceSvc, err := trace.NewService(metaSvc, pipeline, metricSvc, pm)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--trace-root-path=" + rootPath,
		"--trace-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, traceSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerRoundtripTraceSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := traceSvc.LoadGroup(roundtripTraceGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "trace group not loaded")
	time.Sleep(time.Second)

	tsA := time.Now().Truncate(time.Millisecond)
	tsB := tsA.Add(time.Hour)
	// Two spans routed to different shards under shardNum=2 (trace_id=trace-0 ->
	// shard 0, trace-abc -> shard 1), with distinct span ids, span payloads and
	// timestamps.
	type traceExpect struct {
		wr        *tracev1.WriteRequest
		traceID   string
		wantShard uint32
	}
	entries := []traceExpect{
		{buildTraceWR(tsA, "trace-0", "span-a", []byte("payload-a")), "trace-0", 0},
		{buildTraceWR(tsB, "trace-abc", "span-b", []byte("payload-b-longer")), "trace-abc", 1},
	}
	expect := make(map[string]traceExpect, len(entries))

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, e := range entries {
		expect[e.traceID] = e
		_, errPub := bp.Publish(context.TODO(), data.TopicTraceWrite, bus.NewMessage(bus.MessageID(i+1), &tracev1.InternalWriteRequest{
			ShardId: e.wantShard,
			Request: e.wr,
		}))
		req.NoError(errPub)
	}
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 2
	}, 30*time.Second, 200*time.Millisecond, "trace parts for both shards not flushed")

	replayer, err := newTraceRowReplayer(context.TODO(), roundtripTraceGroup, 2, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer"), nil)
	req.NoError(err)
	defer replayer.Close()

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	seen := make(map[string]bool)
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		reader, openErr := dumptrace.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			wr, iwr := replayer.buildWriteRequest(row)
			traceID := wr.GetTags()[0].GetStr().GetValue()
			e, ok := expect[traceID]
			require.Truef(t, ok, "replay produced an unexpected trace_id %q", traceID)
			assertTraceWriteRequestEqual(t, e.wr, wr)
			require.NotNil(t, iwr.Request, "iwr must wrap a WriteRequest")
			require.Equal(t, wr, iwr.Request, "iwr.Request must point at the same WriteRequest")
			require.Equalf(t, e.wantShard, iwr.ShardId, "trace_id %q must route to shard %d under shardNum=2", traceID, e.wantShard)
			require.Falsef(t, seen[traceID], "trace_id %q reconstructed twice", traceID)
			seen[traceID] = true
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Len(t, seen, len(entries), "both spans (shard 0 and shard 1) must roundtrip exactly once")
}

// buildMeasureWR builds a measure WriteRequest whose entity (series), other
// tags, fields, version and timestamp are all caller-controlled so two rows can
// be made fully distinct.
func buildMeasureWR(ts time.Time, series, strTag string, intTag, intField int64, floatField float64, version int64) *measurev1.WriteRequest {
	return &measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripMeasureGroup, Name: roundtripMeasureName},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(ts),
			Version:     version,
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{stringTagValue(series), stringTagValue(strTag), intTagValue(intTag)}}},
			Fields:      []*modelv1.FieldValue{intFieldValue(intField), floatFieldValue(floatField)},
		},
	}
}

func buildStreamWR(ts time.Time, series, strTag string, intTag int64, elementID string) *streamv1.WriteRequest {
	return &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripStreamGroup, Name: roundtripStreamName},
		Element: &streamv1.ElementValue{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(ts),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{stringTagValue(series), stringTagValue(strTag), intTagValue(intTag)}}},
		},
	}
}

func buildTraceWR(ts time.Time, traceID, spanID string, span []byte) *tracev1.WriteRequest {
	return &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: roundtripTraceGroup, Name: roundtripTraceName},
		Tags: []*modelv1.TagValue{
			stringTagValue(traceID),
			stringTagValue(spanID),
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}},
		},
		Span:    span,
		Version: 1,
	}
}

// assertMeasureWriteRequestEqual asserts proto.Equal ignoring MessageId.
func assertMeasureWriteRequestEqual(t *testing.T, want, got *measurev1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*measurev1.WriteRequest)
	gotC := proto.Clone(got).(*measurev1.WriteRequest)
	wantC.MessageId = 0
	gotC.MessageId = 0
	require.Truef(t, proto.Equal(wantC, gotC), "measure WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

// assertStreamWriteRequestEqual asserts proto.Equal ignoring MessageId and
// ElementId (the replayer base64-encodes the storage uint64; receiver uses
// raw_element_id instead).
func assertStreamWriteRequestEqual(t *testing.T, want, got *streamv1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*streamv1.WriteRequest)
	gotC := proto.Clone(got).(*streamv1.WriteRequest)
	wantC.MessageId = 0
	gotC.MessageId = 0
	if wantC.Element != nil {
		wantC.Element.ElementId = ""
	}
	if gotC.Element != nil {
		gotC.Element.ElementId = ""
	}
	require.Truef(t, proto.Equal(wantC, gotC), "stream WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

// assertTraceWriteRequestEqual asserts proto.Equal ignoring Version.
func assertTraceWriteRequestEqual(t *testing.T, want, got *tracev1.WriteRequest) {
	t.Helper()
	wantC := proto.Clone(want).(*tracev1.WriteRequest)
	gotC := proto.Clone(got).(*tracev1.WriteRequest)
	wantC.Version = 0
	gotC.Version = 0
	require.Truef(t, proto.Equal(wantC, gotC), "trace WriteRequest mismatch:\nwant: %v\ngot:  %v", wantC, gotC)
}

func registerRoundtripMeasureSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripMeasureGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: roundtripMeasureName, Group: roundtripMeasureGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "strTag", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "intTag", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "intField",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
			{
				Name:              "floatField",
				FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
}

func registerRoundtripStreamSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripStreamGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: roundtripStreamName, Group: roundtripStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "strTag", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "intTag", Type: databasev1.TagType_TAG_TYPE_INT},
			},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
}

func registerRoundtripTraceSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripTraceGroup},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateTrace(ctx, &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: roundtripTraceName, Group: roundtripTraceGroup},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	})
	require.NoError(t, err)
}

func findRoundtripPartDirs(root string) []string {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d gofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if d.Name() == "metadata.json" {
			if _, parseErr := strconv.ParseUint(filepath.Base(filepath.Dir(path)), 16, 64); parseErr == nil {
				dirs = append(dirs, filepath.Dir(path))
			}
		}
		return nil
	})
	return dirs
}

const (
	roundtripE2EGroup       = "e2e_roundtrip_indexed"
	roundtripE2EMeasure     = "e2e_roundtrip_metric"
	roundtripE2ETagFamily   = "default"
	roundtripE2EIndexRule   = "service_index"
	roundtripE2EPointCount  = 10
	roundtripE2EFlushWait   = 30 * time.Second
	roundtripE2EBatchTimout = 5 * time.Second
)

// TestRoundtrip_MeasureIndexed proves the lifecycle row-replay reverse-decode reconstructs
// exactly what was written for a measure that carries an INVERTED index rule.
//
// Flow: register a measure (entity tag + column tags including an indexed
// "service" tag + two INT fields + an INVERTED index rule on "service" + a
// binding) → write 10 data points through the normal measure write path →
// flush to on-disk part files + sidx → reverse-decode the part via the
// lifecycle row-replay build → assert the 10 reconstructed InternalWriteRequests
// equal the 10 originals. The indexed "service" tag must roundtrip back to the
// written value, which proves the index-rule decode path through sidx.
func TestRoundtrip_MeasureIndexed(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterTestingT(t)

	pipeline := queue.Local()
	metadataService, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metadataService, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	measureService, err := measure.NewStandalone(metadataService, pipeline, nil, metricSvc, pm)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	measureRoot, mrDefer, err := test.NewSpace()
	req.NoError(err)
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--measure-root-path=" + measureRoot,
		"--measure-flush-timeout=1s",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metadataService, measureService)
	// metaDefer + mrDefer always run; moduleDefer is invoked explicitly before
	// the reverse-decode so the bluge sidx writer commits and releases locks,
	// but guard against a double-call via the stopped flag.
	stopped := false
	stopServices := func() {
		if !stopped {
			moduleDefer()
			stopped = true
		}
	}
	defer func() {
		stopServices()
		mrDefer()
		metaDefer()
	}()

	ctx := context.TODO()
	registerRoundtripE2ESchema(t, metadataService)

	// Wait for the measure event to propagate to the schemaRepo so the first
	// writes are not rejected with "cannot find measure definition".
	require.Eventually(t, func() bool {
		_, mErr := measureService.Measure(&commonv1.Metadata{Name: roundtripE2EMeasure, Group: roundtripE2EGroup})
		return mErr == nil
	}, roundtripE2EFlushWait, 200*time.Millisecond, "measure service should resolve the registered measure")

	originals := writeRoundtripE2EPoints(t, pipeline)

	// Wait until the flusher materializes at least one segment on disk.
	require.Eventually(t, func() bool {
		info, ciErr := measureService.CollectDataInfo(ctx, roundtripE2EGroup)
		if ciErr != nil || info == nil {
			return false
		}
		return len(info.SegmentInfo) >= 1
	}, roundtripE2EFlushWait, time.Second, "expected at least one segment after writes flush")

	dataPath := measureService.(interface{ GetDataPath() string }).GetDataPath()
	groupRoot := filepath.Join(dataPath, roundtripE2EGroup)
	// Wait until every <seg>/sidx/ dir carries a committed .snp snapshot so the
	// indexed-tag values are resolvable when the resolver opens the sidx.
	require.Eventually(t, func() bool {
		return roundtripAllSidxDirsHaveSnapshot(groupRoot)
	}, roundtripE2EFlushWait, time.Second, "every <seg>/sidx/ under %s should carry a committed .snp", groupRoot)

	// Build the replayer BEFORE stopping the services: newMeasureRowReplayer
	// eagerly ListMeasure/ListIndexRule/ListIndexRuleBinding at construction,
	// so the schema is snapshotted while metadata is still live.
	selector, err := node.NewPickFirstSelector()
	req.NoError(err)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "n1"}})
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	mockClient := queue.NewMockClient(ctrl)
	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).
		DoAndReturn(func(time.Duration) queue.BatchPublisher { return &marshalingBatchPublisher{} }).AnyTimes()
	r, err := newMeasureRowReplayer(ctx, roundtripE2EGroup, 1, selector, mockClient,
		metadataService, localfs.NewLocalFileSystem(), logger.GetLogger("roundtrip-e2e"), nil)
	req.NoError(err)
	defer r.Close()

	// Locate the on-disk part dir (parent is a 16-hex partID).
	partDirs := findRoundtripPartDirs(groupRoot)
	req.NotEmpty(partDirs, "expected at least one part dir under %s", groupRoot)

	// Stop the services so the bluge sidx writer commits and releases its
	// exclusive lock before the resolver opens it read-only. Metadata is no
	// longer needed because the replayer already snapshotted the schema.
	stopServices()

	reconstructed := reverseDecodeRoundtripParts(t, r, partDirs)
	req.Len(reconstructed, roundtripE2EPointCount,
		"expected %d reconstructed rows across all parts", roundtripE2EPointCount)

	assertRoundtripEqual(t, originals, reconstructed)
}

// registerRoundtripE2ESchema creates a CATALOG_MEASURE group, a measure with an
// entity tag, two STRING column tags ("id", "service") plus the entity tag, two
// INT fields, an INVERTED index rule on "service", and a binding for it.
func registerRoundtripE2ESchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: roundtripE2EGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: roundtripE2EMeasure, Group: roundtripE2EGroup},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: roundtripE2ETagFamily, Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "total",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRule(ctx, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: roundtripE2EIndexRule, Group: roundtripE2EGroup},
		Tags:     []string{"service"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: roundtripE2EIndexRule + "_binding", Group: roundtripE2EGroup},
		Subject: &databasev1.Subject{
			Name:    roundtripE2EMeasure,
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
		},
		Rules:    []string{roundtripE2EIndexRule},
		BeginAt:  timestamppb.New(time.Now().Add(-24 * time.Hour)),
		ExpireAt: timestamppb.New(time.Now().Add(365 * 24 * time.Hour)),
	})
	require.NoError(t, err)
}

// writeRoundtripE2EPoints publishes 10 data points, each in its own series
// (distinct entity_id) with a distinct indexed "service" value, "id" value and
// field values, and returns the 10 *measurev1.InternalWriteRequest published.
func writeRoundtripE2EPoints(t *testing.T, pipeline queue.Queue) []*measurev1.InternalWriteRequest {
	t.Helper()
	bp := pipeline.NewBatchPublisher(roundtripE2EBatchTimout)
	defer bp.Close()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	originals := make([]*measurev1.InternalWriteRequest, 0, roundtripE2EPointCount)
	for i := 0; i < roundtripE2EPointCount; i++ {
		iStr := strconv.Itoa(i)
		entityValue := "entity-" + iStr
		wr := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: roundtripE2EMeasure, Group: roundtripE2EGroup},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(i) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id-" + iStr}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityValue}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "service-" + iStr}}},
					},
				}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(100 + i)}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}}},
				},
			},
		}
		iwr := &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityValue}}},
			},
			Request: wr,
		}
		_, err := bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i), iwr))
		require.NoError(t, err)
		originals = append(originals, proto.Clone(iwr).(*measurev1.InternalWriteRequest))
	}
	return originals
}

// reverseDecodeRoundtripParts opens every part dir, reconstructs each row via
// the columnar full-pool build path (the production reverse-decode), and
// returns deep copies of every reconstructed InternalWriteRequest. The proto
// tree is reused per row, so each row is cloned immediately before Next().
func reverseDecodeRoundtripParts(t *testing.T, r *measureRowReplayer, partDirs []string) []*measurev1.InternalWriteRequest {
	t.Helper()
	fsys := localfs.NewLocalFileSystem()
	var out []*measurev1.InternalWriteRequest
	for _, partDir := range partDirs {
		partID, err := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		require.NoError(t, err)
		shardPath := filepath.Dir(partDir)
		segmentPath := filepath.Dir(shardPath)

		ir, err := r.loadIndexResolver(segmentPath)
		require.NoError(t, err)
		reader, err := dumpmeasure.OpenPart(partID, shardPath, fsys)
		require.NoError(t, err)
		reader.SetIndexResolver(ir)
		reader.SetReuseBuffers(true)
		cur := reader.ColumnarIterator()

		var bc measureBlockCtx
		var pb measureProtoBuilder
		for cur.Next() {
			iwr, _, fillErr := r.fillWriteRequestColumnar(ir, &bc, &pb, cur.Block(), cur.Index())
			require.NoError(t, fillErr)
			out = append(out, proto.Clone(iwr).(*measurev1.InternalWriteRequest))
		}
		require.NoError(t, cur.Err())
		cur.Close()
		reader.Close()
	}
	return out
}

// assertRoundtripEqual matches each reconstructed row to its original by the
// "id" column tag (a stable key independent of part/series ordering) and
// asserts the meaningful fields roundtrip: group/name, timestamp instant,
// every tag (including the indexed "service" tag), and every field. All 10
// must match.
func assertRoundtripEqual(t *testing.T, originals, reconstructed []*measurev1.InternalWriteRequest) {
	t.Helper()
	byID := make(map[string]*measurev1.InternalWriteRequest, len(originals))
	for _, orig := range originals {
		byID[roundtripTagValue(orig, "id")] = orig
	}
	matched := 0
	for _, got := range reconstructed {
		idVal := roundtripTagValue(got, "id")
		orig, ok := byID[idVal]
		require.True(t, ok, "reconstructed row with id=%q has no original", idVal)

		require.Equal(t, orig.Request.Metadata.Group, got.Request.Metadata.Group, "group mismatch for id=%q", idVal)
		require.Equal(t, orig.Request.Metadata.Name, got.Request.Metadata.Name, "name mismatch for id=%q", idVal)
		require.Equal(t, orig.Request.DataPoint.Timestamp.AsTime().UnixNano(),
			got.Request.DataPoint.Timestamp.AsTime().UnixNano(), "timestamp mismatch for id=%q", idVal)

		// Every written tag (id, entity_id, and the INDEXED service tag) must
		// roundtrip to its written value. The "service" assertion is the proof
		// that the index-rule decode path through sidx reconstructs correctly.
		require.NotEmpty(t, roundtripTagValue(got, "service"),
			"reconstructed indexed service tag must be non-empty for id=%q", idVal)
		for _, tagName := range []string{"id", "entity_id", "service"} {
			require.Equal(t, roundtripTagValue(orig, tagName), roundtripTagValue(got, tagName),
				"tag %q mismatch for id=%q", tagName, idVal)
		}

		origFields := orig.Request.DataPoint.Fields
		gotFields := got.Request.DataPoint.Fields
		require.Len(t, gotFields, len(origFields), "field count mismatch for id=%q", idVal)
		for fi := range origFields {
			require.True(t, proto.Equal(origFields[fi], gotFields[fi]),
				"field %d mismatch for id=%q: orig=%v got=%v", fi, idVal, origFields[fi], gotFields[fi])
		}
		matched++
	}
	require.Equal(t, roundtripE2EPointCount, matched, "all %d originals must be matched", roundtripE2EPointCount)
}

// roundtripTagValue returns the string value of the named tag in the default
// tag family of the given write request, or "" if absent. The reconstructed
// request only carries tag names, so it is matched positionally against the
// schema's tag order (id, entity_id, service).
func roundtripTagValue(iwr *measurev1.InternalWriteRequest, tagName string) string {
	order := []string{"id", "entity_id", "service"}
	idx := -1
	for i, n := range order {
		if n == tagName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ""
	}
	for _, tf := range iwr.Request.DataPoint.TagFamilies {
		if len(tf.Tags) <= idx {
			continue
		}
		if str := tf.Tags[idx].GetStr(); str != nil {
			return str.Value
		}
	}
	return ""
}

// roundtripAllSidxDirsHaveSnapshot returns true when every <groupRoot>/seg-*/sidx
// dir carries at least one .snp file (bluge's snapshot marker).
func roundtripAllSidxDirsHaveSnapshot(groupRoot string) bool {
	segs, err := os.ReadDir(groupRoot)
	if err != nil {
		return false
	}
	saw := 0
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), "seg-") {
			continue
		}
		sidxDir := filepath.Join(groupRoot, seg.Name(), "sidx")
		entries, readErr := os.ReadDir(sidxDir)
		if readErr != nil {
			return false
		}
		hasSnap := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".snp") {
				hasSnap = true
				break
			}
		}
		if !hasSnap {
			return false
		}
		saw++
	}
	return saw > 0
}

// marshalingBatchPublisher is a queue.BatchPublisher stub that satisfies the
// row-replayer's constructor (newBatchSender opens one publisher eagerly). This
// test drives the reverse-decode build path directly and never publishes, so
// Publish only validates the payload shape and is otherwise a sink.
type marshalingBatchPublisher struct{}

func (marshalingBatchPublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	for i := range messages {
		switch m := messages[i].Data().(type) {
		case []byte:
			// Already marshaled by the measure enqueue path.
		case proto.Message:
			if _, err := proto.Marshal(m); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected message payload %T", messages[i].Data())
		}
	}
	return nil, nil
}

func (marshalingBatchPublisher) Close() (map[string]*common.Error, error) { return nil, nil }

// TestRoundtrip_StreamIndexed registers a stream whose "endpoint" tag carries an
// INVERTED index rule, writes one element, flushes to disk, then verifies the
// indexed tag still roundtrips through buildWriteRequest. Unlike measure, stream
// keeps every tag value in the part column (the IndexResolver is built with a nil
// rule map and buildStreamTagFamilies reads only row.Tags), so the indexed tag is
// reconstructed from the column, not from sidx. This guards that adding an index
// rule never strands the tag value out of the replayable column data.
func TestRoundtrip_StreamIndexed(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	streamSvc, err := stream.NewService(metaSvc, pipeline, metricSvc, pm, nil)
	req.NoError(err)

	metaPath, metaDefer, err := test.NewSpace()
	req.NoError(err)
	defer metaDefer()
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	defer rootDefer()

	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--stream-root-path=" + rootPath,
		"--stream-flush-timeout=200ms",
	}
	moduleDefer := test.SetupModules(flags, pipeline, metaSvc, streamSvc)
	moduleStopped := false
	defer func() {
		if !moduleStopped {
			moduleDefer()
		}
	}()

	registerStreamIndexedSchema(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := streamSvc.LoadGroup(streamIdxGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "indexed stream group not loaded")
	time.Sleep(time.Second)

	ts := time.Now().Truncate(time.Millisecond)
	const endpoint = "/api/login"
	wr := &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: streamIdxGroup, Name: streamIdxName},
		Element: &streamv1.ElementValue{
			ElementId: "elem-idx",
			Timestamp: timestamppb.New(ts),
			TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
				stringTagValue("svc-1"),
				stringTagValue("payload-1"),
				stringTagValue(endpoint),
			}}},
		},
	}
	bp := pipeline.NewBatchPublisher(5 * time.Second)
	_, errPub := bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(1), &streamv1.InternalWriteRequest{
		ShardId:      0,
		EntityValues: []*modelv1.TagValue{stringTagValue("svc-1")},
		Request:      wr,
	}))
	req.NoError(errPub)
	closeNodeErrs, closeErr := bp.Close()
	req.NoError(closeErr)
	req.Empty(closeNodeErrs)

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findRoundtripPartDirs(rootPath)
		return len(partDirs) >= 1
	}, 30*time.Second, 200*time.Millisecond, "indexed stream part not flushed")

	replayer := newStreamRowReplayer(streamIdxGroup, 1, nil, pipeline,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("test-replayer-idx"), nil)
	defer replayer.Close()
	// Warm the schema cache so buildWriteRequest does not need metadata after stop.
	_, err = replayer.loadSchema(context.TODO(), streamIdxName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	fileSystem := localfs.NewLocalFileSystem()
	reconstructed := 0
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		shardPath := filepath.Dir(partDir)
		segPath := filepath.Dir(shardPath)
		reader, openErr := dumpstream.OpenPart(partID, shardPath, fileSystem)
		req.NoError(openErr)
		ir, irErr := replayer.loadIndexResolver(segPath)
		req.NoError(irErr)
		reader.SetIndexResolver(ir)
		it := reader.Iterator()
		for it.Next() {
			row := it.Row()
			// The indexed tag value must remain in the part column; stream does not
			// move it to sidx, and the replayer rebuilds tags only from columns.
			require.Contains(t, row.Tags, "default.endpoint",
				"indexed endpoint tag must stay in the replayable part column")
			wrGot, _, buildErr := replayer.buildWriteRequest(context.TODO(), row)
			req.NoError(buildErr)
			tags := wrGot.GetElement().GetTagFamilies()[0].GetTags()
			require.Len(t, tags, 3, "reconstructed element must carry all three tags")
			require.Equal(t, endpoint, tags[2].GetStr().GetValue(),
				"the INVERTED-indexed endpoint tag must roundtrip to its written value")
			reconstructed++
		}
		require.NoError(t, it.Err())
		it.Close()
		reader.Close()
	}
	require.Equal(t, 1, reconstructed, "exactly one indexed element must roundtrip")
}

func registerStreamIndexedSchema(t *testing.T, metaSvc metadataservice.Service) {
	t.Helper()
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: streamIdxGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: streamIdxName, Group: streamIdxGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "payload", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRule(ctx, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: streamIdxRule, Group: streamIdxGroup},
		Tags:     []string{"endpoint"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: streamIdxRule + "_binding", Group: streamIdxGroup},
		Subject: &databasev1.Subject{
			Name:    streamIdxName,
			Catalog: commonv1.Catalog_CATALOG_STREAM,
		},
		Rules:    []string{streamIdxRule},
		BeginAt:  timestamppb.New(time.Now().Add(-24 * time.Hour)),
		ExpireAt: timestamppb.New(time.Now().Add(365 * 24 * time.Hour)),
	})
	require.NoError(t, err)
}
