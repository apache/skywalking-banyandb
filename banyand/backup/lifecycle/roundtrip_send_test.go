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
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// captureSink collects every marshaled body a rawCapturingPublisher publishes.
// The row-replay sender hands out a fresh publisher per flush, so every instance
// shares one sink; access is locked because the sender confirms batches on
// background goroutines.
type captureSink struct {
	raw [][]byte
	mu  sync.Mutex
}

// add stores a COPY of b: the sender borrows the body from a size-classed buffer
// pool and returns it to the pool the moment Publish returns, so retaining the
// original slice would read recycled bytes.
func (s *captureSink) add(b []byte) {
	s.mu.Lock()
	s.raw = append(s.raw, append([]byte(nil), b...))
	s.mu.Unlock()
}

func (s *captureSink) bytes() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.raw))
	copy(out, s.raw)
	return out
}

// rawCapturingPublisher is a queue.BatchPublisher that records the exact bodies
// the row-replay sender emits, so a test can assert the migration sends precisely
// the expected InternalWriteRequests. Close returns no error so the confirm
// pipeline treats every batch as durably delivered.
type rawCapturingPublisher struct {
	sink *captureSink
}

func (p rawCapturingPublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	for i := range messages {
		switch m := messages[i].Data().(type) {
		case []byte:
			p.sink.add(m)
		case proto.Message:
			b, err := proto.Marshal(m)
			if err != nil {
				return nil, err
			}
			p.sink.add(b)
		default:
			return nil, fmt.Errorf("unexpected row-replay payload %T", messages[i].Data())
		}
	}
	return nil, nil
}

func (rawCapturingPublisher) Close() (map[string]*common.Error, error) { return nil, nil }

func capturingMockClient(t *testing.T) (*queue.MockClient, *captureSink) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	sink := &captureSink{}
	mockClient := queue.NewMockClient(ctrl)
	mockClient.EXPECT().NewBatchPublisher(gomock.Any()).
		DoAndReturn(func(time.Duration) queue.BatchPublisher { return rawCapturingPublisher{sink: sink} }).AnyTimes()
	return mockClient, sink
}

func singleNodeSelector(t *testing.T) node.Selector {
	t.Helper()
	selector, err := node.NewPickFirstSelector()
	require.NoError(t, err)
	selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "n1"}})
	return selector
}

// TestRoundtrip_MeasureSend is the end-to-end send check: write N measure points
// through the real write path, flush to disk, drive the actual lifecycle
// replayPart with a capturing publisher, and assert it emits EXACTLY N
// InternalWriteRequests, each deep-equal (proto.Equal) to the original.
func TestRoundtrip_MeasureSend(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

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
	require.Eventually(t, func() bool {
		_, mErr := measureService.Measure(&commonv1.Metadata{Name: roundtripE2EMeasure, Group: roundtripE2EGroup})
		return mErr == nil
	}, roundtripE2EFlushWait, 200*time.Millisecond, "measure service should resolve the registered measure")

	originals := writeRoundtripE2EPoints(t, pipeline)

	require.Eventually(t, func() bool {
		info, ciErr := measureService.CollectDataInfo(ctx, roundtripE2EGroup)
		if ciErr != nil || info == nil {
			return false
		}
		return len(info.SegmentInfo) >= 1
	}, roundtripE2EFlushWait, time.Second, "expected at least one segment after writes flush")

	dataPath := measureService.(interface{ GetDataPath() string }).GetDataPath()
	groupRoot := filepath.Join(dataPath, roundtripE2EGroup)
	require.Eventually(t, func() bool {
		return roundtripAllSidxDirsHaveSnapshot(groupRoot)
	}, roundtripE2EFlushWait, time.Second, "every <seg>/sidx/ under %s should carry a committed .snp", groupRoot)

	// Build the replayer while metadata is live (it eagerly snapshots schema).
	mockClient, sink := capturingMockClient(t)
	r, err := newMeasureRowReplayer(ctx, roundtripE2EGroup, 1, singleNodeSelector(t), mockClient,
		metadataService, localfs.NewLocalFileSystem(), logger.GetLogger("roundtrip-measure-send"), nil)
	req.NoError(err)
	defer r.Close()

	partDirs := findRoundtripPartDirs(groupRoot)
	req.NotEmpty(partDirs, "expected at least one part dir under %s", groupRoot)

	// Stop the services so the bluge sidx writer releases its lock before the
	// resolver opens it read-only inside replayPart.
	stopServices()

	for _, partDir := range partDirs {
		_, replayErr := r.replayPart(ctx, partDir)
		req.NoError(replayErr)
	}

	captured := make([]*measurev1.InternalWriteRequest, 0, roundtripE2EPointCount)
	for _, b := range sink.bytes() {
		iwr := &measurev1.InternalWriteRequest{}
		req.NoError(proto.Unmarshal(b, iwr))
		captured = append(captured, iwr)
	}
	req.Len(captured, roundtripE2EPointCount,
		"lifecycle replayPart must emit exactly %d measure internal write requests", roundtripE2EPointCount)
	assertRoundtripEqual(t, originals, captured)
}

// TestRoundtrip_StreamSend is the stream end-to-end send check: write two
// elements, flush, drive the real replayPart with a capturing publisher, and
// assert it emits exactly two InternalWriteRequests, each deep-equal to source.
func TestRoundtrip_StreamSend(t *testing.T) {
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

	mockClient, sink := capturingMockClient(t)
	replayer := newStreamRowReplayer(roundtripStreamGroup, 2, singleNodeSelector(t), mockClient,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("roundtrip-stream-send"), nil)
	defer replayer.Close()
	_, err = replayer.loadSchema(context.TODO(), roundtripStreamName)
	req.NoError(err)

	moduleDefer()
	moduleStopped = true

	for _, partDir := range partDirs {
		_, replayErr := replayer.replayPart(context.TODO(), partDir)
		req.NoError(replayErr)
	}

	seen := make(map[string]bool)
	for _, b := range sink.bytes() {
		iwr := &streamv1.InternalWriteRequest{}
		req.NoError(proto.Unmarshal(b, iwr))
		series := iwr.GetRequest().GetElement().GetTagFamilies()[0].GetTags()[0].GetStr().GetValue()
		e, ok := expect[series]
		require.Truef(t, ok, "replay emitted an unexpected series %q", series)
		assertStreamWriteRequestEqual(t, e.wr, iwr.Request)
		require.Equalf(t, e.wantShard, iwr.ShardId, "series %q must route to shard %d", series, e.wantShard)
		require.Falsef(t, seen[series], "series %q emitted twice", series)
		seen[series] = true
	}
	require.Len(t, seen, len(entries), "lifecycle must emit exactly the %d written elements", len(entries))
}

// TestRoundtrip_TraceSend is the trace end-to-end send check: write two spans,
// flush, drive the real replayPart with a capturing publisher, and assert it
// emits exactly two InternalWriteRequests, each deep-equal to source.
func TestRoundtrip_TraceSend(t *testing.T) {
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

	mockClient, sink := capturingMockClient(t)
	replayer, err := newTraceRowReplayer(context.TODO(), roundtripTraceGroup, 2, singleNodeSelector(t), mockClient,
		metaSvc, localfs.NewLocalFileSystem(), logger.GetLogger("roundtrip-trace-send"), nil)
	req.NoError(err)
	defer replayer.Close()

	moduleDefer()
	moduleStopped = true

	for _, partDir := range partDirs {
		_, replayErr := replayer.replayPart(context.TODO(), partDir)
		req.NoError(replayErr)
	}

	seen := make(map[string]bool)
	for _, b := range sink.bytes() {
		iwr := &tracev1.InternalWriteRequest{}
		req.NoError(proto.Unmarshal(b, iwr))
		traceID := iwr.GetRequest().GetTags()[0].GetStr().GetValue()
		e, ok := expect[traceID]
		require.Truef(t, ok, "replay emitted an unexpected trace_id %q", traceID)
		assertTraceWriteRequestEqual(t, e.wr, iwr.Request)
		require.Equalf(t, e.wantShard, iwr.ShardId, "trace_id %q must route to shard %d", traceID, e.wantShard)
		require.Falsef(t, seen[traceID], "trace_id %q emitted twice", traceID)
		seen[traceID] = true
	}
	require.Len(t, seen, len(entries), "lifecycle must emit exactly the %d written spans", len(entries))
}
