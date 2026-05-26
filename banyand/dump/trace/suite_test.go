// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	rtGroup = "dump_rt_group"
	rtTrace = "dump_rt_trace"
)

// TestTraceWriteRoundTrip drives the top-level trace write path (the same
// writeCallback.Rev reached via the data pipeline), flushes real parts to disk,
// then reads them back with the dump iterator and verifies every written field.
func TestTraceWriteRoundTrip(t *testing.T) {
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
	defer moduleDefer()

	registerRoundTripTrace(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := traceSvc.LoadGroup(rtGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "trace group not loaded")
	time.Sleep(time.Second)

	tagNames := []string{"trace_id", "span_id", "timestamp", "service_id", "duration"}
	const total = 3
	baseTS := time.Now().Truncate(time.Minute)
	type want struct {
		traceID  string
		spanID   string
		service  string
		span     string
		duration int64
		tsMilli  int64
	}
	wants := map[string]want{}

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i := 0; i < total; i++ {
		iStr := strconv.Itoa(i)
		ts := baseTS.Add(time.Duration(i) * time.Minute)
		w := want{
			traceID:  "trace" + iStr,
			spanID:   "span" + iStr,
			service:  "svc" + iStr,
			duration: int64(100 + i),
			span:     "span_data_" + iStr,
			tsMilli:  ts.UnixMilli(),
		}
		wants[w.spanID] = w
		tags := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.traceID}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.spanID}}},
			{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.service}}},
			{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: w.duration}}},
		}
		writeReq := &tracev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: rtTrace, Group: rtGroup},
			Tags:     tags,
			Span:     []byte(w.span),
			TagSpec:  &tracev1.TagSpec{TagNames: tagNames},
		}
		_, errPub := bp.Publish(context.TODO(), data.TopicTraceWrite, bus.NewMessage(bus.MessageID(i), &tracev1.InternalWriteRequest{
			Request: writeReq,
		}))
		req.NoError(errPub)
	}
	req.Empty(bp.Close())

	var partDirs []string
	require.Eventually(t, func() bool {
		partDirs = findPartDirs(rootPath)
		return len(partDirs) > 0
	}, 30*time.Second, 200*time.Millisecond, "no on-disk part was flushed")

	fileSystem := localfs.NewLocalFileSystem()
	seen := map[string]bool{}
	seriesIDs := map[uint64]bool{}
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		p, openErr := OpenPart(partID, filepath.Dir(partDir), fileSystem)
		req.NoError(openErr)
		it := p.Iterator()
		for it.Next() {
			r := it.Row()
			w, ok := wants[r.SpanID]
			require.True(t, ok, "unexpected row spanID=%q", r.SpanID)
			require.False(t, seen[r.SpanID], "duplicate row for %s", r.SpanID)
			seen[r.SpanID] = true
			seriesIDs[uint64(r.SeriesID)] = true

			require.Equal(t, w.traceID, r.TraceID)
			require.Equal(t, w.span, string(r.Span))
			require.Equal(t, w.service, dump.DecodeTagValue(r.TagTypes["service_id"], r.Tags["service_id"], nil).GetStr().GetValue())
			require.Equal(t, w.duration, dump.DecodeTagValue(r.TagTypes["duration"], r.Tags["duration"], nil).GetInt().GetValue())
			require.Equal(t, w.tsMilli, time.Unix(0, r.Timestamp).UnixMilli(), "timestamp round-trip for %s", r.SpanID)
			require.NotZero(t, r.SeriesID, "seriesID derived from tags")
		}
		require.NoError(t, it.Err())
		it.Close()
		p.Close()
	}
	require.Len(t, seen, total, "every written span must be read back exactly once")
	require.Len(t, seriesIDs, total, "each span must map to a distinct series")
}

func registerRoundTripTrace(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: rtGroup},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateTrace(ctx, &databasev1.Trace{
		Metadata: &commonv1.Metadata{Name: rtTrace, Group: rtGroup},
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
		},
		TraceIdTagName:   "trace_id",
		SpanIdTagName:    "span_id",
		TimestampTagName: "timestamp",
	})
	require.NoError(t, err)
}

func findPartDirs(root string) []string {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
