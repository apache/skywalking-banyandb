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

package stream

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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	rtGroup  = "dump_rt_group"
	rtStream = "dump_rt_stream"
)

// TestStreamWriteRoundTrip drives the top-level stream write path (the same
// writeCallback.Rev reached via the data pipeline), flushes real parts to disk,
// then reads them back with the dump iterator and verifies every written field.
func TestStreamWriteRoundTrip(t *testing.T) {
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
	defer moduleDefer()

	registerRoundTripStream(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := streamSvc.LoadGroup(rtGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "stream group not loaded")
	time.Sleep(time.Second)

	const total = 3
	baseTS := time.Now().Truncate(time.Minute)
	type want struct {
		series  string
		strTag  string
		intTag  int64
		tsMilli int64
	}
	wants := map[string]want{}

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i := 0; i < total; i++ {
		iStr := strconv.Itoa(i)
		ts := baseTS.Add(time.Duration(i) * time.Minute)
		w := want{series: "series" + iStr, strTag: "str" + iStr, intTag: int64(10 + i), tsMilli: ts.UnixMilli()}
		wants[w.strTag] = w
		writeReq := &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: rtStream, Group: rtGroup},
			Element: &streamv1.ElementValue{
				Timestamp: timestamppb.New(ts),
				ElementId: "element" + iStr,
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.series}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.strTag}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: w.intTag}}},
					},
				}},
			},
		}
		_, errPub := bp.Publish(context.TODO(), data.TopicStreamWrite, bus.NewMessage(bus.MessageID(i), &streamv1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.series}}}},
			Request:      writeReq,
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
	elementIDs := map[uint64]bool{}
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		p, openErr := OpenPart(partID, filepath.Dir(partDir), fileSystem)
		req.NoError(openErr)
		it := p.Iterator()
		for it.Next() {
			r := it.Row()
			strTag := dump.DecodeTagValue(r.TagTypes["default.strTag"], r.Tags["default.strTag"], nil).GetStr().GetValue()
			w, ok := wants[strTag]
			require.True(t, ok, "unexpected row strTag=%q sid=%d", strTag, r.SeriesID)
			require.False(t, seen[w.strTag], "duplicate row for %s", w.strTag)
			seen[w.strTag] = true
			seriesIDs[uint64(r.SeriesID)] = true
			elementIDs[r.ElementID] = true

			require.Equal(t, w.intTag, dump.DecodeTagValue(r.TagTypes["default.intTag"], r.Tags["default.intTag"], nil).GetInt().GetValue())
			require.Equal(t, w.tsMilli, time.Unix(0, r.Timestamp).UnixMilli(), "timestamp round-trip for %s", w.strTag)
			require.NotZero(t, r.SeriesID, "seriesID derived from entity")
			require.NotZero(t, r.ElementID, "elementID set")
			require.NotContains(t, r.Tags, "default.series")
		}
		require.NoError(t, it.Err())
		it.Close()
		p.Close()
	}
	require.Len(t, seen, total, "every written element must be read back exactly once")
	require.Len(t, seriesIDs, total, "each distinct entity must map to a distinct series")
	require.Len(t, elementIDs, total, "each element must have a distinct elementID")
}

func registerRoundTripStream(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: rtGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: rtStream, Group: rtGroup},
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
