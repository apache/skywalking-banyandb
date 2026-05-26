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

package measure

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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/dump"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	rtGroup   = "dump_rt_group"
	rtMeasure = "dump_rt_measure"
)

// TestMeasureWriteRoundTrip drives the top-level measure write path (the same
// writeCallback.Rev reached via the data pipeline), flushes real parts to disk,
// then reads them back with the dump iterator and verifies every written field.
func TestMeasureWriteRoundTrip(t *testing.T) {
	req := require.New(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	// Bridge gomega (used inside test.SetupModules) onto the test process.
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
	defer moduleDefer()

	registerRoundTripMeasure(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := measureSvc.LoadGroup(rtGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "measure group not loaded")
	// Let the measure schema event propagate after the group is ready.
	time.Sleep(time.Second)

	const total = 3
	baseTS := time.Now().Truncate(time.Minute)
	type want struct {
		series  string
		strTag  string
		intTag  int64
		intF    int64
		floatF  float64
		tsMilli int64
	}
	wants := map[string]want{}

	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i := 0; i < total; i++ {
		iStr := strconv.Itoa(i)
		ts := baseTS.Add(time.Duration(i) * time.Minute)
		w := want{
			series:  "series" + iStr,
			strTag:  "str" + iStr,
			intTag:  int64(10 + i),
			intF:    int64(100 + i),
			floatF:  float64(i) + 0.5,
			tsMilli: ts.UnixMilli(),
		}
		wants[w.strTag] = w
		writeReq := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: rtMeasure, Group: rtGroup},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(ts),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.series}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: w.strTag}}},
						{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: w.intTag}}},
					},
				}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: w.intF}}},
					{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: w.floatF}}},
				},
			},
		}
		_, errPub := bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i), &measurev1.InternalWriteRequest{
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

			require.Equal(t, w.intTag, dump.DecodeTagValue(r.TagTypes["default.intTag"], r.Tags["default.intTag"], nil).GetInt().GetValue())
			require.Equal(t, w.intF, DecodeFieldValue(r.FieldTypes["intField"], r.Fields["intField"]).GetInt().GetValue())
			require.InDelta(t, w.floatF, DecodeFieldValue(r.FieldTypes["floatField"], r.Fields["floatField"]).GetFloat().GetValue(), 1e-9)
			require.Equal(t, w.tsMilli, time.Unix(0, r.Timestamp).UnixMilli(), "timestamp round-trip for %s", w.strTag)
			require.NotZero(t, r.SeriesID, "seriesID derived from entity")
			// The entity tag itself is not stored as a data column.
			require.NotContains(t, r.Tags, "default.series")
		}
		require.NoError(t, it.Err())
		it.Close()
		p.Close()
	}
	require.Len(t, seen, total, "every written data point must be read back exactly once")
	require.Len(t, seriesIDs, total, "each distinct entity must map to a distinct series")
}

func registerRoundTripMeasure(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: rtGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: rtMeasure, Group: rtGroup},
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

// findPartDirs walks root and returns every directory that directly contains a
// metadata.json file and whose name is a 16-hex part id (an on-disk part dir).
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
