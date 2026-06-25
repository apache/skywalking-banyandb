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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	localfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	rtGroup     = "dump_rt_group"
	rtMeasure   = "dump_rt_measure"
	idxrGroup   = "dump_idxr_group"
	idxrMeasure = "dump_idxr_measure"
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

// TestMeasureIndexedTagResolvedFromIndex verifies the dump recovers a measure's
// indexed (non-entity) tag values from the series index via an IndexResolver,
// even though they are absent from the data columns. It covers a scalar string,
// a scalar int and a string-array indexed tag.
func TestMeasureIndexedTagResolvedFromIndex(t *testing.T) {
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
	stopped := false
	defer func() {
		if !stopped {
			moduleDefer()
		}
	}()

	registerIndexedResolveMeasure(t, metaSvc)
	require.Eventually(t, func() bool {
		_, ok := measureSvc.LoadGroup(idxrGroup)
		return ok
	}, 30*time.Second, 200*time.Millisecond, "measure group not loaded")
	time.Sleep(time.Second)

	const total = 2
	baseTS := time.Now().Truncate(time.Minute)
	// Keep all `total` points (one minute apart) inside a single daily segment.
	// Near local midnight the default span would cross the day boundary and split
	// the series across two segments, which breaks the single-segment resolver
	// below; pull the window back so it ends before midnight.
	midnight := time.Date(baseTS.Year(), baseTS.Month(), baseTS.Day(), 0, 0, 0, 0, baseTS.Location()).Add(24 * time.Hour)
	if span := time.Duration(total) * time.Minute; baseTS.Add(span).After(midnight) {
		baseTS = midnight.Add(-span - time.Minute)
	}
	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i := 0; i < total; i++ {
		iStr := strconv.Itoa(i)
		ts := baseTS.Add(time.Duration(i) * time.Minute)
		writeReq := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: idxrMeasure, Group: idxrGroup},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(ts),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						strTagValue("series" + iStr),                   // series (entity)
						strTagValue("plain" + iStr),                    // strTag (plain column)
						strTagValue("indexed" + iStr),                  // idxStr (indexed)
						intTagValue(int64(70 + i)),                     // idxInt (indexed)
						strArrTagValue("arr"+iStr+"a", "arr"+iStr+"b"), // idxArr (indexed)
					},
				}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(100 + i)}}},
				},
			},
		}
		_, errPub := bp.Publish(context.TODO(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(i), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{strTagValue("series" + iStr)},
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

	// Resolve each rule's assigned IndexRuleID while the schema server is still
	// up, so we can later assert per-rule that multiple rules on one row stay
	// separate (each under its own id).
	reg := metaSvc.SchemaRegistry()
	ruleID := func(name string) uint32 {
		ir, gErr := reg.GetIndexRule(context.TODO(), &commonv1.Metadata{Name: name, Group: idxrGroup})
		req.NoError(gErr)
		return ir.GetMetadata().GetId()
	}
	strRuleID, intRuleID, arrRuleID := ruleID("idxr_str_rule"), ruleID("idxr_int_rule"), ruleID("idxr_arr_rule")

	// Stop the live service so it releases bluge's exclusive lock on the series
	// index; the dump (like the offline CLI) reads the index from a quiesced
	// database. The write path above is synchronous (safe-batch insert blocks
	// until the series index is persisted), so the index is already durable on
	// disk before this stop. Deferred cleanup is suppressed via the stopped flag.
	moduleDefer()
	stopped = true

	segmentPath := findSidxSegmentPath(t, rootPath)

	ruleToTag := map[uint32]dump.IndexedTagSpec{
		strRuleID: {Family: "default", Name: "idxStr", Type: pbv1.ValueTypeStr},
		intRuleID: {Family: "default", Name: "idxInt", Type: pbv1.ValueTypeInt64},
		arrRuleID: {Family: "default", Name: "idxArr", Type: pbv1.ValueTypeStrArr},
	}
	// The standalone write path persists no part-level smeta.bin, so the resolver
	// recovers EntityValues for each part by scanning the series index scoped to
	// that part (PartSeriesMap) — the per-part bounded fallback, no segment-wide
	// map.
	resolver, err := dump.NewIndexResolver(segmentPath, 0, ruleToTag)
	req.NoError(err)
	// Closed explicitly before the smeta-path phase reopens the same index
	// (bluge holds an exclusive lock, so only one store may be open at a time).

	fileSystem := localfs.NewLocalFileSystem()
	seen := 0
	entityBySeries := map[common.SeriesID]string{}
	partSeriesByDir := map[string]map[common.SeriesID][]byte{}
	for _, partDir := range partDirs {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		p, openErr := OpenPart(partID, filepath.Dir(partDir), fileSystem)
		req.NoError(openErr)
		// Standalone parts carry no smeta.bin, so the iterator must take the
		// per-part fallback scan to recover EntityValues.
		req.Nil(p.SeriesMap(), "standalone part has no smeta.bin")
		p.SetIndexResolver(resolver)
		curSeries := map[common.SeriesID][]byte{}
		it := p.Iterator()
		for it.Next() {
			r := it.Row()
			seen++
			// Identify the row by its plain (column) tag, e.g. "plain1" -> n=1.
			plain := dump.DecodeTagValue(r.TagTypes["default.strTag"], r.Tags["default.strTag"], nil).GetStr().GetValue()
			n, atoiErr := strconv.Atoi(plain[len("plain"):])
			req.NoError(atoiErr)
			iStr := strconv.Itoa(n)
			// Indexed and entity tags are NOT stored as data columns; the plain
			// tag and the field are.
			req.NotContains(r.Tags, "default.idxStr")
			req.NotContains(r.Tags, "default.idxInt")
			req.NotContains(r.Tags, "default.idxArr")
			req.NotContains(r.Tags, "default.series")
			req.Contains(r.Fields, "intField")
			// No part-level smeta exists (standalone write path), so EntityValues
			// here proves the per-part fallback scan recovered it; the entity tag
			// value is part of the encoded EntityValues.
			req.NotEmpty(r.EntityValues, "fallback scan must recover EntityValues")
			req.Contains(string(r.EntityValues), "series"+iStr)
			entityBySeries[r.SeriesID] = string(r.EntityValues)
			curSeries[r.SeriesID] = append([]byte(nil), r.EntityValues...)
			// All three rules' values are recovered, each under its own
			// IndexRuleID (multiple rules on one row stay separate, not merged).
			req.Len(r.IndexedTags, 3, "all three index rules resolved")
			strVals := r.IndexedTags[strRuleID]
			req.Len(strVals, 1, "scalar string indexed tag -> one value")
			req.Equal("indexed"+iStr, string(strVals[0]))
			intVals := r.IndexedTags[intRuleID]
			req.Len(intVals, 1, "scalar int indexed tag -> one value")
			req.Equal(convert.Int64ToBytes(int64(70+n)), intVals[0])
			arrVals := r.IndexedTags[arrRuleID]
			req.Len(arrVals, 2, "array indexed tag -> two values")
			arrSet := map[string]bool{string(arrVals[0]): true, string(arrVals[1]): true}
			req.True(arrSet["arr"+iStr+"a"] && arrSet["arr"+iStr+"b"], "both array elements recovered")
			// With the rule->tag mapping, the row's already-resolved IndexedTags
			// decode to typed TagValues keyed by "family.tag".
			named := resolver.DecodeTagValues(r.IndexedTags)
			req.Equal("indexed"+iStr, named["default.idxStr"].GetStr().GetValue())
			req.Equal(int64(70+n), named["default.idxInt"].GetInt().GetValue())
			req.ElementsMatch([]string{"arr" + iStr + "a", "arr" + iStr + "b"}, named["default.idxArr"].GetStrArray().GetValue())
		}
		req.NoError(it.Err())
		it.Close()
		p.Close()
		partSeriesByDir[partDir] = curSeries
	}
	req.Equal(total, seen, "every data point should be read back")
	req.Len(entityBySeries, total, "each entity maps to a distinct series")

	// Direct coverage of the bounded per-part scan: it returns EntityValues only
	// for the requested series, and nothing for unknown series.
	allIDs := make(map[common.SeriesID]struct{}, len(entityBySeries))
	for sid := range entityBySeries {
		allIDs[sid] = struct{}{}
	}
	full, err := resolver.PartSeriesMap(allIDs)
	req.NoError(err)
	req.Len(full, len(entityBySeries), "scan returns exactly the requested series")
	for sid, ev := range entityBySeries {
		req.Equal(ev, string(full[sid]))
	}

	var oneID common.SeriesID
	for sid := range allIDs {
		oneID = sid
		break
	}
	subset, err := resolver.PartSeriesMap(map[common.SeriesID]struct{}{oneID: {}})
	req.NoError(err)
	req.Len(subset, 1, "a subset request stays scoped to that subset")
	req.Equal(entityBySeries[oneID], string(subset[oneID]))

	bogus, err := resolver.PartSeriesMap(map[common.SeriesID]struct{}{common.SeriesID(0xdeadbeef): {}})
	req.NoError(err)
	req.Empty(bogus, "an unknown series resolves to nothing")

	// Distributed write-path coverage: a part produced by the data-node path
	// carries a part-level smeta.bin, so OpenPart loads its series map directly
	// and the iterator skips the index scan. Simulate that by writing a real
	// smeta.bin per part, then reopen with a fresh resolver (empty cache) so the
	// indexed tags are genuinely re-resolved from the smeta-provided EntityValues.
	req.NoError(resolver.Close())
	smetaResolver, err := dump.NewIndexResolver(segmentPath, 0, ruleToTag)
	req.NoError(err)
	defer smetaResolver.Close()
	smetaSeen := 0
	for partDir, series := range partSeriesByDir {
		partID, parseErr := strconv.ParseUint(filepath.Base(partDir), 16, 64)
		req.NoError(parseErr)
		docs := make(index.Documents, 0, len(series))
		for _, entityValues := range series {
			docs = append(docs, index.Document{EntityValues: entityValues})
		}
		smeta, marshalErr := docs.Marshal()
		req.NoError(marshalErr)
		req.NoError(os.WriteFile(filepath.Join(partDir, "smeta.bin"), smeta, 0o600))

		p, openErr := OpenPart(partID, filepath.Dir(partDir), fileSystem)
		req.NoError(openErr)
		req.NotNil(p.SeriesMap(), "smeta.bin present -> series map loaded, index scan skipped")
		p.SetIndexResolver(smetaResolver)
		it := p.Iterator()
		for it.Next() {
			r := it.Row()
			smetaSeen++
			req.Equal(series[r.SeriesID], r.EntityValues, "EntityValues sourced from smeta.bin")
			plain := dump.DecodeTagValue(r.TagTypes["default.strTag"], r.Tags["default.strTag"], nil).GetStr().GetValue()
			n, atoiErr := strconv.Atoi(plain[len("plain"):])
			req.NoError(atoiErr)
			iStr := strconv.Itoa(n)
			req.Len(r.IndexedTags, 3, "indexed tags still resolve on the smeta fast path")
			req.Equal("indexed"+iStr, string(r.IndexedTags[strRuleID][0]))
			req.Equal(convert.Int64ToBytes(int64(70+n)), r.IndexedTags[intRuleID][0])
			arrVals := r.IndexedTags[arrRuleID]
			req.Len(arrVals, 2)
			arrSet := map[string]bool{string(arrVals[0]): true, string(arrVals[1]): true}
			req.True(arrSet["arr"+iStr+"a"] && arrSet["arr"+iStr+"b"], "both array elements recovered")
		}
		req.NoError(it.Err())
		it.Close()
		p.Close()
	}
	req.Equal(total, smetaSeen, "every data point read back on the smeta path too")
}

func registerIndexedResolveMeasure(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.TODO()
	_, err := reg.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: idxrGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: idxrMeasure, Group: idxrGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "series", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "strTag", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "idxStr", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "idxInt", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "idxArr", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			},
		}},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "intField",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{TagNames: []string{"series"}},
	})
	require.NoError(t, err)
	for _, ir := range []struct {
		name string
		tag  string
	}{
		{"idxr_str_rule", "idxStr"},
		{"idxr_int_rule", "idxInt"},
		{"idxr_arr_rule", "idxArr"},
	} {
		_, err = reg.CreateIndexRule(ctx, &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: ir.name, Group: idxrGroup},
			Tags:     []string{ir.tag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		})
		require.NoError(t, err)
	}
	_, err = reg.CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "idxr_binding", Group: idxrGroup},
		Subject:  &databasev1.Subject{Name: idxrMeasure, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		Rules:    []string{"idxr_str_rule", "idxr_int_rule", "idxr_arr_rule"},
		BeginAt:  timestamppb.New(time.Now().Add(-time.Hour)),
		ExpireAt: timestamppb.New(time.Now().Add(24 * time.Hour)),
	})
	require.NoError(t, err)
}

func strTagValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func intTagValue(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func strArrTagValue(vals ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: vals}}}
}

// findSidxSegmentPath returns the segment directory whose series index (sidx)
// actually holds the written series. When the data lands near a segment-interval
// boundary an empty adjacent (e.g. next-day) segment dir can also exist, so
// picking the lexically-last sidx is wrong -- it can select the empty segment and
// make later reads see zero docs. Choose the sidx with the most documents.
func findSidxSegmentPath(t *testing.T, root string) string {
	t.Helper()
	var seg, candidates string
	best := int64(-1)
	count := 0
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && d.Name() == "sidx" {
			c, e := inverted.ReadOnlyDocCount(p)
			candidates += fmt.Sprintf("{path=%s count=%d err=%v} ", p, c, e)
			count++
			if c > best {
				best = c
				seg = filepath.Dir(p)
			}
		}
		return nil
	})
	require.NotEmpty(t, seg, "sidx directory not found under %s", root)
	// Record the selection: near a segment-interval boundary more than one sidx
	// dir can exist (an empty adjacent segment), so logging the candidates and the
	// chosen one makes any future mis-selection self-evident.
	logger.GetLogger("dump-measure-test").Info().
		Int("candidates", count).Str("chosen", seg).Int64("chosenDocs", best).
		Msgf("findSidxSegmentPath selected series-index segment among: %s", candidates)
	return seg
}
