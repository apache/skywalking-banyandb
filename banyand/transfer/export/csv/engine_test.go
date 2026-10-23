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

package csv

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

const (
	engStreamGroup  = "exp_stream"
	engMeasureGroup = "exp_measure"
	engTraceGroup   = "exp_trace"
)

// indexModeWalker reads index-mode measure rows the way the measure backend exposes them
// to the export unit.
type indexModeWalker struct{}

func (indexModeWalker) ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error,
) error {
	return measure.WalkIndexModeRows(ctx, sidxDir, measures, rules, func(r *measure.IndexModeRow) error {
		return visit(r.Subject, r.Timestamp, r.Version, r.Tags, r.UnknownFields)
	})
}

// unmergeableTopN is a Top-N result value the fake merger refuses, like a corrupt one.
const unmergeableTopN = "unmergeable"

// MergeTopNResults stands in for the measure backend's merge of Top-N result versions: the
// test values are not engine encodings, so it joins them, the latest first.
func (indexModeWalker) MergeTopNResults(entity []*modelv1.TagValue, _, _, _ int64, left, right []byte) ([]byte, error) {
	if len(entity) != 4 {
		return nil, fmt.Errorf("unexpected Top-N entity %v", entity)
	}
	if string(left) == unmergeableTopN || string(right) == unmergeableTopN {
		return nil, errors.New("cannot merge a corrupt Top-N result")
	}
	return []byte(string(left) + "+" + string(right)), nil
}

// snapshotter is the part of a storage service that takes export snapshots.
type snapshotter interface {
	GetExportSnapshotDir() string
	TakeExportSnapshot(ctx context.Context, name string, groups []string) error
}

// engineGroup is a group of a storage service.
type engineGroup struct {
	svc     snapshotter
	name    string
	catalog commonv1.Catalog
}

// producedUnit is one segment unit and what producing it returned.
type producedUnit struct {
	unit     *transferv1.SegmentUnit
	files    []*exportedFile
	warnings []string
}

func strTV(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func intTV(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func dayGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  catalog,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

func registerEngineSchemas(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.Background()
	for _, g := range []*commonv1.Group{
		dayGroup(engStreamGroup, commonv1.Catalog_CATALOG_STREAM),
		dayGroup(engMeasureGroup, commonv1.Catalog_CATALOG_MEASURE),
		dayGroup(engTraceGroup, commonv1.Catalog_CATALOG_TRACE),
	} {
		_, err := reg.CreateGroup(ctx, g)
		require.NoError(t, err)
	}
	str := databasev1.TagType_TAG_TYPE_STRING
	_, err := reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "log", Group: engStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: str}, {Name: "msg", Type: str}, {Name: "code", Type: databasev1.TagType_TAG_TYPE_INT}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "event", Group: engStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: str}, {Name: "kind", Type: str}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)

	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "cpm", Group: engMeasureGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: str},
				{Name: "plain", Type: str},
				{Name: "layer", Type: str},
				{Name: "zone", Type: str},
				{Name: "rack", Type: str},
			},
		}},
		Fields: []*databasev1.FieldSpec{
			{
				Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
			{
				Name: "ratio", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "traffic", Group: engMeasureGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "id", Type: str}, {Name: "svc_name", Type: str}},
		}},
		Entity:    &databasev1.Entity{TagNames: []string{"id"}},
		IndexMode: true,
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRule(ctx, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "layer_rule", Group: engMeasureGroup},
		Tags:     []string{"layer"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	})
	require.NoError(t, err)
	// zone and rack share one multi-tag rule: the write path stores both values under its
	// id only, so they cannot be exported.
	_, err = reg.CreateIndexRule(ctx, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "zone_rack", Group: engMeasureGroup},
		Tags:     []string{"zone", "rack"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	})
	require.NoError(t, err)
	_, err = reg.CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "cpm_binding", Group: engMeasureGroup},
		Subject:  &databasev1.Subject{Name: "cpm", Catalog: commonv1.Catalog_CATALOG_MEASURE},
		Rules:    []string{"layer_rule", "zone_rack"},
		BeginAt:  timestamppb.New(time.Now().Add(-time.Hour)),
		ExpireAt: timestamppb.New(time.Now().Add(24 * time.Hour)),
	})
	require.NoError(t, err)

	traceTags := func(extra ...*databasev1.TraceTagSpec) []*databasev1.TraceTagSpec {
		return append([]*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: str},
			{Name: "span_id", Type: str},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		}, extra...)
	}
	for name, extra := range map[string][]*databasev1.TraceTagSpec{
		"segment": {{Name: "service", Type: str}},
		"zipkin":  {{Name: "endpoint", Type: str}, {Name: "latency", Type: databasev1.TagType_TAG_TYPE_INT}},
	} {
		_, err = reg.CreateTrace(ctx, &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: name, Group: engTraceGroup},
			Tags:     traceTags(extra...), TraceIdTagName: "trace_id", SpanIdTagName: "span_id", TimestampTagName: "timestamp",
		})
		require.NoError(t, err)
	}
}

// unitWarnings collects the warnings of units, without the empty-unit one: near the end of a
// segment the engine creates the next, still empty, segment ahead of time.
func unitWarnings(units []*producedUnit) []string {
	var out []string
	for _, u := range units {
		for _, w := range u.warnings {
			if w != "unit has no rows" {
				out = append(out, w)
			}
		}
	}
	return out
}

// topNPoint is a version of the data point of the Top-N aggregation name in the group's
// internal _top_n_result measure.
func topNPoint(base time.Time, name string, version int64, value string) *measurev1.InternalWriteRequest {
	topNTags := []*modelv1.TagValue{strTV(name), intTV(1), strTV(engMeasureGroup), strTV("{}")}
	return &measurev1.InternalWriteRequest{
		EntityValues: topNTags,
		Request: &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: csvfmt.TopNResultMeasure, Group: engMeasureGroup},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(base),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: topNTags}},
				Fields:      []*modelv1.FieldValue{{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte(value)}}},
				Version:     version,
			},
		},
	}
}

// writeSecondBatch writes an older version of every cpm data point writeEngineData wrote and an
// older version of its Top-N result. Sent once the first points are flushed, they land in a
// part of their own.
func writeSecondBatch(t *testing.T, pipeline queue.Queue, base time.Time) {
	bp := pipeline.NewBatchPublisher(5 * time.Second)
	_, err := bp.Publish(context.Background(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(999), topNPoint(base, "cpm_top", 1, "older")))
	require.NoError(t, err)
	_, err = bp.Publish(context.Background(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(998), topNPoint(base, "cpm_bad", 1, unmergeableTopN)))
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		svc := "svc" + strconv.Itoa(i)
		_, err = bp.Publish(context.Background(), data.TopicMeasureWrite, bus.NewMessage(bus.MessageID(1000+i), &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{strTV(svc)},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "cpm", Group: engMeasureGroup},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(base.Add(time.Duration(i) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						strTV(svc), strTV("stale"), strTV("GENERAL"), strTV("zone"), strTV("rack"),
					}}},
					Fields: []*modelv1.FieldValue{
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 900}}},
						{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 9.5}}},
					},
					Version: int64(3 + i),
				},
			},
		}))
		require.NoError(t, err)
	}
	errs, err := bp.Close()
	require.NoError(t, err)
	for _, e := range errs {
		require.NoError(t, e)
	}
}

func writeEngineData(t *testing.T, pipeline queue.Queue, base time.Time) {
	// A local batch publisher sends everything to the topic of its first message. Index-mode
	// points also get a batch of their own: the measure write path panics when one batch
	// mixes normal and index-mode points of the same shard and segment.
	publishers := map[string]queue.BatchPublisher{}
	var order []string
	ctx := context.Background()
	id := 0
	publishTo := func(key string, topic bus.Topic, msg any) {
		id++
		bp, ok := publishers[key]
		if !ok {
			bp = pipeline.NewBatchPublisher(5 * time.Second)
			publishers[key] = bp
			order = append(order, key)
		}
		_, err := bp.Publish(ctx, topic, bus.NewMessage(bus.MessageID(id), msg))
		require.NoError(t, err)
	}
	publish := func(topic bus.Topic, msg any) { publishTo(topic.String(), topic, msg) }
	for i := 0; i < 3; i++ {
		ts := timestamppb.New(base.Add(time.Duration(i) * time.Second))
		svc := "svc" + strconv.Itoa(i)
		publish(data.TopicStreamWrite, &streamv1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{strTV(svc)},
			Request: &streamv1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "log", Group: engStreamGroup},
				Element: &streamv1.ElementValue{
					Timestamp: ts, ElementId: "log" + strconv.Itoa(i),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTV(svc), strTV(`\N,` + svc), intTV(int64(200 + i))}}},
				},
			},
		})
		publish(data.TopicMeasureWrite, &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{strTV(svc)},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "cpm", Group: engMeasureGroup},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: ts,
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						strTV(svc), strTV("plain" + strconv.Itoa(i)), strTV("GENERAL"), strTV("zone" + strconv.Itoa(i)), strTV("rack" + strconv.Itoa(i)),
					}}},
					Fields: []*modelv1.FieldValue{
						{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(100 + i)}}},
						{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(i) + 0.25}}},
					},
					Version: int64(7 + i),
				},
			},
		})
	}
	publish(data.TopicMeasureWrite, topNPoint(base, "cpm_top", 2, "latest"))
	publish(data.TopicMeasureWrite, topNPoint(base, "cpm_bad", 2, "bad-latest"))
	publish(data.TopicStreamWrite, &streamv1.InternalWriteRequest{
		EntityValues: []*modelv1.TagValue{strTV("svc9")},
		Request: &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: "event", Group: engStreamGroup},
			Element: &streamv1.ElementValue{
				Timestamp: timestamppb.New(base), ElementId: "event0",
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTV("svc9"), strTV("")}}},
			},
		},
	})
	for i := 0; i < 2; i++ {
		publishTo("index-mode", data.TopicMeasureWrite, &measurev1.InternalWriteRequest{
			EntityValues: []*modelv1.TagValue{strTV("id" + strconv.Itoa(i))},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "traffic", Group: engMeasureGroup},
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(base),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTV("id" + strconv.Itoa(i)), strTV("name" + strconv.Itoa(i))}}},
					Version:     1,
				},
			},
		})
	}
	tsTag := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base)}}
	publish(data.TopicTraceWrite, &tracev1.InternalWriteRequest{Request: &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: "segment", Group: engTraceGroup},
		Tags:     []*modelv1.TagValue{strTV("t1"), strTV("s1"), tsTag, strTV("checkout")},
		Span:     []byte("span-one"),
		TagSpec:  &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "service"}},
		Version:  1,
	}})
	publish(data.TopicTraceWrite, &tracev1.InternalWriteRequest{Request: &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: "zipkin", Group: engTraceGroup},
		Tags:     []*modelv1.TagValue{strTV("t2"), strTV("s2"), tsTag, strTV(""), intTV(15)},
		Span:     []byte("span-two"),
		TagSpec:  &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "endpoint", "latency"}},
		Version:  1,
	}})
	for _, key := range order {
		errs, err := publishers[key].Close()
		require.NoError(t, err)
		require.Empty(t, errs)
	}
}

// produceAll snapshots every group's service as name and produces each segment of
// it as one unit holding every shard; it returns the units per catalog.
func produceAll(t *testing.T, meta metadata.Repo, groups []engineGroup, name string) map[commonv1.Catalog][]*producedUnit {
	t.Helper()
	ctx := context.Background()
	out := map[commonv1.Catalog][]*producedUnit{}
	for _, g := range groups {
		catalog := g.catalog
		root := filepath.Join(g.svc.GetExportSnapshotDir(), name)
		require.NoError(t, os.MkdirAll(root, 0o755))
		require.NoError(t, g.svc.TakeExportSnapshot(ctx, name, []string{g.name}))
		before := hashTree(t, root)
		groupDir := filepath.Join(root, g.name)
		segs, err := os.ReadDir(groupDir)
		require.NoError(t, err)
		for _, seg := range segs {
			if !seg.IsDir() || !strings.HasPrefix(seg.Name(), segDirPrefix) {
				continue
			}
			unit := &transferv1.SegmentUnit{Catalog: catalog, Group: g.name, SegmentSuffix: strings.TrimPrefix(seg.Name(), segDirPrefix)}
			segDir := filepath.Join(groupDir, seg.Name())
			shards, readErr := os.ReadDir(segDir)
			require.NoError(t, readErr)
			for _, sh := range shards {
				if id, ok := strings.CutPrefix(sh.Name(), shardDirPrefix); ok && sh.IsDir() {
					n, parseErr := strconv.ParseUint(id, 10, 32)
					require.NoError(t, parseErr)
					// The Plan names only the shards holding parts.
					partIDs, partsErr := LivePartIDs(filepath.Join(segDir, sh.Name()))
					require.NoError(t, partsErr)
					if len(partIDs) > 0 {
						unit.ShardIds = append(unit.ShardIds, uint32(n))
					}
				}
			}
			var sent []*transferv1.ExportResponse
			p := NewProducer(ctx, meta, indexModeWalker{}, &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_GZIP},
				func(r *transferv1.ExportResponse) error {
					sent = append(sent, r)
					return nil
				})
			warnings, rows, produceErr := p.Segment(unit, segDir)
			require.NoError(t, produceErr)
			files := decodeFiles(t, sent)
			var counted uint64
			for _, f := range files {
				for _, n := range f.finished.GetRowCounts() {
					counted += n
				}
			}
			require.Equal(t, counted, rows)
			out[catalog] = append(out[catalog], &producedUnit{unit: unit, files: files, warnings: warnings})
		}
		require.Equal(t, before, hashTree(t, root), "producing must not change the %s snapshot", catalog)
	}
	return out
}

// allRows decodes every file of the units.
func allRows(t *testing.T, units []*producedUnit) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, u := range units {
		for _, f := range u.files {
			out = append(out, csvRows(t, f)...)
		}
	}
	return out
}

func rowsByName(rows []map[string]string) map[string][]map[string]string {
	out := map[string][]map[string]string{}
	for _, r := range rows {
		out[r["_name"]] = append(out[r["_name"]], r)
	}
	return out
}

func TestProducer_RealEngineData(t *testing.T) {
	req := require.New(t)
	req.NoError(logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })

	pipeline := queue.Local()
	metaSvc, err := metadataservice.NewService()
	req.NoError(err)
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	streamSvc, err := stream.NewService(metaSvc, pipeline, metricSvc, pm, nil)
	req.NoError(err)
	measureSvc, err := measure.NewStandalone(metaSvc, pipeline, nil, metricSvc, pm)
	req.NoError(err)
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
	moduleDefer := test.SetupModules([]string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--stream-root-path=" + rootPath, "--stream-flush-timeout=200ms",
		"--measure-root-path=" + rootPath, "--measure-flush-timeout=200ms",
		"--trace-root-path=" + rootPath, "--trace-flush-timeout=200ms",
	}, pipeline, metaSvc, streamSvc, measureSvc, traceSvc)
	defer moduleDefer()

	registerEngineSchemas(t, metaSvc)
	req.Eventually(func() bool {
		_, okStream := streamSvc.LoadGroup(engStreamGroup)
		_, okMeasure := measureSvc.LoadGroup(engMeasureGroup)
		_, okTrace := traceSvc.LoadGroup(engTraceGroup)
		return okStream && okMeasure && okTrace
	}, 30*time.Second, 200*time.Millisecond, "groups not loaded")
	// Writes before a resource or its index rules are loaded would be lost or stored as
	// columns: wait until every resource is served with its rules.
	req.Eventually(func() bool {
		for _, name := range []string{"log", "event"} {
			if _, loadErr := streamSvc.Stream(&commonv1.Metadata{Name: name, Group: engStreamGroup}); loadErr != nil {
				return false
			}
		}
		for _, name := range []string{"segment", "zipkin"} {
			if _, loadErr := traceSvc.Trace(&commonv1.Metadata{Name: name, Group: engTraceGroup}); loadErr != nil {
				return false
			}
		}
		if _, loadErr := measureSvc.Measure(&commonv1.Metadata{Name: "traffic", Group: engMeasureGroup}); loadErr != nil {
			return false
		}
		cpm, loadErr := measureSvc.Measure(&commonv1.Metadata{Name: "cpm", Group: engMeasureGroup})
		return loadErr == nil && len(cpm.GetIndexRules()) == 2
	}, 30*time.Second, 100*time.Millisecond, "resources not loaded")
	// Keep every point inside one daily segment.
	base := time.Now().Truncate(time.Hour)
	writeEngineData(t, pipeline, base)

	groups := []engineGroup{
		{catalog: commonv1.Catalog_CATALOG_STREAM, name: engStreamGroup, svc: streamSvc.(snapshotter)},
		{catalog: commonv1.Catalog_CATALOG_MEASURE, name: engMeasureGroup, svc: measureSvc.(snapshotter)},
		{catalog: commonv1.Catalog_CATALOG_TRACE, name: engTraceGroup, svc: traceSvc.(snapshotter)},
	}

	// Parts flush asynchronously: produce until every written row is there.
	var units map[commonv1.Catalog][]*producedUnit
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		units = produceAll(t, metaSvc, groups, fmt.Sprintf("%04x", attempt))
		streamRows := len(allRows(t, units[commonv1.Catalog_CATALOG_STREAM]))
		measureRows := len(allRows(t, units[commonv1.Catalog_CATALOG_MEASURE]))
		traceRows := len(allRows(t, units[commonv1.Catalog_CATALOG_TRACE]))
		if streamRows == 4 && measureRows == 7 && traceRows == 2 {
			break
		}
		req.True(time.Now().Before(deadline), "not every written row was exported: stream %d, measure %d, trace %d",
			streamRows, measureRows, traceRows)
		time.Sleep(500 * time.Millisecond)
	}
	// Older versions of the cpm points and of the Top-N result in a part of their own: a query
	// returns the latest version of a data point, and so must the export, except for Top-N
	// results, whose versions a query merges. Produce until a snapshot holds the second part.
	writeSecondBatch(t, pipeline, base)
	for attempt := 1; ; attempt++ {
		name := fmt.Sprintf("stale%04x", attempt)
		units[commonv1.Catalog_CATALOG_MEASURE] = produceAll(t, metaSvc, groups[1:2], name)[commonv1.Catalog_CATALOG_MEASURE]
		parts, globErr := filepath.Glob(filepath.Join(rootPath, "measure", "export-snapshots", name, engMeasureGroup,
			segDirPrefix+"*", "shard-*", strings.Repeat("[0-9a-f]", 16)))
		req.NoError(globErr)
		if len(parts) > 1 {
			break
		}
		req.True(time.Now().Before(deadline.Add(30*time.Second)), "the stale versions never reached a snapshot")
		time.Sleep(500 * time.Millisecond)
	}

	t.Run("stream", func(t *testing.T) {
		byName := rowsByName(allRows(t, units[commonv1.Catalog_CATALOG_STREAM]))
		require.Len(t, byName["log"], 3)
		require.Len(t, byName["event"], 1)
		for _, row := range byName["log"] {
			svc := row["searchable.svc"]
			require.Regexp(t, `^svc[0-2]$`, svc, "the entity tag is filled from the series")
			require.Equal(t, `\\N,`+svc, row["searchable.msg"], "a literal leading backslash is escaped")
			require.Equal(t, strconv.Itoa(200+int(svc[3]-'0')), row["searchable.code"])
			require.Equal(t, `\N`, row["searchable.kind"], "a column of the other stream is null")
			wantID := hex.EncodeToString(convert.Uint64ToBytes(convert.HashStr(engStreamGroup + "|log|log" + svc[3:])))
			require.Equal(t, wantID, row["_element_id"], "the element id is the hex form a stream query returns")
			require.Equal(t, csvfmt.EncodeTimestamp(base.Add(time.Duration(svc[3]-'0')*time.Second).UnixNano()), row["_timestamp"])
		}
		require.Equal(t, "svc9", byName["event"][0]["searchable.svc"])
		require.Equal(t, "", byName["event"][0]["searchable.kind"], "an empty string is not null")
		require.Empty(t, unitWarnings(units[commonv1.Catalog_CATALOG_STREAM]))
		for _, u := range units[commonv1.Catalog_CATALOG_STREAM] {
			for _, f := range u.files {
				require.Regexp(t, `^stream/exp_stream/seg-[0-9]{8}/shard-0-00001\.csv\.gz$`, f.started.GetRelativePath())
				require.False(t, f.started.GetSegmentLevel())
			}
		}
	})

	t.Run("measure", func(t *testing.T) {
		var segmentFiles, shardFiles int
		var rows []map[string]string
		for _, u := range units[commonv1.Catalog_CATALOG_MEASURE] {
			for _, f := range u.files {
				if f.started.GetSegmentLevel() {
					segmentFiles++
					require.Regexp(t, `^measure/exp_measure/seg-[0-9]{8}/segment-00001\.csv\.gz$`, f.started.GetRelativePath())
					require.Equal(t, uint32(len(u.unit.GetShardIds())), f.started.GetShardIndex())
					require.Equal(t, map[string]uint64{"traffic": 2}, f.finished.GetRowCounts())
				} else {
					shardFiles++
				}
				rows = append(rows, csvRows(t, f)...)
			}
		}
		require.Equal(t, 1, segmentFiles, "index-mode rows go to one segment-level file")
		require.NotZero(t, shardFiles)
		byName := rowsByName(rows)
		require.Len(t, byName["cpm"], 3, "one row per data point, its latest version")
		for _, row := range byName["cpm"] {
			require.Regexp(t, `^svc[0-2]$`, row["default.svc"], "entity tag from the series")
			require.Equal(t, "GENERAL", row["default.layer"], "indexed tag from the series index")
			n := row["default.svc"][3] - '0'
			require.Equal(t, "plain"+string(n+'0'), row["default.plain"])
			// The Top-N result declares a binary "value" field, which splits the column.
			require.Equal(t, strconv.Itoa(100+int(n)), row["cpm::_field.value"])
			require.Equal(t, `\N`, row["_top_n_result::_field.value"])
			require.Equal(t, csvfmt.EncodeFloat(float64(n)+0.25), row["_field.ratio"])
			require.Equal(t, strconv.Itoa(7+int(n)), row["_version"])
			require.Equal(t, `\N`, row["default.zone"], "only the multi-tag rule stores zone")
			require.Equal(t, `\N`, row["default.rack"], "only the multi-tag rule stores rack")
		}
		require.Equal(t, []string{
			"measure _top_n_result: 1 data points kept their highest version because their versions could not be merged",
			"measure cpm: tags [zone rack] are indexed by multi-tag rule zone_rack, which may hold their values; such values cannot be exported and their cells are null",
		}, unitWarnings(units[commonv1.Catalog_CATALOG_MEASURE]), "only the Top-N result that does not merge warns")
		topNByName := map[string]map[string]string{}
		for _, row := range byName[csvfmt.TopNResultMeasure] {
			topNByName[row["_topN.name"]] = row
		}
		require.Len(t, byName[csvfmt.TopNResultMeasure], 2)
		require.Len(t, topNByName, 2, "one row per Top-N data point")
		bad := topNByName["cpm_bad"]
		require.Equal(t, csvfmt.EncodeBinary([]byte("bad-latest")), bad["_top_n_result::_field.value"], "a version that does not merge keeps the latest")
		require.Equal(t, "2", bad["_version"])
		topN := topNByName["cpm_top"]
		require.Equal(t, "cpm_top", topN["_topN.name"])
		require.Equal(t, "1", topN["_topN.direction"])
		require.Equal(t, engMeasureGroup, topN["_topN.group"])
		require.Equal(t, "{}", topN["_topN.parameters"])
		require.Equal(t, csvfmt.EncodeBinary([]byte("latest+older")), topN["_top_n_result::_field.value"], "the versions are merged")
		require.Equal(t, `\N`, topN["cpm::_field.value"])
		require.Equal(t, "2", topN["_version"])
		require.Equal(t, csvfmt.EncodeTimestamp(base.UnixNano()), topN["_timestamp"])
		require.Len(t, byName["traffic"], 2)
		for _, row := range byName["traffic"] {
			require.Regexp(t, `^id[01]$`, row["default.id"])
			require.Equal(t, "name"+row["default.id"][2:], row["default.svc_name"])
			require.Equal(t, `\N`, row["cpm::_field.value"], "index-mode measures have no fields")
			require.Equal(t, `\N`, row["_top_n_result::_field.value"], "index-mode measures have no fields")
		}
	})

	t.Run("trace", func(t *testing.T) {
		byName := rowsByName(allRows(t, units[commonv1.Catalog_CATALOG_TRACE]))
		require.Len(t, byName["segment"], 1)
		require.Len(t, byName["zipkin"], 1)
		seg := byName["segment"][0]
		require.Equal(t, "t1", seg["trace_id"])
		require.Equal(t, "s1", seg["span_id"])
		require.Equal(t, csvfmt.EncodeBinary([]byte("span-one")), seg["_span"])
		require.Equal(t, "checkout", seg["service"])
		require.Equal(t, `\N`, seg["endpoint"])
		require.Equal(t, csvfmt.EncodeTimestamp(base.UnixNano()), seg["timestamp"])
		zip := byName["zipkin"][0]
		require.Equal(t, "", zip["endpoint"], "an empty string is not null")
		require.Equal(t, "15", zip["latency"])
		require.Equal(t, `\N`, zip["service"])
		require.Empty(t, unitWarnings(units[commonv1.Catalog_CATALOG_TRACE]))
	})
}
