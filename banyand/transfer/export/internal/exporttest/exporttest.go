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

// Package exporttest holds the test fixtures of the export tests: standalone storage
// engines with sharded groups, the writes the tests make into them, and snapshots of them.
package exporttest

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
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// IndexModeWalker reads index-mode measure rows the way the measure backend exposes them
// to the export unit.
type IndexModeWalker struct{}

// ExportIndexModeRows implements rowsrc.IndexModeReader with the measure backend's walk.
func (IndexModeWalker) ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error,
) error {
	return measure.WalkIndexModeRows(ctx, sidxDir, measures, rules, func(r *measure.IndexModeRow) error {
		return visit(r.Subject, r.Timestamp, r.Version, r.Tags, r.UnknownFields)
	})
}

// UnmergeableTopN is a Top-N result value the fake merger refuses, like a corrupt one.
const UnmergeableTopN = "unmergeable"

// MergeTopNResults stands in for the measure backend's merge of Top-N result versions: the
// test values are not engine encodings, so it joins them, the latest first.
func (IndexModeWalker) MergeTopNResults(entity []*modelv1.TagValue, _, _, _ int64, left, right []byte) ([]byte, error) {
	if len(entity) != 4 {
		return nil, fmt.Errorf("unexpected Top-N entity %v", entity)
	}
	if string(left) == UnmergeableTopN || string(right) == UnmergeableTopN {
		return nil, errors.New("cannot merge a corrupt Top-N result")
	}
	return []byte(string(left) + "+" + string(right)), nil
}

// Snapshotter is the part of a storage service that takes export snapshots.
type Snapshotter interface {
	GetExportSnapshotDir() string
	TakeExportSnapshot(ctx context.Context, name string, groups []string) error
}

// EngineGroup is a group of a storage service.
type EngineGroup struct {
	Svc     Snapshotter
	Name    string
	Catalog commonv1.Catalog
}

// StrTV is a string tag value.
func StrTV(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// IntTV is an int tag value.
func IntTV(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

// DayGroup is a group of catalog with one shard and daily segments.
func DayGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
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

// The groups of a CountEngine, their shards, and how many batches and series the tests write.
const (
	// cntStreamGroup is the stream group.
	cntStreamGroup = "cnt_stream"
	// cntMeasureGroup is the measure group.
	cntMeasureGroup = "cnt_measure"
	// cntTraceGroup is the trace group.
	cntTraceGroup = "cnt_trace"
	// CntShards is the shard number of every group.
	CntShards = 2
	// CntBatches is how many batches a test writes.
	CntBatches = 3
	// CntSeries is how many series (traces) a batch writes.
	CntSeries = 4
)

// partMetadataReader is the part of a storage service the inventory reads part counts with.
type partMetadataReader interface {
	ReadPartMetadata(partDir string) (queue.StreamingPartData, error)
}

// ExpectedRow is what one written row must look like in the export.
type ExpectedRow struct {
	Cells map[string]string
	Shard uint32
}

// shardedGroup is a DayGroup with CntShards shards.
func shardedGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
	g := DayGroup(name, catalog)
	g.ResourceOpts.ShardNum = CntShards
	return g
}

// registerCountSchemas registers the groups of a CountEngine and their resources.
func registerCountSchemas(t *testing.T, metaSvc metadataservice.Service) {
	reg := metaSvc.SchemaRegistry()
	ctx := context.Background()
	for _, g := range []*commonv1.Group{
		shardedGroup(cntStreamGroup, commonv1.Catalog_CATALOG_STREAM),
		shardedGroup(cntMeasureGroup, commonv1.Catalog_CATALOG_MEASURE),
		shardedGroup(cntTraceGroup, commonv1.Catalog_CATALOG_TRACE),
	} {
		_, err := reg.CreateGroup(ctx, g)
		require.NoError(t, err)
	}
	str := databasev1.TagType_TAG_TYPE_STRING
	_, err := reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "log", Group: cntStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: str}, {Name: "msg", Type: str}, {Name: "code", Type: databasev1.TagType_TAG_TYPE_INT}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)
	_, err = reg.CreateStream(ctx, &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "event", Group: cntStreamGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: str}, {Name: "kind", Type: str}},
		}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)
	_, err = reg.CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "cpm", Group: cntMeasureGroup},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "svc", Type: str}, {Name: "plain", Type: str}},
		}},
		Fields: []*databasev1.FieldSpec{{
			Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
		Entity: &databasev1.Entity{TagNames: []string{"svc"}},
	})
	require.NoError(t, err)
	for name, extra := range map[string][]*databasev1.TraceTagSpec{
		"segment": {{Name: "service", Type: str}},
		"zipkin":  {{Name: "endpoint", Type: str}, {Name: "latency", Type: databasev1.TagType_TAG_TYPE_INT}},
	} {
		_, err = reg.CreateTrace(ctx, &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: name, Group: cntTraceGroup},
			Tags: append([]*databasev1.TraceTagSpec{
				{Name: "trace_id", Type: str},
				{Name: "span_id", Type: str},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			}, extra...),
			TraceIdTagName: "trace_id", SpanIdTagName: "span_id", TimestampTagName: "timestamp",
		})
		require.NoError(t, err)
	}
}

// publishAll sends msgs to topic in one batch and waits for every write to be applied.
func publishAll(t *testing.T, pipeline queue.Queue, topic bus.Topic, msgs []any) {
	t.Helper()
	bp := pipeline.NewBatchPublisher(5 * time.Second)
	for i, msg := range msgs {
		_, err := bp.Publish(context.Background(), topic, bus.NewMessage(bus.MessageID(i+1), msg))
		require.NoError(t, err)
	}
	errs, err := bp.Close()
	require.NoError(t, err)
	require.Empty(t, errs)
}

// streamElementID is the element id column of a stream element written as id.
func streamElementID(name, id string) string {
	return hex.EncodeToString(convert.Uint64ToBytes(convert.HashStr(cntStreamGroup + "|" + name + "|" + id)))
}

// WriteStreamBatch writes batch b of the stream group: per series, three log elements and
// one event, each series pinned to shard i%CntShards; want gets the rows it wrote.
func WriteStreamBatch(t *testing.T, pipeline queue.Queue, base time.Time, b int, want map[string]ExpectedRow) {
	t.Helper()
	var msgs []any
	add := func(shard uint32, svc, name, id string, ts time.Time, tags []*modelv1.TagValue, cells map[string]string) {
		msgs = append(msgs, &streamv1.InternalWriteRequest{
			ShardId:      shard,
			EntityValues: []*modelv1.TagValue{StrTV(svc)},
			Request: &streamv1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: name, Group: cntStreamGroup},
				Element: &streamv1.ElementValue{
					Timestamp: timestamppb.New(ts), ElementId: id,
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: tags}},
				},
			},
		})
		cells["_name"] = name
		cells["_timestamp"] = csvfmt.EncodeTimestamp(ts.UnixNano())
		cells["searchable.svc"] = svc
		want[streamElementID(name, id)] = ExpectedRow{Shard: shard, Cells: cells}
	}
	for i := 0; i < CntSeries; i++ {
		svc := "svc" + strconv.Itoa(i)
		shard := uint32(i % CntShards)
		for j := 0; j < 3; j++ {
			id := fmt.Sprintf("log-%d-%d-%d", b, i, j)
			ts := base.Add(time.Duration(b)*time.Minute + time.Duration(i)*time.Second + time.Duration(j)*time.Millisecond)
			code := int64(1000*b + 10*i + j)
			add(shard, svc, "log", id, ts, []*modelv1.TagValue{StrTV(svc), StrTV("msg " + id), IntTV(code)}, map[string]string{
				"searchable.msg": "msg " + id, "searchable.code": strconv.FormatInt(code, 10), "searchable.kind": csvfmt.NullCell,
			})
		}
		id := fmt.Sprintf("event-%d-%d", b, i)
		add(shard, svc, "event", id, base.Add(time.Duration(b)*time.Minute+time.Duration(i)*time.Second+500*time.Millisecond),
			[]*modelv1.TagValue{StrTV(svc), StrTV("kind " + id)}, map[string]string{
				"searchable.kind": "kind " + id, "searchable.msg": csvfmt.NullCell, "searchable.code": csvfmt.NullCell,
			})
	}
	publishAll(t, pipeline, data.TopicStreamWrite, msgs)
}

// WriteTraceBatch writes batch b of the trace group: per trace, three spans of the segment
// or the zipkin trace, each trace pinned to shard k%CntShards; want gets the spans it wrote.
func WriteTraceBatch(t *testing.T, pipeline queue.Queue, base time.Time, b int, want map[string]ExpectedRow) {
	t.Helper()
	var msgs []any
	for k := 0; k < CntSeries; k++ {
		traceID := fmt.Sprintf("trace-%d-%d", b, k)
		shard := uint32(k % CntShards)
		for s := 0; s < 3; s++ {
			spanID := fmt.Sprintf("span-%d", s)
			span := []byte(fmt.Sprintf("payload %d/%d/%d", b, k, s))
			ts := base.Add(time.Duration(b)*time.Minute + time.Duration(k)*time.Second + time.Duration(s)*time.Millisecond)
			tsTag := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(ts)}}
			cells := map[string]string{
				"trace_id": traceID, "span_id": spanID, "_span": csvfmt.EncodeBinary(span),
				"timestamp": csvfmt.EncodeTimestamp(ts.UnixNano()),
			}
			req := &tracev1.WriteRequest{Span: span, Version: 1}
			if k%2 == 0 {
				service := "service " + traceID
				req.Metadata = &commonv1.Metadata{Name: "segment", Group: cntTraceGroup}
				req.Tags = []*modelv1.TagValue{StrTV(traceID), StrTV(spanID), tsTag, StrTV(service)}
				req.TagSpec = &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "service"}}
				cells["_name"], cells["service"], cells["endpoint"], cells["latency"] = "segment", service, csvfmt.NullCell, csvfmt.NullCell
			} else {
				latency := int64(100*b + 10*k + s)
				req.Metadata = &commonv1.Metadata{Name: "zipkin", Group: cntTraceGroup}
				req.Tags = []*modelv1.TagValue{StrTV(traceID), StrTV(spanID), tsTag, StrTV("/" + traceID), IntTV(latency)}
				req.TagSpec = &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "endpoint", "latency"}}
				cells["_name"], cells["service"], cells["endpoint"], cells["latency"] = "zipkin", csvfmt.NullCell, "/"+traceID, strconv.FormatInt(latency, 10)
			}
			msgs = append(msgs, &tracev1.InternalWriteRequest{ShardId: shard, Request: req})
			want[traceID+"/"+spanID] = ExpectedRow{Shard: shard, Cells: cells}
		}
	}
	publishAll(t, pipeline, data.TopicTraceWrite, msgs)
}

// MeasurePoint is one write of a cpm data point.
type MeasurePoint struct {
	Svc     int
	Tick    int
	Version int64
}

// Key names the data point.
func (p MeasurePoint) Key() string { return fmt.Sprintf("svc%d@%d", p.Svc, p.Tick) }

// Value is the field value the write carries.
func (p MeasurePoint) Value() int64 { return 1000*p.Version + int64(10*p.Svc+p.Tick) }

// WriteMeasurePoints writes points in one batch, each series pinned to shard svc%CntShards.
func WriteMeasurePoints(t *testing.T, pipeline queue.Queue, base time.Time, points []MeasurePoint) {
	t.Helper()
	msgs := make([]any, 0, len(points))
	for _, p := range points {
		svc := "svc" + strconv.Itoa(p.Svc)
		msgs = append(msgs, &measurev1.InternalWriteRequest{
			ShardId:      uint32(p.Svc % CntShards),
			EntityValues: []*modelv1.TagValue{StrTV(svc)},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "cpm", Group: cntMeasureGroup},
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(base.Add(time.Duration(p.Tick) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{StrTV(svc), StrTV(fmt.Sprintf("v%d", p.Version))}}},
					Fields:      []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: p.Value()}}}},
					Version:     p.Version,
				},
			},
		})
	}
	publishAll(t, pipeline, data.TopicMeasureWrite, msgs)
}

// CountEngine is a standalone stream, measure and trace engine with the sharded groups of
// the row count tests.
type CountEngine struct {
	Meta     metadata.Repo
	Pipeline queue.Queue
	// Base keeps every row inside one daily segment.
	Base    time.Time
	Stream  EngineGroup
	Measure EngineGroup
	Trace   EngineGroup
}

// StartCountEngine starts the engines of a CountEngine and registers the schemas of its groups.
func StartCountEngine(t *testing.T) *CountEngine {
	t.Helper()
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
	t.Cleanup(metaDefer)
	ports, err := test.AllocateFreePorts(1)
	req.NoError(err)
	rootPath, rootDefer, err := test.NewSpace()
	req.NoError(err)
	t.Cleanup(rootDefer)
	moduleDefer := test.SetupModules([]string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--stream-root-path=" + rootPath, "--stream-flush-timeout=200ms",
		"--measure-root-path=" + rootPath, "--measure-flush-timeout=200ms",
		"--trace-root-path=" + rootPath, "--trace-flush-timeout=200ms",
	}, pipeline, metaSvc, streamSvc, measureSvc, traceSvc)
	t.Cleanup(moduleDefer)

	registerCountSchemas(t, metaSvc)
	req.Eventually(func() bool {
		for _, name := range []string{"log", "event"} {
			if _, loadErr := streamSvc.Stream(&commonv1.Metadata{Name: name, Group: cntStreamGroup}); loadErr != nil {
				return false
			}
		}
		for _, name := range []string{"segment", "zipkin"} {
			if _, loadErr := traceSvc.Trace(&commonv1.Metadata{Name: name, Group: cntTraceGroup}); loadErr != nil {
				return false
			}
		}
		_, loadErr := measureSvc.Measure(&commonv1.Metadata{Name: "cpm", Group: cntMeasureGroup})
		return loadErr == nil
	}, 30*time.Second, 100*time.Millisecond, "resources not loaded")
	return &CountEngine{
		Meta:     metaSvc,
		Pipeline: pipeline,
		Base:     time.Now().Truncate(time.Hour),
		Stream:   EngineGroup{Catalog: commonv1.Catalog_CATALOG_STREAM, Name: cntStreamGroup, Svc: streamSvc.(Snapshotter)},
		Measure:  EngineGroup{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: cntMeasureGroup, Svc: measureSvc.(Snapshotter)},
		Trace:    EngineGroup{Catalog: commonv1.Catalog_CATALOG_TRACE, Name: cntTraceGroup, Svc: traceSvc.(Snapshotter)},
	}
}

// SnapshotSegment is one segment of an export snapshot with the inventory of its shards.
type SnapshotSegment struct {
	// Unit is the segment's unit; it names the shards holding parts, as the Plan does.
	Unit *transferv1.SegmentUnit
	Dir  string
	// TotalCount adds up the total_count of the live parts of every shard.
	TotalCount uint64
	// Parts is the number of live parts of every shard.
	Parts int
}

// TakeSnapshot snapshots the group of g as name and lists its segments, reading the live parts
// of every shard the way the inventory does.
func TakeSnapshot(t *testing.T, g EngineGroup, name string) []SnapshotSegment {
	t.Helper()
	root := filepath.Join(g.Svc.GetExportSnapshotDir(), name)
	require.NoError(t, os.MkdirAll(root, 0o755))
	require.NoError(t, g.Svc.TakeExportSnapshot(context.Background(), name, []string{g.Name}))
	reader, ok := g.Svc.(partMetadataReader)
	require.True(t, ok, "the %s service reads part metadata", g.Catalog)
	groupDir := filepath.Join(root, g.Name)
	segs, err := os.ReadDir(groupDir)
	require.NoError(t, err)
	var out []SnapshotSegment
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), nativefmt.SegDirPrefix) {
			continue
		}
		s := SnapshotSegment{
			Unit: &transferv1.SegmentUnit{Catalog: g.Catalog, Group: g.Name, SegmentSuffix: strings.TrimPrefix(seg.Name(), nativefmt.SegDirPrefix)},
			Dir:  filepath.Join(groupDir, seg.Name()),
		}
		shards, readErr := os.ReadDir(s.Dir)
		require.NoError(t, readErr)
		for _, sh := range shards {
			id, isShard := strings.CutPrefix(sh.Name(), nativefmt.ShardDirPrefix)
			if !isShard || !sh.IsDir() {
				continue
			}
			n, parseErr := strconv.ParseUint(id, 10, 32)
			require.NoError(t, parseErr)
			shardDir := filepath.Join(s.Dir, sh.Name())
			partIDs, partsErr := snapshot.LivePartIDs(shardDir)
			require.NoError(t, partsErr)
			if len(partIDs) > 0 {
				s.Unit.ShardIds = append(s.Unit.ShardIds, uint32(n))
			}
			for _, partID := range partIDs {
				pm, metaErr := reader.ReadPartMetadata(filepath.Join(shardDir, fmt.Sprintf("%016x", partID)))
				require.NoError(t, metaErr)
				s.TotalCount += pm.TotalCount
				s.Parts++
			}
		}
		out = append(out, s)
	}
	return out
}
