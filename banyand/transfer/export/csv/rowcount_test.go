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
	cntStreamGroup  = "cnt_stream"
	cntMeasureGroup = "cnt_measure"
	cntTraceGroup   = "cnt_trace"
	cntShards       = 2
	cntBatches      = 3
	cntSeries       = 4
)

// partMetadataReader is the part of a storage service the inventory reads part counts with.
type partMetadataReader interface {
	ReadPartMetadata(partDir string) (queue.StreamingPartData, error)
}

// countedUnit is one produced segment unit with the inventory of its snapshot: the live
// parts of its shards and the rows they hold.
type countedUnit struct {
	unit       *transferv1.SegmentUnit
	files      []*exportedFile
	warnings   []string
	rows       uint64
	totalCount uint64
	parts      int
}

// expectedRow is what one written row must look like in the export.
type expectedRow struct {
	cells map[string]string
	shard uint32
}

func shardedGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
	g := dayGroup(name, catalog)
	g.ResourceOpts.ShardNum = cntShards
	return g
}

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

// produceCounted snapshots the group of g as name and produces each of its segments as
// one unit holding every shard, with init as the stream settings. It also reads the live
// parts of every shard the way the inventory does.
func produceCounted(t *testing.T, meta metadata.Repo, g engineGroup, name string, init *transferv1.ExportRequest_Init) []*countedUnit {
	t.Helper()
	ctx := context.Background()
	root := filepath.Join(g.svc.GetExportSnapshotDir(), name)
	require.NoError(t, os.MkdirAll(root, 0o755))
	require.NoError(t, g.svc.TakeExportSnapshot(ctx, name, []string{g.name}))
	reader, ok := g.svc.(partMetadataReader)
	require.True(t, ok, "the %s service reads part metadata", g.catalog)
	groupDir := filepath.Join(root, g.name)
	segs, err := os.ReadDir(groupDir)
	require.NoError(t, err)
	var out []*countedUnit
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), segDirPrefix) {
			continue
		}
		segDir := filepath.Join(groupDir, seg.Name())
		unit := &transferv1.SegmentUnit{Catalog: g.catalog, Group: g.name, SegmentSuffix: strings.TrimPrefix(seg.Name(), segDirPrefix)}
		cu := &countedUnit{unit: unit}
		shards, readErr := os.ReadDir(segDir)
		require.NoError(t, readErr)
		for _, sh := range shards {
			id, isShard := strings.CutPrefix(sh.Name(), shardDirPrefix)
			if !isShard || !sh.IsDir() {
				continue
			}
			n, parseErr := strconv.ParseUint(id, 10, 32)
			require.NoError(t, parseErr)
			shardDir := filepath.Join(segDir, sh.Name())
			partIDs, partsErr := LivePartIDs(shardDir)
			require.NoError(t, partsErr)
			// The Plan names only the shards holding parts.
			if len(partIDs) > 0 {
				unit.ShardIds = append(unit.ShardIds, uint32(n))
			}
			for _, partID := range partIDs {
				pm, metaErr := reader.ReadPartMetadata(filepath.Join(shardDir, fmt.Sprintf("%016x", partID)))
				require.NoError(t, metaErr)
				cu.totalCount += pm.TotalCount
				cu.parts++
			}
		}
		var sent []*transferv1.ExportResponse
		p := NewProducer(ctx, meta, indexModeWalker{}, init, func(r *transferv1.ExportResponse) error {
			sent = append(sent, r)
			return nil
		})
		cu.warnings, cu.rows, err = p.Segment(unit, segDir)
		require.NoError(t, err)
		cu.files = decodeFiles(t, sent)
		out = append(out, cu)
	}
	return out
}

// countedRows decodes every file of the units, checking that each file's row_counts
// match the rows it holds, resource by resource.
func countedRows(t *testing.T, units []*countedUnit) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, u := range units {
		var rows uint64
		for _, f := range u.files {
			fileRows := csvRows(t, f)
			byName := map[string]uint64{}
			for _, r := range fileRows {
				byName[r["_name"]]++
			}
			counts := map[string]uint64{}
			for resource, n := range f.finished.GetRowCounts() {
				if n > 0 {
					counts[resource] = n
				}
				rows += n
			}
			require.Equal(t, byName, counts, "row_counts of %s", f.started.GetRelativePath())
			out = append(out, fileRows...)
		}
		require.Equal(t, rows, u.rows, "the unit reports the rows of its files")
	}
	return out
}

// rowCounts adds up file_finished.row_counts of every file of the units, per resource.
func rowCounts(units []*countedUnit) map[string]uint64 {
	out := map[string]uint64{}
	for _, u := range units {
		for _, f := range u.files {
			for resource, n := range f.finished.GetRowCounts() {
				if n > 0 {
					out[resource] += n
				}
			}
		}
	}
	return out
}

// inventory adds up the live parts and their total_count over the units.
func inventory(units []*countedUnit) (int, uint64) {
	var parts int
	var total uint64
	for _, u := range units {
		parts += u.parts
		total += u.totalCount
	}
	return parts, total
}

// produceUntil produces g under fresh snapshot names until done accepts the units, which
// waits for asynchronously flushed parts to reach a snapshot.
func produceUntil(t *testing.T, meta metadata.Repo, g engineGroup, prefix string, init *transferv1.ExportRequest_Init,
	done func([]*countedUnit) bool,
) []*countedUnit {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		units := produceCounted(t, meta, g, fmt.Sprintf("%s%04x", prefix, attempt), init)
		if done(units) {
			return units
		}
		require.True(t, time.Now().Before(deadline), "the %s snapshot %s%04x never held the written rows", g.catalog, prefix, attempt)
		time.Sleep(300 * time.Millisecond)
	}
}

// matchRows checks that every exported row is one written row, exported once with every
// expected cell, and that no written row is missing. key names the row.
func matchRows(t *testing.T, rows []map[string]string, want map[string]expectedRow, key func(map[string]string) string) {
	t.Helper()
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		k := key(row)
		exp, ok := want[k]
		require.True(t, ok, "row %s was never written: %v", k, row)
		_, dup := seen[k]
		require.False(t, dup, "row %s is exported twice", k)
		seen[k] = struct{}{}
		for column, cell := range exp.cells {
			require.Equal(t, cell, row[column], "row %s column %s", k, column)
		}
	}
	for k := range want {
		_, ok := seen[k]
		require.True(t, ok, "written row %s is missing from the export", k)
	}
}

func streamElementID(name, id string) string {
	return hex.EncodeToString(convert.Uint64ToBytes(convert.HashStr(cntStreamGroup + "|" + name + "|" + id)))
}

// writeStreamBatch writes batch b of the stream group: per series, three log elements and
// one event, each series pinned to shard i%cntShards; want gets the rows it wrote.
func writeStreamBatch(t *testing.T, pipeline queue.Queue, base time.Time, b int, want map[string]expectedRow) {
	t.Helper()
	var msgs []any
	add := func(shard uint32, svc, name, id string, ts time.Time, tags []*modelv1.TagValue, cells map[string]string) {
		msgs = append(msgs, &streamv1.InternalWriteRequest{
			ShardId:      shard,
			EntityValues: []*modelv1.TagValue{strTV(svc)},
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
		want[streamElementID(name, id)] = expectedRow{shard: shard, cells: cells}
	}
	for i := 0; i < cntSeries; i++ {
		svc := "svc" + strconv.Itoa(i)
		shard := uint32(i % cntShards)
		for j := 0; j < 3; j++ {
			id := fmt.Sprintf("log-%d-%d-%d", b, i, j)
			ts := base.Add(time.Duration(b)*time.Minute + time.Duration(i)*time.Second + time.Duration(j)*time.Millisecond)
			code := int64(1000*b + 10*i + j)
			add(shard, svc, "log", id, ts, []*modelv1.TagValue{strTV(svc), strTV("msg " + id), intTV(code)}, map[string]string{
				"searchable.msg": "msg " + id, "searchable.code": strconv.FormatInt(code, 10), "searchable.kind": csvfmt.NullCell,
			})
		}
		id := fmt.Sprintf("event-%d-%d", b, i)
		add(shard, svc, "event", id, base.Add(time.Duration(b)*time.Minute+time.Duration(i)*time.Second+500*time.Millisecond),
			[]*modelv1.TagValue{strTV(svc), strTV("kind " + id)}, map[string]string{
				"searchable.kind": "kind " + id, "searchable.msg": csvfmt.NullCell, "searchable.code": csvfmt.NullCell,
			})
	}
	publishAll(t, pipeline, data.TopicStreamWrite, msgs)
}

// writeTraceBatch writes batch b of the trace group: per trace, three spans of the segment
// or the zipkin trace, each trace pinned to shard k%cntShards; want gets the spans it wrote.
func writeTraceBatch(t *testing.T, pipeline queue.Queue, base time.Time, b int, want map[string]expectedRow) {
	t.Helper()
	var msgs []any
	for k := 0; k < cntSeries; k++ {
		traceID := fmt.Sprintf("trace-%d-%d", b, k)
		shard := uint32(k % cntShards)
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
				req.Tags = []*modelv1.TagValue{strTV(traceID), strTV(spanID), tsTag, strTV(service)}
				req.TagSpec = &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "service"}}
				cells["_name"], cells["service"], cells["endpoint"], cells["latency"] = "segment", service, csvfmt.NullCell, csvfmt.NullCell
			} else {
				latency := int64(100*b + 10*k + s)
				req.Metadata = &commonv1.Metadata{Name: "zipkin", Group: cntTraceGroup}
				req.Tags = []*modelv1.TagValue{strTV(traceID), strTV(spanID), tsTag, strTV("/" + traceID), intTV(latency)}
				req.TagSpec = &tracev1.TagSpec{TagNames: []string{"trace_id", "span_id", "timestamp", "endpoint", "latency"}}
				cells["_name"], cells["service"], cells["endpoint"], cells["latency"] = "zipkin", csvfmt.NullCell, "/"+traceID, strconv.FormatInt(latency, 10)
			}
			msgs = append(msgs, &tracev1.InternalWriteRequest{ShardId: shard, Request: req})
			want[traceID+"/"+spanID] = expectedRow{shard: shard, cells: cells}
		}
	}
	publishAll(t, pipeline, data.TopicTraceWrite, msgs)
}

// measurePoint is one write of a cpm data point.
type measurePoint struct {
	svc     int
	tick    int
	version int64
}

func (p measurePoint) key() string { return fmt.Sprintf("svc%d@%d", p.svc, p.tick) }

func (p measurePoint) value() int64 { return 1000*p.version + int64(10*p.svc+p.tick) }

// writeMeasurePoints writes points in one batch, each series pinned to shard svc%cntShards.
func writeMeasurePoints(t *testing.T, pipeline queue.Queue, base time.Time, points []measurePoint) {
	t.Helper()
	msgs := make([]any, 0, len(points))
	for _, p := range points {
		svc := "svc" + strconv.Itoa(p.svc)
		msgs = append(msgs, &measurev1.InternalWriteRequest{
			ShardId:      uint32(p.svc % cntShards),
			EntityValues: []*modelv1.TagValue{strTV(svc)},
			Request: &measurev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: "cpm", Group: cntMeasureGroup},
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(base.Add(time.Duration(p.tick) * time.Second)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{strTV(svc), strTV(fmt.Sprintf("v%d", p.version))}}},
					Fields:      []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: p.value()}}}},
					Version:     p.version,
				},
			},
		})
	}
	publishAll(t, pipeline, data.TopicMeasureWrite, msgs)
}

func shardOfFile(f *exportedFile) uint32 { return f.started.GetShardId() }

// countEngine is a standalone stream, measure and trace engine with the sharded groups of
// the row count tests.
type countEngine struct {
	meta     metadata.Repo
	pipeline queue.Queue
	gzip     *transferv1.ExportRequest_Init
	// base keeps every row inside one daily segment.
	base    time.Time
	stream  engineGroup
	measure engineGroup
	trace   engineGroup
}

func startCountEngine(t *testing.T) *countEngine {
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
	return &countEngine{
		meta:     metaSvc,
		pipeline: pipeline,
		base:     time.Now().Truncate(time.Hour),
		gzip:     &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_GZIP},
		stream:   engineGroup{catalog: commonv1.Catalog_CATALOG_STREAM, name: cntStreamGroup, svc: streamSvc.(snapshotter)},
		measure:  engineGroup{catalog: commonv1.Catalog_CATALOG_MEASURE, name: cntMeasureGroup, svc: measureSvc.(snapshotter)},
		trace:    engineGroup{catalog: commonv1.Catalog_CATALOG_TRACE, name: cntTraceGroup, svc: traceSvc.(snapshotter)},
	}
}

// TestProducer_ExportsEveryWrittenRow writes rows into several parts of several shards of
// one segment with the real storage engines, and checks that the export holds exactly the
// written rows, each once, and that its row counts agree with the parts' total_count.
func TestProducer_ExportsEveryWrittenRow(t *testing.T) {
	e := startCountEngine(t)
	t.Run("stream", func(t *testing.T) { checkStreamRows(t, e) })
	t.Run("trace", func(t *testing.T) { checkTraceRows(t, e) })
	t.Run("measure", func(t *testing.T) { checkMeasureRows(t, e) })
}

// checkStreamRows exports every element of the stream group once, then again rolled over
// several files per shard.
func checkStreamRows(t *testing.T, e *countEngine) {
	want := map[string]expectedRow{}
	var units []*countedUnit
	// Each batch is flushed before the next is written, so it lands in parts of its own.
	for b := 0; b < cntBatches; b++ {
		writeStreamBatch(t, e.pipeline, e.base, b, want)
		units = produceUntil(t, e.meta, e.stream, fmt.Sprintf("s%d", b), e.gzip, func(us []*countedUnit) bool {
			_, total := inventory(us)
			return total == uint64(len(want))
		})
	}
	parts, total := inventory(units)
	t.Logf("stream: %d live parts holding %d rows", parts, total)
	require.GreaterOrEqual(t, parts, cntShards, "every shard holds parts")
	rows := countedRows(t, units)
	require.Equal(t, total, uint64(len(rows)), "the export holds as many rows as the parts")
	require.Equal(t, map[string]uint64{"log": cntBatches * cntSeries * 3, "event": cntBatches * cntSeries}, rowCounts(units))
	matchRows(t, rows, want, func(r map[string]string) string { return r["_element_id"] })
	var exported uint64
	for _, u := range units {
		exported += u.rows
		require.Empty(t, unitWarnings([]*producedUnit{{warnings: u.warnings}}))
		for _, f := range u.files {
			for _, row := range csvRows(t, f) {
				require.Equal(t, want[row["_element_id"]].shard, shardOfFile(f), "row %s is in the file of its shard", row["_element_id"])
			}
		}
	}
	require.Equal(t, total, exported)

	t.Run("rolling", func(t *testing.T) {
		// A tiny max_file_size rolls every shard over several files.
		rollInit := &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE, MaxFileSize: 1 << 10}
		rolled := produceCounted(t, e.meta, e.stream, "roll", rollInit)
		_, rolledTotal := inventory(rolled)
		require.Equal(t, uint64(len(want)), rolledTotal)
		perShard := map[uint32]int{}
		for _, u := range rolled {
			var seq uint32
			var shard uint32
			for i, f := range u.files {
				if i == 0 || f.started.GetShardId() != shard {
					shard, seq = f.started.GetShardId(), 0
				}
				seq++
				require.Equal(t, seq, f.started.GetSeq(), "%s", f.started.GetRelativePath())
				require.Equal(t, fmt.Sprintf("%s-%05d.csv", shardPrefix(u.unit, shard), seq), f.started.GetRelativePath())
				perShard[shard]++
			}
		}
		for shard := uint32(0); shard < cntShards; shard++ {
			require.Greater(t, perShard[shard], 1, "shard %d spans several files", shard)
		}
		rolledRows := countedRows(t, rolled)
		require.Len(t, rolledRows, len(want), "no row is lost or duplicated at a roll boundary")
		require.Equal(t, rowCounts(units), rowCounts(rolled))
		matchRows(t, rolledRows, want, func(r map[string]string) string { return r["_element_id"] })
	})
}

// checkTraceRows exports every span of the trace group once.
func checkTraceRows(t *testing.T, e *countEngine) {
	want := map[string]expectedRow{}
	var units []*countedUnit
	for b := 0; b < cntBatches; b++ {
		writeTraceBatch(t, e.pipeline, e.base, b, want)
		units = produceUntil(t, e.meta, e.trace, fmt.Sprintf("t%d", b), e.gzip, func(us []*countedUnit) bool {
			_, total := inventory(us)
			return total == uint64(len(want))
		})
	}
	parts, total := inventory(units)
	t.Logf("trace: %d live parts holding %d spans", parts, total)
	require.GreaterOrEqual(t, parts, cntShards, "every shard holds parts")
	rows := countedRows(t, units)
	require.Equal(t, total, uint64(len(rows)), "the export holds as many spans as the parts")
	require.Equal(t, map[string]uint64{"segment": cntBatches * cntSeries / 2 * 3, "zipkin": cntBatches * cntSeries / 2 * 3}, rowCounts(units))
	matchRows(t, rows, want, func(r map[string]string) string { return r["trace_id"] + "/" + r["span_id"] })
	for _, u := range units {
		require.Empty(t, unitWarnings([]*producedUnit{{warnings: u.warnings}}))
		for _, f := range u.files {
			for _, row := range csvRows(t, f) {
				require.Equal(t, want[row["trace_id"]+"/"+row["span_id"]].shard, shardOfFile(f))
			}
		}
	}
}

// checkMeasureRows exports the latest version of every data point once, with its values.
func checkMeasureRows(t *testing.T, e *countEngine) {
	latest := map[string]measurePoint{}
	var written int
	write := func(points []measurePoint) {
		writeMeasurePoints(t, e.pipeline, e.base, points)
		written += len(points)
		for _, p := range points {
			if old, ok := latest[p.key()]; !ok || p.version > old.version {
				latest[p.key()] = p
			}
		}
	}
	// exportedLatest accepts units that show the latest version of every point.
	exportedLatest := func(us []*countedUnit) bool {
		rows := countedRows(t, us)
		if len(rows) != len(latest) {
			return false
		}
		for _, row := range rows {
			p, ok := latest[row["default.svc"]+"@"+strconv.FormatInt(tickOf(t, e.base, row["_timestamp"]), 10)]
			if !ok || row["_version"] != strconv.FormatInt(p.version, 10) {
				return false
			}
		}
		return true
	}
	var first []measurePoint
	for s := 0; s < cntSeries; s++ {
		for tick := 0; tick < 3; tick++ {
			first = append(first, measurePoint{svc: s, tick: tick, version: 5})
		}
	}
	write(first)
	produceUntil(t, e.meta, e.measure, "m0", e.gzip, exportedLatest)
	// Newer versions of every tick-0 point.
	var newer []measurePoint
	for s := 0; s < cntSeries; s++ {
		newer = append(newer, measurePoint{svc: s, tick: 0, version: 9})
	}
	write(newer)
	produceUntil(t, e.meta, e.measure, "m1", e.gzip, exportedLatest)
	// Older versions of every tick-1 point, and one new point that shows the batch arrived.
	var older []measurePoint
	for s := 0; s < cntSeries; s++ {
		older = append(older, measurePoint{svc: s, tick: 1, version: 1})
	}
	older = append(older, measurePoint{svc: 0, tick: 3, version: 2})
	write(older)
	units := produceUntil(t, e.meta, e.measure, "m2", e.gzip, exportedLatest)

	parts, total := inventory(units)
	t.Logf("measure: %d live parts holding %d data points, %d written, %d distinct", parts, total, written, len(latest))
	rows := countedRows(t, units)
	require.Len(t, rows, len(latest), "one row per distinct data point")
	require.LessOrEqual(t, uint64(len(rows)), total, "the export never holds more rows than the parts")
	require.Equal(t, map[string]uint64{"cpm": uint64(len(latest))}, rowCounts(units))
	want := make(map[string]expectedRow, len(latest))
	for k, p := range latest {
		want[k] = expectedRow{cells: map[string]string{
			"_name":         "cpm",
			"_timestamp":    csvfmt.EncodeTimestamp(e.base.Add(time.Duration(p.tick) * time.Second).UnixNano()),
			"_version":      strconv.FormatInt(p.version, 10),
			"default.svc":   "svc" + strconv.Itoa(p.svc),
			"default.plain": fmt.Sprintf("v%d", p.version),
			"_field.value":  strconv.FormatInt(p.value(), 10),
		}}
	}
	matchRows(t, rows, want, func(r map[string]string) string {
		return r["default.svc"] + "@" + strconv.FormatInt(tickOf(t, e.base, r["_timestamp"]), 10)
	})
	for _, u := range units {
		require.Empty(t, unitWarnings([]*producedUnit{{warnings: u.warnings}}))
	}
}

// tickOf turns an exported timestamp cell back into whole seconds after base.
func tickOf(t *testing.T, base time.Time, cell string) int64 {
	t.Helper()
	for tick := int64(0); tick < 60; tick++ {
		if csvfmt.EncodeTimestamp(base.Add(time.Duration(tick)*time.Second).UnixNano()) == cell {
			return tick
		}
	}
	t.Fatalf("timestamp %s is not a whole second after %s", cell, base)
	return -1
}
