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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// gzipInit is the stream settings of a gzip-compressed export.
var gzipInit = &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_GZIP}

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

// produceCounted snapshots the group of g as name and produces each of its segments as
// one unit holding every shard, with init as the stream settings. It also reads the live
// parts of every shard the way the inventory does.
func produceCounted(t *testing.T, meta metadata.Repo, g EngineGroup, name string, init *transferv1.ExportRequest_Init) []*countedUnit {
	t.Helper()
	var out []*countedUnit
	for _, seg := range TakeSnapshot(t, g, name) {
		cu := &countedUnit{unit: seg.Unit, totalCount: seg.TotalCount, parts: seg.Parts}
		var sent []*transferv1.ExportResponse
		p := NewProducer(context.Background(), meta, IndexModeWalker{}, init, func(r *transferv1.ExportResponse) error {
			sent = append(sent, r)
			return nil
		})
		var err error
		cu.warnings, cu.rows, err = p.Segment(seg.Unit, seg.Dir)
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
func produceUntil(t *testing.T, meta metadata.Repo, g EngineGroup, prefix string, init *transferv1.ExportRequest_Init,
	done func([]*countedUnit) bool,
) []*countedUnit {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		units := produceCounted(t, meta, g, fmt.Sprintf("%s%04x", prefix, attempt), init)
		if done(units) {
			return units
		}
		require.True(t, time.Now().Before(deadline), "the %s snapshot %s%04x never held the written rows", g.Catalog, prefix, attempt)
		time.Sleep(300 * time.Millisecond)
	}
}

// matchRows checks that every exported row is one written row, exported once with every
// expected cell, and that no written row is missing. key names the row.
func matchRows(t *testing.T, rows []map[string]string, want map[string]ExpectedRow, key func(map[string]string) string) {
	t.Helper()
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		k := key(row)
		exp, ok := want[k]
		require.True(t, ok, "row %s was never written: %v", k, row)
		_, dup := seen[k]
		require.False(t, dup, "row %s is exported twice", k)
		seen[k] = struct{}{}
		for column, cell := range exp.Cells {
			require.Equal(t, cell, row[column], "row %s column %s", k, column)
		}
	}
	for k := range want {
		_, ok := seen[k]
		require.True(t, ok, "written row %s is missing from the export", k)
	}
}

func shardOfFile(f *exportedFile) uint32 { return f.started.GetShardId() }

// TestProducer_ExportsEveryWrittenRow writes rows into several parts of several shards of
// one segment with the real storage engines, and checks that the export holds exactly the
// written rows, each once, and that its row counts agree with the parts' total_count.
func TestProducer_ExportsEveryWrittenRow(t *testing.T) {
	e := StartCountEngine(t)
	t.Run("stream", func(t *testing.T) { checkStreamRows(t, e) })
	t.Run("trace", func(t *testing.T) { checkTraceRows(t, e) })
	t.Run("measure", func(t *testing.T) { checkMeasureRows(t, e) })
}

// checkStreamRows exports every element of the stream group once, then again rolled over
// several files per shard.
func checkStreamRows(t *testing.T, e *CountEngine) {
	want := map[string]ExpectedRow{}
	var units []*countedUnit
	// Each batch is flushed before the next is written, so it lands in parts of its own.
	for b := 0; b < CntBatches; b++ {
		WriteStreamBatch(t, e.Pipeline, e.Base, b, want)
		units = produceUntil(t, e.Meta, e.Stream, fmt.Sprintf("s%d", b), gzipInit, func(us []*countedUnit) bool {
			_, total := inventory(us)
			return total == uint64(len(want))
		})
	}
	parts, total := inventory(units)
	t.Logf("stream: %d live parts holding %d rows", parts, total)
	require.GreaterOrEqual(t, parts, CntShards, "every shard holds parts")
	rows := countedRows(t, units)
	require.Equal(t, total, uint64(len(rows)), "the export holds as many rows as the parts")
	require.Equal(t, map[string]uint64{"log": CntBatches * CntSeries * 3, "event": CntBatches * CntSeries}, rowCounts(units))
	matchRows(t, rows, want, func(r map[string]string) string { return r["_element_id"] })
	var exported uint64
	for _, u := range units {
		exported += u.rows
		require.Empty(t, unitWarnings([]*producedUnit{{warnings: u.warnings}}))
		for _, f := range u.files {
			for _, row := range csvRows(t, f) {
				require.Equal(t, want[row["_element_id"]].Shard, shardOfFile(f), "row %s is in the file of its shard", row["_element_id"])
			}
		}
	}
	require.Equal(t, total, exported)

	t.Run("rolling", func(t *testing.T) {
		// A tiny max_file_size rolls every shard over several files.
		rollInit := &transferv1.ExportRequest_Init{Compress: transferv1.Compress_COMPRESS_NONE, MaxFileSize: 1 << 10}
		rolled := produceCounted(t, e.Meta, e.Stream, "roll", rollInit)
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
		for shard := uint32(0); shard < CntShards; shard++ {
			require.Greater(t, perShard[shard], 1, "shard %d spans several files", shard)
		}
		rolledRows := countedRows(t, rolled)
		require.Len(t, rolledRows, len(want), "no row is lost or duplicated at a roll boundary")
		require.Equal(t, rowCounts(units), rowCounts(rolled))
		matchRows(t, rolledRows, want, func(r map[string]string) string { return r["_element_id"] })
	})
}

// checkTraceRows exports every span of the trace group once.
func checkTraceRows(t *testing.T, e *CountEngine) {
	want := map[string]ExpectedRow{}
	var units []*countedUnit
	for b := 0; b < CntBatches; b++ {
		WriteTraceBatch(t, e.Pipeline, e.Base, b, want)
		units = produceUntil(t, e.Meta, e.Trace, fmt.Sprintf("t%d", b), gzipInit, func(us []*countedUnit) bool {
			_, total := inventory(us)
			return total == uint64(len(want))
		})
	}
	parts, total := inventory(units)
	t.Logf("trace: %d live parts holding %d spans", parts, total)
	require.GreaterOrEqual(t, parts, CntShards, "every shard holds parts")
	rows := countedRows(t, units)
	require.Equal(t, total, uint64(len(rows)), "the export holds as many spans as the parts")
	perTrace := uint64(CntBatches * CntSeries / 2 * 3)
	require.Equal(t, map[string]uint64{"segment": perTrace, "zipkin": perTrace}, rowCounts(units))
	matchRows(t, rows, want, func(r map[string]string) string { return r["trace_id"] + "/" + r["span_id"] })
	for _, u := range units {
		require.Empty(t, unitWarnings([]*producedUnit{{warnings: u.warnings}}))
		for _, f := range u.files {
			for _, row := range csvRows(t, f) {
				require.Equal(t, want[row["trace_id"]+"/"+row["span_id"]].Shard, shardOfFile(f))
			}
		}
	}
}

// checkMeasureRows exports the latest version of every data point once, with its values.
func checkMeasureRows(t *testing.T, e *CountEngine) {
	latest := map[string]MeasurePoint{}
	var written int
	write := func(points []MeasurePoint) {
		WriteMeasurePoints(t, e.Pipeline, e.Base, points)
		written += len(points)
		for _, p := range points {
			if old, ok := latest[p.Key()]; !ok || p.Version > old.Version {
				latest[p.Key()] = p
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
			p, ok := latest[row["default.svc"]+"@"+strconv.FormatInt(tickOf(t, e.Base, row["_timestamp"]), 10)]
			if !ok || row["_version"] != strconv.FormatInt(p.Version, 10) {
				return false
			}
		}
		return true
	}
	var first []MeasurePoint
	for s := 0; s < CntSeries; s++ {
		for tick := 0; tick < 3; tick++ {
			first = append(first, MeasurePoint{Svc: s, Tick: tick, Version: 5})
		}
	}
	write(first)
	produceUntil(t, e.Meta, e.Measure, "m0", gzipInit, exportedLatest)
	// Newer versions of every tick-0 point.
	var newer []MeasurePoint
	for s := 0; s < CntSeries; s++ {
		newer = append(newer, MeasurePoint{Svc: s, Tick: 0, Version: 9})
	}
	write(newer)
	produceUntil(t, e.Meta, e.Measure, "m1", gzipInit, exportedLatest)
	// Older versions of every tick-1 point, and one new point that shows the batch arrived.
	var older []MeasurePoint
	for s := 0; s < CntSeries; s++ {
		older = append(older, MeasurePoint{Svc: s, Tick: 1, Version: 1})
	}
	older = append(older, MeasurePoint{Svc: 0, Tick: 3, Version: 2})
	write(older)
	units := produceUntil(t, e.Meta, e.Measure, "m2", gzipInit, exportedLatest)

	parts, total := inventory(units)
	t.Logf("measure: %d live parts holding %d data points, %d written, %d distinct", parts, total, written, len(latest))
	rows := countedRows(t, units)
	require.Len(t, rows, len(latest), "one row per distinct data point")
	require.LessOrEqual(t, uint64(len(rows)), total, "the export never holds more rows than the parts")
	require.Equal(t, map[string]uint64{"cpm": uint64(len(latest))}, rowCounts(units))
	want := make(map[string]ExpectedRow, len(latest))
	for k, p := range latest {
		want[k] = ExpectedRow{Cells: map[string]string{
			"_name":         "cpm",
			"_timestamp":    csvfmt.EncodeTimestamp(e.Base.Add(time.Duration(p.Tick) * time.Second).UnixNano()),
			"_version":      strconv.FormatInt(p.Version, 10),
			"default.svc":   "svc" + strconv.Itoa(p.Svc),
			"default.plain": fmt.Sprintf("v%d", p.Version),
			// The group's internal Top-N result measure declares a binary "value" field, which splits the column.
			"cpm::_field.value": strconv.FormatInt(p.Value(), 10),
		}}
	}
	matchRows(t, rows, want, func(r map[string]string) string {
		return r["default.svc"] + "@" + strconv.FormatInt(tickOf(t, e.Base, r["_timestamp"]), 10)
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
