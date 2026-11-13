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

package measure

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const calibrationMinute = 60_000_000_000

// calibrationPoints builds series blocks shaped like the perf cluster's
// endpoint_apdex_minute: one entity_id tag of idLen bytes, constant within a series,
// and one int field, one row per minute.
func calibrationPoints(series, rows, idLen int) *dataPoints {
	dps := &dataPoints{}
	for sid := 1; sid <= series; sid++ {
		id := fmt.Sprintf("%0*d", idLen, sid)
		for ts := int64(0); ts < int64(rows); ts++ {
			dps.seriesIDs = append(dps.seriesIDs, common.SeriesID(sid))
			dps.timestamps = append(dps.timestamps, ts*calibrationMinute)
			dps.versions = append(dps.versions, 1)
			dps.tagFamilies = append(dps.tagFamilies, []nameValues{{name: "storage-only", values: []*nameValue{
				fxValue("entity_id", pbv1.ValueTypeStr, []byte(id)),
			}}})
			dps.fields = append(dps.fields, nameValues{name: "f", values: []*nameValue{
				fxValue("int_value", pbv1.ValueTypeInt64, convert.Int64ToBytes(int64(sid)*1000+ts%97)),
			}})
		}
	}
	return dps
}

func settledHeap() int64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

type calibrationShape struct {
	name       string
	points     func(series, rows int) *dataPoints
	tagTypes   map[string]pbv1.ValueType
	projection []model.TagProjection
	fields     []string
	columns    uint64 // as searchBlocks counts them: 4 + fields + tags
	rowBytes   uint64 // projectedRowBytes: 16 for timestamp and version, 8 per fixed-width column, 24 per string, binary or array
}

func calibrationShapes() []calibrationShape {
	wide := calibrationShape{
		name: "wide", tagTypes: fxTagTypes,
		projection: tagProjectionOf(fxFamily, fxTagSvc, fxTagURL, fxTagN, fxTagN2, fxTagArr, fxTagIAr, fxTagBin),
		fields:     []string{fxFldInt, fxFldFlt, fxFldNul, fxFldStr, fxFldBin},
		columns:    4 + 5 + 7,
		rowBytes:   16 + (24 + 24 + 8 + 8 + 24 + 24 + 24) + (8 + 8 + 8 + 24 + 24),
		points: func(series, rows int) *dataPoints {
			var part fxPart
			for sid := 1; sid <= series; sid++ {
				part = append(part, fxRange{common.SeriesID(sid), 0, int64(rows) - 1, 1})
			}
			dps := fxPoints(part)
			for i := range dps.timestamps {
				dps.timestamps[i] *= calibrationMinute
			}
			return dps
		},
	}
	cluster := func(name string, idLen int) calibrationShape {
		return calibrationShape{
			name: name, tagTypes: map[string]pbv1.ValueType{"entity_id": pbv1.ValueTypeStr},
			projection: tagProjectionOf("storage-only", "entity_id"), fields: []string{"int_value"}, columns: 4 + 1 + 1, rowBytes: 16 + 24 + 8,
			points: func(series, rows int) *dataPoints { return calibrationPoints(series, rows, idLen) },
		}
	}
	constant := cluster("constant-id8", 8)
	constant.points = func(series, rows int) *dataPoints {
		dps := calibrationPoints(series, rows, 8)
		for i := range dps.fields {
			dps.fields[i].values[0].value = convert.Int64ToBytes(7) // compresses to almost nothing, decodes to 8 bytes a row
		}
		return dps
	}
	return []calibrationShape{cluster("cluster-id190", 190), cluster("cluster-id60", 60), constant, wide}
}

// TestCalibrateSharedEnvelope measures the heap one decode of a block allocates
// (what the cursor retains plus the part of the expansion that is not pooled) against
// the shared envelope, for several block shapes and query windows, and requires the
// envelope to keep a margin over what a decoded cursor retains. With BYDB_CALIBRATE=1 it
// also checks the pre-decode envelope against the allocation and prints the table.
func TestCalibrateSharedEnvelope(t *testing.T) {
	// The retained-size margin is deterministic and always checked. The allocation
	// margin reads process-wide counters, and the table is for people, so both are
	// BYDB_CALIBRATE=1 only.
	calibrate := os.Getenv("BYDB_CALIBRATE") != ""
	assertMargins := os.Getenv("BYDB_CALIBRATE") != "2"
	const series = 200
	var out strings.Builder
	fmt.Fprintf(&out, "\n%-14s %5s %7s %9s | %9s %9s %9s %9s | %7s %7s | %6s %6s\n", "shape", "rows", "inRange", "encoded",
		"preEst", "keptEst", "alloc", "kept", "pre/A", "kept/R", "A/enc", "A/rc")
	for _, shape := range calibrationShapes() {
		for _, rows := range []int{100, 360, 611, 2000} {
			mp := generateMemPart()
			mp.mustInitFromDataPoints(shape.points(series, rows))
			p := openMemPart(mp)
			ref := partRef{p: p, cache: storage.NewShardCache("calibrate", 0, 0)}
			sids := make([]common.SeriesID, series)
			for i := range sids {
				sids[i] = common.SeriesID(i + 1)
			}
			for _, frac := range []float64{1, 0.25, 0.04} {
				qo := queryOptions{schemaTagTypes: shape.tagTypes}
				qo.MeasureQueryOptions.TagProjection = shape.projection
				qo.MeasureQueryOptions.FieldProjection = shape.fields
				qo.minTimestamp, qo.maxTimestamp = 0, max(int64(float64(rows)*frac)-1, 0)*calibrationMinute
				ti := generateTstIter()
				ti.init([]partRef{ref}, sids, qo.minTimestamp, qo.maxTimestamp)
				var cursors []*blockCursor
				for ti.nextBlock() {
					bc := generateBlockCursor()
					bc.init(ti.piHeap[0].p, ti.piHeap[0].curBlock, qo)
					cursors = append(cursors, bc)
				}
				require.NoError(t, ti.Error())
				releaseTstIter(ti)
				require.Len(t, cursors, series)

				settledHeap()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				var inRange, enc, retained int
				for _, bc := range cursors {
					// A budgeted decode drops oversized pooled buffers first (boundDecodeBacking); give the
					// cursor a reservation so the decode runs the way it does in a scan.
					bc.res = &scanReservation{budget: &scanBudget{columns: shape.columns, rowBytes: shape.rowBytes, shared: vectorized.NewMemoryTracker(1 << 60)}}
					ok, err := decodeCursor(t.Context(), bc, false)
					require.NoError(t, err)
					require.True(t, ok)
					inRange += len(bc.timestamps)
					enc += int(bc.bm.uncompressedSizeBytes)
					retained += bc.retainedBytes()
				}
				runtime.ReadMemStats(&after)
				alloc := int64(after.TotalAlloc-before.TotalAlloc) / series
				inRangeAvg, encAvg := uint64(inRange/series), uint64(enc/series)
				bc := &blockCursor{bm: blockMetadata{count: uint64(rows), uncompressedSizeBytes: encAvg}}
				bc.bm.timestamps.max = int64(rows-1) * calibrationMinute
				bc.minTimestamp, bc.maxTimestamp = qo.minTimestamp, qo.maxTimestamp
				pre := (&scanBudget{columns: shape.columns, rowBytes: shape.rowBytes, shared: vectorized.NewMemoryTracker(1 << 60)}).sharedBlockBytes(bc)
				kept := sharedRetainedBytes(shape.rowBytes*cursorRowFactor, inRangeAvg, uint64(rows), encAvg)
				retainedReal := int64(retained / series)
				if assertMargins {
					require.GreaterOrEqualf(t, float64(kept), 1.1*float64(retainedReal), "%s rows=%d window=%v: retained envelope %d vs real %d", shape.name, rows, frac, kept, retainedReal)
				}
				if calibrate && assertMargins {
					require.GreaterOrEqualf(t, float64(pre), 1.5*float64(alloc), "%s rows=%d window=%v: pre-decode envelope %d vs allocation %d", shape.name, rows, frac, pre, alloc)
				}
				fmt.Fprintf(&out, "%-14s %5d %7d %9d | %9d %9d %9d %9d | %6.1fx %6.1fx | %6.2f %6.1f\n", shape.name, rows, inRangeAvg, encAvg,
					pre, kept, alloc, retainedReal, float64(pre)/float64(max(alloc, 1)), float64(kept)/float64(max(retainedReal, 1)),
					float64(alloc)/float64(max(encAvg, 1)), float64(alloc)/float64(uint64(rows)*shape.columns))
				for _, c := range cursors {
					releaseBlockCursor(c)
				}
			}
			releaseMemPart(mp)
		}
	}
	if calibrate {
		t.Log(out.String())
	}
}

func tagProjectionOf(family string, names ...string) []model.TagProjection {
	return []model.TagProjection{{Family: family, Names: names}}
}
