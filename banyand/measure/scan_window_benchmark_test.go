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
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// BenchmarkVectorizedScan times only the storage scan: PullBatch until EOF over
// the gate layouts, releasing every batch, with the window scan and with the
// legacy decode-everything-then-merge scan in the same binary. Unlike
// BenchmarkVectorizedStorage there is no pipeline or egress, whose per-row
// allocations otherwise dominate and hide what the scan costs.
//
//	go test ./banyand/measure -run '^$' -bench BenchmarkVectorizedScan -benchmem -count=6
func BenchmarkVectorizedScan(b *testing.B) {
	hs := newGateHarnesses(b)
	scanOpts := model.MeasureQueryOptions{
		TagProjection: gateProj(gateTSTag, gateTagEnv), FieldProjection: []string{gateFieldInt, gateFieldFlt},
	}
	layouts := []struct {
		name        string
		layout      gateLayout
		timeOrdered bool
	}{
		{"single_series_10k", gateLayoutSingle, false},
		{"multi_series_one_part", gateLayoutMulti, false},
		{"multi_series_4parts_disjoint", gateLayoutDisjoint, false},
		{"multi_series_2parts_overlap", gateLayoutOverlap, false},
		// No ORDER BY: the series share one time range, so their rows interleave.
		{"time_ordered_multi_series_one_part", gateLayoutMulti, true},
		{"time_ordered_multi_series_4parts_disjoint", gateLayoutDisjoint, true},
		{"time_ordered_multi_series_2parts_overlap", gateLayoutOverlap, true},
	}
	for _, l := range layouts {
		h := hs.get(l.layout)
		for _, legacy := range []bool{false, true} {
			name := "window"
			if legacy {
				name = "legacy"
			}
			b.Run(l.name+"/"+name, func(b *testing.B) {
				defer setLegacyBatchScan(setLegacyBatchScan(legacy))
				schema := gateSchema(false, scanOpts)
				b.ReportAllocs()
				b.ResetTimer()
				rows := 0
				for b.Loop() {
					qr := h.newQueryResult(schema, scanOpts, l.timeOrdered)
					for {
						mb, err := qr.PullBatch(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						if mb == nil {
							break
						}
						rows += mb.ActiveRows()
						mb.Release()
					}
					qr.Release()
				}
				gateSink = rows
			})
		}
	}
}
