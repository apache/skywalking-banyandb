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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
)

// recordedGauge keeps one value per label combination, like a real gauge vector.
type recordedGauge struct {
	series map[string]float64
}

func (g *recordedGauge) Set(value float64, labelValues ...string) {
	if g.series == nil {
		g.series = make(map[string]float64)
	}
	g.series[strings.Join(labelValues, "/")] = value
}

func (g *recordedGauge) Add(delta float64, labelValues ...string) {
	if g.series == nil {
		g.series = make(map[string]float64)
	}
	g.series[strings.Join(labelValues, "/")] += delta
}

func (g *recordedGauge) Delete(labelValues ...string) bool {
	key := strings.Join(labelValues, "/")
	_, ok := g.series[key]
	delete(g.series, key)
	return ok
}

func newUnsyncedTestMetrics() *metrics {
	return &metrics{
		tbMetrics: tbMetrics{
			unsyncedParts:                &recordedGauge{},
			oldestUnsyncedPartAgeSeconds: &recordedGauge{},
		},
	}
}

func newUnsyncedTestTable(m *metrics, parts ...*partWrapper) *tsTable {
	return &tsTable{
		p:        common.Position{Segment: "seg-1"},
		metrics:  m,
		snapshot: &snapshot{parts: parts, ref: 1},
	}
}

func gaugeValue(t *testing.T, g any) float64 {
	t.Helper()
	rg := g.(*recordedGauge)
	require.Len(t, rg.series, 1)
	v, ok := rg.series["seg-1/0"]
	require.True(t, ok, "series seg-1/0 must exist, got %v", rg.series)
	return v
}

func TestCollectUnsynced(t *testing.T) {
	now := time.Now()
	stamped := func(size uint64, age time.Duration) *partWrapper {
		return &partWrapper{p: &part{partMetadata: partMetadata{TotalCount: 1, CompressedSizeBytes: size, QueuedAtUnixNano: now.Add(-age).UnixNano()}}}
	}
	unstamped := func(size uint64) *partWrapper {
		return &partWrapper{p: &part{partMetadata: partMetadata{TotalCount: 1, CompressedSizeBytes: size}}}
	}
	mem := func(size uint64, age time.Duration) *partWrapper {
		pw := stamped(size, age)
		pw.mp = &memPart{}
		return pw
	}
	tests := []struct {
		name      string
		parts     []*partWrapper
		wantParts float64
		wantAge   float64
	}{
		{name: "empty snapshot"},
		{name: "unstamped part after stamped", parts: []*partWrapper{stamped(20, time.Minute), unstamped(10)}, wantParts: 2, wantAge: 60},
		{name: "unstamped part before stamped", parts: []*partWrapper{unstamped(10), stamped(20, time.Minute)}, wantParts: 2, wantAge: 60},
		{name: "queue time in the future", parts: []*partWrapper{stamped(5, -time.Hour)}, wantParts: 1},
		{name: "oldest part first", parts: []*partWrapper{stamped(10, 2*time.Minute), stamped(20, time.Minute)}, wantParts: 2, wantAge: 120},
		{name: "oldest across mem and file parts", parts: []*partWrapper{stamped(100, time.Minute), mem(5, 2*time.Minute)}, wantParts: 2, wantAge: 120},
		{
			name: "file part without rows is never synced",
			parts: []*partWrapper{
				{p: &part{partMetadata: partMetadata{CompressedSizeBytes: 5, QueuedAtUnixNano: now.Add(-time.Hour).UnixNano()}}},
				stamped(20, time.Minute),
			},
			wantParts: 1, wantAge: 60,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newUnsyncedTestMetrics()
			tst := newUnsyncedTestTable(m, tt.parts...)
			tst.collectUnsynced()
			require.Equal(t, int32(1), tst.snapshot.ref, "collectUnsynced must release the snapshot reference it takes")
			require.Equal(t, tt.wantParts, gaugeValue(t, m.unsyncedParts))
			if tt.wantAge == 0 {
				require.Zero(t, gaugeValue(t, m.oldestUnsyncedPartAgeSeconds))
			} else {
				require.InDelta(t, tt.wantAge, gaugeValue(t, m.oldestUnsyncedPartAgeSeconds), 5)
			}
		})
	}
}
