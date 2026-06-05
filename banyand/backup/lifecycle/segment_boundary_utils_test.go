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

package lifecycle

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

func TestCalculateTargetSegments(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	tests := []struct {
		name           string
		expected       []time.Time
		targetInterval storage.IntervalRule
		partMinTS      int64
		partMaxTS      int64
	}{
		{
			name:      "2-day to 3-day segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 2, 0, 0, 0, 0, time.Local).UnixNano(), // Day 2
			partMaxTS: time.Date(2024, 1, 4, 0, 0, 0, 0, time.Local).UnixNano(), // Day 4
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expected: []time.Time{
				time.Date(2023, 12, 31, 0, 0, 0, 0, time.Local), // Day 0-3
				time.Date(2024, 1, 3, 0, 0, 0, 0, time.Local),   // Day 3-6
			},
		},
		{
			name:      "2-day to 3-day segments - part fits in single segment",
			partMinTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local).UnixNano(), // Day 1
			partMaxTS: time.Date(2024, 1, 2, 0, 0, 0, 0, time.Local).UnixNano(), // Day 2
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expected: []time.Time{
				time.Date(2023, 12, 31, 0, 0, 0, 0, time.Local), // Day 0-3
			},
		},
		{
			name:      "hour to day segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 1, 12, 0, 0, 0, time.Local).UnixNano(), // Hour 12
			partMaxTS: time.Date(2024, 1, 1, 18, 0, 0, 0, time.Local).UnixNano(), // Hour 18
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  1,
			},
			expected: []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local), // Day 1
			},
		},
		{
			name:      "day to hour segments - part spans multiple segments",
			partMinTS: time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local).UnixNano(),    // Day 1
			partMaxTS: time.Date(2024, 1, 1, 23, 59, 59, 0, time.Local).UnixNano(), // End of Day 1
			targetInterval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  6,
			},
			expected: []time.Time{
				time.Date(2024, 1, 1, 0, 0, 0, 0, time.Local),  // Hour 0-6
				time.Date(2024, 1, 1, 6, 0, 0, 0, time.Local),  // Hour 6-12
				time.Date(2024, 1, 1, 12, 0, 0, 0, time.Local), // Hour 12-18
				time.Date(2024, 1, 1, 18, 0, 0, 0, time.Local), // Hour 18-24
			},
		},
		{
			// Exclusive-end regression: a source whose end lands exactly on the
			// next target boundary must map to ONE bucket, not the empty next one.
			name:      "source end exactly on a 1-day target boundary stays single",
			partMinTS: time.Date(2026, 5, 18, 0, 0, 0, 0, time.Local).UnixNano(),
			partMaxTS: time.Date(2026, 5, 19, 0, 0, 0, 0, time.Local).UnixNano(),
			targetInterval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  1,
			},
			expected: []time.Time{
				time.Date(2026, 5, 18, 0, 0, 0, 0, time.Local),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateTargetSegments(time.Unix(0, tt.partMinTS), time.Unix(0, tt.partMaxTS), tt.targetInterval)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetSegmentTimeRange(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	tests := []struct {
		segmentStart  time.Time
		expectedStart time.Time
		expectedEnd   time.Time
		name          string
		interval      storage.IntervalRule
	}{
		{
			name:         "3-day segment",
			segmentStart: time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC),
			interval: storage.IntervalRule{
				Unit: storage.DAY,
				Num:  3,
			},
			expectedStart: time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "6-hour segment",
			segmentStart: time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),
			interval: storage.IntervalRule{
				Unit: storage.HOUR,
				Num:  6,
			},
			expectedStart: time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC),
			expectedEnd:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSegmentTimeRange(tt.segmentStart, tt.interval)
			assert.Equal(t, tt.expectedStart, result.Start)
			assert.Equal(t, tt.expectedEnd, result.End)
		})
	}
}

const (
	stageWarm      = "warm"
	stageCold      = "cold"
	stageFrozen    = "frozen"
	selectorWarm   = "type=warm"
	selectorCold   = "type=cold"
	selectorFrozen = "type=frozen"
)

func dayInterval(num uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: num}
}

func stage(name, selector string, segNum, ttlNum uint32) *commonv1.LifecycleStage {
	return &commonv1.LifecycleStage{
		Name:            name,
		SegmentInterval: dayInterval(segNum),
		Ttl:             dayInterval(ttlNum),
		NodeSelector:    selector,
	}
}

// TestGetTargetStageInterval pins every branch of getTargetStageInterval:
// the explicit TargetSegmentInterval, the ResourceOpts.SegmentInterval
// fallback, and the final 1d default.
func TestGetTargetStageInterval(t *testing.T) {
	//nolint:govet // fieldalignment: test struct optimization not critical
	type tc struct {
		name     string
		group    *GroupConfig
		expected storage.IntervalRule
	}

	cases := []tc{
		{
			name: "3-stage warm->cold returns cold's 15d (sw_metricsHour)",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(5),
						Stages: []*commonv1.LifecycleStage{
							stage(stageWarm, selectorWarm, 7, 7),
							stage(stageCold, selectorCold, 15, 30),
						},
					},
				},
				SegmentInterval:       dayInterval(7),
				TargetSegmentInterval: dayInterval(15),
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 15},
		},
		{
			name: "3-stage warm->cold returns cold's 5d (sw_metricsMinute)",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(1),
						Stages: []*commonv1.LifecycleStage{
							stage(stageWarm, selectorWarm, 3, 7),
							stage(stageCold, selectorCold, 5, 30),
						},
					},
				},
				SegmentInterval:       dayInterval(3),
				TargetSegmentInterval: dayInterval(5),
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 5},
		},
		{
			name: "2-stage hot->cold returns cold's interval",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(1),
						Stages: []*commonv1.LifecycleStage{
							stage(stageCold, selectorCold, 15, 30),
						},
					},
				},
				SegmentInterval:       dayInterval(1),
				TargetSegmentInterval: dayInterval(15),
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 15},
		},
		{
			name: "4-stage chain warm->cold picks cold, not the last stage",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(1),
						Stages: []*commonv1.LifecycleStage{
							stage(stageWarm, selectorWarm, 3, 7),
							stage(stageCold, selectorCold, 15, 30),
							stage(stageFrozen, selectorFrozen, 30, 365),
						},
					},
				},
				SegmentInterval:       dayInterval(3),
				TargetSegmentInterval: dayInterval(15),
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 15},
		},
		{
			name: "fallback to ResourceOpts.SegmentInterval when TargetSegmentInterval is nil",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(7),
					},
				},
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 7},
		},
		{
			name: "fallback to default 1d when nothing is set",
			group: &GroupConfig{
				Group: &commonv1.Group{ResourceOpts: &commonv1.ResourceOpts{}},
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 1},
		},
		{
			name: "fallback handles nil ResourceOpts",
			group: &GroupConfig{
				Group: &commonv1.Group{},
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 1},
		},
		{
			name: "TargetSegmentInterval wins over ResourceOpts.SegmentInterval",
			group: &GroupConfig{
				Group: &commonv1.Group{
					ResourceOpts: &commonv1.ResourceOpts{
						SegmentInterval: dayInterval(1),
						Stages: []*commonv1.LifecycleStage{
							stage(stageWarm, selectorWarm, 7, 7),
							stage(stageCold, selectorCold, 15, 30),
						},
					},
				},
				SegmentInterval:       dayInterval(7),
				TargetSegmentInterval: dayInterval(15),
			},
			expected: storage.IntervalRule{Unit: storage.DAY, Num: 15},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := getTargetStageInterval(c.group)
			assert.Equal(t, c.expected, got,
				"target stage interval mismatch: expected %d(%s), got %d(%s)",
				c.expected.Num, c.expected.Unit.String(),
				got.Num, got.Unit.String())
		})
	}
}

// TestGetTargetStageInterval_ConcurrentReaders pins down that the function is
// safe for concurrent readers - lifecycle migration code calls it from
// multiple goroutines (per-shard / per-group fan-out).
func TestGetTargetStageInterval_ConcurrentReaders(t *testing.T) {
	group := &GroupConfig{
		Group: &commonv1.Group{
			ResourceOpts: &commonv1.ResourceOpts{
				SegmentInterval: dayInterval(5),
				Stages: []*commonv1.LifecycleStage{
					stage(stageWarm, selectorWarm, 7, 7),
					stage(stageCold, selectorCold, 15, 30),
				},
			},
		},
		SegmentInterval:       dayInterval(7),
		TargetSegmentInterval: dayInterval(15),
	}
	expected := storage.IntervalRule{Unit: storage.DAY, Num: 15}

	const goroutines = 64
	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				got := getTargetStageInterval(group)
				if got != expected {
					t.Errorf("concurrent read returned %v, expected %v", got, expected)
					return
				}
			}
		}()
	}
	wg.Wait()
}
