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
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// calculateTargetSegments returns every target bucket overlapping the half-open
// source range [start, end); the Before(end) guard drops the empty trailing
// bucket when the source ends on a boundary. start/end are normalized to
// time.Local so the bucket math stays on the receiver's grid regardless of the
// caller's time zone.
func calculateTargetSegments(start, end time.Time, targetInterval storage.IntervalRule) []time.Time {
	start = start.In(time.Local)
	end = end.In(time.Local)
	var targetSegments []time.Time
	for current := targetInterval.Standard(start); current.Before(end); current = targetInterval.NextTime(current) {
		targetSegments = append(targetSegments, current)
	}
	return targetSegments
}

func getSegmentTimeRange(segmentStart time.Time, interval storage.IntervalRule) timestamp.TimeRange {
	segmentEnd := interval.NextTime(segmentStart)
	return timestamp.NewSectionTimeRange(segmentStart, segmentEnd)
}

// getTargetStageInterval returns the segment interval of the migration target
// (the next stage relative to the current node), as populated by parseGroup.
func getTargetStageInterval(group *GroupConfig) storage.IntervalRule {
	if group.TargetSegmentInterval != nil {
		return storage.MustToIntervalRule(group.TargetSegmentInterval)
	}

	if group.ResourceOpts != nil && group.ResourceOpts.SegmentInterval != nil {
		return storage.MustToIntervalRule(group.ResourceOpts.SegmentInterval)
	}

	return storage.IntervalRule{Unit: storage.DAY, Num: 1}
}
