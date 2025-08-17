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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func calculateTargetSegments(partMinTS, partMaxTS int64, targetInterval storage.IntervalRule) []time.Time {
	minTime := time.Unix(0, partMinTS).UTC()
	maxTime := time.Unix(0, partMaxTS).UTC()

	var targetSegments []time.Time

	var segmentStart time.Time
	switch targetInterval.Unit {
	case storage.DAY:
		daysSinceEpoch := minTime.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24
		segmentIndex := int(daysSinceEpoch) / targetInterval.Num
		segmentStart = time.Date(1970, 1, 1+segmentIndex*targetInterval.Num, 0, 0, 0, 0, time.UTC)
	case storage.HOUR:
		hoursSinceEpoch := minTime.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours()
		segmentIndex := int(hoursSinceEpoch) / targetInterval.Num
		segmentStart = time.Date(1970, 1, 1, segmentIndex*targetInterval.Num, 0, 0, 0, time.UTC)
	default:
		segmentStart = targetInterval.Unit.Standard(minTime)
	}

	current := segmentStart
	for !current.After(maxTime) {
		segmentEnd := targetInterval.NextTime(current)
		if !(segmentEnd.Before(minTime) || current.After(maxTime)) {
			targetSegments = append(targetSegments, current)
		}
		current = segmentEnd
	}

	return targetSegments
}

func getSegmentTimeRange(segmentStart time.Time, interval storage.IntervalRule) timestamp.TimeRange {
	segmentEnd := interval.NextTime(segmentStart)
	return timestamp.NewSectionTimeRange(segmentStart, segmentEnd)
}

func getTargetStageInterval(group *commonv1.Group) storage.IntervalRule {
	if group.ResourceOpts != nil && len(group.ResourceOpts.Stages) > 0 {
		stage := group.ResourceOpts.Stages[0]
		if stage.SegmentInterval != nil {
			return storage.MustToIntervalRule(stage.SegmentInterval)
		}
	}

	if group.ResourceOpts != nil && group.ResourceOpts.SegmentInterval != nil {
		return storage.MustToIntervalRule(group.ResourceOpts.SegmentInterval)
	}

	return storage.IntervalRule{Unit: storage.DAY, Num: 1}
}
