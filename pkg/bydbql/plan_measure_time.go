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

package bydbql

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/xhit/go-str2duration/v2"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	minMeasureMillis = math.MinInt64 / int64(time.Millisecond)
	maxMeasureMillis = math.MaxInt64 / int64(time.Millisecond)
	measureEndMillis = maxMeasureMillis + 1
)

var fixedIntervalPattern = regexp.MustCompile(`^(?:[0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s|m|h|d|w))+$`)

func allMeasureTime() []ql.MillisRange {
	return []ql.MillisRange{{Begin: minMeasureMillis, End: measureEndMillis}}
}

func fixedInterval(adjustment *GrammarIntervalAdjustment) (time.Duration, error) {
	if !fixedIntervalPattern.MatchString(adjustment.Duration) {
		return 0, fmt.Errorf("INTERVAL %q is not a fixed duration", adjustment.Duration)
	}
	duration, err := str2duration.ParseDuration(adjustment.Duration)
	if err != nil {
		return 0, fmt.Errorf("invalid INTERVAL: %w", err)
	}
	if duration < 0 {
		return 0, fmt.Errorf("INTERVAL is out of range")
	}
	if adjustment.Sign == "-" {
		duration = -duration
	}
	return duration, nil
}

func bindTimeComparison(run *transformRun, now time.Time, comparison *GrammarCompareTail) ([]ql.MillisRange, error) {
	value := comparison.Value
	var bound time.Time
	if value.Now != nil {
		bound = now
	} else {
		var text string
		switch {
		case value.Param:
			parameter := run.bound[value.ParamIndex]
			text = parameter.timeStr
			if parameter.expression != nil {
				switch parameter.expression.Type {
				case ql.ExpressionString:
					text = parameter.expression.Literal.String
				case ql.ExpressionTimestamp:
					text = time.Unix(0, parameter.expression.Literal.Int64).UTC().Format(time.RFC3339Nano)
				default:
					return nil, fmt.Errorf("time comparison requires timestamp strings or timestamp parameters")
				}
			}
		case value.String != nil:
			text = *value.String
		default:
			return nil, fmt.Errorf("invalid time expression")
		}
		if strings.EqualFold(text, "now") {
			bound = now
		} else {
			parsed, err := time.Parse(time.RFC3339Nano, text)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp %q: %w", text, err)
			}
			bound = parsed
		}
	}
	if comparison.Adjustment != nil {
		duration, err := fixedInterval(comparison.Adjustment)
		if err != nil {
			return nil, err
		}
		bound = bound.Add(duration)
	}
	if bound.Before(time.Unix(0, timestamp.MinNanoTime)) || bound.After(time.Unix(0, timestamp.MaxNanoTime)) {
		return nil, fmt.Errorf("time bound is outside the stored timestamp domain")
	}
	floor := bound.UnixMilli()
	ceil := floor
	aligned := bound.Nanosecond()%int(time.Millisecond) == 0
	if !aligned {
		ceil++
	}
	interval := ql.MillisRange{Begin: minMeasureMillis, End: measureEndMillis}
	switch comparison.Operator {
	case "=":
		if !aligned {
			return nil, nil
		}
		interval.Begin, interval.End = floor, floor+1
	case "!=":
		if !aligned {
			return allMeasureTime(), nil
		}
		return normalizeTimeRanges([]ql.MillisRange{{Begin: minMeasureMillis, End: floor}, {Begin: floor + 1, End: measureEndMillis}}), nil
	case ">":
		interval.Begin = floor + 1
	case ">=":
		interval.Begin = ceil
	case "<":
		interval.End = ceil
	case "<=":
		interval.End = floor + 1
	default:
		return nil, fmt.Errorf("unsupported time comparator %q", comparison.Operator)
	}
	return normalizeTimeRanges([]ql.MillisRange{interval}), nil
}

func nativeTimeIntervals(value *modelv1.TimeRange) ([]ql.MillisRange, error) {
	if err := timestamp.CheckTimeRange(value); err != nil {
		return nil, fmt.Errorf("invalid legacy TIME range: %w", err)
	}
	return normalizeTimeRanges([]ql.MillisRange{{Begin: value.Begin.AsTime().UnixMilli(), End: value.End.AsTime().UnixMilli() + 1}}), nil
}

func normalizeTimeRanges(ranges []ql.MillisRange) []ql.MillisRange {
	slices.SortFunc(ranges, func(a, b ql.MillisRange) int {
		if a.Begin < b.Begin {
			return -1
		}
		if a.Begin > b.Begin {
			return 1
		}
		return 0
	})
	result := make([]ql.MillisRange, 0, len(ranges))
	for _, interval := range ranges {
		interval.Begin = max(interval.Begin, minMeasureMillis)
		interval.End = min(interval.End, measureEndMillis)
		if interval.Begin >= interval.End {
			continue
		}
		if len(result) > 0 && interval.Begin <= result[len(result)-1].End {
			result[len(result)-1].End = max(result[len(result)-1].End, interval.End)
		} else {
			result = append(result, interval)
		}
	}
	return result
}

func unionTimeRanges(left, right []ql.MillisRange) []ql.MillisRange {
	return normalizeTimeRanges(append(slices.Clone(left), right...))
}

func intersectTimeRanges(left, right []ql.MillisRange) []ql.MillisRange {
	var result []ql.MillisRange
	for i, j := 0, 0; i < len(left) && j < len(right); {
		begin, end := max(left[i].Begin, right[j].Begin), min(left[i].End, right[j].End)
		if begin < end {
			result = append(result, ql.MillisRange{Begin: begin, End: end})
		}
		if left[i].End < right[j].End {
			i++
		} else {
			j++
		}
	}
	return result
}
