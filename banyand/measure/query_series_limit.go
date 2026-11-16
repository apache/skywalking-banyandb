// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const (
	// initialSeriesRound is the most series the first round of a limited normal-mode query asks the index for.
	initialSeriesRound = 64
	// seriesRoundGrowth is the factor the series limit grows by from one round to the next.
	seriesRoundGrowth = 4
)

// seriesRoundCap is the last series limit a round may use. A query that still lacks points after it asks for every series.
// It is a variable only so that a test can reach that fallback with few series.
var seriesRoundCap = 1 << 14

// seriesLimitRounds counts the rounds of limited normal-mode queries that asked the index for a limited number of series, and
// seriesLimitFallbacks the final rounds that asked for every series. Tests read them to prove how a query ran.
var (
	seriesLimitRounds    atomic.Int64
	seriesLimitFallbacks atomic.Int64
)

// limitsSeries reports that a normal-mode query can stop at the first series that hold mqo.Limit data points: it orders
// by an index rule, whose series order the index computes, and the plan promises that nothing after the scan needs more
// than mqo.Limit points. A time order does not use the series order of the index, so it never qualifies.
func limitsSeries(mqo model.MeasureQueryOptions) bool {
	return mqo.Limit > 0 && mqo.Order != nil && mqo.Order.Type == index.OrderByTypeIndex && mqo.Order.Index != nil &&
		mqo.GroupBy == nil && mqo.Agg == nil && mqo.Name != TopNSchemaName
}

// queryLimitedSeries serves a normal-mode query ordered by an index rule with a limit of data points. The index holds
// one document per series with no time, so it ranks series by the order tag without knowing which of them have points in
// the time range, and a series in the first K may have none. The query therefore runs in rounds. A round asks every
// segment for its first K series, merges them and scans their blocks; when the merged list is complete (no segment had
// more) or the scanned series are guaranteed to hold mqo.Limit points inside the time range, the first mqo.Limit points
// of the result are exactly those of the query over every series, and the round is the answer. Otherwise the round is
// released and K grows, until a final round asks for every series, which is the unlimited query.
//
// The caller keeps the references to segments through the rounds; the accepted result takes them over, and
// segmentsNeedRelease tells the caller's cleanup so.
func (m *measure) queryLimitedSeries(ctx context.Context, mqo model.MeasureQueryOptions, series []*pbv1.Series,
	segments []storage.Segment[*tsTable, option], segmentsNeedRelease *bool,
) (model.MeasureQueryResult, error) {
	limit := min(mqo.Limit, initialSeriesRound)
	for {
		if limit > 0 {
			seriesLimitRounds.Add(1)
		} else {
			seriesLimitFallbacks.Add(1)
		}
		result, moreSeries, err := m.queryRound(ctx, mqo, series, segments, limit)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nilResult, nil
		}
		if limit == 0 || !moreSeries || guaranteedPoints(result, mqo.TimeRange.Start.UnixNano(), mqo.TimeRange.End.UnixNano(), mqo.Limit) >= mqo.Limit {
			result.segments = segments
			*segmentsNeedRelease = false
			m.finishResult(result, mqo)
			return result, nil
		}
		result.Release()
		if limit *= seriesRoundGrowth; limit > seriesRoundCap {
			limit = 0
		}
	}
}

// guaranteedPoints returns how many distinct data points the blocks of result hold strictly inside (minTimestamp, maxTimestamp),
// counting at most stopAt of them. It reads only the timestamp column of each block, never
// a tag or field, and counts distinct timestamps per series, because the query merges the rows of a series that share a
// timestamp into one (a block can hold several versions of it). So it never exceeds what the query returns for these
// series. A block it cannot read counts as nothing.
func guaranteedPoints(result *queryResult, minTimestamp, maxTimestamp int64, stopAt int) (counted int) {
	defer func() {
		if r := recover(); r != nil {
			logger.GetLogger("measure-query-series-limit").Error().Interface("panic", r).Msg("cannot count the points of a round")
			counted = 0
		}
	}()
	// A block never holds more rows than its metadata count, so the upper bound is free; only when it reaches stopAt can
	// the round be sufficient.
	var upper uint64
	for _, bc := range result.data {
		upper += bc.bm.count
	}
	if upper < uint64(stopAt) {
		return int(upper)
	}
	seen := make(map[common.SeriesID]map[int64]struct{})
	var timestamps, versions []int64
	for _, bc := range result.data {
		if bc.bm.timestamps.max < minTimestamp || bc.bm.timestamps.min > maxTimestamp {
			continue
		}
		timestamps, versions = mustReadTimestampsFrom(timestamps[:0], versions[:0], &bc.bm.timestamps, int(bc.bm.count), bc.p.timestamps)
		points := seen[bc.bm.seriesID]
		if points == nil {
			points = make(map[int64]struct{})
			seen[bc.bm.seriesID] = points
		}
		for _, ts := range timestamps {
			// Strictly inside: whether the bounds themselves belong to the range is up to the query's range flags.
			if ts <= minTimestamp || ts >= maxTimestamp {
				continue
			}
			if _, dup := points[ts]; !dup {
				points[ts] = struct{}{}
				counted++
				if counted >= stopAt {
					return counted
				}
			}
		}
	}
	return counted
}
