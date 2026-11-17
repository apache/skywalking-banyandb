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
	// initialSeriesRound is how many series the first round of a limited normal-mode query asks the index for.
	initialSeriesRound = 64
	// seriesRoundGrowth is the factor the series limit grows by from one round to the next.
	seriesRoundGrowth = 16
	// guaranteedPointsCheckEvery is how many blocks guaranteedPoints reads between two checks of the context.
	guaranteedPointsCheckEvery = 16
)

// Test-only hooks. Production code never changes them.
var (
	// seriesRoundCap is the last series limit a round may use; a query that still lacks points after it asks for every
	// series. It is a variable only so that a test can reach that fallback with few series.
	seriesRoundCap = 1 << 14
	// seriesLimitRounds counts the rounds of limited normal-mode queries that asked the index for a limited number of
	// series, and seriesLimitFallbacks the final rounds that asked for every series.
	seriesLimitRounds    atomic.Int64
	seriesLimitFallbacks atomic.Int64
	// seriesLimitCountPanics counts the rounds whose points could not be counted because a timestamp read panicked.
	seriesLimitCountPanics atomic.Int64
	// seriesRoundObserver, when set, is called with the series limit (0 for the final round) before each round runs.
	seriesRoundObserver func(limit int)
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
// K runs 64, 1024, 16384 and then every series: at most three limited rounds. Each round costs about what ranking and
// scanning its K series costs, so with growth by 16 the limited rounds before the unlimited one cost at most about 1/15 more
// than the last of them, and that last one asks for no more than 16384 series. A query that needs the unlimited round pays
// the three limited ones on top of it; a query that stops early pays only for the series it needs.
//
// The caller keeps the references to segments through the rounds; the accepted result takes them over, and
// segmentsNeedRelease tells the caller's cleanup so.
func (m *measure) queryLimitedSeries(ctx context.Context, mqo model.MeasureQueryOptions, series []*pbv1.Series,
	segments []storage.Segment[*tsTable, option], segmentsNeedRelease *bool,
) (model.MeasureQueryResult, error) {
	limit := initialSeriesRound
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if seriesRoundObserver != nil {
			seriesRoundObserver(limit)
		}
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
		enough := limit == 0 || !moreSeries
		if !enough && pointsUpperBound(result) >= uint64(mqo.Limit) {
			counted, countErr := guaranteedPoints(ctx, result, mqo.TimeRange.Start.UnixNano(), mqo.TimeRange.End.UnixNano(), mqo.Limit)
			if countErr != nil {
				result.Release()
				return nil, countErr
			}
			enough = counted >= mqo.Limit
		}
		if enough {
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

// pointsUpperBound returns the most data points result can hold: the rows of its blocks, which costs no read. A round whose
// bound is below the limit cannot be sufficient.
func pointsUpperBound(result *queryResult) uint64 {
	var upper uint64
	for _, bc := range result.data {
		upper += bc.bm.count
	}
	return upper
}

// guaranteedPoints returns how many distinct data points the blocks of result hold strictly inside (minTimestamp, maxTimestamp),
// counting at most stopAt of them. It reads only the timestamp column of each block, never
// a tag or field, and counts distinct timestamps per series, because the query merges the rows of a series that share a
// timestamp into one (a block can hold several versions of it). So it never exceeds what the query returns for these
// series. It returns the context's error when the context ends. A timestamp read that panics (the reader reports a corrupt
// block that way) is logged and counted, and the round counts as holding nothing, so the query takes the next round.
func guaranteedPoints(ctx context.Context, result *queryResult, minTimestamp, maxTimestamp int64, stopAt int) (counted int, err error) {
	defer func() {
		if r := recover(); r != nil {
			seriesLimitCountPanics.Add(1)
			logger.GetLogger("measure-query-series-limit").Error().Interface("panic", r).Msg("cannot count the points of a round")
			counted, err = 0, nil
		}
	}()
	seen := make(map[common.SeriesID]map[int64]struct{})
	var timestamps, versions []int64
	for i, bc := range result.data {
		if i%guaranteedPointsCheckEvery == 0 {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
		}
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
					return counted, nil
				}
			}
		}
	}
	return counted, nil
}
