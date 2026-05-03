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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// seriesRow is the ephemeral per-row view exposed by SeriesCursor.NextRow.
// The slices and pointers reference data owned by the underlying MeasureResult;
// callers must not retain a row past the next NextRow call.
type seriesRow struct {
	tagFamilies []model.TagFamily
	fields      []model.Field
	timestamp   int64
	version     int64
	sid         common.SeriesID
	shardID     common.ShardID
	rowIdx      int
}

// SeriesCursor walks across MeasureResult instances yielded by a MeasureQueryResult,
// presenting either single-row access (NextRow) or bulk-copy access (Current+Advance)
// to BatchScan.
//
// Sticky-error contract: once Pull yields a result with Error != nil, the
// cursor stores the error and every subsequent NextRow returns it. Init is
// the only way to reset.
type SeriesCursor struct {
	qr        model.MeasureQueryResult
	current   *model.MeasureResult
	err       error
	pos       int
	exhausted bool
}

// Init resets the cursor and advances to the first non-empty series (or EOF/err).
func (c *SeriesCursor) Init(qr model.MeasureQueryResult) {
	c.qr = qr
	c.current = nil
	c.pos = 0
	c.err = nil
	c.exhausted = false
	c.advanceSeries()
}

// Err returns the sticky storage error, or nil. Once non-nil it remains so
// until Init is called again.
func (c *SeriesCursor) Err() error { return c.err }

// Exhausted reports whether the cursor has run out of input (clean EOF or
// after an error).
func (c *SeriesCursor) Exhausted() bool { return c.exhausted }

// RemainingInSeries returns how many rows are left in the current MeasureResult.
// 0 when at EOF, on error, or between series.
func (c *SeriesCursor) RemainingInSeries() int {
	if c.exhausted || c.current == nil {
		return 0
	}
	return len(c.current.Timestamps) - c.pos
}

// Current returns the underlying MeasureResult and the cursor's position within
// it. Used by BatchScan's bulk-copy path to read parallel arrays directly.
func (c *SeriesCursor) Current() (*model.MeasureResult, int) {
	return c.current, c.pos
}

// Advance moves the cursor n rows forward. Crosses to the next series if the
// new position reaches the end of the current MeasureResult.
func (c *SeriesCursor) Advance(n int) {
	c.pos += n
	if c.current != nil && c.pos >= len(c.current.Timestamps) {
		c.advanceSeries()
	}
}

// NextRow returns the next row across series boundaries.
//
// Returns:
//   - (row, nil)         when a row is available;
//   - (zero, nil)        on clean EOF;
//   - (zero, stickyErr)  if a storage error has been observed; subsequent calls
//     return the same error until Init is called again.
func (c *SeriesCursor) NextRow() (seriesRow, error) {
	if c.err != nil {
		return seriesRow{}, c.err
	}
	if c.exhausted {
		return seriesRow{}, nil
	}
	if c.current == nil || c.pos >= len(c.current.Timestamps) {
		c.advanceSeries()
		if c.err != nil {
			return seriesRow{}, c.err
		}
		if c.exhausted {
			return seriesRow{}, nil
		}
	}
	row := seriesRow{
		sid:         c.current.SID,
		timestamp:   c.current.Timestamps[c.pos],
		version:     c.current.Versions[c.pos],
		rowIdx:      c.pos,
		tagFamilies: c.current.TagFamilies,
		fields:      c.current.Fields,
	}
	if c.pos < len(c.current.ShardIDs) {
		row.shardID = c.current.ShardIDs[c.pos]
	}
	c.pos++
	return row, nil
}

// Close releases the underlying MeasureQueryResult exactly once. Idempotent
// and safe after EOF.
func (c *SeriesCursor) Close() {
	if c.qr != nil {
		c.qr.Release()
		c.qr = nil
	}
	c.exhausted = true
}

// advanceSeries pulls successive MeasureResults until one yields rows, EOF is
// reached, or an error is observed. Empty results (zero timestamps) are skipped.
func (c *SeriesCursor) advanceSeries() {
	for {
		next := c.qr.Pull()
		if next == nil {
			c.exhausted = true
			c.current = nil
			return
		}
		if next.Error != nil {
			c.err = next.Error
			c.exhausted = true
			c.current = nil
			return
		}
		if len(next.Timestamps) == 0 {
			continue
		}
		c.current = next
		c.pos = 0
		return
	}
}
