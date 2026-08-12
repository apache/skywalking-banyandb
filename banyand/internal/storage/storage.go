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

// Package storage implements a time-series-based storage engine.
// It provides:
//   - Partition data based on a time axis.
//   - Sharding data based on a series id which represents a unique entity of stream/measure
//   - Retrieving data based on index.Filter.
//   - Cleaning expired data, or the data retention.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	shardPathPrefix = "shard"
	shardTemplate   = shardPathPrefix + "-%d"
	metadataPath    = "metadata"
	segTemplate     = "seg-%s"
	segPathPrefix   = "seg"

	hourFormat = "2006010215"
	dayFormat  = "20060102"

	// DirPerm is the permission of the directory.
	DirPerm = 0o700
	// SnapshotsDir is the directory for snapshots.
	SnapshotsDir = "snapshots"
	// RepairDir is the directory for repairs.
	RepairDir = "repairs"
	// DataDir is the directory for data.
	DataDir = "data"
	// FilePerm is the permission of the file.
	FilePerm = 0o600
)

var (
	// ErrUnknownShard indicates that the shard is not found.
	ErrUnknownShard = errors.New("unknown shard")
	// ErrNoCurrentSnapshot is returned when a shard has no current snapshot available.
	ErrNoCurrentSnapshot = errors.New("no current snapshot available")
	errOpenDatabase      = errors.New("fails to open the database")

	lfs = fs.NewLocalFileSystemWithLogger(logger.GetLogger("storage"))
)

// SupplyTSDB allows getting a tsdb's runtime.
type SupplyTSDB[T TSTable] func() T

// IndexSearchOpts is the options for searching index.
type IndexSearchOpts struct {
	Query       index.Query
	Order       *index.OrderBy
	TimeRange   *timestamp.TimeRange
	Projection  []index.FieldKey
	PreloadSize int
}

// FieldResult is the result of a field.
type FieldResult map[string][]byte

// FieldResultList is a list of FieldResult.
type FieldResultList []FieldResult

// SeriesData is the result of a series.
type SeriesData struct {
	SeriesList pbv1.SeriesList
	Fields     FieldResultList
	Timestamps []int64
	Versions   []int64
}

// IndexDB is the interface of index database.
type IndexDB interface {
	Insert(docs index.Documents) error
	Update(docs index.Documents) error
	Search(ctx context.Context, series []*pbv1.Series, opts IndexSearchOpts) (SeriesData, [][]byte, error)
	SearchWithoutSeries(ctx context.Context, opts IndexSearchOpts) (sd SeriesData, sortedValues [][]byte, err error)
	EnableExternalSegments() (index.ExternalSegmentStreamer, error)
	Stats() (dataCount int64, dataSizeBytes int64)
}

// TSDB allows listing and getting shard details.
type TSDB[T TSTable, O any] interface {
	io.Closer
	CreateSegmentIfNotExist(ts time.Time) (Segment[T, O], error)
	// SelectSegments returns the segments overlapping timeRange. reopenClosed=true
	// (query) reopens closed segments and marks them accessed; false (read-only
	// stats) returns them without reopening or refreshing their idle timer. The
	// caller must DecRef every returned segment (a no-op for a closed one).
	SelectSegments(timeRange timestamp.TimeRange, reopenClosed bool) ([]Segment[T, O], error)
	// PeekSegments returns lightweight descriptors of the segments overlapping
	// timeRange WITHOUT opening (incRef-ing) any of them. It lets background
	// housekeeping inspect per-shard on-disk state cheaply and decide whether a
	// segment is worth reopening for real work, avoiding a reopen storm over cold
	// segments. The returned ShardPaths are on-disk directories; the caller reads
	// them read-only.
	PeekSegments(timeRange timestamp.TimeRange) []SegmentPeek
	// SegmentInterval returns the current segment interval rule.
	SegmentInterval() IntervalRule
	Tick(ts int64)
	UpdateOptions(opts *commonv1.ResourceOpts)
	TakeFileSnapshot(dst string) (bool, error)
	GetExpiredSegmentsTimeRange() *timestamp.TimeRange
	DeleteExpiredSegments(segmentSuffixes []string) int64
	// PeekOldestSegmentEndTime returns the end time of the oldest segment.
	// Returns the zero time and false if no segments exist or retention gate cannot be acquired.
	PeekOldestSegmentEndTime() (time.Time, bool)
	// DeleteOldestSegment deletes exactly one oldest segment if it exists and meets safety rules.
	// Returns true if a segment was deleted, false otherwise.
	DeleteOldestSegment() (bool, error)
	// Drop closes the database and removes all data files from disk.
	Drop() error
}

// SegmentPeek describes a segment discoverable WITHOUT opening (incRef-ing) it — the
// segment's time range plus the on-disk paths of its existing shard directories.
// Background housekeeping (e.g. the trace finalize scanner) uses it to inspect
// per-shard on-disk state before deciding whether to reopen the segment.
type SegmentPeek struct {
	Start      time.Time
	End        time.Time
	ShardPaths []string
}

// Segment is a time range of data.
type Segment[T TSTable, O any] interface {
	DecRef()
	GetTimeRange() timestamp.TimeRange
	CreateTSTableIfNotExist(shardID common.ShardID) (T, error)
	Tables() ([]T, []Cache)
	TablesWithShardIDs() ([]T, []common.ShardID, []Cache)
	Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error)
	IndexDB() IndexDB
	// Location returns the on-disk directory of the segment.
	Location() string
	// SeriesIndexStats returns the series index document count and on-disk size.
	// It works without reopening a closed segment: an open segment is reported
	// from its live index, a closed one is read from disk read-only.
	SeriesIndexStats() (dataCount int64, dataSizeBytes int64)
}

// TSTable is time series table.
type TSTable interface {
	io.Closer
	Collect(Metrics)
	TakeFileSnapshot(dst string) (bool, error)
}

// TSTableCreator creates a TSTable.
type TSTableCreator[T TSTable, O any] func(fileSystem fs.FileSystem, root string, position common.Position,
	l *logger.Logger, timeRange timestamp.TimeRange, option O, metrics any) (T, error)

// Metrics is the interface of metrics.
type Metrics interface {
	// DeleteAll deletes all metrics.
	DeleteAll()
}

// IntervalUnit denotes the unit of a time point.
type IntervalUnit int

// Available IntervalUnits. HOUR and DAY are adequate for the APM scenario.
const (
	HOUR IntervalUnit = iota
	DAY
)

func (iu IntervalUnit) String() string {
	switch iu {
	case HOUR:
		return "hour"
	case DAY:
		return "day"
	}
	panic("invalid interval unit")
}

// Standard returns a standardized time based on the interval unit.
func (iu IntervalUnit) Standard(t time.Time) time.Time {
	switch iu {
	case HOUR:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case DAY:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}
	panic("invalid interval unit")
}

// IntervalRule defines a length of two points in time.
type IntervalRule struct {
	Unit IntervalUnit
	Num  int
}

// NextTime returns the next time point based on the current time and interval rule.
func (ir IntervalRule) NextTime(current time.Time) time.Time {
	switch ir.Unit {
	case HOUR:
		return current.Add(time.Hour * time.Duration(ir.Num))
	case DAY:
		return current.AddDate(0, 0, ir.Num)
	}
	panic("invalid interval unit")
}

// Standard aligns t down to the nearest Num*Unit boundary on a grid anchored
// at 1970-01-01 in t.Location(). Boundaries always fall on local-day midnight
// (DAY) or local-hour boundaries (HOUR), so the result format/parses cleanly
// in t.Location() and Num=1 reduces to IntervalUnit.Standard's local alignment.
// The grid is per-timezone, so two nodes in different geographic timezones may
// produce different bucket starts for the same instant.
func (ir IntervalRule) Standard(t time.Time) time.Time {
	if ir.Num <= 0 {
		logger.Panicf("interval rule Num must be positive: %+v", ir)
	}
	if ir.Num == 1 {
		return ir.Unit.Standard(t)
	}
	epochLocal := time.Date(1970, 1, 1, 0, 0, 0, 0, t.Location())
	switch ir.Unit {
	case DAY:
		todayMidnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		// +12 absorbs DST-affected days that are 23 or 25 absolute hours;
		// floorDiv handles pre-epoch (negative) inputs correctly.
		days := floorDiv(int64(todayMidnight.Sub(epochLocal).Hours()+12), 24)
		bucketIdx := floorDiv(days, int64(ir.Num))
		return time.Date(1970, 1, 1+int(bucketIdx)*ir.Num, 0, 0, 0, 0, t.Location())
	case HOUR:
		todayHour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		hours := floorDiv(int64(todayHour.Sub(epochLocal)), int64(time.Hour))
		bucketIdx := floorDiv(hours, int64(ir.Num))
		return time.Date(1970, 1, 1, int(bucketIdx)*ir.Num, 0, 0, 0, t.Location())
	}
	panic("invalid interval unit")
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func (ir IntervalRule) estimatedDuration() time.Duration {
	switch ir.Unit {
	case HOUR:
		return time.Hour * time.Duration(ir.Num)
	case DAY:
		return 24 * time.Hour * time.Duration(ir.Num)
	}
	panic("invalid interval unit")
}

// MustToIntervalRule converts a commonv1.IntervalRule to IntervalRule.
func MustToIntervalRule(ir *commonv1.IntervalRule) (result IntervalRule) {
	switch ir.Unit {
	case commonv1.IntervalRule_UNIT_DAY:
		result.Unit = DAY
	case commonv1.IntervalRule_UNIT_HOUR:
		result.Unit = HOUR
	default:
		logger.Panicf("unknown interval rule:%v", ir)
	}
	result.Num = int(ir.Num)
	return result
}
