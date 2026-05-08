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
	SelectSegments(timeRange timestamp.TimeRange) ([]Segment[T, O], error)
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

// Segment is a time range of data.
type Segment[T TSTable, O any] interface {
	DecRef()
	GetTimeRange() timestamp.TimeRange
	CreateTSTableIfNotExist(shardID common.ShardID) (T, error)
	Tables() ([]T, []Cache)
	TablesWithShardIDs() ([]T, []common.ShardID, []Cache)
	Lookup(ctx context.Context, series []*pbv1.Series) (pbv1.SeriesList, error)
	IndexDB() IndexDB
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

// epochUTC is the Unix epoch as a UTC time. Used as the anchor for the
// grid produced by IntervalRule.Standard.
var epochUTC = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// Standard aligns t down to the nearest Num*Unit boundary. The grid origin
// is anchored at the Unix epoch in UTC for cross-node consistency, but the
// returned time is in t.Location() so callers see their own timezone. For
// Num<=1 it delegates to IntervalUnit.Standard, preserving the existing
// per-unit local alignment.
func (ir IntervalRule) Standard(t time.Time) time.Time {
	if ir.Num <= 1 {
		return ir.Unit.Standard(t)
	}
	var bucketWidth time.Duration
	switch ir.Unit {
	case DAY:
		bucketWidth = time.Duration(ir.Num) * 24 * time.Hour
	case HOUR:
		bucketWidth = time.Duration(ir.Num) * time.Hour
	default:
		panic("invalid interval unit")
	}
	width := bucketWidth.Nanoseconds()
	nanos := t.Sub(epochUTC).Nanoseconds()
	return epochUTC.Add(time.Duration(floorDiv(nanos, width) * width)).In(t.Location())
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
