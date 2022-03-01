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

package tsdb

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var ErrEndOfSegment = errors.New("reached the end of the segment")

type segment struct {
	id     uint16
	path   string
	suffix string

	globalIndex kv.Store
	l           *logger.Logger
	timestamp.TimeRange
	bucket.Reporter
	blockController     *blockController
	blockManageStrategy *bucket.Strategy
}

func openSegment(ctx context.Context, startTime time.Time, path, suffix string, segmentSize, blockSize IntervalRule, blockQueue bucket.Queue) (s *segment, err error) {
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	// TODO: fix id overflow
	id := uint16(segmentSize.Unit)<<12 | ((uint16(suffixInteger) << 4) >> 4)
	timeRange := timestamp.NewTimeRange(startTime, segmentSize.NextTime(startTime), true, false)
	l := logger.Fetch(ctx, "segment")
	s = &segment{
		id:              id,
		path:            path,
		suffix:          suffix,
		l:               l,
		blockController: newBlockController(id, path, timeRange, blockSize, l, blockQueue),
		TimeRange:       timeRange,
		Reporter:        bucket.NewTimeBasedReporter(timeRange),
	}
	err = s.blockController.open(context.WithValue(ctx, logger.ContextKey, s.l))
	if err != nil {
		return nil, err
	}

	indexPath, err := mkdir(globalIndexTemplate, path)
	if err != nil {
		return nil, err
	}
	if s.globalIndex, err = kv.OpenStore(0, indexPath, kv.StoreWithLogger(s.l)); err != nil {
		return nil, err
	}
	s.blockManageStrategy, err = bucket.NewStrategy(s.blockController, bucket.WithLogger(s.l))
	if err != nil {
		return nil, err
	}
	s.blockManageStrategy.Run()
	return s, nil
}

func (s *segment) close() {
	s.blockManageStrategy.Close()
	s.blockController.close()
	s.globalIndex.Close()
	s.Stop()
}

func (s *segment) closeBlock(id uint16) {
	s.blockController.closeBlock(id)
}

func (s segment) String() string {
	return s.Reporter.String()
}

type blockController struct {
	sync.RWMutex
	segID        uint16
	location     string
	segTimeRange timestamp.TimeRange
	blockSize    IntervalRule
	lst          []*block
	blockQueue   bucket.Queue

	l *logger.Logger
}

func newBlockController(segID uint16, location string, segTimeRange timestamp.TimeRange,
	blockSize IntervalRule, l *logger.Logger, blockQueue bucket.Queue) *blockController {
	return &blockController{
		segID:        segID,
		location:     location,
		blockSize:    blockSize,
		segTimeRange: segTimeRange,
		blockQueue:   blockQueue,
		l:            l,
	}
}

func (bc *blockController) Current() bucket.Reporter {
	bc.RLock()
	defer bc.RUnlock()
	now := time.Now()
	for _, s := range bc.lst {
		if s.suffix == bc.Format(now) {
			return s
		}
	}
	// return the latest segment before now
	if len(bc.lst) > 0 {
		return bc.lst[len(bc.lst)-1]
	}
	return nil
}

func (bc *blockController) Next() (bucket.Reporter, error) {
	b := bc.Current().(*block)
	reporter, err := bc.create(context.TODO(),
		bc.blockSize.NextTime(b.Start))
	if errors.Is(err, ErrEndOfSegment) {
		return nil, bucket.ErrNoMoreBucket
	}
	err = reporter.open()
	if err != nil {
		return nil, err
	}
	return reporter, err
}

func (bc *blockController) OnMove(prev bucket.Reporter, next bucket.Reporter) {
	bc.l.Info().Stringer("prev", prev).Stringer("next", next).Msg("move to the next block")
	bc.blockQueue.Push(BlockID{
		SegID:   bc.segID,
		BlockID: prev.(*block).blockID,
	})
}

func (bc *blockController) Format(tm time.Time) string {
	switch bc.blockSize.Unit {
	case HOUR:
		return tm.Format(blockHourFormat)
	case DAY:
		return tm.Format(blockDayFormat)
	case MILLISECOND:
		return tm.Format(millisecondFormat)
	}
	panic("invalid interval unit")
}

func (bc *blockController) Parse(value string) (time.Time, error) {
	switch bc.blockSize.Unit {
	case HOUR:
		return time.Parse(blockHourFormat, value)
	case DAY:
		return time.Parse(blockDayFormat, value)
	case MILLISECOND:
		return time.Parse(millisecondFormat, value)
	}
	panic("invalid interval unit")
}

func (bc *blockController) span(timeRange timestamp.TimeRange) (bb []*block) {
	return bc.ensureBlockOpen(bc.search(func(b *block) bool {
		return b.Overlapping(timeRange)
	}))
}

func (bc *blockController) get(blockID uint16) *block {
	b := bc.getBlock(blockID)
	if b != nil {
		return bc.ensureBlockOpen([]*block{b})[0]
	}
	return nil
}

func (bc *blockController) getBlock(blockID uint16) *block {
	bb := bc.search(func(b *block) bool {
		return b.blockID == blockID
	})
	if len(bb) > 0 {
		return bb[0]
	}
	return nil
}

func (bc *blockController) blocks() []*block {
	bc.RLock()
	defer bc.RUnlock()
	r := make([]*block, len(bc.lst))
	copy(r, bc.lst)
	return r
}

func (bc *blockController) search(matcher func(*block) bool) (bb []*block) {
	snapshotLst := func() []*block {
		bc.RLock()
		defer bc.RUnlock()
		return bc.lst
	}
	lst := snapshotLst()
	last := len(lst) - 1
	for i := range lst {
		b := lst[last-i]
		if matcher(b) {
			bb = append(bb, b)
		}
	}
	return bb
}

func (bc *blockController) ensureBlockOpen(blocks []*block) (openedBlocks []*block) {
	if blocks == nil {
		return nil
	}
	for _, b := range blocks {
		if b.isClosed() {
			if err := b.open(); err != nil {
				bc.l.Error().Err(err).Stringer("block", b).Msg("fail to open block")
				continue
			}
		}
		openedBlocks = append(openedBlocks, b)
		bc.blockQueue.Push(BlockID{
			BlockID: b.blockID,
			SegID:   b.segID,
		})
	}
	return openedBlocks
}

func (bc *blockController) closeBlock(blockID uint16) {
	bc.RLock()
	defer bc.RUnlock()
	b := bc.getBlock(blockID)
	if b == nil {
		return
	}
	b.close()
}

func (bc *blockController) startTime(suffix string) (time.Time, error) {
	t, err := bc.Parse(suffix)
	if err != nil {
		return time.Time{}, err
	}
	startTime := bc.segTimeRange.Start
	switch bc.blockSize.Unit {
	case HOUR:
		return time.Date(startTime.Year(), startTime.Month(),
			startTime.Day(), t.Hour(), 0, 0, 0, startTime.Location()), nil
	case DAY:
		return time.Date(startTime.Year(), startTime.Month(),
			t.Day(), t.Hour(), 0, 0, 0, startTime.Location()), nil
	case MILLISECOND:
		return time.ParseInLocation(millisecondFormat, suffix, startTime.Location())
	}
	panic("invalid interval unit")
}

func (bc *blockController) open(ctx context.Context) error {
	err := WalkDir(
		bc.location,
		segPathPrefix,
		func(suffix, absolutePath string) error {
			_, err := bc.load(ctx, suffix, absolutePath)
			return err
		})
	if err != nil {
		return err
	}
	if bc.Current() == nil {
		b, err := bc.create(ctx, time.Now())
		if err != nil {
			return err
		}
		if err = b.open(); err != nil {
			return err
		}
	}
	return nil
}

func (bc *blockController) create(ctx context.Context, startTime time.Time) (*block, error) {
	if startTime.Before(bc.segTimeRange.Start) {
		startTime = bc.segTimeRange.Start
	}
	if !startTime.Before(bc.segTimeRange.End) {
		return nil, ErrEndOfSegment
	}
	suffix := bc.Format(startTime)
	segPath, err := mkdir(blockTemplate, bc.location, suffix)
	if err != nil {
		return nil, err
	}
	return bc.load(ctx, suffix, segPath)
}

func (bc *blockController) load(ctx context.Context, suffix, path string) (b *block, err error) {
	starTime, err := bc.startTime(suffix)
	if err != nil {
		return nil, err
	}
	if b, err = newBlock(ctx, blockOpts{
		segID:     bc.segID,
		path:      path,
		startTime: starTime,
		suffix:    suffix,
		blockSize: bc.blockSize,
	}); err != nil {
		return nil, err
	}
	bc.Lock()
	defer bc.Unlock()
	bc.lst = append(bc.lst, b)
	return b, nil
}

func (bc *blockController) close() {
	for _, s := range bc.lst {
		s.close()
	}
}
