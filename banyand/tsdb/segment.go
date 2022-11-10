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
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
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
	closeOnce           sync.Once
}

func openSegment(ctx context.Context, startTime time.Time, path, suffix string,
	segmentSize, blockSize IntervalRule, blockQueue bucket.Queue,
) (s *segment, err error) {
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	id := GenerateInternalID(segmentSize.Unit, suffixInteger)
	timeRange := timestamp.NewSectionTimeRange(startTime, segmentSize.NextTime(startTime))
	l := logger.Fetch(ctx, "segment")
	segCtx := context.WithValue(ctx, logger.ContextKey, l)
	clock, segCtx := timestamp.GetClock(segCtx)
	s = &segment{
		id:              id,
		path:            path,
		suffix:          suffix,
		l:               l,
		blockController: newBlockController(segCtx, id, path, timeRange, blockSize, l, blockQueue),
		TimeRange:       timeRange,
		Reporter:        bucket.NewTimeBasedReporter(timeRange, clock),
	}
	err = s.blockController.open()
	if err != nil {
		return nil, err
	}

	indexPath, err := mkdir(globalIndexTemplate, path)
	if err != nil {
		return nil, err
	}
	o := ctx.Value(optionsKey)
	if o != nil {
		options := o.(DatabaseOpts)
		if options.EnableGlobalIndex {
			memSize := options.GlobalIndexMemSize
			if memSize == 0 {
				memSize = defaultKVMemorySize
			}
			if s.globalIndex, err = kv.OpenStore(
				0,
				indexPath,
				kv.StoreWithLogger(s.l),
				kv.StoreWithMemTableSize(memSize),
			); err != nil {
				return nil, err
			}
		}
	}
	if !s.End.After(clock.Now()) {
		return
	}
	s.blockManageStrategy, err = bucket.NewStrategy(s.blockController, bucket.WithLogger(s.l))
	if err != nil {
		return nil, err
	}
	s.blockManageStrategy.Run()
	return s, nil
}

func (s *segment) close(ctx context.Context) (err error) {
	s.closeOnce.Do(func() {
		if err = s.blockController.close(ctx); err != nil {
			return
		}
		if s.globalIndex != nil {
			if err = s.globalIndex.Close(); err != nil {
				return
			}
		}
		if s.blockManageStrategy != nil {
			s.blockManageStrategy.Close()
		}
		if s.Reporter != nil {
			s.Stop()
		}
	})
	return nil
}

func (s *segment) closeBlock(ctx context.Context, id uint16) error {
	return s.blockController.closeBlock(ctx, id)
}

func (s *segment) delete(ctx context.Context) error {
	if err := s.close(ctx); err != nil {
		return err
	}
	return os.RemoveAll(s.path)
}

func (s *segment) String() string {
	return "SegID-" + s.suffix
}

func (s *segment) Stats() observability.Statistics {
	if s.globalIndex == nil {
		return observability.Statistics{}
	}
	return s.globalIndex.Stats()
}

type blockController struct {
	sync.RWMutex
	segCtx       context.Context
	segID        uint16
	location     string
	segTimeRange timestamp.TimeRange
	blockSize    IntervalRule
	lst          []*block
	blockQueue   bucket.Queue
	clock        timestamp.Clock

	l *logger.Logger
}

func newBlockController(segCtx context.Context, segID uint16, location string, segTimeRange timestamp.TimeRange,
	blockSize IntervalRule, l *logger.Logger, blockQueue bucket.Queue,
) *blockController {
	clock, _ := timestamp.GetClock(segCtx)
	return &blockController{
		segCtx:       segCtx,
		segID:        segID,
		location:     location,
		blockSize:    blockSize,
		segTimeRange: segTimeRange,
		blockQueue:   blockQueue,
		l:            l,
		clock:        clock,
	}
}

func (bc *blockController) Current() (bucket.Reporter, error) {
	now := bc.clock.Now()
	ns := uint64(now.UnixNano())
	if b := func() *block {
		bc.RLock()
		defer bc.RUnlock()
		for _, s := range bc.lst {
			if s.Contains(ns) {
				return s
			}
		}
		return nil
	}(); b != nil {
		if b.Closed() {
			if err := b.open(); err != nil {
				return nil, err
			}
		}
		return b, nil
	}
	return bc.newHeadBlock(now)
}

func (bc *blockController) Next() (bucket.Reporter, error) {
	c, err := bc.Current()
	if err != nil {
		return nil, err
	}
	b := c.(*block)

	return bc.newHeadBlock(bc.blockSize.NextTime(b.Start))
}

func (bc *blockController) newHeadBlock(now time.Time) (*block, error) {
	b, err := bc.create(now)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (bc *blockController) OnMove(prev bucket.Reporter, next bucket.Reporter) {
	event := bc.l.Info()
	if prev != nil {
		event.Stringer("prev", prev)
		b := prev.(*block)
		ctx, cancel := context.WithTimeout(context.Background(), defaultEnqueueTimeout)
		defer cancel()
		if err := bc.blockQueue.Push(ctx, BlockID{
			SegID:   bc.segID,
			BlockID: b.blockID,
		}, nil); err != nil {
			bc.l.Debug().Err(err).Msg("failed to push a expired head block to the queue")
			ctxClosing, cancelClosing := context.WithTimeout(context.Background(), defaultEnqueueTimeout)
			defer cancelClosing()
			b.close(ctxClosing)
		}
	}
	if next != nil {
		event.Stringer("next", next)
	}
	event.Msg("move to the next block")
}

func (bc *blockController) Format(tm time.Time) string {
	switch bc.blockSize.Unit {
	case HOUR:
		return tm.Format(blockHourFormat)
	case DAY:
		return tm.Format(blockDayFormat)
	}
	panic("invalid interval unit")
}

func (bc *blockController) Parse(value string) (time.Time, error) {
	switch bc.blockSize.Unit {
	case HOUR:
		return time.ParseInLocation(blockHourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(blockDayFormat, value, time.Local)
	}
	panic("invalid interval unit")
}

func (bc *blockController) span(ctx context.Context, timeRange timestamp.TimeRange) ([]BlockDelegate, error) {
	bb := bc.search(func(b *block) bool {
		return b.Overlapping(timeRange)
	})
	if bb == nil {
		return nil, nil
	}
	dd := make([]BlockDelegate, len(bb))
	for i, b := range bb {
		d, err := b.delegate(ctx)
		if err != nil {
			return nil, err
		}
		dd[i] = d
	}
	return dd, nil
}

func (bc *blockController) get(ctx context.Context, blockID uint16) (BlockDelegate, error) {
	b := bc.getBlock(blockID)
	if b != nil {
		return b.delegate(ctx)
	}
	return nil, nil
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

func (bc *blockController) closeBlock(ctx context.Context, blockID uint16) error {
	bc.RLock()
	b := bc.getBlock(blockID)
	bc.RUnlock()
	if b == nil {
		return nil
	}
	return b.close(ctx)
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
	}
	panic("invalid interval unit")
}

func (bc *blockController) open() error {
	return WalkDir(
		bc.location,
		blockPathPrefix,
		func(suffix, absolutePath string) error {
			bc.Lock()
			defer bc.Unlock()
			_, err := bc.load(suffix, absolutePath)
			return err
		})
}

func (bc *blockController) create(startTime time.Time) (*block, error) {
	if startTime.Before(bc.segTimeRange.Start) {
		startTime = bc.segTimeRange.Start
	}
	if !startTime.Before(bc.segTimeRange.End) {
		return nil, bucket.ErrNoMoreBucket
	}
	bc.Lock()
	defer bc.Unlock()
	suffix := bc.Format(startTime)
	for _, b := range bc.lst {
		if b.suffix == suffix {
			return b, nil
		}
	}
	segPath, err := mkdir(blockTemplate, bc.location, suffix)
	if err != nil {
		return nil, err
	}
	b, err := bc.load(suffix, segPath)
	if err != nil {
		return nil, err
	}
	err = b.open()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (bc *blockController) load(suffix, path string) (b *block, err error) {
	starTime, err := bc.startTime(suffix)
	if err != nil {
		return nil, err
	}
	endTime := bc.blockSize.NextTime(starTime)
	if endTime.After(bc.segTimeRange.End) {
		endTime = bc.segTimeRange.End
	}
	if b, err = newBlock(
		common.SetPosition(bc.segCtx, func(p common.Position) common.Position {
			p.Block = suffix
			return p
		}),
		blockOpts{
			segID:     bc.segID,
			path:      path,
			timeRange: timestamp.NewSectionTimeRange(starTime, endTime),
			suffix:    suffix,
			blockSize: bc.blockSize,
			queue:     bc.blockQueue,
		}); err != nil {
		return nil, err
	}
	bc.lst = append(bc.lst, b)
	bc.sortLst()
	return b, nil
}

func (bc *blockController) sortLst() {
	sort.Slice(bc.lst, func(i, j int) bool {
		return bc.lst[i].blockID < bc.lst[j].blockID
	})
}

func (bc *blockController) close(ctx context.Context) (err error) {
	bc.Lock()
	defer bc.Unlock()
	for _, s := range bc.lst {
		err = multierr.Append(err, s.close(ctx))
	}
	return err
}

func (bc *blockController) remove(ctx context.Context, deadline time.Time) (err error) {
	for _, b := range bc.blocks() {
		if b.End.Before(deadline) {
			bc.Lock()
			if errDel := b.delete(ctx); errDel != nil {
				err = multierr.Append(err, errDel)
			} else {
				b.queue.Remove(BlockID{
					BlockID: b.blockID,
					SegID:   b.segID,
				})
				bc.removeBlock(b.blockID)
			}
			bc.Unlock()
		}
	}
	return err
}

func (bc *blockController) removeBlock(blockID uint16) {
	for i, b := range bc.lst {
		if b.blockID == blockID {
			bc.lst = append(bc.lst[:i], bc.lst[i+1:]...)
			break
		}
	}
}
