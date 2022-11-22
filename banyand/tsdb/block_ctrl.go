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
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type blockController struct {
	sync.RWMutex
	segCtx       context.Context
	segID        SectionID
	segSuffix    string
	location     string
	segTimeRange timestamp.TimeRange
	blockSize    IntervalRule
	lst          []*block
	blockQueue   bucket.Queue
	scheduler    *timestamp.Scheduler
	clock        timestamp.Clock

	l *logger.Logger
}

func newBlockController(segCtx context.Context, segID SectionID, segSuffix, location string, segTimeRange timestamp.TimeRange,
	blockSize IntervalRule, l *logger.Logger, blockQueue bucket.Queue, scheduler *timestamp.Scheduler,
) *blockController {
	clock, _ := timestamp.GetClock(segCtx)
	return &blockController{
		segCtx:       segCtx,
		segID:        segID,
		segSuffix:    segSuffix,
		location:     location,
		blockSize:    blockSize,
		segTimeRange: segTimeRange,
		blockQueue:   blockQueue,
		l:            l,
		clock:        clock,
		scheduler:    scheduler,
	}
}

func (bc *blockController) Current() (bucket.Reporter, error) {
	now := bc.Standard(bc.clock.Now())
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
		if err := b.openSafely(); err != nil {
			return nil, err
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
			bc.l.Error().Err(err).Msg("failed to push a expired head block to the queue")
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

func (bc *blockController) Standard(t time.Time) time.Time {
	switch bc.blockSize.Unit {
	case HOUR:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case DAY:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}
	panic("invalid interval unit")
}

func (bc *blockController) Format(tm time.Time) string {
	switch bc.blockSize.Unit {
	case HOUR:
		return tm.Format(hourFormat)
	case DAY:
		return tm.Format(dayFormat)
	}
	panic("invalid interval unit")
}

func (bc *blockController) Parse(value string) (time.Time, error) {
	switch bc.blockSize.Unit {
	case HOUR:
		return time.ParseInLocation(hourFormat, value, time.Local)
	case DAY:
		return time.ParseInLocation(dayFormat, value, time.Local)
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

func (bc *blockController) get(ctx context.Context, blockID SectionID) (BlockDelegate, error) {
	b := bc.getBlock(blockID)
	if b != nil {
		return b.delegate(ctx)
	}
	return nil, nil
}

func (bc *blockController) getBlock(blockID SectionID) *block {
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
	lst := bc.blocks()
	last := len(lst) - 1
	for i := range lst {
		b := lst[last-i]
		if matcher(b) {
			bb = append(bb, b)
		}
	}
	return bb
}

func (bc *blockController) closeBlock(ctx context.Context, blockID SectionID) error {
	bc.RLock()
	b := bc.getBlock(blockID)
	bc.RUnlock()
	if b == nil {
		return nil
	}
	return b.close(ctx)
}

func (bc *blockController) open() error {
	bc.Lock()
	defer bc.Unlock()
	return loadSections(bc.location, bc, bc.blockSize, func(start, end time.Time) error {
		_, err := bc.load(start, end, bc.location)
		return err
	})
}

func (bc *blockController) create(start time.Time) (*block, error) {
	start = bc.Standard(start)
	if start.Before(bc.segTimeRange.Start) {
		start = bc.segTimeRange.Start
	}
	if !start.Before(bc.segTimeRange.End) {
		return nil, bucket.ErrNoMoreBucket
	}
	bc.Lock()
	defer bc.Unlock()
	var next *block
	for _, s := range bc.lst {
		if s.Start.Equal(start) {
			return s, nil
		}
		if next == nil && s.Start.After(start) {
			next = s
		}
	}
	stdEnd := bc.blockSize.NextTime(start)
	var end time.Time
	if next != nil && next.Start.Before(stdEnd) {
		end = next.Start
	} else {
		end = stdEnd
	}
	if end.After(bc.segTimeRange.End) {
		end = bc.segTimeRange.End
	}
	_, err := mkdir(blockTemplate, bc.location, bc.Format(start))
	if err != nil {
		return nil, err
	}
	b, err := bc.load(start, end, bc.location)
	if err != nil {
		return nil, err
	}
	if err = b.openSafely(); err != nil {
		return nil, err
	}
	return b, nil
}

func (bc *blockController) load(startTime, endTime time.Time, root string) (b *block, err error) {
	suffix := bc.Format(startTime)
	if b, err = newBlock(
		common.SetPosition(bc.segCtx, func(p common.Position) common.Position {
			p.Block = suffix
			return p
		}),
		blockOpts{
			segID:     bc.segID,
			segSuffix: bc.segSuffix,
			path:      fmt.Sprintf(blockTemplate, root, suffix),
			timeRange: timestamp.NewSectionTimeRange(startTime, endTime),
			suffix:    suffix,
			blockSize: bc.blockSize,
			queue:     bc.blockQueue,
			scheduler: bc.scheduler,
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
	bc.lst = bc.lst[:0]
	return err
}

func (bc *blockController) remove(ctx context.Context, deadline time.Time) (err error) {
	for _, b := range bc.blocks() {
		if b.End.Before(deadline) {
			if e := bc.l.Debug(); e.Enabled() {
				e.Stringer("block", b).Msg("start to remove data in a block")
			}
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

func (bc *blockController) removeBlock(blockID SectionID) {
	for i, b := range bc.lst {
		if b.blockID == blockID {
			bc.lst = append(bc.lst[:i], bc.lst[i+1:]...)
			break
		}
	}
}
