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

package streaming

import (
	"container/heap"
	"context"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type triggerResult bool

const (
	fire triggerResult = true
	cont               = false
)

var (
	_ flow.Operator       = (*tumblingTimeWindows)(nil)
	_ flow.WindowAssigner = (*tumblingTimeWindows)(nil)
	_ flow.Window         = (*timeWindow)(nil)

	defaultCacheSize = 2
)

func (f *streamingFlow) Window(w flow.WindowAssigner) flow.WindowedFlow {
	switch v := w.(type) {
	case *tumblingTimeWindows:
		v.errorHandler = f.drainErr
		v.l = f.l
		f.ops = append(f.ops, v)
	default:
		f.drainErr(errors.New("window type is not supported"))
	}

	return &windowedFlow{
		f:  f,
		wa: w,
		l:  f.l,
	}
}

func (s *windowedFlow) AllowedMaxWindows(windowCnt int) flow.WindowedFlow {
	switch v := s.wa.(type) {
	case *tumblingTimeWindows:
		v.windowCount = windowCnt
	default:
		s.f.drainErr(errors.New("windowCnt is not supported"))
	}
	return s
}

type tumblingTimeWindows struct {
	errorHandler       func(error)
	snapshots          *lru.Cache
	timerHeap          *flow.DedupPriorityQueue
	aggregationFactory flow.AggregationOpFactory
	in                 chan flow.StreamRecord
	out                chan flow.StreamRecord
	flow.ComponentState
	windowSize       int64
	windowCount      int
	currentWatermark int64
	lastFlushTime    int64
	timerMu          sync.Mutex
	flushInterval    int64
	l                *logger.Logger
}

func (s *tumblingTimeWindows) In() chan<- flow.StreamRecord {
	return s.in
}

func (s *tumblingTimeWindows) Out() <-chan flow.StreamRecord {
	return s.out
}

func (s *tumblingTimeWindows) Setup(_ context.Context) (err error) {
	if s.snapshots == nil {
		if s.windowCount <= 0 {
			s.windowCount = defaultCacheSize
		}
		s.snapshots, err = lru.NewWithEvict(s.windowCount, func(key interface{}, value interface{}) {
			flushed := s.flushSnapshot(key.(timeWindow), value.(flow.AggregationOp))
			if e := s.l.Debug(); e.Enabled() {
				e.Stringer("window", key.(timeWindow)).Bool("flushed", flushed).Msg("evict window on lru cache is full")
			}
		})
		if err != nil {
			return err
		}
	}
	// start processing
	s.Add(1)
	go s.receive()

	return
}

func (s *tumblingTimeWindows) flushSnapshot(w timeWindow, snapshot flow.AggregationOp) bool {
	if snapshot.Dirty() {
		s.out <- flow.NewStreamRecord(snapshot.Snapshot(), w.start)
		return true
	}
	return false
}

func (s *tumblingTimeWindows) flushWindow(w timeWindow) {
	if snapshot, ok := s.snapshots.Get(w); ok {
		flushed := s.flushSnapshot(w, snapshot.(flow.AggregationOp))
		if e := s.l.Debug(); e.Enabled() {
			e.Stringer("window", w).Bool("flushed", flushed).Msg("flush window")
		}
	}
}

func (s *tumblingTimeWindows) flushDueWindows() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	for {
		if lookAhead, ok := s.timerHeap.Peek().(*internalTimer); ok {
			if lookAhead.triggerTimeMillis <= s.currentWatermark {
				oldestTimer := heap.Pop(s.timerHeap).(*internalTimer)
				s.flushWindow(oldestTimer.w)
				continue
			}
		}
		return
	}
}

func (s *tumblingTimeWindows) flushDirtyWindows() {
	for _, key := range s.snapshots.Keys() {
		s.flushWindow(key.(timeWindow))
	}
}

func (s *tumblingTimeWindows) receive() {
	defer s.Done()

	for elem := range s.in {
		assignedWindows, err := s.AssignWindows(elem.TimestampMillis())
		if err != nil {
			s.errorHandler(err)
			continue
		}
		ctx := triggerContext{
			delegation: s,
		}
		for _, w := range assignedWindows {
			// drop if the window is late
			if s.isWindowLate(w) {
				continue
			}
			tw := w.(timeWindow)
			ctx.window = tw
			// add elem to the bucket
			if oldAggr, ok := s.snapshots.Get(tw); ok {
				oldAggr.(flow.AggregationOp).Add([]flow.StreamRecord{elem})
			} else {
				newAggr := s.aggregationFactory()
				newAggr.Add([]flow.StreamRecord{elem})
				s.snapshots.Add(tw, newAggr)
				if e := s.l.Debug(); e.Enabled() {
					e.Stringer("window", tw).Msg("create new window")
				}
			}

			result := ctx.OnElement(elem)
			if result == fire {
				s.flushWindow(tw)
			}
		}

		// even if the incoming elements do not follow strict order,
		// the watermark could increase monotonically.
		now := time.Now().UnixNano() / int64(time.Millisecond)
		pastDataDur := elem.TimestampMillis() - s.currentWatermark
		if s.lastFlushTime == 0 {
			s.lastFlushTime = now
		}
		pastDur := now - s.lastFlushTime
		if pastDur > 0 || pastDataDur > 0 {
			previousWaterMark := s.currentWatermark
			s.currentWatermark = elem.TimestampMillis()

			// Currently, assume the current watermark is t,
			// then we allow lateness items by not purging the window
			// of which the flush trigger time is less and equal than t,
			// i.e. triggerTime <= t
			s.flushDueWindows()

			// flush dirty windows if the necessary
			// use 40% of the data point interval as the flush interval,
			// which means roughly the record located in the same time bucket will be persistent twice.
			// |---------------------------------|
			// |    40%     |    40%     |  20%  |
			// |          flush        flush     |
			// |---------------------------------|
			// the max flush interval is 1 minute.
			if (pastDur > s.flushInterval) || (previousWaterMark > 0 && pastDataDur > s.flushInterval) {
				s.lastFlushTime = now
				s.flushDirtyWindows()
			}
		}
	}
	close(s.out)
}

// isWindowLate checks whether this window is valid. The window is late if and only if
// it meets all the following conditions,
// 1) the max timestamp is before the current watermark
// 2) the LRU cache is full
// 3) the LRU cache does not contain the window entry.
func (s *tumblingTimeWindows) isWindowLate(w flow.Window) bool {
	return w.MaxTimestamp() <= s.currentWatermark && s.snapshots.Len() >= s.windowCount && !s.snapshots.Contains(w)
}

func (s *tumblingTimeWindows) Teardown(_ context.Context) error {
	s.Wait()
	return nil
}

func (s *tumblingTimeWindows) Exec(downstream flow.Inlet) {
	s.Add(1)
	go flow.Transmit(&s.ComponentState, downstream, s)
}

// NewTumblingTimeWindows return tumbling-time windows.
func NewTumblingTimeWindows(size time.Duration, maxFlushInterval time.Duration) flow.WindowAssigner {
	ws := size.Milliseconds()
	mfi := maxFlushInterval.Milliseconds()
	it := int64(float64(ws) * 0.4)
	if it > mfi {
		it = mfi
	}
	return &tumblingTimeWindows{
		windowSize: ws,
		timerHeap: flow.NewPriorityQueue(func(a, b interface{}) int {
			return int(a.(*internalTimer).triggerTimeMillis - b.(*internalTimer).triggerTimeMillis)
		}, false),
		in:               make(chan flow.StreamRecord),
		out:              make(chan flow.StreamRecord),
		currentWatermark: 0,
		flushInterval:    it,
	}
}

type timeWindow struct {
	start int64
	end   int64
}

func (t timeWindow) MaxTimestamp() int64 {
	return t.end - 1
}

func (t timeWindow) String() string {
	st := time.Unix(0, t.start*int64(time.Millisecond)).String()
	et := time.Unix(0, t.end*int64(time.Millisecond)).String()
	return st + " - " + et
}

// AssignWindows assigns windows according to the given timestamp.
func (s *tumblingTimeWindows) AssignWindows(timestamp int64) ([]flow.Window, error) {
	if timestamp > math.MinInt64 {
		start := getWindowStart(timestamp, s.windowSize)
		return []flow.Window{
			timeWindow{
				start: start,
				end:   start + s.windowSize,
			},
		}, nil
	}
	return nil, errors.New("invalid timestamp from the element")
}

// getWindowStart calculates the window start for a timestamp.
func getWindowStart(timestamp, windowSize int64) int64 {
	remainder := timestamp % windowSize
	return timestamp - remainder
}

// eventTimeTriggerOnElement processes element(s) with EventTimeTrigger.
func eventTimeTriggerOnElement(window timeWindow, ctx *triggerContext) triggerResult {
	if window.MaxTimestamp() <= ctx.GetCurrentWatermark() {
		// if watermark is already past the window fire immediately
		return fire
	}
	ctx.RegisterEventTimeTimer(window.MaxTimestamp())
	return cont
}

type triggerContext struct {
	delegation *tumblingTimeWindows
	window     timeWindow
}

func (ctx *triggerContext) GetCurrentWatermark() int64 {
	return ctx.delegation.currentWatermark
}

func (ctx *triggerContext) RegisterEventTimeTimer(triggerTime int64) {
	ctx.delegation.timerMu.Lock()
	defer ctx.delegation.timerMu.Unlock()
	heap.Push(ctx.delegation.timerHeap, &internalTimer{
		triggerTimeMillis: triggerTime,
		w:                 ctx.window,
	})
}

func (ctx *triggerContext) OnElement(_ flow.StreamRecord) triggerResult {
	return eventTimeTriggerOnElement(ctx.window, ctx)
}

var _ flow.Element = (*internalTimer)(nil)

type internalTimer struct {
	w                 timeWindow
	triggerTimeMillis int64
	index             int
}

func (t *internalTimer) GetIndex() int {
	return t.index
}

func (t *internalTimer) SetIndex(idx int) {
	t.index = idx
}
