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
)

type TriggerResult bool

const (
	FIRE     TriggerResult = true
	CONTINUE               = false
)

var (
	_ flow.Operator       = (*TumblingTimeWindows)(nil)
	_ flow.WindowAssigner = (*TumblingTimeWindows)(nil)
	_ flow.Window         = (*timeWindow)(nil)
)

func (f *streamingFlow) Window(w flow.WindowAssigner) flow.WindowedFlow {
	switch v := w.(type) {
	case *TumblingTimeWindows:
		v.errorHandler = f.drainErr
		f.ops = append(f.ops, v)
	default:
		f.drainErr(errors.New("window type is not supported"))
	}

	return &windowedFlow{
		f:  f,
		wa: w,
	}
}

func (s *windowedFlow) AllowedMaxWindows(windowCnt int) flow.WindowedFlow {
	switch v := s.wa.(type) {
	case *TumblingTimeWindows:
		v.windowCount = windowCnt
	default:
		s.f.drainErr(errors.New("windowCnt is not supported"))
	}
	return s
}

type TumblingTimeWindows struct {
	// internal state of the sliding time window
	flow.ComponentState
	// errorHandler is the error handler and set by the streamingFlow
	errorHandler func(error)
	// For TumblingTimeWindows
	// Unit: Milliseconds
	windowSize int64
	// windowCount is the maximum allowed windows kept in the memory
	windowCount int

	// thread-safe snapshots
	snapshots *lru.Cache

	currentWatermark int64
	// guard timerHeap
	timerMu   sync.Mutex
	timerHeap *flow.DedupPriorityQueue

	// aggregationFactory is the factory for creating aggregation operator
	aggregationFactory flow.AggregationOpFactory

	// For api.Operator
	in  chan flow.StreamRecord
	out chan flow.StreamRecord
}

func (s *TumblingTimeWindows) In() chan<- flow.StreamRecord {
	return s.in
}

func (s *TumblingTimeWindows) Out() <-chan flow.StreamRecord {
	return s.out
}

func (s *TumblingTimeWindows) Setup(ctx context.Context) (err error) {
	if s.snapshots == nil {
		s.snapshots, err = lru.NewWithEvict(s.windowCount, func(key interface{}, value interface{}) {
			s.flushSnapshot(key.(timeWindow), value.(flow.AggregationOp))
		})
	}
	// start processing
	s.Add(1)
	go s.receive()

	return
}

func (s *TumblingTimeWindows) flushSnapshot(w timeWindow, snapshot flow.AggregationOp) {
	if snapshot.Dirty() {
		s.out <- flow.NewStreamRecord(snapshot.Snapshot(), w.start)
	}
}

func (s *TumblingTimeWindows) flushWindow(w timeWindow) {
	if snapshot, ok := s.snapshots.Get(w); ok {
		s.flushSnapshot(w, snapshot.(flow.AggregationOp))
	}
}

func (s *TumblingTimeWindows) flushDueWindows() {
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

func (s *TumblingTimeWindows) flushDirtyWindows() {
	for _, key := range s.snapshots.Keys() {
		s.flushWindow(key.(timeWindow))
	}
}

func (s *TumblingTimeWindows) receive() {
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
			}

			result := ctx.OnElement(elem)
			if result == FIRE {
				s.flushWindow(tw)
			}
		}

		// even if the incoming elements do not follow strict order,
		// the watermark could increase monotonically.
		if pastDur := elem.TimestampMillis() - s.currentWatermark; pastDur > 0 {
			s.currentWatermark = elem.TimestampMillis()

			// Currently, assume the current watermark is t,
			// then we allow lateness items by not purging the window
			// of which the flush trigger time is less and equal than t - lateness,
			// i.e. triggerTime + lateness <= t
			s.flushDueWindows()

			// flush dirty windows if the necessary
			// use 40% of the data point interval as the flush interval,
			// which means roughly the record located in the same time bucket will be persistent twice.
			// |---------------------------------|
			// |    40%     |    40%     |  20%  |
			// |          flush        flush     |
			// |---------------------------------|
			// TODO: how to determine the threshold
			if float64(pastDur) > float64(s.windowSize)*0.4 {
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
// 3) the LRU cache does not contain the window entry
func (s *TumblingTimeWindows) isWindowLate(w flow.Window) bool {
	return w.MaxTimestamp() <= s.currentWatermark && s.snapshots.Len() >= s.windowCount && !s.snapshots.Contains(w)
}

func (s *TumblingTimeWindows) Teardown(ctx context.Context) error {
	s.Wait()
	return nil
}

func (s *TumblingTimeWindows) Exec(downstream flow.Inlet) {
	s.Add(1)
	go flow.Transmit(&s.ComponentState, downstream, s)
}

func NewTumblingTimeWindows(size time.Duration) *TumblingTimeWindows {
	return &TumblingTimeWindows{
		windowSize: size.Milliseconds(),
		timerHeap: flow.NewPriorityQueue(func(a, b interface{}) int {
			return int(a.(*internalTimer).triggerTimeMillis - b.(*internalTimer).triggerTimeMillis)
		}, false),
		in:               make(chan flow.StreamRecord),
		out:              make(chan flow.StreamRecord),
		currentWatermark: 0,
	}
}

type timeWindow struct {
	start int64
	end   int64
}

func (t timeWindow) MaxTimestamp() int64 {
	return t.end - 1
}

// AssignWindows assigns windows according to the given timestamp
func (s *TumblingTimeWindows) AssignWindows(timestamp int64) ([]flow.Window, error) {
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

// eventTimeTriggerOnElement processes element(s) with EventTimeTrigger
func eventTimeTriggerOnElement(window timeWindow, ctx *triggerContext) TriggerResult {
	if window.MaxTimestamp() <= ctx.GetCurrentWatermark() {
		// if watermark is already past the window fire immediately
		return FIRE
	}
	ctx.RegisterEventTimeTimer(window.MaxTimestamp())
	return CONTINUE
}

type triggerContext struct {
	window     timeWindow
	delegation *TumblingTimeWindows
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

func (ctx *triggerContext) OnElement(record flow.StreamRecord) TriggerResult {
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
