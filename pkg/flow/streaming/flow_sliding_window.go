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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

type TriggerResult bool

const (
	FIRE     TriggerResult = true
	CONTINUE               = false
)

var (
	_ flow.Operator       = (*SlidingTimeWindows)(nil)
	_ flow.WindowAssigner = (*SlidingTimeWindows)(nil)
	_ flow.Window         = (*timeWindow)(nil)
)

func (f *streamingFlow) Window(w flow.WindowAssigner) flow.WindowedFlow {
	switch v := w.(type) {
	case *SlidingTimeWindows:
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

func (s *windowedFlow) Aggregate(aggrFunc flow.AggregateFunction) flow.Flow {
	switch v := s.wa.(type) {
	case *SlidingTimeWindows:
		v.Aggregate(aggrFunc)
	default:
		s.f.drainErr(errors.New("aggregation is not supported"))
	}
	return s.f
}

type SlidingTimeWindows struct {
	// internal state of the sliding time window
	flow.ComponentState
	// errorHandler is the error handler and set by the streamingFlow
	errorHandler func(error)
	// For SlidingTimeWindows
	size  int64
	slide int64
	queue *flow.DedupPriorityQueue
	// guard queue
	queueMu          sync.Mutex
	currentWatermark int64
	timerHeap        *flow.DedupPriorityQueue
	// guard timerHeap
	timerMu  sync.Mutex
	aggrFunc flow.AggregateFunction

	// For api.Operator
	in           chan flow.StreamRecord
	out          chan flow.StreamRecord
	done         chan struct{}
	purgedWindow chan timeWindow
}

func (s *SlidingTimeWindows) In() chan<- flow.StreamRecord {
	return s.in
}

func (s *SlidingTimeWindows) Out() <-chan flow.StreamRecord {
	return s.out
}

func (s *SlidingTimeWindows) Setup(ctx context.Context) error {
	// start processing
	go s.receive()
	// start emitting
	go s.emit()

	return nil
}

func (s *SlidingTimeWindows) emit() {
	s.Add(1)
	defer s.Done()
	for w := range s.purgedWindow {
		if err := func(window timeWindow) error {
			s.queueMu.Lock()
			defer s.queueMu.Unlock()
			// build a window slice and send it to the out chan
			var windowBottomIndex int
			windowUpperIndex := s.queue.Len()
			slideUpperIndex := windowUpperIndex
			slideUpperTime := window.start + s.slide
			windowBottomTime := window.start
			for i, item := range s.queue.Items {
				if item.(*TimestampedValue).TimestampMillis() < windowBottomTime {
					windowBottomIndex = i
				}
				if item.(*TimestampedValue).TimestampMillis() > slideUpperTime {
					slideUpperIndex = i
					break
				}
			}
			slidingSlices := s.queue.Slice(windowBottomIndex, slideUpperIndex)
			// if we've collected some items, reallocate queue
			if len(slidingSlices) > 0 {
				remainingItems := s.queue.Slice(slideUpperIndex, windowUpperIndex)
				// reset the queue
				var err error
				s.queue, err = s.queue.WithNewItems(remainingItems)
				if err != nil {
					return err
				}
				heap.Init(s.queue)
			}

			if len(slidingSlices) > 0 {
				data := make([]interface{}, 0, len(slidingSlices))
				for _, elem := range slidingSlices {
					data = append(data, elem.(*TimestampedValue).Data())
				}
				s.out <- flow.NewStreamRecord(s.aggrFunc(data), slideUpperTime)
			}
			return nil
		}(w); err != nil {
			s.errorHandler(err)
		}
	}
}

func (s *SlidingTimeWindows) purgeOutdatedWindows() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	for {
		if lookAhead, ok := s.timerHeap.Peek().(*internalTimer); ok {
			if lookAhead.triggerTimeMillis <= s.currentWatermark {
				oldestTimer := heap.Pop(s.timerHeap).(*internalTimer)
				s.purgedWindow <- oldestTimer.w
				continue
			}
		}
		return
	}
}

func (s *SlidingTimeWindows) receive() {
	s.Add(1)
	defer s.Done()
	for elem := range s.in {
		// even if the incoming elements do not follow strict order,
		// the watermark could increase monotonically.
		if elem.TimestampMillis() > s.currentWatermark {
			s.currentWatermark = elem.TimestampMillis()
		}
		// TODO: add various strategies to allow lateness items, which come later than the current watermark
		// Currently, assume the current watermark is t,
		// then we allow lateness items coming after t-windowSize+slideSize, but before t,
		// i.e. [t-windowSize+slideSize, t)
		s.purgeOutdatedWindows()

		assignedWindows, err := s.AssignWindows(elem.TimestampMillis())
		if err != nil {
			s.errorHandler(err)
			continue
		}
		ctx := triggerContext{
			delegation: s,
		}
		for _, w := range assignedWindows {
			tw := w.(timeWindow)
			ctx.window = tw
			result := ctx.OnElement(elem)
			if result == FIRE {
				s.purgedWindow <- tw
			}
		}
		item := &TimestampedValue{elem, 0}
		s.queueMu.Lock()
		heap.Push(s.queue, item)
		s.queueMu.Unlock()
	}
	close(s.purgedWindow)
	close(s.done)
	close(s.out)
}

func (s *SlidingTimeWindows) Teardown(ctx context.Context) error {
	s.Wait()
	return nil
}

func (s *SlidingTimeWindows) Exec(downstream flow.Inlet) {
	go flow.Transmit(&s.ComponentState, downstream, s)
}

func NewSlidingTimeWindows(size, slide time.Duration) *SlidingTimeWindows {
	return &SlidingTimeWindows{
		size:             size.Milliseconds(),
		slide:            slide.Milliseconds(),
		queue:            flow.NewPriorityQueue(true),
		timerHeap:        flow.NewPriorityQueue(false),
		in:               make(chan flow.StreamRecord),
		out:              make(chan flow.StreamRecord),
		done:             make(chan struct{}),
		purgedWindow:     make(chan timeWindow),
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

func (s *SlidingTimeWindows) Aggregate(aggrFunc flow.AggregateFunction) {
	s.aggrFunc = aggrFunc
}

func (s *SlidingTimeWindows) AssignWindows(timestamp int64) ([]flow.Window, error) {
	if timestamp > math.MinInt64 {
		windows := make([]flow.Window, 0, s.size/s.slide)
		lastStart := getWindowStart(timestamp, s.slide)
		for start := lastStart; start > timestamp-s.size; start -= s.slide {
			windows = append(windows, timeWindow{
				start: start,
				end:   start + s.size,
			})
		}
		return windows, nil
	}
	return nil, errors.New("invalid timestamp from the element")
}

// getWindowStart calculates the window start for a timestamp.
func getWindowStart(timestamp, windowSize int64) int64 {
	remainder := timestamp % windowSize
	return timestamp - remainder
}

// eventTimeTriggerOnElement processes element(s) with EventTimeTrigger
func eventTimeTriggerOnElement(window timeWindow, ctx TriggerContext) TriggerResult {
	if window.MaxTimestamp() <= ctx.GetCurrentWatermark() {
		// if the watermark is already past the window fire immediately
		return FIRE
	}
	ctx.RegisterEventTimeTimer(window.MaxTimestamp())
	return CONTINUE
}

var (
	_ TriggerContext = (*triggerContext)(nil)
)

type TriggerContext interface {
	GetCurrentWatermark() int64
	RegisterEventTimeTimer(int64)
	OnElement(flow.StreamRecord) TriggerResult
}

type triggerContext struct {
	window     timeWindow
	delegation *SlidingTimeWindows
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

var _ flow.Element = (*TimestampedValue)(nil)

type TimestampedValue struct {
	flow.StreamRecord
	index int
}

func (t *TimestampedValue) String() string {
	return fmt.Sprintf("TimestampedValue{timestamp=%d}", t.TimestampMillis())
}

func (t *TimestampedValue) GetIndex() int {
	return t.index
}

func (t *TimestampedValue) SetIndex(i int) {
	t.index = i
}

func (t *TimestampedValue) Compare(other flow.Element) int {
	return int(t.StreamRecord.TimestampMillis() - other.(*TimestampedValue).TimestampMillis())
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

func (t *internalTimer) Compare(elem flow.Element) int {
	return int(t.triggerTimeMillis - elem.(*internalTimer).triggerTimeMillis)
}
