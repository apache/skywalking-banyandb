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

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
)

type TriggerResult uint8

const (
	FIRE TriggerResult = 1 << iota
	CONTINUE
)

var (
	_ streamingApi.Operator = (*SlidingTimeWindows)(nil)
	_ TriggerContext        = (*SlidingTimeWindows)(nil)
	_ api.WindowAssigner    = (*SlidingTimeWindows)(nil)
	_ api.Window            = (*timeWindow)(nil)
)

func (flow *streamingFlow) Window(w api.WindowAssigner) api.WindowedFlow {
	switch v := w.(type) {
	case *SlidingTimeWindows:
		flow.ops = append(flow.ops, v)
	default:
		flow.drainErr(errors.New("window is not supported"))
	}

	return &windowedFlow{
		f:  flow,
		wa: w,
	}
}

func (s *windowedFlow) Aggregate(aggrFunc api.AggregateFunction) api.Flow {
	switch v := s.wa.(type) {
	case *SlidingTimeWindows:
		v.Aggregate(aggrFunc)
	default:
		s.f.drainErr(errors.New("aggregation is not supported"))
	}
	return s.f
}

type SlidingTimeWindows struct {
	// For SlidingTimeWindows
	currentWindow timeWindow
	size          int64
	slide         int64
	queue         *api.PriorityQueue
	// guard queue
	queueMu          sync.Mutex
	currentWatermark int64
	timerHeap        *api.PriorityQueue
	// guard timerHeap
	timerMu  sync.Mutex
	trigger  EventTimeTrigger
	aggrFunc api.AggregateFunction

	// For api.Operator
	in           chan interface{}
	out          chan interface{}
	done         chan struct{}
	purgedWindow chan timeWindow
}

func (s *SlidingTimeWindows) GetCurrentWatermark() int64 {
	return s.currentWatermark
}

func (s *SlidingTimeWindows) RegisterEventTimeTimer(triggerTime int64) {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	heap.Push(s.timerHeap, &internalTimer{
		triggerTimeMillis: triggerTime,
		w:                 s.currentWindow,
	})
}

func (s *SlidingTimeWindows) In() chan<- interface{} {
	return s.in
}

func (s *SlidingTimeWindows) Out() <-chan interface{} {
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
	for w := range s.purgedWindow {
		s.queueMu.Lock()
		// build a window slice and send it to the out chan
		var windowBottomIndex int
		windowUpperIndex := s.queue.Len()
		slideUpperIndex := windowUpperIndex
		slideUpperTime := w.start + s.slide
		windowBottomTime := w.start
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
				// TODO: drain error
				panic("drain error")
			}
			heap.Init(s.queue)
		}
		s.queueMu.Unlock()

		if len(slidingSlices) > 0 {
			data := make([]interface{}, 0, len(slidingSlices))
			for _, elem := range slidingSlices {
				data = append(data, elem.(*TimestampedValue).Data())
			}
			s.out <- api.NewStreamRecord(s.aggrFunc(data), slideUpperTime)
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
	for elem := range s.in {
		record := elem.(api.StreamRecord)
		// assume records are consumed one by one in strict time order
		s.currentWatermark = record.TimestampMillis()
		s.purgeOutdatedWindows()

		assignedWindows, err := s.AssignWindows(record.TimestampMillis())
		if err != nil {
			// TODO: drainError
			continue
		}
		for _, w := range assignedWindows {
			tw := w.(timeWindow)
			s.currentWindow = tw
			result := s.trigger.OnElement(record.TimestampMillis(), tw, s)
			if result == FIRE {
				s.purgedWindow <- tw
			}
		}
		item := &TimestampedValue{elem.(api.StreamRecord), 0}
		s.queueMu.Lock()
		heap.Push(s.queue, item)
		s.queueMu.Unlock()
	}
	close(s.purgedWindow)
	close(s.done)
	close(s.out)
}

func (s *SlidingTimeWindows) Teardown(ctx context.Context) error {
	return nil
}

func (s *SlidingTimeWindows) Exec(downstream streamingApi.Inlet) {
	go streamingApi.Transmit(downstream, s)
}

func NewSlidingTimeWindows(size, slide time.Duration) *SlidingTimeWindows {
	return &SlidingTimeWindows{
		size:             size.Milliseconds(),
		slide:            slide.Milliseconds(),
		queue:            api.NewPriorityQueue(true),
		timerHeap:        api.NewPriorityQueue(false),
		in:               make(chan interface{}),
		out:              make(chan interface{}),
		done:             make(chan struct{}),
		purgedWindow:     make(chan timeWindow),
		trigger:          EventTimeTrigger{},
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

func (s *SlidingTimeWindows) Aggregate(aggrFunc api.AggregateFunction) {
	s.aggrFunc = aggrFunc
}

func (s *SlidingTimeWindows) AssignWindows(timestamp int64) ([]api.Window, error) {
	if timestamp > math.MinInt64 {
		windows := make([]api.Window, 0, s.size/s.slide)
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
	// handle both positive and negative cases
	if remainder < 0 {
		return timestamp - (remainder + windowSize)
	}
	return timestamp - remainder
}

type EventTimeTrigger struct{}

func (t EventTimeTrigger) OnElement(timestamp int64, window timeWindow, ctx TriggerContext) TriggerResult {
	if window.MaxTimestamp() <= ctx.GetCurrentWatermark() {
		// if the watermark is already past the window fire immediately
		return FIRE
	}
	ctx.RegisterEventTimeTimer(window.MaxTimestamp())
	return CONTINUE
}

func (t EventTimeTrigger) OnEventTime(time int64, window timeWindow, ctx TriggerContext) TriggerResult {
	if time == window.MaxTimestamp() {
		return FIRE
	}
	return CONTINUE
}

type TriggerContext interface {
	GetCurrentWatermark() int64
	RegisterEventTimeTimer(int64)
}

var _ api.Element = (*TimestampedValue)(nil)

type TimestampedValue struct {
	api.StreamRecord
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

func (t *TimestampedValue) Compare(other api.Element) int {
	return int(t.StreamRecord.TimestampMillis() - other.(*TimestampedValue).TimestampMillis())
}

var _ api.Element = (*internalTimer)(nil)

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

func (t *internalTimer) Compare(elem api.Element) int {
	return int(t.triggerTimeMillis - elem.(*internalTimer).triggerTimeMillis)
}
