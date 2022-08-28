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

func (s *windowedFlow) AllowedLateness(lateness time.Duration) flow.Flow {
	switch v := s.wa.(type) {
	case *TumblingTimeWindows:
		v.lateness = lateness.Milliseconds()
	default:
		s.f.drainErr(errors.New("lateness is not supported"))
	}
	return s.f
}

type TumblingTimeWindows struct {
	// internal state of the sliding time window
	flow.ComponentState
	// errorHandler is the error handler and set by the streamingFlow
	errorHandler func(error)
	// For TumblingTimeWindows
	// Unit: Millisecond
	size int64
	// lateness is the maximum allowed lateness for incoming elements
	// Unit: Millisecond
	lateness int64

	// guard snapshots
	snapshotsMu sync.Mutex
	snapshots   map[int64]flow.AggregationOp
	acc         flow.AggregationOp

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

func (s *TumblingTimeWindows) Setup(ctx context.Context) error {
	if s.acc == nil {
		s.acc = s.aggregationFactory()
	}
	// start processing
	s.Add(1)
	go s.receive()

	return nil
}

func (s *TumblingTimeWindows) purgeWindow(w timeWindow) {
	s.snapshotsMu.Lock()
	if snapshot, ok := s.snapshots[w.MaxTimestamp()]; ok {
		// get and remove the key/value
		delete(s.snapshots, w.MaxTimestamp())
		s.snapshotsMu.Unlock()
		if err := s.acc.Merge(snapshot); err != nil {
			s.errorHandler(err)
			return
		}

		s.out <- flow.NewStreamRecord(s.acc.Snapshot(), w.start)
		return
	}
	s.snapshotsMu.Unlock()
}

func (s *TumblingTimeWindows) purgeOutdatedWindows() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()
	for {
		if lookAhead, ok := s.timerHeap.Peek().(*internalTimer); ok {
			if lookAhead.triggerTimeMillis <= s.currentWatermark {
				oldestTimer := heap.Pop(s.timerHeap).(*internalTimer)
				s.purgeWindow(oldestTimer.w)
				continue
			}
		}
		return
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
			s.snapshotsMu.Lock()
			if oldAggr, ok := s.snapshots[tw.MaxTimestamp()]; ok {
				oldAggr.Add([]flow.StreamRecord{elem})
			} else {
				newAggr := s.aggregationFactory()
				newAggr.Add([]flow.StreamRecord{elem})
				s.snapshots[tw.MaxTimestamp()] = newAggr
			}
			s.snapshotsMu.Unlock()

			result := ctx.OnElement(elem)
			if result == FIRE {
				s.purgeWindow(tw)
			}
		}

		// even if the incoming elements do not follow strict order,
		// the watermark could increase monotonically.
		if elem.TimestampMillis() > s.currentWatermark {
			s.currentWatermark = elem.TimestampMillis()

			// TODO: add various strategies to allow lateness items, which come later than the current watermark
			// Currently, assume the current watermark is t,
			// then we allow lateness items coming after t-windowSize+slideSize, but before t,
			// i.e. [t-windowSize+slideSize, t)
			// NOTE: This call is the normal path to flush outdated windows immediately
			// without considering lateness.
			// The purged windows may be accumulated again due to non-zero lateness.
			s.purgeOutdatedWindows()
		}
	}
	close(s.out)
}

// isWindowLate checks whether this window is valid
func (s *TumblingTimeWindows) isWindowLate(w flow.Window) bool {
	return w.MaxTimestamp()+s.lateness <= s.currentWatermark
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
		size:             size.Milliseconds(),
		lateness:         0,
		snapshots:        make(map[int64]flow.AggregationOp),
		timerHeap:        flow.NewPriorityQueue(false),
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
		start := getWindowStart(timestamp, s.size)
		return []flow.Window{
			timeWindow{
				start: start,
				end:   start + s.size,
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
		// if the watermark is already past the window fire immediately
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

func (t *internalTimer) Compare(elem flow.Element) int {
	return int(t.triggerTimeMillis - elem.(*internalTimer).triggerTimeMillis)
}
