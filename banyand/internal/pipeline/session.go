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

package pipeline

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

var executionSlots = make(chan struct{}, cgroups.CPUs())

// Session serializes a pinned chain and bounds calls which outlive their deadline.
// Batches remain owned by the caller until reusable is true or Wait returns.
type Session struct {
	timer            *time.Timer
	done             chan struct{}
	slots            chan struct{}
	stop             chan struct{}
	results          chan sdk.Verdict
	requests         chan sessionRequest
	samplers         []sdk.Sampler
	projection       sdk.Projection
	workers          sync.WaitGroup
	timeouts         int
	threshold        int
	closePluginsOnce sync.Once
	mu               sync.Mutex
	busy             bool
	closed           bool
	open             bool
}

type sessionRequest struct {
	batch    *sdk.TraceBatch
	evaluate func([]sdk.Sampler, *sdk.TraceBatch, []bool) sdk.Verdict
	mask     []bool
}

// NewSession builds a session over sampler instances owned by the caller.
func NewSession(samplers []sdk.Sampler, circuitBreakN int) *Session {
	s := &Session{threshold: circuitBreakN, slots: executionSlots}
	seen := make(map[string]bool)
	for _, sampler := range samplers {
		if sampler == nil {
			continue
		}
		s.samplers = append(s.samplers, sampler)
		projection := sampler.Project()
		for _, tag := range projection.Tags {
			if !seen[tag] {
				seen[tag] = true
				s.projection.Tags = append(s.projection.Tags, tag)
			}
		}
		s.projection.Spans = s.projection.Spans || projection.Spans
		s.projection.SpanIDs = s.projection.SpanIDs || projection.SpanIDs
	}
	return s
}

// NewSessionChecked validates plugin projections without letting a native panic
// terminate the host during policy preflight.
func NewSessionChecked(samplers []sdk.Sampler, circuitBreakN int) (session *Session, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			session = nil
			err = fmt.Errorf("sampler projection panic: %v", recovered)
		}
	}()
	return NewSession(samplers, circuitBreakN), nil
}

// Projection returns the union of the chain's requested columns.
func (s *Session) Projection() sdk.Projection { return s.projection }

// SetExecutionSlots overrides the shared bound before the session is first used.
func (s *Session) SetExecutionSlots(slots chan struct{}) {
	if s.slots != slots {
		s.slots = slots
	}
}

// Execute evaluates the chain, retaining all traces on a deadline or open circuit.
func (s *Session) Execute(batch *sdk.TraceBatch, timeout time.Duration) (sdk.Verdict, error) {
	verdict, _, err := s.ExecuteInto(batch, timeout, nil, nil)
	return verdict, err
}

// ExecuteInto reports whether the caller may recycle batch and mask immediately.
// A false result means an outstanding native call still owns their buffers.
func (s *Session) ExecuteInto(batch *sdk.TraceBatch, timeout time.Duration, mask []bool,
	evaluate func([]sdk.Sampler, *sdk.TraceBatch, []bool) sdk.Verdict,
) (sdk.Verdict, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samplers) == 0 || s.open || s.closed {
		return retainAll(len(batch.Traces), mask), true, nil
	}
	if s.busy {
		select {
		case <-s.done:
			s.busy = false
			s.requests = nil
			s.results = nil
		default:
			return s.timeout(batch, mask, true)
		}
	}
	if s.timer == nil {
		s.timer = time.NewTimer(timeout)
	} else {
		s.stopTimer()
		s.timer.Reset(timeout)
	}
	defer s.stopTimer()
	select {
	case s.slots <- struct{}{}:
	case <-s.timer.C:
		return s.timeout(batch, mask, true)
	}
	if s.requests == nil {
		s.requests = make(chan sessionRequest)
		s.results = make(chan sdk.Verdict)
		s.stop = make(chan struct{})
		s.done = make(chan struct{})
		s.workers.Add(1)
		go s.run()
	}
	s.requests <- sessionRequest{batch: batch, mask: mask, evaluate: evaluate}
	select {
	case verdict := <-s.results:
		s.timeouts = 0
		return verdict, true, nil
	case <-s.timer.C:
		s.busy = true
		close(s.stop)
		return s.timeout(batch, nil, false)
	}
}

func (s *Session) run() {
	defer s.workers.Done()
	defer close(s.done)
	for {
		select {
		case request := <-s.requests:
			var verdict sdk.Verdict
			if request.evaluate == nil {
				verdict = sdk.EvaluateChainInto(s.samplers, request.batch, request.mask, nil)
			} else {
				verdict = request.evaluate(s.samplers, request.batch, request.mask)
			}
			<-s.slots
			select {
			case s.results <- verdict:
			case <-s.stop:
				return
			}
		case <-s.stop:
			return
		}
	}
}

func (s *Session) stopTimer() {
	if s.timer == nil || s.timer.Stop() {
		return
	}
	select {
	case <-s.timer.C:
	default:
	}
}

func (s *Session) timeout(batch *sdk.TraceBatch, mask []bool, reusable bool) (sdk.Verdict, bool, error) {
	s.timeouts++
	reason := "timeout"
	if s.threshold > 0 && s.timeouts >= s.threshold {
		s.open = true
		reason = "circuit_open"
	}
	return retainAll(len(batch.Traces), mask), reusable, fmt.Errorf("%s", reason)
}

func retainAll(n int, mask []bool) sdk.Verdict {
	if cap(mask) < n {
		mask = make([]bool, n)
	} else {
		mask = mask[:n]
	}
	for i := range mask {
		mask[i] = true
	}
	return sdk.Verdict{Keep: mask}
}

// State returns the circuit state and consecutive deadline count.
func (s *Session) State() (bool, int) { s.mu.Lock(); defer s.mu.Unlock(); return s.open, s.timeouts }

// Close prevents new work. It does not close instances while native calls run.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.stop != nil && !s.busy {
		close(s.stop)
	}
	s.closed = true
}

// Wait waits for native calls before sampler instances or their input are released.
func (s *Session) Wait() { s.workers.Wait() }

// ClosePluginsWhenIdle stops new execution and asynchronously closes the owned
// instances after native workers finish. An uncooperative plugin cannot stall
// its caller or be closed while still accessing its input.
func (s *Session) ClosePluginsWhenIdle() {
	s.closePluginsOnce.Do(func() {
		s.Close()
		go func() {
			s.Wait()
			seen := make(map[sdk.Sampler]bool)
			for _, sampler := range s.samplers {
				if reflect.TypeOf(sampler).Comparable() {
					if seen[sampler] {
						continue
					}
					seen[sampler] = true
				}
				_ = sampler.Close()
			}
		}()
	})
}
