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

package queue

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// testConfig never flushes by count or interval, so a test opts into the
// trigger it is about.
func testConfig() BufferedConfig {
	return BufferedConfig{
		Name:          "buffered-test",
		Logger:        logger.GetLogger("buffered-test"),
		MaxBytes:      1000,
		MaxCount:      100,
		BatchCount:    1000,
		BatchBytes:    1000,
		FlushInterval: time.Hour,
		MaxRetries:    3,
		DrainTimeout:  time.Second,
	}
}

// tracked is a request that records how it ended.
type tracked struct {
	*BufferedRequest
	outcomes []Outcome
	released int
	mu       sync.Mutex
}

func newTracked(data any, size int64) *tracked {
	t := &tracked{}
	t.BufferedRequest = &BufferedRequest{
		Data:  data,
		Bytes: size,
		Done: func(o Outcome) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.outcomes = append(t.outcomes, o)
		},
		Release: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.released++
		},
	}
	return t
}

func (t *tracked) state() (outcomes []Outcome, released int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Outcome(nil), t.outcomes...), t.released
}

// settled reports whether Done and Release each ran exactly once.
func (t *tracked) settled() bool {
	outcomes, released := t.state()
	return len(outcomes) == 1 && released == 1
}

// recorder is a Delivery that records every batch and answers with deliver.
type recorder struct {
	deliver func(ctx context.Context, batch []any) error
	batches [][]any
	mu      sync.Mutex
}

func (r *recorder) Deliver(ctx context.Context, batch []any) error {
	r.mu.Lock()
	r.batches = append(r.batches, append([]any(nil), batch...))
	r.mu.Unlock()
	if r.deliver != nil {
		return r.deliver(ctx, batch)
	}
	return nil
}

func (r *recorder) seen() [][]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]any(nil), r.batches...)
}

func offerAll(t *testing.T, p BufferedPublisher, reqs ...*tracked) {
	t.Helper()
	for i, r := range reqs {
		require.Equal(t, Accepted, p.Offer(r.BufferedRequest), "request %d", i)
	}
}

func requests(n int, size int64) []*tracked {
	reqs := make([]*tracked, n)
	for i := range reqs {
		reqs[i] = newTracked(i, size)
	}
	return reqs
}

func charged(p BufferedPublisher) int64 {
	c, _ := p.Bytes()
	return c
}

// TestOfferReportsEachRefusal checks that each refusal names its own cause,
// and that a refused request stays with the caller: neither Done nor Release
// runs, and nothing is charged.
func TestOfferReportsEachRefusal(t *testing.T) {
	high := atomic.Bool{}
	cfg := testConfig()
	cfg.MaxCount = 2
	cfg.Pressure = high.Load
	p := NewBufferedPublisher(context.Background(), cfg)

	refused := newTracked("refused", 10)
	high.Store(true)
	assert.Equal(t, RefusedPressure, p.Offer(refused.BufferedRequest))
	high.Store(false)

	assert.Equal(t, RefusedFull, p.Offer(newTracked("too large", 1001).BufferedRequest), "the byte allowance")
	offerAll(t, p, requests(2, 10)...)
	assert.Equal(t, RefusedFull, p.Offer(refused.BufferedRequest), "the count bound")
	assert.Equal(t, int64(20), charged(p), "a refusal charged its bytes")

	p.Close()
	assert.Equal(t, RefusedClosed, p.Offer(refused.BufferedRequest))
	outcomes, released := refused.state()
	assert.Empty(t, outcomes, "Done ran for a refused request")
	assert.Zero(t, released, "Release ran for a refused request")
}

// TestChargedCountsQueuedAndInFlightUntilRelease holds a batch in Deliver:
// both the queued and the in-flight bytes count against the allowance, and
// they are returned only on Release.
func TestChargedCountsQueuedAndInFlightUntilRelease(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 2
	p := NewBufferedPublisher(context.Background(), cfg)
	entered, unblock := make(chan struct{}, 1), make(chan struct{})
	d := &recorder{deliver: func(context.Context, []any) error {
		entered <- struct{}{}
		<-unblock
		return nil
	}}
	p.Start(d)
	reqs := requests(3, 100)
	offerAll(t, p, reqs...)
	<-entered

	c, inFlight := p.Bytes()
	assert.Equal(t, int64(300), c, "queued and in-flight bytes are both charged")
	assert.Equal(t, int64(200), inFlight, "the first batch of two is in flight")
	assert.Equal(t, RefusedFull, p.Offer(newTracked("over", 701).BufferedRequest), "in-flight bytes left room")

	close(unblock)
	p.Close()
	for i, r := range reqs {
		outcomes, released := r.state()
		require.Len(t, outcomes, 1, "request %d", i)
		assert.True(t, outcomes[0].Delivered, "request %d", i)
		assert.Equal(t, 1, released, "request %d", i)
	}
	c, inFlight = p.Bytes()
	assert.Zero(t, c)
	assert.Zero(t, inFlight)
}

// TestRequestsBeforeStartAreHeldThenDelivered covers startup: requests are
// accepted before the destination is ready, within the same allowance, and
// delivered in order once Start runs.
func TestRequestsBeforeStartAreHeldThenDelivered(t *testing.T) {
	cfg := testConfig()
	cfg.FlushInterval = 10 * time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	reqs := requests(10, 100)
	offerAll(t, p, reqs...)
	assert.Equal(t, RefusedFull, p.Offer(newTracked("over", 1).BufferedRequest), "the allowance holds before Start")

	d := &recorder{}
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, d.seen(), "delivered before Start")
	p.Start(d)
	require.Eventually(t, func() bool {
		for _, r := range reqs {
			if !r.settled() {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)

	var order []any
	for _, b := range d.seen() {
		order = append(order, b...)
	}
	assert.Equal(t, []any{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, order)
	p.Close()
}

// TestCloseBeforeStartSettlesEverything covers a process that stops before
// its destination was ever ready.
func TestCloseBeforeStartSettlesEverything(t *testing.T) {
	p := NewBufferedPublisher(context.Background(), testConfig())
	reqs := requests(3, 10)
	offerAll(t, p, reqs...)
	p.Close()
	for i, r := range reqs {
		outcomes, released := r.state()
		require.Len(t, outcomes, 1, "request %d", i)
		assert.Equal(t, OutcomeShutdownDeadline, outcomes[0].Reason, "request %d", i)
		assert.Equal(t, 1, released, "request %d", i)
	}
	assert.Zero(t, charged(p))

	d := &recorder{}
	goroutines := runtime.NumGoroutine()
	p.Start(d)
	time.Sleep(10 * time.Millisecond)
	assert.Empty(t, d.seen(), "Start after Close delivered")
	assert.LessOrEqual(t, runtime.NumGoroutine(), goroutines, "Start after Close left a delivery goroutine running")
}

// TestWriterPausedBeforeOfferIsRefusedAfterClose is the review's race: a
// writer that obtained the publisher before Close and calls Offer after Close
// has drained. Its request is refused as closed, so the caller counts it,
// instead of queueing it where nothing would deliver it.
func TestWriterPausedBeforeOfferIsRefusedAfterClose(t *testing.T) {
	p := NewBufferedPublisher(context.Background(), testConfig())
	p.Start(&recorder{})
	loaded, resume := make(chan BufferedPublisher), make(chan struct{})
	result := make(chan Admission)
	//panicdiag:allow-rawgo test writer
	go func() {
		pub := <-loaded
		<-resume
		result <- pub.Offer(newTracked("late", 10).BufferedRequest)
	}()
	loaded <- p
	p.Close()
	close(resume)
	assert.Equal(t, RefusedClosed, <-result)
	assert.Zero(t, charged(p))
}

// TestCloseWaitsForAnOfferInProgress pauses a writer inside Offer, after it
// passed the closed check: Close waits for it, and its request is delivered.
func TestCloseWaitsForAnOfferInProgress(t *testing.T) {
	inside, resume := make(chan struct{}), make(chan struct{})
	var pause atomic.Bool
	cfg := testConfig()
	cfg.Pressure = func() bool {
		if pause.Load() {
			close(inside)
			<-resume
		}
		return false
	}
	p := NewBufferedPublisher(context.Background(), cfg)
	d := &recorder{}
	p.Start(d)

	pause.Store(true)
	req := newTracked("in progress", 10)
	//panicdiag:allow-rawgo test writer
	go func() {
		p.Offer(req.BufferedRequest)
	}()
	<-inside
	closed := make(chan struct{})
	//panicdiag:allow-rawgo test closer
	go func() {
		p.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while an Offer was in progress")
	case <-time.After(50 * time.Millisecond):
	}
	close(resume)
	<-closed
	outcomes, released := req.state()
	require.Len(t, outcomes, 1)
	assert.True(t, outcomes[0].Delivered, "the request accepted during Close was not delivered: %+v", outcomes[0])
	assert.Equal(t, 1, released)
}

// TestOfferOutlastingTheDeadlineIsStillSettled pauses a writer inside Offer
// past the drain deadline. Close returns on time, and the request the writer
// then adds is settled as shutdown_deadline rather than left queued.
func TestOfferOutlastingTheDeadlineIsStillSettled(t *testing.T) {
	inside, resume := make(chan struct{}), make(chan struct{})
	var pause atomic.Bool
	cfg := testConfig()
	cfg.DrainTimeout = 50 * time.Millisecond
	cfg.Pressure = func() bool {
		if pause.Load() {
			close(inside)
			<-resume
		}
		return false
	}
	p := NewBufferedPublisher(context.Background(), cfg)
	p.Start(&recorder{})

	pause.Store(true)
	req := newTracked("outlasting", 10)
	result := make(chan Admission)
	//panicdiag:allow-rawgo test writer
	go func() {
		result <- p.Offer(req.BufferedRequest)
	}()
	<-inside
	start := time.Now()
	p.Close()
	assert.Less(t, time.Since(start), time.Second, "Close waited past its deadline")

	close(resume)
	assert.Equal(t, Accepted, <-result)
	outcomes, released := req.state()
	require.Len(t, outcomes, 1, "the late request was left queued")
	assert.Equal(t, OutcomeShutdownDeadline, outcomes[0].Reason)
	assert.Equal(t, 1, released)
	assert.Zero(t, charged(p))
}

// TestConcurrentOfferAndClose races writers against Close: every accepted
// request is settled exactly once, and every refusal stays with its writer.
func TestConcurrentOfferAndClose(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBytes = 1 << 20
	cfg.MaxCount = 1 << 14
	cfg.BatchCount = 16
	cfg.FlushInterval = time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	p.Start(&recorder{})

	const writers, each = 8, 500
	var accepted, refused sync.Map
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test writer
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				r := newTracked(w*each+i, 1)
				if p.Offer(r.BufferedRequest) == Accepted {
					accepted.Store(r, struct{}{})
				} else {
					refused.Store(r, struct{}{})
				}
			}
		}(w)
	}
	time.Sleep(2 * time.Millisecond)
	p.Close()
	wg.Wait()

	accepted.Range(func(k, _ any) bool {
		outcomes, released := k.(*tracked).state()
		assert.Len(t, outcomes, 1)
		assert.Equal(t, 1, released)
		return true
	})
	refused.Range(func(k, _ any) bool {
		outcomes, released := k.(*tracked).state()
		assert.Empty(t, outcomes)
		assert.Zero(t, released)
		return true
	})
	assert.Zero(t, charged(p))
}

// TestRetriedRequestsKeepTheBatchLimits fails delivery while requests
// accumulate, then recovers: every batch, retried or not, stays within the
// count and byte limits.
func TestRetriedRequestsKeepTheBatchLimits(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBytes = 4000
	cfg.BatchCount = 4
	cfg.BatchBytes = 250
	cfg.FlushInterval = 20 * time.Millisecond
	cfg.MaxRetries = 100
	p := NewBufferedPublisher(context.Background(), cfg)
	var unready atomic.Bool
	unready.Store(true)
	d := &recorder{deliver: func(context.Context, []any) error {
		if unready.Load() {
			return Retryable(bus.ErrTopicNotExist)
		}
		return nil
	}}
	p.Start(d)
	reqs := requests(30, 100)
	offerAll(t, p, reqs...)
	require.Eventually(t, func() bool { return len(d.seen()) >= 3 }, 3*time.Second, time.Millisecond)
	unready.Store(false)
	require.Eventually(t, func() bool {
		for _, r := range reqs {
			if !r.settled() {
				return false
			}
		}
		return true
	}, 3*time.Second, 5*time.Millisecond)

	for i, b := range d.seen() {
		assert.LessOrEqual(t, len(b), 4, "batch %d is over the count limit", i)
		assert.LessOrEqual(t, int64(len(b))*100, int64(250), "batch %d is over the byte limit", i)
	}
	for i, r := range reqs {
		outcomes, _ := r.state()
		assert.True(t, outcomes[0].Delivered, "request %d", i)
	}
	p.Close()
}

// TestRetriesWaitForTheInterval keeps a retry to once per interval: while a
// batch waits for its retry, new requests wait with it instead of each
// starting another attempt.
func TestRetriesWaitForTheInterval(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	cfg.FlushInterval = time.Hour
	p := NewBufferedPublisher(context.Background(), cfg)
	var attempts atomic.Int64
	p.Start(&recorder{deliver: func(context.Context, []any) error {
		attempts.Add(1)
		return Retryable(bus.ErrTopicNotExist)
	}})
	offerAll(t, p, newTracked("first", 1))
	require.Eventually(t, func() bool { return attempts.Load() == 1 }, 3*time.Second, time.Millisecond)
	offerAll(t, p, requests(5, 1)...)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(1), attempts.Load(), "new requests started attempts while a retry was pending")
	p.Close()
}

// TestRetriesAreBounded pins the limit: a destination that never becomes
// ready costs each request its first attempt and MaxRetries more.
func TestRetriesAreBounded(t *testing.T) {
	cfg := testConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.BatchCount = 2
	p := NewBufferedPublisher(context.Background(), cfg)
	var attempts atomic.Int64
	d := &recorder{deliver: func(context.Context, []any) error {
		attempts.Add(1)
		return Retryable(bus.ErrTopicNotExist)
	}}
	p.Start(d)
	reqs := requests(2, 10)
	offerAll(t, p, reqs...)
	require.Eventually(t, func() bool { return reqs[0].settled() && reqs[1].settled() }, 3*time.Second, time.Millisecond)

	assert.Equal(t, int64(cfg.MaxRetries+1), attempts.Load())
	for i, r := range reqs {
		outcomes, _ := r.state()
		assert.Equal(t, OutcomeRetriesExhausted, outcomes[0].Reason, "request %d", i)
		assert.ErrorIs(t, outcomes[0].Err, bus.ErrTopicNotExist, "request %d", i)
	}
	p.Close()
}

// TestFailureEndsTheBatchWithItsError covers a failure that is not retried,
// and a panic, which costs that batch and not the delivery goroutine.
func TestFailureEndsTheBatchWithItsError(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	p := NewBufferedPublisher(context.Background(), cfg)
	refused := errors.New("refused")
	d := &recorder{deliver: func(_ context.Context, batch []any) error {
		switch batch[0] {
		case "refused":
			return refused
		case "panic":
			panic("induced")
		}
		return nil
	}}
	p.Start(d)
	reqs := []*tracked{newTracked("refused", 1), newTracked("panic", 1), newTracked("fine", 1)}
	offerAll(t, p, reqs...)
	p.Close()

	outcomes, _ := reqs[0].state()
	assert.Equal(t, OutcomeDeliveryFailed, outcomes[0].Reason)
	assert.ErrorIs(t, outcomes[0].Err, refused)
	outcomes, _ = reqs[1].state()
	assert.Equal(t, OutcomeDeliveryFailed, outcomes[0].Reason)
	assert.ErrorContains(t, outcomes[0].Err, "panicked")
	outcomes, _ = reqs[2].state()
	assert.True(t, outcomes[0].Delivered, "the batch after a panic was not delivered")
}

// TestBatchCloses checks the three triggers: the count, the bytes and the
// interval.
func TestBatchCloses(t *testing.T) {
	for name, adjust := range map[string]func(*BufferedConfig){
		"count":    func(c *BufferedConfig) { c.BatchCount = 3 },
		"bytes":    func(c *BufferedConfig) { c.BatchBytes = 30 },
		"interval": func(c *BufferedConfig) { c.FlushInterval = 10 * time.Millisecond },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			adjust(&cfg)
			p := NewBufferedPublisher(context.Background(), cfg)
			d := &recorder{}
			p.Start(d)
			reqs := requests(3, 10)
			offerAll(t, p, reqs...)
			require.Eventually(t, func() bool { return reqs[2].settled() }, 3*time.Second, time.Millisecond)
			p.Close()
		})
	}
}

// TestDrainDeliversWhatWasAccepted covers shutdown: no trigger fires before
// Close, and the drain still delivers every accepted request, in batches.
func TestDrainDeliversWhatWasAccepted(t *testing.T) {
	cfg := testConfig()
	cfg.BatchBytes = 30
	p := NewBufferedPublisher(context.Background(), cfg)
	d := &recorder{}
	reqs := requests(10, 10)
	offerAll(t, p, reqs...)
	p.Start(d)
	p.Close()

	for i, r := range reqs {
		outcomes, released := r.state()
		require.Len(t, outcomes, 1, "request %d", i)
		assert.True(t, outcomes[0].Delivered, "request %d was not drained: %+v", i, outcomes[0])
		assert.Equal(t, 1, released, "request %d", i)
	}
	for i, b := range d.seen() {
		assert.LessOrEqual(t, len(b), 3, "drained batch %d is over the byte limit", i)
	}
}

// TestDrainDoesNotRetry makes a retryable failure final at shutdown, because
// there is no next tick to retry on.
func TestDrainDoesNotRetry(t *testing.T) {
	p := NewBufferedPublisher(context.Background(), testConfig())
	d := &recorder{deliver: func(context.Context, []any) error {
		return Retryable(bus.ErrTopicNotExist)
	}}
	failing := newTracked("failing", 10)
	offerAll(t, p, failing)
	p.Start(d)
	p.Close()

	outcomes, released := failing.state()
	require.Len(t, outcomes, 1)
	assert.Equal(t, OutcomeDeliveryFailed, outcomes[0].Reason)
	assert.ErrorIs(t, outcomes[0].Err, bus.ErrTopicNotExist)
	assert.Equal(t, 1, released)
	assert.Len(t, d.seen(), 1, "the drain retried")
}

// TestDrainStopsAtTheDeadline publishes slowly: the drain delivers what fits
// before its deadline and settles the rest as shutdown_deadline. Every request
// is settled by the time Close returns; one still in a Deliver is released
// when that Deliver returns.
func TestDrainStopsAtTheDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	cfg.DrainTimeout = 30 * time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	d := &recorder{deliver: func(context.Context, []any) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}}
	reqs := requests(20, 10)
	offerAll(t, p, reqs...)
	p.Start(d)
	p.Close()

	var delivered, lost int
	for _, r := range reqs {
		outcomes, _ := r.state()
		require.Len(t, outcomes, 1, "not settled by the time Close returned")
		if outcomes[0].Delivered {
			delivered++
		} else if outcomes[0].Reason == OutcomeShutdownDeadline {
			lost++
		}
	}
	assert.Equal(t, len(reqs), delivered+lost)
	assert.Positive(t, lost, "the drain ran past its deadline")
	require.Eventually(t, func() bool {
		for _, r := range reqs {
			if !r.settled() {
				return false
			}
		}
		return true
	}, 3*time.Second, time.Millisecond)
	assert.Zero(t, charged(p))
}

// TestStuckDeliveryIsSettledOnTimeAndReleasedLater is the stuck-delivery
// contract: Close returns at the deadline, the stuck requests receive Done
// with shutdown_deadline by then and stay charged, and Release runs only
// once Deliver returns. A late success does not change the outcome.
func TestStuckDeliveryIsSettledOnTimeAndReleasedLater(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	cfg.DrainTimeout = 50 * time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	entered, unblock := make(chan struct{}, 1), make(chan struct{})
	d := &recorder{deliver: func(context.Context, []any) error {
		entered <- struct{}{}
		<-unblock
		return nil
	}}
	p.Start(d)
	stuck, queued := newTracked("stuck", 10), newTracked("queued", 10)
	offerAll(t, p, stuck, queued)
	<-entered

	start := time.Now()
	p.Close()
	assert.Less(t, time.Since(start), time.Second, "Close waited for the stuck delivery")

	outcomes, released := stuck.state()
	require.Len(t, outcomes, 1, "Done did not run by the deadline")
	assert.Equal(t, OutcomeShutdownDeadline, outcomes[0].Reason)
	assert.Zero(t, released, "released while Deliver can still read it")
	assert.Equal(t, int64(10), charged(p), "the stuck request stopped being charged")
	assert.True(t, queued.settled(), "the request not in the stuck delivery was not settled")

	close(unblock)
	require.Eventually(t, func() bool {
		_, released := stuck.state()
		return released == 1
	}, 3*time.Second, time.Millisecond)
	outcomes, _ = stuck.state()
	assert.Len(t, outcomes, 1, "Done ran again when the stuck delivery returned")
	assert.Zero(t, charged(p))
}

// TestReleaseWaitsForTheDoneCloseIsRunning returns a stuck delivery while
// Close is still inside the Done it runs at the deadline. The request must not
// be released, and so reused, until that Done has returned.
func TestReleaseWaitsForTheDoneCloseIsRunning(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	cfg.DrainTimeout = 50 * time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	entered, unblock := make(chan struct{}, 1), make(chan struct{})
	p.Start(&recorder{deliver: func(context.Context, []any) error {
		entered <- struct{}{}
		<-unblock
		return nil
	}})
	var inDone, overlapped atomic.Bool
	doneEntered, resumeDone, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	req := &BufferedRequest{
		Data:  "stuck",
		Bytes: 10,
		Done: func(Outcome) {
			inDone.Store(true)
			close(doneEntered)
			<-resumeDone
			inDone.Store(false)
		},
		Release: func() {
			overlapped.Store(inDone.Load())
			close(released)
		},
	}
	require.Equal(t, Accepted, p.Offer(req))
	<-entered

	closed := make(chan struct{})
	//panicdiag:allow-rawgo test closer
	go func() {
		p.Close()
		close(closed)
	}()
	<-doneEntered
	close(unblock)
	require.Eventually(t, func() bool {
		_, inFlight := p.Bytes()
		return inFlight == 0
	}, 3*time.Second, time.Millisecond, "the stuck delivery did not return")
	select {
	case <-released:
		t.Fatal("released while Close was still running Done")
	case <-time.After(100 * time.Millisecond):
	}

	close(resumeDone)
	for name, ch := range map[string]chan struct{}{"Close did not return": closed, "never released": released} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal(name)
		}
	}
	assert.False(t, overlapped.Load(), "Release ran before Done returned")
	assert.Zero(t, charged(p))
}

type ctxKey struct{}

var testTopic = bus.BiTopic("buffered-test")

// TestDeliverRunsUnderThePublisherContext keeps a value the caller puts on
// the context, such as the native write mark, on the delivery goroutine.
func TestDeliverRunsUnderThePublisherContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "marked")
	cfg := testConfig()
	cfg.BatchCount = 1
	p := NewBufferedPublisher(ctx, cfg)
	var seen atomic.Value
	p.Start(&recorder{deliver: func(ctx context.Context, _ []any) error {
		seen.Store(ctx.Value(ctxKey{}))
		return nil
	}})
	offerAll(t, p, newTracked("m", 1))
	p.Close()
	assert.Equal(t, "marked", seen.Load())
}

// TestLocalDeliveryReportsWhatTheDestinationDid covers the local adapter:
// a refusal in Close's map is a failure, a topic with no subscriber is
// retryable, and an empty map is success.
func TestLocalDeliveryReportsWhatTheDestinationDid(t *testing.T) {
	refusal := common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high")
	for name, tc := range map[string]struct {
		check   func(t *testing.T, err error)
		nodeErr map[string]*common.Error
		err     error
	}{
		"delivered": {check: func(t *testing.T, err error) { assert.NoError(t, err) }},
		"refused in the map": {
			nodeErr: map[string]*common.Error{"local": refusal},
			check: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, refusal)
				assert.NotErrorIs(t, err, ErrRetryable)
			},
		},
		"no subscriber": {
			err: bus.ErrTopicNotExist,
			check: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, ErrRetryable)
				assert.ErrorIs(t, err, bus.ErrTopicNotExist)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client, publisher := NewMockClient(ctrl), NewMockBatchPublisher(ctrl)
			client.EXPECT().NewBatchPublisher(time.Second).Return(publisher)
			publisher.EXPECT().Publish(gomock.Any(), testTopic, gomock.Len(2)).Return(nil, nil)
			publisher.EXPECT().Close().Return(tc.nodeErr, tc.err)
			d := NewLocalDelivery(client, testTopic, time.Second)
			tc.check(t, d.Deliver(context.Background(), []any{"a", "b"}))
		})
	}
}

// TestRemoteDeliveryReportsWhatTheNodeDid covers the remote adapter: every
// message carries the located node; what never reached a node is retryable;
// what the node answered, or may have stored, is not.
func TestRemoteDeliveryReportsWhatTheNodeDid(t *testing.T) {
	refusal := common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high")
	notSent := func() error { return NotSent(errors.New("circuit breaker open for node data-1")) }
	sendFailed := errors.New("failed to send message to node data-1")
	retryable := func(t *testing.T, err error) {
		assert.ErrorIs(t, err, ErrRetryable)
	}
	final := func(t *testing.T, err error) {
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrRetryable)
	}
	for name, tc := range map[string]struct {
		publishErr error
		nodeErr    map[string]*common.Error
		check      func(t *testing.T, err error)
		stall      bool
	}{
		"delivered":        {check: func(t *testing.T, err error) { assert.NoError(t, err) }},
		"no message sent":  {publishErr: multierr.Combine(notSent(), notSent()), check: retryable},
		"one message sent": {publishErr: notSent(), check: final},
		"a send failed":    {publishErr: multierr.Combine(notSent(), sendFailed), check: final},
		"refused in the map": {
			nodeErr: map[string]*common.Error{"data-1": refusal},
			check: func(t *testing.T, err error) {
				final(t, err)
				assert.ErrorIs(t, err, refusal)
			},
		},
		"no answer in time": {
			stall: true,
			check: func(t *testing.T, err error) {
				final(t, err)
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client, publisher := NewMockClient(ctrl), NewMockBatchPublisher(ctrl)
			timeout := time.Second
			if tc.stall {
				timeout = 20 * time.Millisecond
			}
			client.EXPECT().NewBatchPublisher(timeout).Return(publisher)
			publisher.EXPECT().Publish(gomock.Any(), testTopic, gomock.Len(2)).DoAndReturn(
				func(ctx context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
					for _, m := range messages {
						assert.Equal(t, "data-1", m.Node())
					}
					if tc.stall {
						<-ctx.Done()
					}
					return nil, tc.publishErr
				})
			publisher.EXPECT().Close().Return(tc.nodeErr, nil)
			d := NewRemoteDelivery(client, testTopic, func() (string, error) { return "data-1", nil }, timeout)
			tc.check(t, d.Deliver(context.Background(), []any{"a", "b"}))
		})
	}
}

// TestRemoteDeliveryRetriesWhatNoNodeReceived covers a failed locate: it is
// retryable, keeps its cause, and publishes nothing.
func TestRemoteDeliveryRetriesWhatNoNodeReceived(t *testing.T) {
	cause := errors.New("no nodes available")
	client := NewMockClient(gomock.NewController(t))
	for name, locate := range map[string]func() (string, error){
		"locate failed":  func() (string, error) { return "", cause },
		"no node to use": func() (string, error) { return "", nil },
	} {
		t.Run(name, func(t *testing.T) {
			err := NewRemoteDelivery(client, testTopic, locate, time.Second).Deliver(context.Background(), []any{"a"})
			assert.ErrorIs(t, err, ErrRetryable)
		})
	}
	err := NewRemoteDelivery(client, testTopic, func() (string, error) { return "", cause }, time.Second).
		Deliver(context.Background(), []any{"a"})
	assert.ErrorIs(t, err, cause)
}

// TestRetriedBatchGoesToTheNodeLocatedThen follows a node failure through the
// publisher: the first attempt reaches no node, and the retry goes to the
// node locate returns by then, as after the client evicted the first one.
func TestRetriedBatchGoesToTheNodeLocatedThen(t *testing.T) {
	cfg := testConfig()
	cfg.BatchCount = 1
	cfg.FlushInterval = 10 * time.Millisecond
	p := NewBufferedPublisher(context.Background(), cfg)
	ctrl := gomock.NewController(t)
	client := NewMockClient(ctrl)
	var mu sync.Mutex
	var nodes []string
	located := []string{"data-1", "data-2"}
	client.EXPECT().NewBatchPublisher(time.Second).DoAndReturn(func(time.Duration) BatchPublisher {
		publisher := NewMockBatchPublisher(ctrl)
		publisher.EXPECT().Publish(gomock.Any(), testTopic, gomock.Len(1)).DoAndReturn(
			func(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
				mu.Lock()
				defer mu.Unlock()
				nodes = append(nodes, messages[0].Node())
				if messages[0].Node() == "data-1" {
					return nil, NotSent(errors.New("failed to get client for node data-1"))
				}
				return nil, nil
			})
		publisher.EXPECT().Close().Return(nil, nil)
		return publisher
	}).Times(2)
	attempt := 0
	p.Start(NewRemoteDelivery(client, testTopic, func() (string, error) {
		node := located[min(attempt, len(located)-1)]
		attempt++
		return node, nil
	}, time.Second))
	req := newTracked("event", 10)
	offerAll(t, p, req)
	require.Eventually(t, req.settled, 3*time.Second, time.Millisecond)
	p.Close()
	outcomes, _ := req.state()
	assert.True(t, outcomes[0].Delivered, "the retry was not delivered: %+v", outcomes[0])
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"data-1", "data-2"}, nodes)
}
