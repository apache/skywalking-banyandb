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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Admission is the decision Offer made, taken at the moment it made it.
type Admission uint8

const (
	// Accepted means the publisher owns the request until it calls Release.
	Accepted Admission = iota
	// RefusedPressure means the memory pressure check reported high pressure.
	RefusedPressure
	// RefusedFull means the allowance had no room for the request.
	RefusedFull
	// RefusedClosed means Close had already started.
	RefusedClosed
)

func (a Admission) String() string {
	switch a {
	case Accepted:
		return "accepted"
	case RefusedPressure:
		return "refused_pressure"
	case RefusedFull:
		return "refused_full"
	case RefusedClosed:
		return "refused_closed"
	default:
		return fmt.Sprintf("admission(%d)", uint8(a))
	}
}

// The reasons an accepted request was not delivered.
const (
	// OutcomeDeliveryFailed is a delivery that failed and is not retried. Outcome.Err holds the error.
	OutcomeDeliveryFailed = "delivery_failed"
	// OutcomeRetriesExhausted is a retryable failure that persisted past the retry limit.
	OutcomeRetriesExhausted = "retries_exhausted"
	// OutcomeShutdownDeadline is a request still held, or still in a delivery, when Close's drain deadline passed.
	OutcomeShutdownDeadline = "shutdown_deadline"
)

// ErrRetryable marks a delivery error that is worth retrying, such as a
// destination that is not ready yet. Any other error ends the request.
var ErrRetryable = errors.New("retryable delivery failure")

// Retryable marks err as retryable. errors.Is still finds err in the result.
func Retryable(err error) error {
	return fmt.Errorf("%w: %w", ErrRetryable, err)
}

// Outcome is how an accepted request ended.
type Outcome struct {
	// Err is the last delivery error, when there was one.
	Err error
	// Reason is why the request was not delivered: one of the Outcome* reasons.
	Reason string
	// Delivered is set only for a completed delivery. Acceptance never counts as written.
	Delivered bool
}

// BufferedRequest is one request offered to a BufferedPublisher. A caller may
// pool requests: once Release runs, the publisher no longer reads it.
type BufferedRequest struct {
	// Data is the message body handed to Delivery.
	Data any
	// Done settles the accounting: exactly once, by the drain deadline at the
	// latest. For a stuck delivery it runs while Deliver may still read and
	// write the request, so it must read only what is safe to share with it.
	Done func(Outcome)
	// Release frees the request: exactly once, after Done has returned, when
	// neither Deliver nor Close can still read it.
	Release func()
	// Bytes is what the request holds, charged against the allowance until Release.
	Bytes    int64
	attempts int
	done     atomic.Bool
	released atomic.Bool
}

// Delivery writes batches to a destination.
type Delivery interface {
	// Deliver writes one batch and reports whether the destination took it. A
	// failure wrapped with Retryable is retried; any other ends the batch. It
	// must not keep batch after it returns.
	Deliver(ctx context.Context, batch []any) error
}

// BufferedPublisher accepts requests without waiting and delivers them in
// batches on its own goroutine.
//
// It owns the whole buffering lifecycle: admission, byte accounting, batching,
// bounded retries and the shutdown drain. Only the Delivery differs between a
// local and a remote destination. BatchPublisher and its callers are unrelated.
type BufferedPublisher interface {
	// Offer accepts or refuses req without waiting for storage, the network
	// or free capacity, and reports the decision it made. On Accepted the
	// publisher owns req until it calls req.Release. On any refusal the caller
	// keeps req.
	Offer(req *BufferedRequest) Admission
	// Start begins delivery. Requests accepted before it are held, within the
	// same allowance. Only the first call has an effect, and none after Close.
	Start(d Delivery)
	// Close stops admission, drains within the drain timeout and returns even
	// when a delivery is stuck.
	Close()
	// Bytes reports the bytes charged against the allowance, and the part of
	// them in a delivery.
	Bytes() (charged, inFlight int64)
}

// BufferedConfig bounds a BufferedPublisher.
type BufferedConfig struct {
	// Pressure reports high memory pressure. It runs on every Offer, so it must
	// not block. Nil means never.
	Pressure func() bool
	// Logger reports a panic of the delivery goroutine itself.
	Logger *logger.Logger
	// Name names the delivery goroutine.
	Name string
	// MaxBytes is the allowance: the bytes held, queued or in a delivery.
	MaxBytes int64
	// MaxCount bounds the number of requests held before delivery takes them.
	MaxCount int
	// BatchCount closes a batch at this many requests.
	BatchCount int
	// BatchBytes closes a batch at this many bytes. Zero means MaxBytes/4.
	BatchBytes int64
	// FlushInterval closes a batch that has not filled, and paces retries.
	FlushInterval time.Duration
	// MaxRetries bounds the retries of one request after its first attempt.
	MaxRetries int
	// DrainTimeout bounds Close.
	DrainTimeout time.Duration
}

type bufferedPublisher struct {
	deadline time.Time
	ctx      context.Context
	queue    chan *BufferedRequest
	stop     chan struct{}
	stopped  chan struct{}
	// settled is closed when abandon no longer reads the batch it took over,
	// Done calls included.
	settled chan struct{}
	batch   []any
	// held is what the delivery goroutine took from queue and has not
	// delivered, oldest first; delivery is the batch in Deliver. Both are
	// guarded by mu, because Close takes them over when a delivery is stuck.
	held      []*BufferedRequest
	delivery  []*BufferedRequest
	cfg       BufferedConfig
	charged   atomic.Int64
	inFlight  atomic.Int64
	active    atomic.Int64
	closeOnce sync.Once
	mu        sync.Mutex
	closed    atomic.Bool
	sealed    atomic.Bool
	started   bool
	abandoned bool
}

// NewBufferedPublisher returns a publisher that holds requests until Start.
// Every Deliver runs under ctx, so a value it carries, such as a mark for the
// logging the destination does, reaches the delivery goroutine.
func NewBufferedPublisher(ctx context.Context, cfg BufferedConfig) BufferedPublisher {
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = cfg.MaxBytes / 4
	}
	if cfg.BatchCount <= 0 {
		cfg.BatchCount = 1
	}
	return &bufferedPublisher{
		ctx:     ctx,
		cfg:     cfg,
		queue:   make(chan *BufferedRequest, cfg.MaxCount),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
		settled: make(chan struct{}),
	}
}

func (p *bufferedPublisher) Offer(req *BufferedRequest) Admission {
	// Registered before the closed flag is read, and Close sets the flag
	// before it reads the count: either Close sees this Offer, or this Offer
	// sees the flag.
	p.active.Add(1)
	defer p.active.Add(-1)
	if p.closed.Load() {
		return RefusedClosed
	}
	if p.cfg.Pressure != nil && p.cfg.Pressure() {
		return RefusedPressure
	}
	if p.charged.Add(req.Bytes) > p.cfg.MaxBytes {
		p.charged.Add(-req.Bytes)
		return RefusedFull
	}
	req.attempts = 0
	req.done.Store(false)
	req.released.Store(false)
	select {
	case p.queue <- req:
	default:
		p.charged.Add(-req.Bytes)
		return RefusedFull
	}
	// Close stopped waiting for this Offer and has already swept the queue.
	// Sweeping again here settles what this Offer added, so it is not left
	// with nothing to deliver it.
	if p.sealed.Load() {
		p.sweep()
	}
	return Accepted
}

func (p *bufferedPublisher) Start(d Delivery) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started || p.closed.Load() {
		return
	}
	p.started = true
	run.Go(p.ctx, p.cfg.Name, p.cfg.Logger, func(context.Context) {
		defer close(p.stopped)
		p.consume(d)
	})
}

func (p *bufferedPublisher) Close() {
	p.closeOnce.Do(func() {
		p.deadline = time.Now().Add(p.cfg.DrainTimeout)
		p.closed.Store(true)
		// Admission never waits, so an Offer in progress finishes quickly. One
		// that has not finished by the deadline is settled by the seal below.
		for p.active.Load() > 0 && time.Now().Before(p.deadline) {
			time.Sleep(100 * time.Microsecond)
		}
		p.mu.Lock()
		started := p.started
		p.mu.Unlock()
		if started {
			close(p.stop)
			timer := time.NewTimer(time.Until(p.deadline))
			select {
			case <-p.stopped:
			case <-timer.C:
				p.abandon()
			}
			timer.Stop()
		}
		p.sealed.Store(true)
		p.sweep()
	})
}

func (p *bufferedPublisher) Bytes() (charged, inFlight int64) {
	return p.charged.Load(), p.inFlight.Load()
}

// consume is the delivery goroutine.
func (p *bufferedPublisher) consume(d Delivery) {
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()
	// retrying is set while the head of held waits for its next attempt. New
	// requests then wait for the tick too, so a retry runs once per interval
	// and not once per request.
	retrying := false
	for {
		select {
		case <-p.stop:
			p.drain(d)
			return
		case <-ticker.C:
			retrying = p.flush(d)
		case req := <-p.queue:
			p.hold(req)
			if !retrying && p.batchReady() {
				retrying = p.flush(d)
			}
		}
	}
}

// hold appends req to held.
func (p *bufferedPublisher) hold(req *BufferedRequest) {
	p.mu.Lock()
	if p.abandoned {
		p.mu.Unlock()
		p.finish(req, Outcome{Reason: OutcomeShutdownDeadline})
		return
	}
	p.held = append(p.held, req)
	p.mu.Unlock()
}

// batchReady reports whether held fills a batch, by count or by bytes.
func (p *bufferedPublisher) batchReady() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.held) >= p.cfg.BatchCount {
		return true
	}
	var size int64
	for _, r := range p.held {
		size += r.Bytes
	}
	return size >= p.cfg.BatchBytes
}

// flush delivers everything held, one batch at a time, and reports whether a
// retryable failure stopped it.
func (p *bufferedPublisher) flush(d Delivery) (retrying bool) {
	for {
		batch := p.take()
		if len(batch) == 0 {
			return false
		}
		if p.deliver(d, batch, true) {
			return true
		}
	}
}

// drain delivers what is held and queued until the deadline, without
// retries: there is no next tick to retry on. What remains is settled as
// shutdown_deadline.
func (p *bufferedPublisher) drain(d Delivery) {
	for time.Now().Before(p.deadline) {
		p.pull()
		batch := p.take()
		if len(batch) == 0 {
			break
		}
		p.deliver(d, batch, false)
	}
	p.pull()
	p.mu.Lock()
	rest := p.held
	p.held = nil
	p.mu.Unlock()
	for _, r := range rest {
		p.finish(r, Outcome{Reason: OutcomeShutdownDeadline})
	}
}

// pull moves what is queued into held.
func (p *bufferedPublisher) pull() {
	for {
		select {
		case req := <-p.queue:
			p.hold(req)
		default:
			return
		}
	}
}

// take removes the next batch from the head of held and marks it in flight.
// A batch closes at the count or before the byte limit; it holds at least one
// request, so a request larger than the limit still goes out alone.
func (p *bufferedPublisher) take() []*BufferedRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.abandoned {
		return nil
	}
	n := 0
	var size int64
	for n < len(p.held) && n < p.cfg.BatchCount {
		if n > 0 && size+p.held[n].Bytes > p.cfg.BatchBytes {
			break
		}
		size += p.held[n].Bytes
		n++
	}
	batch := p.held[:n:n]
	p.held = p.held[n:]
	p.delivery = batch
	return batch
}

// deliver hands one batch to d and settles it. It reports whether the batch,
// or part of it, went back to the head of held for a retry.
func (p *bufferedPublisher) deliver(d Delivery, batch []*BufferedRequest, retainable bool) (retained bool) {
	var size int64
	p.batch = p.batch[:0]
	for _, r := range batch {
		size += r.Bytes
		p.batch = append(p.batch, r.Data)
	}
	p.inFlight.Add(size)
	err := p.call(d, p.batch)
	clear(p.batch)
	p.inFlight.Add(-size)

	p.mu.Lock()
	p.delivery = nil
	abandoned := p.abandoned
	p.mu.Unlock()
	if abandoned {
		// Close settled this batch at its deadline, so only the memory is left
		// to free, now that Deliver no longer reads it. Close may still be
		// running a Done for it, so the batch is freed only once Close is done
		// with it. Only the delivery goroutine waits, so Close stays bounded.
		<-p.settled
		for _, r := range batch {
			p.finish(r, Outcome{Reason: OutcomeShutdownDeadline})
		}
		return false
	}

	switch {
	case err == nil:
		for _, r := range batch {
			p.finish(r, Outcome{Delivered: true})
		}
		return false
	case retainable && errors.Is(err, ErrRetryable):
		var keep []*BufferedRequest
		for _, r := range batch {
			r.attempts++
			if r.attempts > p.cfg.MaxRetries {
				p.finish(r, Outcome{Reason: OutcomeRetriesExhausted, Err: err})
				continue
			}
			keep = append(keep, r)
		}
		if len(keep) == 0 {
			return false
		}
		p.mu.Lock()
		p.held = append(keep, p.held...)
		p.mu.Unlock()
		return true
	default:
		for _, r := range batch {
			p.finish(r, Outcome{Reason: OutcomeDeliveryFailed, Err: err})
		}
		return false
	}
}

// call runs Deliver and turns a panic into an error, so one bad batch costs
// that batch and not the delivery goroutine.
func (p *bufferedPublisher) call(d Delivery, batch []any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("delivery panicked: %v", r)
		}
	}()
	return d.Deliver(p.ctx, batch)
}

// abandon runs when the drain deadline passes with the delivery goroutine
// still busy, most likely in a Deliver that does not return. Everything it
// holds is settled now. What is not in that Deliver is freed now too; what is
// in it is freed by the delivery goroutine if Deliver ever returns, after
// abandon closes settled.
func (p *bufferedPublisher) abandon() {
	p.mu.Lock()
	p.abandoned = true
	held, inDelivery := p.held, p.delivery
	p.held = nil
	p.mu.Unlock()
	for _, r := range inDelivery {
		p.complete(r, Outcome{Reason: OutcomeShutdownDeadline})
	}
	close(p.settled)
	for _, r := range held {
		p.finish(r, Outcome{Reason: OutcomeShutdownDeadline})
	}
}

// sweep settles everything still queued as shutdown_deadline.
func (p *bufferedPublisher) sweep() {
	for {
		select {
		case req := <-p.queue:
			p.finish(req, Outcome{Reason: OutcomeShutdownDeadline})
		default:
			return
		}
	}
}

// finish settles a request and frees it.
func (p *bufferedPublisher) finish(r *BufferedRequest, o Outcome) {
	p.complete(r, o)
	p.release(r)
}

// complete calls Done once. A request Close settled at the deadline keeps
// that outcome, even when its Deliver returns later.
func (p *bufferedPublisher) complete(r *BufferedRequest, o Outcome) {
	if r.done.CompareAndSwap(false, true) && r.Done != nil {
		r.Done(o)
	}
}

// release frees a request once. The bytes are returned first: after Release
// the caller may reuse the request.
func (p *bufferedPublisher) release(r *BufferedRequest) {
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	p.charged.Add(-r.Bytes)
	if r.Release != nil {
		r.Release()
	}
}
