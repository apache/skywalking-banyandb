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

package resume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ErrClosed is what Sync returns once the persister is closed.
var ErrClosed = errors.New("the progress persister is closed")

// Persister is the one goroutine of a run that owns its progress file. Workers send it
// changes of the state and never touch the file: each change is applied in the persister's
// goroutine, and the state is written (WriteFile) once after every batch of queued changes that
// holds one that is not lazy (UpdateLazy).
// The first write error is sticky: nothing more is written, onError is called once, and Sync
// and Close return it.
type Persister[T any] struct {
	lastWrite time.Time
	ctx       context.Context
	state     *T
	write     func(*T) error
	onError   func(error)
	reqs      chan request[T]
	done      chan struct{}
	err       error
	opts      PersisterOptions
	// closeMu guards closed and the sends on reqs: a send holds it for reading, Close for
	// writing, so no send races the close of reqs.
	closeMu sync.RWMutex
	closed  bool
	// dirty is set while a lazy change is applied but not written yet; only the loop uses it.
	dirty bool
	// pending is set while a change that is not lazy waits for the write MinInterval defers;
	// only the loop uses it.
	pending bool
}

// PersisterOptions tune a persister (NewPersisterWith).
type PersisterOptions struct {
	// MinInterval, when set, spaces the writes Update causes at least that far apart: an Update
	// within it of the last write is written with the next write, at the latest once it has
	// passed. Sync and Close write at once. It suits changes whose loss in a crash only costs
	// work done again, such as finished export units, while the file is large.
	MinInterval time.Duration
	// CancelableUpdates makes Update and UpdateLazy give up, dropping the change, when the
	// queue is full and the context NewPersisterWith was given ends: an interrupted run does not
	// wait on a persister behind on its writes. Use it only where a dropped change costs work
	// done again and nothing more.
	CancelableUpdates bool
}

type request[T any] struct {
	change func(*T)
	synced chan error // nil for Update and UpdateLazy
	lazy   bool
}

// NewPersister starts the persister of state; write persists it (WriteFile of the path with
// any last-minute field such as updatedAt), and onError (may be nil) is told of the first
// failed write, so the run can stop. The persister outlives the cancellation of ctx: an
// interrupted run still records what finished; Close stops it.
func NewPersister[T any](ctx context.Context, state *T, write func(*T) error, onError func(error)) *Persister[T] {
	return NewPersisterWith(ctx, state, write, onError, PersisterOptions{})
}

// NewPersisterWith is NewPersister tuned by opts.
func NewPersisterWith[T any](ctx context.Context, state *T, write func(*T) error, onError func(error), opts PersisterOptions) *Persister[T] {
	p := &Persister[T]{
		ctx: ctx, state: state, write: write, onError: onError, opts: opts,
		reqs: make(chan request[T], 256), done: make(chan struct{}),
	}
	run.Go(context.WithoutCancel(ctx), "transfer-progress-persister", logger.GetLogger("transfer"), func(context.Context) {
		defer close(p.done)
		p.loop()
	})
	return p
}

func (p *Persister[T]) loop() {
	// due fires when a write MinInterval deferred is due; nil while none is.
	var due <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		var req request[T]
		var ok bool
		select {
		case req, ok = <-p.reqs:
		case <-due:
			due = nil
			if p.pending {
				p.apply(nil, true)
			}
			continue
		}
		if !ok {
			break
		}
		batch := []request[T]{req}
		// At most a queue's worth per batch: producers as fast as the writes would otherwise keep
		// the batch growing, and its write and the Syncs in it waiting.
	drain:
		for len(batch) < cap(p.reqs) {
			select {
			case more, open := <-p.reqs:
				if !open {
					break drain
				}
				batch = append(batch, more)
			default:
				break drain
			}
		}
		p.apply(batch, false)
		for _, r := range batch {
			if r.synced != nil {
				r.synced <- p.err
			}
		}
		if !p.pending && due != nil {
			// A write (a Sync's) took what waited: the next wait counts from it, not from the
			// write before.
			timer.Stop()
			due = nil
		}
		if p.pending && due == nil {
			wait := p.opts.MinInterval - time.Since(p.lastWrite)
			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				timer.Reset(wait)
			}
			due = timer.C
		}
	}
	if p.dirty || p.pending {
		// Closed with changes not written yet: write them, so the file says what the run had
		// finished and in flight when it stopped.
		p.apply(nil, true)
	}
}

// apply applies a batch of changes and writes the state, unless every change of the batch is
// lazy and force is not set. A panic in a change or in write becomes the sticky error, so the
// run stops with it instead of waiting forever for a persister that is gone; the loop keeps
// answering Sync and Close.
func (p *Persister[T]) apply(batch []request[T], force bool) {
	defer func() {
		if r := recover(); r != nil && p.err == nil {
			p.fail(fmt.Errorf("the progress persister panicked: %v", r))
		}
	}()
	write, update := force, false
	for _, r := range batch {
		if r.change != nil {
			r.change(p.state)
		}
		write = write || r.synced != nil
		update = update || (!r.lazy && r.synced == nil)
	}
	if update && !write {
		// An Update writes at once, unless MinInterval has not passed since the last write.
		write = p.opts.MinInterval <= 0 || time.Since(p.lastWrite) >= p.opts.MinInterval
		p.pending = p.pending || !write
	}
	if !write {
		p.dirty = true
		return
	}
	p.dirty, p.pending = false, false
	p.lastWrite = time.Now()
	if p.err == nil {
		if err := p.write(p.state); err != nil {
			p.fail(err)
		}
	}
}

func (p *Persister[T]) fail(err error) {
	p.err = err
	if p.onError != nil {
		p.onError(err)
	}
}

// Update queues a change of the state; it is written with the next batch, or once
// MinInterval has passed since the last write. It returns at once unless the queue is full. A
// change queued after Close is dropped.
func (p *Persister[T]) Update(change func(*T)) {
	p.send(request[T]{change: change})
}

// send queues an Update or UpdateLazy request, unless the persister is closed. While the queue
// is full it waits, and with CancelableUpdates gives up once the persister's context ends.
func (p *Persister[T]) send(req request[T]) {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	if p.closed {
		return
	}
	if !p.opts.CancelableUpdates {
		p.reqs <- req
		return
	}
	select {
	case p.reqs <- req:
		return
	default:
	}
	select {
	case p.reqs <- req:
	case <-p.ctx.Done():
	}
}

// UpdateLazy queues a change of the state that does not cause a write by itself: it is written
// with the next write another change causes, or when the persister is closed. It suits what is
// worth recording but not worth a write, such as which unit a worker started. It returns at
// once unless the queue is full. A change queued after Close is dropped.
func (p *Persister[T]) UpdateLazy(change func(*T)) {
	p.send(request[T]{change: change, lazy: true})
}

// Sync applies change (may be nil) and returns once the state with every change queued before
// it is written, with the write error, if any so far. After Close it applies nothing and
// returns ErrClosed.
func (p *Persister[T]) Sync(change func(*T)) error {
	synced := make(chan error, 1)
	p.closeMu.RLock()
	if p.closed {
		p.closeMu.RUnlock()
		return ErrClosed
	}
	p.reqs <- request[T]{change: change, synced: synced}
	p.closeMu.RUnlock()
	return <-synced
}

// Close writes what is still queued, stops the goroutine and returns the first write error.
// It is safe to call more than once.
func (p *Persister[T]) Close() error {
	p.closeMu.Lock()
	if !p.closed {
		p.closed = true
		close(p.reqs)
	}
	p.closeMu.Unlock()
	<-p.done
	return p.err
}
