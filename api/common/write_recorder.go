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

package common

import (
	"context"
	"sort"
	"sync"
	"time"
)

// The back-off between the attempts of RetryWrite.
var (
	writeRetryBackoff    = 100 * time.Millisecond
	writeRetryBackoffMax = 2 * time.Second
)

// WriteRecorder collects the failures a write listener meets while it writes one message. A
// write listener logs such a failure and goes on with the rest of the message; a caller that
// needs to know about them puts a recorder in the context it calls the listener with. A row
// that failed before it was stored is recorded by its index in the message, so the caller can
// write it again; a failure after the rows were stored is recorded on its own. Until its
// deadline, the recorder also lets RetryWrite retry a write that may be repeated.
type WriteRecorder struct {
	deadline time.Time
	rows     map[int]error
	failures []error
	mu       sync.Mutex
}

// RowFailure is a row of a message that failed before it was stored.
type RowFailure struct {
	Err error
	Row int
}

// NewWriteRecorder returns a recorder whose RetryWrite retries until deadline; a zero deadline
// never retries.
func NewWriteRecorder(deadline time.Time) *WriteRecorder {
	return &WriteRecorder{deadline: deadline}
}

type writeRecorderKey struct{}

// WithWriteRecorder returns ctx carrying r, so that the write listener called with it records
// its failures in r.
func WithWriteRecorder(ctx context.Context, r *WriteRecorder) context.Context {
	return context.WithValue(ctx, writeRecorderKey{}, r)
}

func recorderOf(ctx context.Context) *WriteRecorder {
	r, _ := ctx.Value(writeRecorderKey{}).(*WriteRecorder)
	return r
}

// RecordRowFailure records that row row of the message failed before it was stored, in the
// recorder ctx carries; without one it does nothing. The first failure of a row is kept.
func RecordRowFailure(ctx context.Context, row int, err error) {
	r := recorderOf(ctx)
	if r == nil || err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rows == nil {
		r.rows = make(map[int]error)
	}
	if _, ok := r.rows[row]; !ok {
		r.rows[row] = err
	}
}

// RecordWriteFailure records a failure after the rows were stored, such as an index write, in
// the recorder ctx carries; without one it does nothing.
func RecordWriteFailure(ctx context.Context, err error) {
	r := recorderOf(ctx)
	if r == nil || err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, err)
}

// RetryWrite runs write, which must be safe to repeat, and returns its error. Under a recorder
// it retries a failed write with back-off until the recorder's deadline or the end of ctx, and
// returns the last error; without one it runs write once.
func RetryWrite(ctx context.Context, write func() error) error {
	err := write()
	r := recorderOf(ctx)
	if err == nil || r == nil {
		return err
	}
	backoff := writeRetryBackoff
	for {
		wait := min(backoff, time.Until(r.deadline))
		if wait <= 0 {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if err = write(); err == nil {
			return nil
		}
		backoff = min(backoff*2, writeRetryBackoffMax)
	}
}

// RowFailures returns the rows that failed before they were stored, by row.
func (r *WriteRecorder) RowFailures() []RowFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RowFailure, 0, len(r.rows))
	for row, err := range r.rows {
		out = append(out, RowFailure{Row: row, Err: err})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Row < out[j].Row })
	return out
}

// Failures returns the failures after the rows were stored, in the order they were recorded.
func (r *WriteRecorder) Failures() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.failures...)
}
