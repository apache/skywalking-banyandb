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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRecordWriteFailureWithoutARecorderDoesNothing(t *testing.T) {
	// Nothing to assert beyond not panicking: a regular write carries no recorder.
	RecordWriteFailure(context.Background(), errors.New("cannot handle write event"))
}

func TestRecordWriteFailureKeepsTheOrder(t *testing.T) {
	r := &WriteRecorder{}
	ctx := WithWriteRecorder(context.Background(), r)
	first, second := errors.New("invalid timestamp"), errors.New("cannot write index")
	RecordWriteFailure(ctx, first)
	RecordWriteFailure(ctx, nil)
	RecordWriteFailure(ctx, second)
	got := r.Failures()
	if len(got) != 2 || !errors.Is(got[0], first) || !errors.Is(got[1], second) {
		t.Fatalf("failures = %v, want [%v %v] without the nil one", got, first, second)
	}
}

func TestRecordWriteFailureSurvivesDerivedContexts(t *testing.T) {
	r := &WriteRecorder{}
	ctx := WithWriteRecorder(context.Background(), r)
	derived, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	RecordWriteFailure(derived, errors.New("cannot write element index"))
	if len(r.Failures()) != 1 {
		t.Fatalf("failures = %v, want the one recorded through a derived context", r.Failures())
	}
}

func TestRecordWriteFailureIsSafeConcurrently(t *testing.T) {
	r := &WriteRecorder{}
	ctx := WithWriteRecorder(context.Background(), r)
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RecordWriteFailure(ctx, errors.New("boom"))
		}()
	}
	wg.Wait()
	if n := len(r.Failures()); n != 50 {
		t.Fatalf("recorded %d failures, want 50", n)
	}
}

func TestWriteRecorderFailuresIsACopy(t *testing.T) {
	r := &WriteRecorder{}
	RecordWriteFailure(WithWriteRecorder(context.Background(), r), errors.New("boom"))
	got := r.Failures()
	got[0] = nil
	if r.Failures()[0] == nil {
		t.Fatal("changing the returned slice changed the recorder")
	}
}

// fastRetry shortens the back-off of RetryWrite until the test ends.
func fastRetry(t *testing.T) {
	backoff, backoffMax := writeRetryBackoff, writeRetryBackoffMax
	writeRetryBackoff, writeRetryBackoffMax = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { writeRetryBackoff, writeRetryBackoffMax = backoff, backoffMax })
}

func TestRecordRowFailureKeepsTheFirstFailureOfARowByRow(t *testing.T) {
	r := NewWriteRecorder(time.Time{})
	ctx := WithWriteRecorder(context.Background(), r)
	first := errors.New("cannot load tsdb")
	RecordRowFailure(ctx, 7, first)
	RecordRowFailure(ctx, 2, errors.New("invalid timestamp"))
	RecordRowFailure(ctx, 7, errors.New("later failure of the same row"))
	RecordRowFailure(ctx, 3, nil)
	got := r.RowFailures()
	if len(got) != 2 || got[0].Row != 2 || got[1].Row != 7 || !errors.Is(got[1].Err, first) {
		t.Fatalf("row failures = %v, want rows 2 and 7, row 7 with its first failure", got)
	}
	if len(r.Failures()) != 0 {
		t.Fatalf("failures = %v, want none: row failures are kept apart", r.Failures())
	}
}

func TestRecordRowFailureWithoutARecorderDoesNothing(t *testing.T) {
	RecordRowFailure(context.Background(), 1, errors.New("cannot load tsdb"))
}

func TestRetryWriteWithoutARecorderRunsOnce(t *testing.T) {
	calls := 0
	boom := errors.New("cannot write index")
	if err := RetryWrite(context.Background(), func() error { calls++; return boom }); !errors.Is(err, boom) {
		t.Fatalf("RetryWrite = %v, want %v", err, boom)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want one attempt for a regular write", calls)
	}
}

func TestRetryWriteWithAZeroDeadlineRunsOnce(t *testing.T) {
	fastRetry(t)
	calls := 0
	ctx := WithWriteRecorder(context.Background(), NewWriteRecorder(time.Time{}))
	_ = RetryWrite(ctx, func() error { calls++; return errors.New("boom") })
	if calls != 1 {
		t.Fatalf("calls = %d, want one attempt without a deadline", calls)
	}
}

func TestRetryWriteSucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	ctx := WithWriteRecorder(context.Background(), NewWriteRecorder(time.Now().Add(time.Minute)))
	if err := RetryWrite(ctx, func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("RetryWrite = %v after %d call(s), want nil after 1", err, calls)
	}
}

func TestRetryWriteRetriesUntilTheWriteSucceeds(t *testing.T) {
	fastRetry(t)
	calls := 0
	ctx := WithWriteRecorder(context.Background(), NewWriteRecorder(time.Now().Add(time.Minute)))
	err := RetryWrite(ctx, func() error {
		calls++
		if calls < 4 {
			return errors.New("owner closing")
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("RetryWrite = %v after %d call(s), want nil after 4", err, calls)
	}
}

func TestRetryWriteGivesUpAtTheDeadline(t *testing.T) {
	fastRetry(t)
	calls := 0
	ctx := WithWriteRecorder(context.Background(), NewWriteRecorder(time.Now().Add(30*time.Millisecond)))
	start := time.Now()
	err := RetryWrite(ctx, func() error { calls++; return fmt.Errorf("attempt %d", calls) })
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("RetryWrite took %v, want it to stop at the 30ms deadline", elapsed)
	}
	if calls < 2 || err == nil || err.Error() != fmt.Sprintf("attempt %d", calls) {
		t.Fatalf("RetryWrite = %v after %d call(s), want the last attempt's error after several", err, calls)
	}
}

func TestRetryWriteStopsWhenTheContextEnds(t *testing.T) {
	fastRetry(t)
	ctx, cancel := context.WithCancel(WithWriteRecorder(context.Background(), NewWriteRecorder(time.Now().Add(time.Minute))))
	calls := 0
	err := RetryWrite(ctx, func() error {
		calls++
		if calls == 2 {
			cancel()
		}
		return errors.New("boom")
	})
	if err == nil || calls != 2 {
		t.Fatalf("RetryWrite = %v after %d call(s), want an error after the canceling attempt", err, calls)
	}
}
