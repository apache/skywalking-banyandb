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

package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func assertRelayRate(t *testing.T, r *Relay, wantLimit rate.Limit) {
	t.Helper()
	if limit := r.Limit(); limit != wantLimit {
		t.Fatalf("got limit %v, want limit %v", limit, wantLimit)
	}
}

// wait takes n bytes step by step, as a relayed chunk does.
func (r *Relay) wait(ctx context.Context, n int) error { return r.Pay(ctx, n) }

func TestRelayLimiter_LastWinsAndIdleIsUnlimited(t *testing.T) {
	r := NewRelay()
	assertRelayRate(t, r, rate.Inf)

	releaseA := r.Open(100)
	assertRelayRate(t, r, 100)
	if r.PieceSize() != 100 {
		t.Fatalf("a rate below one chunk per second relays pieces of one second's worth, got %d", r.PieceSize())
	}
	releaseShared := r.Open(100)
	if r.PieceSize() != 50 {
		t.Fatalf("two relays share one second's worth, got %d", r.PieceSize())
	}
	releaseShared()
	releaseB := r.Open(4 << 20)
	assertRelayRate(t, r, 4<<20)
	releaseC := r.Open(0)
	assertRelayRate(t, r, rate.Inf) // 0 overrides an earlier limit
	releaseC()
	assertRelayRate(t, r, rate.Inf) // closing a stream does not restore an older value
	releaseD := r.Open(1 << 40)
	assertRelayRate(t, r, 1<<40)
	releaseB()
	releaseD()
	assertRelayRate(t, r, 1<<40) // A is still active
	releaseA()
	assertRelayRate(t, r, rate.Inf)
	if r.PieceSize() != MaxBurst {
		t.Fatalf("an idle relay goes back to whole chunks, got %d", r.PieceSize())
	}
}

func TestRelayLimiter_TakeLargerThanBurstWaits(t *testing.T) {
	r := NewRelay()
	defer r.Open(2 << 20)()
	start := time.Now()
	// The bucket starts full at one 1 MiB burst, so the other 2 MiB wait about one second.
	if err := r.wait(context.Background(), 3<<20); err != nil {
		t.Fatalf("a take larger than the burst must wait, not fail: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 750*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("waited %v, want about 1s", elapsed)
	}
}

// A relay opening with a lower rate shrinks the burst under a wait that sized its step for the
// old one: the wait takes smaller steps instead of failing.
func TestRelayLimiter_LowerRateDuringWaitTakesSmallerSteps(t *testing.T) {
	r := NewRelay()
	defer r.Open(8 << 20)()
	done := make(chan struct{})
	//panicdiag:allow-rawgo the test toggles the shared rate while waits run
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			// The last relay opened sets the rate: a low one shrinks the burst for a moment,
			// the high one restores it.
			r.Open(64 << 10)()
			r.Open(8 << 20)()
			time.Sleep(time.Millisecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for waits := 0; ; waits++ {
		if err := r.wait(ctx, 256<<10); err != nil {
			<-done
			t.Fatalf("wait %d failed while the rate changed: %v", waits, err)
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func TestRelayLimiter_UnlimitedDoesNotWait(t *testing.T) {
	r := NewRelay()
	start := time.Now()
	if err := r.wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	defer r.Open(0)()
	if err := r.wait(context.Background(), 1<<30); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("an unlimited relay must not wait, took %v", elapsed)
	}
}

func TestRelayLimiter_CanceledWaitReturnsCanceled(t *testing.T) {
	r := NewRelay()
	defer r.Open(1)()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if err := r.wait(ctx, 2<<20); status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", err)
	}
}

// A wait canceled while it waits returns Canceled promptly and gives its reservation back,
// so the waits canceled before it do not delay the next one.
func TestRelayLimiter_CanceledWaitGivesItsReservationBack(t *testing.T) {
	const bytesPerSecond = 1000
	r := NewRelay()
	defer r.Open(bytesPerSecond)()
	// Empty the bucket: every following step of the burst waits about a second.
	if err := r.wait(context.Background(), bytesPerSecond); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		err := r.wait(ctx, bytesPerSecond)
		cancel()
		if status.Code(err) != codes.Canceled {
			t.Fatalf("wait %d: got %v, want Canceled", i, err)
		}
		if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
			t.Fatalf("wait %d returned %v after it started, want right after the cancel", i, elapsed)
		}
	}
	// Without the three reservations given back the next second's worth would wait about 4s.
	start := time.Now()
	if err := r.wait(context.Background(), bytesPerSecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
		t.Fatalf("the wait after canceled ones took %v, want about 1s", elapsed)
	}
}

// fakeClockNode returns a Node whose clock stands still until the returned function moves it.
func fakeClockNode() (*Node, func(time.Duration)) {
	now := time.Unix(1_800_000_000, 0)
	l := NewNode()
	l.now = func() time.Time { return now }
	return l, func(d time.Duration) { now = now.Add(d) }
}

// mustAdmit admits a payment and returns how long it waits.
func mustAdmit(t *testing.T, l *Node, bytesPerSecond, rowsPerSecond uint64, n, rows int) time.Duration {
	t.Helper()
	wait, release, err := l.admit(context.Background(), bytesPerSecond, rowsPerSecond, n, rows)
	if err != nil {
		t.Fatalf("admit(%d B/s, %d rows/s, %d B, %d rows) = %v, want admitted", bytesPerSecond, rowsPerSecond, n, rows, err)
	}
	if release == nil {
		t.Fatal("an admitted payment has no release")
	}
	return wait
}

func wantBusy(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
}

func TestNode_UnlimitedPaymentTouchesNoPace(t *testing.T) {
	l := NewNode()
	if err := l.Pay(context.Background(), 0, 0, 1<<30, 1<<30); err != nil {
		t.Fatal(err)
	}
	if !l.bytes.IsZero() || !l.rows.IsZero() {
		t.Fatal("an unlimited payment was charged")
	}
}

func TestNode_EmptyPaymentIsNotCharged(t *testing.T) {
	l, _ := fakeClockNode()
	if wait := mustAdmit(t, l, 10, 10, 0, 0); wait != 0 {
		t.Fatalf("wait = %v, want none for an empty payment", wait)
	}
	if !l.bytes.IsZero() || !l.rows.IsZero() {
		t.Fatal("an empty payment was charged")
	}
}

func TestNode_OneSecondPassesWithoutWaiting(t *testing.T) {
	l, _ := fakeClockNode()
	if wait := mustAdmit(t, l, 0, 20, 0, 20); wait != 0 {
		t.Fatalf("20 rows at 20 rows/s on an idle node wait %v, want none", wait)
	}
	if wait := mustAdmit(t, l, 0, 20, 0, 10); wait != 500*time.Millisecond {
		t.Fatalf("10 more rows wait %v, want 500ms behind the first payment", wait)
	}
}

func TestNode_IdleTimeCountsAtMostOneSecond(t *testing.T) {
	l, advance := fakeClockNode()
	mustAdmit(t, l, 0, 20, 0, 20)
	advance(time.Hour)
	if wait := mustAdmit(t, l, 0, 20, 0, 40); wait != time.Second {
		t.Fatalf("40 rows at 20 rows/s after an hour idle wait %v, want 1s: idle time adds no more than one second", wait)
	}
}

func TestNode_PaymentLargerThanOneSecondPaysItsShare(t *testing.T) {
	l, _ := fakeClockNode()
	// 1000 rows at 2 rows/s take 500s; the first second's worth passes at once.
	if wait := mustAdmit(t, l, 0, 2, 0, 1000); wait != 499*time.Second {
		t.Fatalf("wait = %v, want 499s", wait)
	}
}

func TestNode_ByteBurstIsCappedAtMaxBurst(t *testing.T) {
	l, _ := fakeClockNode()
	// At 8 MiB/s one second would be 8 MiB, but at most MaxBurst passes at once: 1/8s.
	if wait := mustAdmit(t, l, 8<<20, 0, 2<<20, 0); wait != 125*time.Millisecond {
		t.Fatalf("wait = %v, want 125ms", wait)
	}
}

func TestNode_TheSlowerPaceSetsTheWait(t *testing.T) {
	l, _ := fakeClockNode()
	// 3 rows of 300 bytes: the bytes take 3s at 100 B/s, the rows 0.3s at 10 rows/s.
	if wait := mustAdmit(t, l, 100, 10, 300, 3); wait != 2*time.Second {
		t.Fatalf("wait = %v, want 2s set by the byte pace", wait)
	}
	if !l.rows.After(l.now()) || !l.bytes.After(l.rows) {
		t.Fatalf("paces bytes=%v rows=%v, want both charged, the byte pace further ahead", l.bytes, l.rows)
	}
}

func TestNode_PaymentsShareOnePace(t *testing.T) {
	l := NewNode()
	ctx := context.Background()
	// 20 rows at 20 rows/s fill the first second; 10 more, paid by another import, wait about 500ms.
	if err := l.Pay(ctx, 0, 20, 0, 20); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.Pay(ctx, 0, 20, 0, 10); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("10 rows past the first second took %v, want them paced by the shared pace", elapsed)
	}
}

func TestNode_QueuedBeyondTheSlackIsBusy(t *testing.T) {
	l, advance := fakeClockNode()
	// 22 rows at 2 rows/s take 11s: the node is paid out 11s from now.
	mustAdmit(t, l, 0, 2, 0, 22)
	paced := l.rows
	// 2 more rows would wait 11s behind it, 1s past the slack of a payment that waits nothing alone.
	_, _, err := l.admit(context.Background(), 0, 2, 0, 2)
	wantBusy(t, err)
	if !l.rows.Equal(paced) {
		t.Fatal("a refused payment was charged")
	}
	// A large payment is refused too: its own share is no excuse to queue behind more than the slack.
	_, _, err = l.admit(context.Background(), 0, 2, 0, 1000)
	wantBusy(t, err)
	// Once the node caught up to within the slack, the same payment is admitted.
	advance(2 * time.Second)
	if wait := mustAdmit(t, l, 0, 2, 0, 2); wait != 9*time.Second {
		t.Fatalf("wait = %v, want 9s behind the earlier payment", wait)
	}
}

func TestNode_QueuedWithinTheSlackIsAdmitted(t *testing.T) {
	l, _ := fakeClockNode()
	mustAdmit(t, l, 0, 2, 0, 20)
	// Paid out 10s from now: 2 more rows wait exactly the slack and are admitted.
	if wait := mustAdmit(t, l, 0, 2, 0, 2); wait != NodeSlack {
		t.Fatalf("wait = %v, want %v", wait, NodeSlack)
	}
}

func TestNode_RateChangeCarriesNoDebt(t *testing.T) {
	for name, tc := range map[string]struct {
		rowsPerSecond uint64
		rows          int
	}{
		"lower rate":  {rowsPerSecond: 1, rows: 1},
		"higher rate": {rowsPerSecond: 100, rows: 100},
	} {
		t.Run(name, func(t *testing.T) {
			l, _ := fakeClockNode()
			// 20 rows at 2 rows/s keep the node busy for 10s.
			mustAdmit(t, l, 0, 2, 0, 20)
			// The next payment waits for those 10s, not for the 20 rows re-priced at its rate.
			if wait := mustAdmit(t, l, 0, tc.rowsPerSecond, 0, tc.rows); wait != 10*time.Second {
				t.Fatalf("wait = %v, want 10s", wait)
			}
		})
	}
}

func TestNode_PaymentPastTheDeadlineIsBusyAtOnce(t *testing.T) {
	l := NewNode()
	// 40 rows at 2 rows/s wait 19s, more than the 1s the request has left.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	wantBusy(t, l.Pay(ctx, 0, 2, 0, 40))
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("a refused payment took %v, want an immediate answer", elapsed)
	}
	if !l.rows.IsZero() {
		t.Fatal("a refused payment was charged")
	}
	// A payment that fits in the time left is admitted.
	if err := l.Pay(ctx, 0, 2, 0, 2); err != nil {
		t.Fatal(err)
	}
}

func TestNode_PayHonoursCancel(t *testing.T) {
	l := NewNode()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Pay(ctx, 10, 0, 16, 1); status.Code(err) != codes.Canceled {
		t.Fatalf("Pay on a canceled context = %v, want Canceled", err)
	}
	if !l.bytes.IsZero() {
		t.Fatal("a canceled payment was charged")
	}
}

func TestNode_CanceledWaitGivesThePaymentBack(t *testing.T) {
	l := NewNode()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(50*time.Millisecond, cancel)
	// 30 rows at 20 rows/s wait 500ms; the context ends first.
	if err := l.Pay(ctx, 0, 20, 0, 30); status.Code(err) != codes.Canceled {
		t.Fatalf("Pay = %v, want Canceled", err)
	}
	start := time.Now()
	if err := l.Pay(context.Background(), 0, 20, 0, 20); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("the payment after a canceled one took %v, want no wait", elapsed)
	}
}

func TestNode_ReleaseKeepsALaterPayment(t *testing.T) {
	l, _ := fakeClockNode()
	_, release, err := l.admit(context.Background(), 0, 2, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	mustAdmit(t, l, 0, 2, 0, 4)
	later := l.rows
	release()
	if !l.rows.Equal(later) {
		t.Fatalf("pace = %v after an earlier payment was given back, want %v kept for the later one", l.rows, later)
	}
}
