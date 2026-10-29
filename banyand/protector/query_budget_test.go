// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package protector

import (
	"context"
	"errors"
	"sync"
	"testing"

	querypkg "github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type budgetProtector struct {
	limit uint64
	avail int64
}

func (p *budgetProtector) AvailableBytes() int64                       { return p.avail }
func (p *budgetProtector) GetLimit() uint64                            { return p.limit }
func (*budgetProtector) AcquireResource(context.Context, uint64) error { return nil }
func (*budgetProtector) ShouldCache(int64) bool                        { return false }
func (*budgetProtector) State() State                                  { return StateLow }
func (*budgetProtector) Name() string                                  { return "test" }
func (*budgetProtector) FlagSet() *run.FlagSet                         { return run.NewFlagSet("test") }
func (*budgetProtector) Validate() error                               { return nil }
func (*budgetProtector) PreRun(context.Context) error                  { return nil }
func (*budgetProtector) Serve() run.StopNotify                         { return make(chan struct{}) }
func (*budgetProtector) GracefulStop()                                 {}

func TestQueryBudgetReserveRelease(t *testing.T) {
	b := NewQueryBudget(&budgetProtector{limit: 1024, avail: 1024})
	if b.Limit() != 256 || b.MaxQuery() != 32 {
		t.Fatalf("unexpected policy: pool=%d max=%d", b.Limit(), b.MaxQuery())
	}
	releases := make([]func(), 0, 8)
	release, err := b.Reserve(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	releases = append(releases, release)
	for i := 0; i < 7; i++ {
		nextRelease, reserveErr := b.Reserve(context.Background(), 32)
		if reserveErr != nil {
			t.Fatal(reserveErr)
		}
		releases = append(releases, nextRelease)
	}
	if _, err = b.Reserve(context.Background(), 1); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("want exhaustion, got %v", err)
	}
	for _, reservedRelease := range releases {
		reservedRelease()
	}
	if b.Reserved() != 0 {
		t.Fatalf("reservation leaked: %d", b.Reserved())
	}
}

func TestQueryBudgetConcurrentReservations(t *testing.T) {
	b := NewQueryBudget(&budgetProtector{limit: 1 << 20, avail: 1 << 20})
	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			innerRelease, reserveErr := b.Reserve(context.Background(), 1024)
			if reserveErr == nil {
				innerRelease()
			}
		}()
	}
	wg.Wait()
	if b.Reserved() != 0 {
		t.Fatalf("reservation leaked: %d", b.Reserved())
	}
}

func TestQueryBudgetFallbackIsFinite(t *testing.T) {
	b := NewQueryBudget(&Nop{})
	if b.Limit() == 0 || b.MaxQuery() == 0 {
		t.Fatal("disabled protector must retain finite limits")
	}
	release, err := b.Admit(context.Background(), 100, 0, 20)
	if err != nil {
		t.Fatalf("default-sized query should fit finite fallback: %v", err)
	}
	release()
}

func TestQueryBudgetFallbackPoolAdmitsLargeWindows(t *testing.T) {
	for _, limit := range []uint32{1792, 1793, 10000, 100000} {
		b := NewQueryBudget(&Nop{})
		release, err := b.Admit(context.Background(), limit, 0, 20)
		if err != nil {
			t.Fatalf("limit %d must be admitted by the fallback pool: %v", limit, err)
		}
		release()
		if b.Reserved() != 0 {
			t.Fatalf("reservation leaked: %d", b.Reserved())
		}
	}
	b := NewQueryBudget(nil)
	release, err := b.Admit(context.Background(), 5000, 5000, 20)
	if err != nil {
		t.Fatalf("limit plus offset 10000 must be admitted: %v", err)
	}
	release()
}

func TestQueryBudgetAbsoluteWindowStillRejected(t *testing.T) {
	b := NewQueryBudget(&Nop{})
	if _, err := b.Admit(context.Background(), 100001, 0, 20); !errors.Is(err, ErrQueryTooLarge) {
		t.Fatalf("want absolute window rejection, got %v", err)
	}
	if _, err := b.Admit(context.Background(), 60000, 40001, 20); !errors.Is(err, ErrQueryTooLarge) {
		t.Fatalf("want absolute window rejection for limit+offset, got %v", err)
	}
}

func TestQueryBudgetExhaustedPoolStillRejected(t *testing.T) {
	b := NewQueryBudget(&Nop{})
	var releases []func()
	for {
		release, err := b.Admit(context.Background(), 10000, 0, 20)
		if err != nil {
			if !errors.Is(err, ErrQueryResourceExhausted) {
				t.Fatalf("want pool exhaustion, got %v", err)
			}
			break
		}
		releases = append(releases, release)
		if len(releases) > 64 {
			t.Fatal("pool never exhausted")
		}
	}
	if len(releases) != int(queryMaxFraction) {
		t.Fatalf("want %d admitted queries, got %d", queryMaxFraction, len(releases))
	}
	for _, release := range releases {
		release()
	}
	if b.Reserved() != 0 {
		t.Fatalf("reservation leaked: %d", b.Reserved())
	}
}

func TestQueryReservationRefund(t *testing.T) {
	b := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, release, err := b.AdmitContext(context.Background(), 10, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, ok := querypkg.BudgetLeaseFromContext(ctx)
	if !ok {
		t.Fatal("no lease")
	}
	base := lease.Used()
	if err = querypkg.Charge(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	querypkg.Refund(ctx, 400)
	if lease.Used() != base+600 {
		t.Fatalf("refund: used=%d, want %d", lease.Used(), base+600)
	}
	// A refunded allowance is chargeable again, and over-refunding clamps at zero.
	if err = querypkg.Charge(ctx, 400); err != nil {
		t.Fatal(err)
	}
	querypkg.Refund(ctx, 1<<40)
	if lease.Used() != 0 {
		t.Fatalf("over-refund: used=%d", lease.Used())
	}
	// Outside an admitted query a refund is a no-op.
	querypkg.Refund(context.Background(), 10)
}

func TestQueryReservationRefundKeepsResultCounterAndIsConcurrent(t *testing.T) {
	b := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, release, err := b.AdmitContext(context.Background(), 10, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, _ := querypkg.BudgetLeaseFromContext(ctx)
	reservation, ok := lease.(*QueryReservation)
	if !ok {
		t.Fatalf("unexpected lease type %T", lease)
	}
	if err = querypkg.ChargeResult(ctx, 100); err != nil {
		t.Fatal(err)
	}
	results := reservation.results
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if chargeErr := querypkg.Charge(ctx, 10); chargeErr == nil {
					querypkg.Refund(ctx, 10)
				}
			}
		}()
	}
	wg.Wait()
	if reservation.results != results {
		t.Fatalf("refund changed the result counter: %d -> %d", results, reservation.results)
	}
	if reservation.Used() != queryFixedOverhead+100 {
		t.Fatalf("unexpected used after balanced charge/refund: %d", reservation.Used())
	}
}
