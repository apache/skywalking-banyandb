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

func TestQueryBudgetFallbackPoolAdmitsLargeRequestedWindow(t *testing.T) {
	for _, window := range []uint32{1793, 10000, 100000} {
		b := NewQueryBudget(&Nop{})
		ctx, release, err := b.AdmitContext(context.Background(), window, 0, 20)
		if err != nil {
			t.Fatalf("limit %d must be admitted without an estimated-window check: %v", window, err)
		}
		if _, ok := querypkg.BudgetLeaseFromContext(ctx); !ok {
			t.Fatalf("limit %d: missing lease", window)
		}
		release()
		if b.Reserved() != 0 {
			t.Fatalf("limit %d: reservation leak %d", window, b.Reserved())
		}
	}
	b := NewQueryBudget(&Nop{})
	release, err := b.Admit(context.Background(), 5000, 5000, 20)
	if err != nil {
		t.Fatalf("limit+offset 10000 must be admitted: %v", err)
	}
	release()
}

func TestQueryBudgetAdmitRejectsWindowAboveAbsoluteCeiling(t *testing.T) {
	b := NewQueryBudget(&Nop{})
	if _, err := b.Admit(context.Background(), 100001, 0, 20); !errors.Is(err, ErrQueryTooLarge) {
		t.Fatalf("want ErrQueryTooLarge, got %v", err)
	}
	if _, err := b.Admit(context.Background(), 50000, 50001, 20); !errors.Is(err, ErrQueryTooLarge) {
		t.Fatalf("want ErrQueryTooLarge for limit+offset, got %v", err)
	}
}

func TestQueryBudgetAdmitRejectsWhenPoolExhausted(t *testing.T) {
	b := NewQueryBudget(&Nop{})
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < 8; i++ {
		release, err := b.Admit(context.Background(), 10000, 0, 20)
		if err != nil {
			t.Fatalf("admission %d within the pool failed: %v", i, err)
		}
		releases = append(releases, release)
	}
	if _, err := b.Admit(context.Background(), 20, 0, 20); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("want ErrQueryResourceExhausted once the pool is reserved, got %v", err)
	}
}

func TestMeasureAdmissionDoesNotPreassignEightHardSlots(t *testing.T) {
	budget := NewQueryBudget(nil)
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < 32; i++ {
		_, release, err := budget.AdmitMeasureContext(t.Context())
		if err != nil {
			t.Fatalf("small measure admission %d: %v", i+1, err)
		}
		releases = append(releases, release)
	}
}

func TestMeasureAdmissionIncrementalChargesRemainBoundedAndMonotonic(t *testing.T) {
	budget := NewQueryBudget(nil)
	ctx, release, err := budget.AdmitMeasureContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, ok := querypkg.BudgetLeaseFromContext(ctx)
	if !ok {
		t.Fatal("missing lease")
	}
	if budget.Reserved() != queryFixedOverhead || lease.Limit() != budget.MaxQuery() {
		t.Fatalf("initial commitment=%d hard limit=%d", budget.Reserved(), lease.Limit())
	}
	growth := lease.Limit() - lease.Used()
	if err = lease.Charge(growth); err != nil {
		t.Fatal(err)
	}
	if budget.Reserved() != lease.Limit() {
		t.Fatal("growth must commit shared pool bytes")
	}
	if err = lease.Charge(1); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("hard limit: %v", err)
	}
	if lease.Used() != lease.Limit() || budget.Reserved() != lease.Limit() {
		t.Fatal("failed growth changed accounting")
	}
	nested, nestedRelease, err := budget.AdmitMeasureContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if nestedLease, _ := querypkg.BudgetLeaseFromContext(nested); nestedLease != lease {
		t.Fatal("nested query replaced lease")
	}
	release()
	if budget.Reserved() != lease.Limit() {
		t.Fatal("nested owner must retain all spent charges")
	}
	nestedRelease()
	if budget.Reserved() != 0 {
		t.Fatal("last owner must release pool commitment")
	}
	if err = lease.Charge(1); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("released lease: %v", err)
	}
}

func TestMeasureAdmissionIncrementalPoolPressureRollsBackCharge(t *testing.T) {
	budget := NewQueryBudget(nil)
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	var last querypkg.BudgetLease
	for i := 0; i < 64; i++ {
		ctx, release, err := budget.AdmitMeasureContext(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
		last, _ = querypkg.BudgetLeaseFromContext(ctx)
	}
	if err := last.Charge(1); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("pool must reject growth: %v", err)
	}
	if last.Used() != queryFixedOverhead || budget.Reserved() != budget.Limit() {
		t.Fatal("failed pool growth spent bytes")
	}
	if _, _, err := budget.AdmitMeasureContext(t.Context()); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("pool must reject admission: %v", err)
	}
}

func TestMeasureAdmissionTransportAndCancellationOwnCommitments(t *testing.T) {
	budget := NewQueryBudget(nil)
	ctx, transport := querypkg.NewTransportScope(t.Context())
	defer transport.Close()
	admitted, release, err := budget.AdmitMeasureContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease, _ := querypkg.BudgetLeaseFromContext(admitted)
	if err = lease.Charge(1024); err != nil {
		t.Fatal(err)
	}
	release()
	if budget.Reserved() != queryFixedOverhead+1024 {
		t.Fatal("transport lost its charged commitment")
	}
	transport.Close()
	if budget.Reserved() != 0 {
		t.Fatal("transport commitment leaked")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err = budget.AdmitMeasureContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission: %v", err)
	}
	if budget.Reserved() != 0 {
		t.Fatal("canceled admission committed bytes")
	}
}

func TestMeasureAdmissionConcurrentGrowthAndRelease(t *testing.T) {
	budget := NewQueryBudget(nil)
	var workers sync.WaitGroup
	for i := 0; i < 128; i++ {
		workers.Go(func() {
			ctx, release, err := budget.AdmitMeasureContext(t.Context())
			if errors.Is(err, ErrQueryResourceExhausted) {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			lease, _ := querypkg.BudgetLeaseFromContext(ctx)
			var owners sync.WaitGroup
			owners.Go(func() {
				chargeErr := lease.Charge(1024)
				if chargeErr != nil && !errors.Is(chargeErr, ErrQueryResourceExhausted) {
					t.Error(chargeErr)
				}
			})
			owners.Go(release)
			owners.Wait()
			release()
		})
	}
	workers.Wait()
	if budget.Reserved() != 0 {
		t.Fatalf("concurrent commitment leaked: %d", budget.Reserved())
	}
}

type measureRefreshingProtector struct {
	budgetProtector
	refreshes int
}

func (p *measureRefreshingProtector) RefreshUsage() { p.refreshes++ }

func TestMeasureAdmissionSamplesOnceAndGrowthChecksReportedPressure(t *testing.T) {
	memory := &measureRefreshingProtector{budgetProtector: budgetProtector{limit: 256 << 20, avail: 256 << 20}}
	budget := NewQueryBudget(memory)
	ctx, release, err := budget.AdmitMeasureContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, _ := querypkg.BudgetLeaseFromContext(ctx)
	for i := 0; i < 10; i++ {
		if err = lease.Charge(1024); err != nil {
			t.Fatal(err)
		}
	}
	if memory.refreshes != 1 {
		t.Fatalf("growth sampled runtime usage: refreshes=%d", memory.refreshes)
	}
	before := lease.Used()
	memory.avail = 0
	if err = lease.Charge(0); err != nil {
		t.Fatalf("zero charge under pressure: %v", err)
	}
	if err = lease.Charge(1); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("reported pressure must reject growth: %v", err)
	}
	if lease.Used() != before || budget.Reserved() != before || memory.refreshes != 1 {
		t.Fatal("rejected growth spent bytes or sampled runtime usage")
	}
	memory.avail = 256 << 20
	_, legacyRelease, err := budget.AdmitScanContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer legacyRelease()
	if memory.refreshes != 2 {
		t.Fatal("legacy admission must retain fresh sampling")
	}
}
