// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you
// under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
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
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func TestQueryBudgetSharedOwners(t *testing.T) {
	fallback := QueryBudgetFor(nil)
	if QueryBudgetFor(Nop{}) != fallback || QueryBudgetFor(&Nop{}) != fallback {
		t.Fatal("nil and no-op protectors must share a finite pool")
	}
	pm := &budgetProtector{limit: 64 << 20, avail: 0}
	budget := QueryBudgetFor(pm)
	if budget != QueryBudgetFor(pm) {
		t.Fatal("one protector must not acquire independent query pools")
	}
	release, admissionErr := budget.Admit(context.Background(), 1, 0, 20)
	if admissionErr == nil {
		release()
		t.Fatal("custom protector reporting zero available memory was ignored")
	}
}

func TestQueryBudgetFreshUsageAndLateInitialization(t *testing.T) {
	pm := &memory{}
	budget := QueryBudgetFor(pm)
	initialMaximum := budget.MaxQuery()
	// Allow ample space for test-process runtime memory without allocating it.
	pm.limit.Store(1 << 40)
	atomic.StoreUint64(&pm.usage, 1<<40)
	if budget.MaxQuery() <= initialMaximum {
		t.Fatal("query budget retained the pre-initialization fallback ceiling")
	}
	release, admissionErr := budget.Admit(context.Background(), 1, 0, 20)
	if admissionErr != nil {
		t.Fatalf("admission failed to refresh stale full-memory usage: %v", admissionErr)
	}
	defer release()
	if atomic.LoadUint64(&pm.usage) == 1<<40 {
		t.Fatal("admission did not refresh runtime usage")
	}
}

func TestQueryBudgetAdmitConcurrentHeldReservations(t *testing.T) {
	budget := NewQueryBudget(nil)
	const callers = 32
	start := make(chan struct{})
	releases := make(chan func(), callers)
	var workers sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			release, admissionErr := budget.Admit(context.Background(), 1, 0, 20)
			if admissionErr == nil {
				releases <- release
			}
		}()
	}
	close(start)
	workers.Wait()
	close(releases)
	if len(releases) != 8 {
		t.Errorf("expected eight held full-budget reservations, got %d", len(releases))
	}
	if budget.Reserved() > budget.Limit() {
		t.Errorf("pool oversubscribed: %d > %d", budget.Reserved(), budget.Limit())
	}
	for release := range releases {
		release()
		release() // Every owner cleanup is idempotent.
	}
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leak: %d", budget.Reserved())
	}
}

func TestQueryBudgetWindowShrinksWithAvailableMemory(t *testing.T) {
	const limit = 256 << 20
	pm := &budgetProtector{limit: limit, avail: limit}
	budget := NewQueryBudget(pm)
	release, initialErr := budget.Admit(context.Background(), 1000, 0, 20)
	if initialErr != nil {
		t.Fatal(initialErr)
	}
	release()
	// Leave only 2 MiB above headroom: 1 MiB overhead plus 256 estimated entries.
	pm.avail = limit/10 + (2 << 20)
	unexpectedRelease, pressureErr := budget.Admit(context.Background(), 1000, 0, 20)
	if pressureErr == nil {
		unexpectedRelease()
		t.Fatal("large query was admitted despite a smaller dynamic window")
	}
	if !errors.Is(pressureErr, ErrQueryWindowPressure) {
		t.Fatalf("expected window-pressure rejection, got %v", pressureErr)
	}
	smallRelease, smallErr := budget.Admit(context.Background(), 100, 0, 20)
	if smallErr != nil {
		t.Fatalf("small query should still fit: %v", smallErr)
	}
	smallRelease()
}

func TestQueryReservationDoubleReleaseAndChargeOverflow(t *testing.T) {
	budget := NewQueryBudget(nil)
	ctx, outerRelease, admissionErr := budget.AdmitContext(context.Background(), 1, 0, 20)
	if admissionErr != nil {
		t.Fatal(admissionErr)
	}
	_, childRelease, childErr := budget.AdmitContext(ctx, 1, 0, 20)
	if childErr != nil {
		outerRelease()
		t.Fatal(childErr)
	}
	outerRelease()
	outerRelease()
	if budget.Reserved() == 0 {
		t.Fatal("repeated parent release freed the child's reservation")
	}
	childRelease()
	childRelease()
	if budget.Reserved() != 0 {
		t.Fatal("child release leaked the reservation")
	}

	lease := &QueryReservation{owner: budget, assigned: 1024, refs: 1}
	if chargeErr := lease.Charge(100); chargeErr != nil {
		t.Fatal(chargeErr)
	}
	if chargeErr := lease.Charge(math.MaxUint64); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("overflow charge accepted: %v", chargeErr)
	}
	if lease.Used() != 100 {
		t.Fatal("failed charge changed usage")
	}
	lease.released.Store(true)
	if chargeErr := lease.Charge(1); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("charge against released lease accepted: %v", chargeErr)
	}
}
