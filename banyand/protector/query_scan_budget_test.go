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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestQueryScanBudgetActualResultCeiling(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 32 << 30, avail: 32 << 30})
	ctx, release, err := budget.AdmitScanContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, ok := query.BudgetLeaseFromContext(ctx)
	if !ok {
		t.Fatal("admitted scan has no lease")
	}
	initialUsed := lease.Used()
	for result := 0; result < 100000; result++ {
		if chargeErr := query.ChargeResult(ctx, 1); chargeErr != nil {
			t.Fatalf("result %d must fit: %v", result+1, chargeErr)
		}
	}
	if lease.Used() != initialUsed+100000 {
		t.Fatalf("unexpected charged bytes: %d", lease.Used())
	}
	if chargeErr := query.ChargeResult(ctx, 1); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("result 100001 must fail, got %v", chargeErr)
	}
	if lease.Used() != initialUsed+100000 {
		t.Fatal("rejected result consumed byte allowance")
	}
}

func TestQueryScanBudgetBytePressurePreservesResultAllowance(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, release, err := budget.AdmitScanContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	lease, ok := query.BudgetLeaseFromContext(ctx)
	if !ok {
		t.Fatal("admitted scan has no lease")
	}
	initialUsed := lease.Used()
	if chargeErr := query.ChargeResult(ctx, lease.Limit()-initialUsed+1); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("oversized result must fail, got %v", chargeErr)
	}
	if lease.Used() != initialUsed {
		t.Fatal("rejected result consumed byte allowance")
	}
	if chargeErr := query.ChargeResult(ctx, lease.Limit()-initialUsed); chargeErr != nil {
		t.Fatal(chargeErr)
	}
	if chargeErr := query.ChargeResult(ctx, 1); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("exhausted byte budget accepted result: %v", chargeErr)
	}
	for result := 1; result < 100000; result++ {
		if chargeErr := query.ChargeResult(ctx, 0); chargeErr != nil {
			t.Fatalf("failed byte charge consumed result allowance at %d: %v", result+1, chargeErr)
		}
	}
	if chargeErr := query.ChargeResult(ctx, 0); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
		t.Fatalf("zero-byte result bypassed result ceiling: %v", chargeErr)
	}
}

func TestQueryScanBudgetNestedTransportLifetime(t *testing.T) {
	for _, closeTransportFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("transport_first_%t", closeTransportFirst), func(t *testing.T) {
			budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
			transportCtx, scope := query.NewTransportScope(context.Background())
			defer scope.Close()
			outerCtx, releaseOuter, err := budget.AdmitScanContext(transportCtx)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseOuter()
			reserved := budget.Reserved()
			childCtx, releaseChild, err := budget.AdmitScanContext(outerCtx)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseChild()
			outerLease, _ := query.BudgetLeaseFromContext(outerCtx)
			childLease, _ := query.BudgetLeaseFromContext(childCtx)
			if outerLease != childLease || budget.Reserved() != reserved {
				t.Fatal("nested scan did not share reservation")
			}
			releaseOuter()
			releaseOuter()
			if closeTransportFirst {
				scope.Close()
			} else {
				releaseChild()
			}
			if budget.Reserved() != reserved {
				t.Fatal("reservation released while still retained")
			}
			if chargeErr := query.ChargeResult(childCtx, 1); chargeErr != nil {
				t.Fatal(chargeErr)
			}
			if closeTransportFirst {
				releaseChild()
			} else {
				scope.Close()
			}
			if budget.Reserved() != 0 {
				t.Fatalf("reservation leaked: %d", budget.Reserved())
			}
			if chargeErr := query.ChargeResult(childCtx, 0); !errors.Is(chargeErr, ErrQueryResourceExhausted) {
				t.Fatalf("released lease accepted result: %v", chargeErr)
			}
			if _, unexpectedRelease, admissionErr := budget.AdmitScanContext(childCtx); !errors.Is(admissionErr, ErrQueryResourceExhausted) {
				if unexpectedRelease != nil {
					unexpectedRelease()
				}
				t.Fatalf("released scan context readmitted: %v", admissionErr)
			}
		})
	}
}

func TestQueryScanBudgetRejectsMemoryPressureAndCancellation(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 1})
	if _, release, err := budget.AdmitScanContext(context.Background()); !errors.Is(err, ErrQueryResourceExhausted) {
		if release != nil {
			release()
		}
		t.Fatalf("memory pressure admitted scan: %v", err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, release, err := budget.AdmitScanContext(canceledCtx); !errors.Is(err, context.Canceled) {
		if release != nil {
			release()
		}
		t.Fatalf("canceled context admitted scan: %v", err)
	}
	if budget.Reserved() != 0 {
		t.Fatalf("rejection leaked reservation: %d", budget.Reserved())
	}
}

func TestQueryScanBudgetConcurrentResultCeiling(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 32 << 30, avail: 32 << 30})
	ctx, release, err := budget.AdmitScanContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var accepted atomic.Uint64
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for result := 0; result < 15000; result++ {
				chargeErr := query.ChargeResult(ctx, 1)
				if chargeErr == nil {
					accepted.Add(1)
				} else if !errors.Is(chargeErr, ErrQueryResourceExhausted) {
					t.Errorf("unexpected charge error: %v", chargeErr)
				}
			}
		}()
	}
	workers.Wait()
	if accepted.Load() != 100000 {
		t.Fatalf("concurrent admission accepted %d results", accepted.Load())
	}
}
