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
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestQueryReservationRetainsOwnerUntilChildRelease(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, releaseOuter, err := budget.AdmitContext(context.Background(), 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	_, releaseChild, err := budget.AdmitContext(ctx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	releaseOuter()
	if budget.Reserved() == 0 {
		t.Fatal("outer release freed reservation while child was active")
	}
	releaseChild()
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leaked after child release: %d", budget.Reserved())
	}
}

func TestQueryReservationTransportScopeRetainsLocalAdmission(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	transportCtx, scope := query.NewTransportScope(context.Background())
	_, release, err := budget.AdmitContext(transportCtx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if budget.Reserved() == 0 {
		t.Fatal("local release freed reservation before transport completion")
	}
	scope.Close()
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leaked after transport close: %d", budget.Reserved())
	}
}

func TestQueryReservationTransportScopeCloseBeforeLocalRelease(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	transportCtx, scope := query.NewTransportScope(context.Background())
	_, release, err := budget.AdmitContext(transportCtx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	scope.Close()
	if budget.Reserved() == 0 {
		t.Fatal("transport close freed active local reservation")
	}
	release()
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leaked after local release: %d", budget.Reserved())
	}
}

func TestQueryReservationTransportScopeNestedReleases(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	transportCtx, scope := query.NewTransportScope(context.Background())
	ctx, releaseOuter, err := budget.AdmitContext(transportCtx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	_, releaseChild, err := budget.AdmitContext(ctx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	releaseOuter()
	releaseOuter()
	releaseChild()
	if budget.Reserved() == 0 {
		t.Fatal("transport scope releases were not retained")
	}
	scope.Close()
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leaked after nested transport releases: %d", budget.Reserved())
	}
}

func TestQueryReservationTransportScopeAlreadyClosed(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	transportCtx, scope := query.NewTransportScope(context.Background())
	scope.Close()
	_, release, err := budget.AdmitContext(transportCtx, 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Reserved() == 0 {
		t.Fatal("admission returned a released lease")
	}
	release()
	if budget.Reserved() != 0 {
		t.Fatalf("reservation leaked after closed-scope admission: %d", budget.Reserved())
	}
}

func TestQueryReservationRejectsCanceledAndReleasedContext(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, release, err := budget.AdmitContext(context.Background(), 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = budget.AdmitContext(canceled, 20, 0, 20); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled borrowed context, got %v", err)
	}
	release()
	if _, _, err = budget.AdmitContext(ctx, 20, 0, 20); !errors.Is(err, ErrQueryResourceExhausted) {
		t.Fatalf("want released context rejection, got %v", err)
	}
}

func TestQueryReservationRejectsWindowBeyondAssignment(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	ctx, release, err := budget.AdmitContext(context.Background(), 20, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, _, err = budget.AdmitContext(ctx, 100000, 0, 20); !errors.Is(err, ErrQueryWindowPressure) {
		t.Fatalf("want window pressure, got %v", err)
	}
}
