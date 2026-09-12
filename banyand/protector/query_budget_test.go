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

func TestQueryBudgetAdmitRejectsWindowUnderPressure(t *testing.T) {
	b := NewQueryBudget(&budgetProtector{limit: 64 << 20, avail: 64 << 20})
	if _, err := b.Admit(context.Background(), 100000, 0, 20); !errors.Is(err, ErrQueryWindowPressure) {
		t.Fatalf("want dynamic window rejection, got %v", err)
	}
}
