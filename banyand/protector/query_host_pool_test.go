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
	"os"
	"testing"
)

// TestMain pins the host memory to "unreadable" so the budget tests that depend on the fixed 64 MiB fallback pool
// (8 MiB per query) do not change with the machine they run on. Tests of the host-sized pool stub their own value.
func TestMain(m *testing.M) {
	SetHostMemoryForTest(0)
	os.Exit(m.Run())
}

type percentProtector struct {
	budgetProtector
	percent int
}

func (p *percentProtector) allowedPercentValue() int { return p.percent }

func TestQueryPoolFromHostMemoryWithoutProtectorLimit(t *testing.T) {
	defer SetHostMemoryForTest(16 << 30)()
	for name, pm := range map[string]Memory{"nil": nil, "nop": &Nop{}, "zero-limit": &budgetProtector{}} {
		budget := NewQueryBudget(pm)
		if got, want := budget.Limit(), uint64(3<<30); got != want {
			t.Fatalf("%s: pool %d, want %d (16 GiB host * 75%% / 4)", name, got, want)
		}
		if got, want := budget.MaxQuery(), uint64(3<<30)/queryMaxFraction; got != want {
			t.Fatalf("%s: max query %d, want pool/8 = %d", name, got, want)
		}
	}
}

func TestQueryPoolUsesConfiguredAllowedPercent(t *testing.T) {
	defer SetHostMemoryForTest(16 << 30)()
	budget := NewQueryBudget(&percentProtector{percent: 50})
	if got, want := budget.Limit(), uint64(2<<30); got != want {
		t.Fatalf("pool %d, want %d (16 GiB * 50%% / 4)", got, want)
	}
	// An out-of-range value is rejected by Validate; the pool falls back to the default percentage.
	if got, want := NewQueryBudget(&percentProtector{percent: 0}).Limit(), uint64(3<<30); got != want {
		t.Fatalf("pool %d, want %d for an invalid percentage", got, want)
	}
	pm := &memory{allowedPercent: 40}
	if got, want := NewQueryBudget(pm).Limit(), uint64(16<<30)*40/100/queryPoolFraction; got != want {
		t.Fatalf("real protector: pool %d, want %d", got, want)
	}
}

func TestQueryPoolExplicitLimitsKeepPrecedence(t *testing.T) {
	defer SetHostMemoryForTest(16 << 30)()
	// A non-zero protector limit (--allowed-bytes or a cgroup limit) wins over the host memory.
	budget := NewQueryBudget(&budgetProtector{limit: 256 << 20, avail: 256 << 20})
	if got, want := budget.Limit(), uint64(64<<20); got != want {
		t.Fatalf("pool %d, want limit/4 = %d", got, want)
	}
	if got, want := budget.MaxQuery(), uint64(8<<20); got != want {
		t.Fatalf("max query %d, want %d", got, want)
	}
	pm := &memory{allowedPercent: 75}
	pm.limit.Store(1 << 30)
	if got, want := NewQueryBudget(pm).Limit(), uint64(256<<20); got != want {
		t.Fatalf("real protector with a limit: pool %d, want %d", got, want)
	}
}

func TestQueryPoolKeepsFallbackWhenHostMemoryUnreadable(t *testing.T) {
	defer SetHostMemoryForTest(0)()
	budget := NewQueryBudget(nil)
	if budget.Limit() != queryFallbackPool || budget.MaxQuery() != queryFallbackPool/queryMaxFraction {
		t.Fatalf("pool %d / max query %d, want the 64 MiB fallback", budget.Limit(), budget.MaxQuery())
	}
}

func TestQueryPoolHostMemoryIsReadOnce(t *testing.T) {
	reads := 0
	restore := SetHostMemoryForTest(0)
	defer restore()
	hostMemory.Lock()
	hostMemory.read = func() uint64 { reads++; return 8 << 30 }
	hostMemory.Unlock()
	budget := NewQueryBudget(nil)
	for range 5 {
		_ = budget.Limit()
	}
	if reads != 1 {
		t.Fatalf("host memory read %d times, want once", reads)
	}
}

func TestQueryPoolNeverEnablesProtector(t *testing.T) {
	defer SetHostMemoryForTest(16 << 30)()
	pm := &memory{allowedPercent: 75}
	budget := NewQueryBudget(pm)
	if budget.Limit() == 0 {
		t.Fatal("expected a host-sized pool")
	}
	if pm.GetLimit() != 0 || pm.AvailableBytes() != -1 || pm.State() != StateLow {
		t.Fatal("sizing the query pool must not enable the memory protector")
	}
	// The pool is available in full: admission against a disabled protector is bounded by the pool alone.
	if got := budget.available(); got != budget.Limit() {
		t.Fatalf("available %d, want the pool %d", got, budget.Limit())
	}
}
