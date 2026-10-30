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
	"os"
	"testing"
)

// TestMain keeps the suite independent of the machine: unless a test stubs host
// memory itself, the host is unreadable and a limitless protector uses the 64 MiB fallback.
func TestMain(m *testing.M) {
	hostMemoryTotal = func() uint64 { return 0 }
	os.Exit(m.Run())
}

type percentProtector struct {
	budgetProtector
	percent int
}

func (p *percentProtector) allowedPercentValue() int { return p.percent }

func TestQueryBudgetPoolFromHostMemory(t *testing.T) {
	const gib = uint64(1) << 30
	restore := SetHostMemoryForTest(func() uint64 { return 16 * gib })
	defer restore()
	b := NewQueryBudget(Nop{})
	if want := 3 * gib; b.Limit() != want {
		t.Fatalf("pool = %d, want %d (16 GiB * 75%% / 4)", b.Limit(), want)
	}
	if want := 3 * gib / 8; b.MaxQuery() != want {
		t.Fatalf("max query = %d, want %d", b.MaxQuery(), want)
	}
	_, release, err := b.AdmitContext(context.Background(), 10, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if b.available() != b.Limit() {
		t.Fatalf("available %d must equal the pool %d without a protector limit", b.available(), b.Limit())
	}
}

func TestQueryBudgetHostPoolHonorsConfiguredPercent(t *testing.T) {
	const gib = uint64(1) << 30
	restore := SetHostMemoryForTest(func() uint64 { return 16 * gib })
	defer restore()
	b := NewQueryBudget(&percentProtector{percent: 50})
	if want := 2 * gib; b.Limit() != want {
		t.Fatalf("pool = %d, want %d (16 GiB * 50%% / 4)", b.Limit(), want)
	}
}

func TestQueryBudgetUnreadableHostKeepsFallback(t *testing.T) {
	restore := SetHostMemoryForTest(func() uint64 { return 0 })
	defer restore()
	if b := NewQueryBudget(Nop{}); b.Limit() != queryFallbackPool || b.MaxQuery() != queryFallbackPool/8 {
		t.Fatalf("unreadable host: pool=%d max=%d", b.Limit(), b.MaxQuery())
	}
	if b := NewQueryBudget(nil); b.Limit() != queryFallbackPool {
		t.Fatalf("nil protector: pool=%d", b.Limit())
	}
}

func TestQueryBudgetConfiguredLimitIgnoresHostMemory(t *testing.T) {
	restore := SetHostMemoryForTest(func() uint64 { return 16 << 30 })
	defer restore()
	// A non-zero limit (from --allowed-bytes or a valid cgroup limit) keeps precedence.
	b := NewQueryBudget(&budgetProtector{limit: 1 << 30, avail: 1 << 30})
	if want := uint64(1<<30) / 4; b.Limit() != want {
		t.Fatalf("pool = %d, want %d", b.Limit(), want)
	}
}

func TestReadHostMemoryIsCached(t *testing.T) {
	first := readHostMemory()
	if first == 0 {
		t.Skip("host memory unreadable on this platform")
	}
	if second := readHostMemory(); second != first {
		t.Fatalf("host memory must be cached: %d != %d", first, second)
	}
}
