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

package query

import (
	"context"
	"errors"
	"testing"
)

type cappedTestLease struct {
	limit uint64
	used  uint64
}

func (lease *cappedTestLease) Charge(bytes uint64) error {
	if lease.used+bytes > lease.limit {
		return errors.New("exhausted")
	}
	lease.used += bytes
	return nil
}

func (lease *cappedTestLease) Limit() uint64 { return lease.limit }
func (lease *cappedTestLease) Used() uint64  { return lease.used }
func (lease *cappedTestLease) Owner() any    { return lease }

func TestPeakChargeChargesOnlyNewPeaks(t *testing.T) {
	lease := &cappedTestLease{limit: 100}
	ctx := WithBudgetLease(context.Background(), lease)
	var charge PeakCharge
	steps := []struct {
		add, release, wantUsed uint64
	}{
		{add: 10, wantUsed: 10},
		{add: 5, wantUsed: 15},
		{release: 10, wantUsed: 15},
		{add: 8, wantUsed: 15},
		{add: 5, wantUsed: 18},
	}
	for i, step := range steps {
		if step.add > 0 {
			if err := charge.Add(ctx, step.add); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
		}
		charge.Release(step.release)
		if lease.used != step.wantUsed {
			t.Fatalf("step %d: lease used %d, want %d", i, lease.used, step.wantUsed)
		}
	}
}

func TestPeakChargeKeepsHeldBytesWhenTheLeaseRefuses(t *testing.T) {
	lease := &cappedTestLease{limit: 10}
	ctx := WithBudgetLease(context.Background(), lease)
	var charge PeakCharge
	if err := charge.Add(ctx, 8); err != nil {
		t.Fatal(err)
	}
	if err := charge.Add(ctx, 5); err == nil {
		t.Fatal("expected the lease to refuse a peak past its limit")
	}
	// The refused bytes were never held, so releasing the first 8 frees room for 10.
	charge.Release(8)
	if err := charge.Add(ctx, 10); err != nil {
		t.Fatalf("held bytes should be back to zero: %v", err)
	}
	if lease.used != 10 {
		t.Fatalf("lease used %d, want 10", lease.used)
	}
}
