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

type byteOnlyResultTestLease struct {
	used uint64
}

func (lease *byteOnlyResultTestLease) Charge(bytes uint64) error {
	lease.used += bytes
	return nil
}

func (*byteOnlyResultTestLease) Limit() uint64      { return 1024 }
func (lease *byteOnlyResultTestLease) Used() uint64 { return lease.used }
func (lease *byteOnlyResultTestLease) Owner() any   { return lease }

func TestChargeResultFallsBackToByteOnlyLease(t *testing.T) {
	lease := &byteOnlyResultTestLease{}
	ctx := WithBudgetLease(context.Background(), lease)
	if err := ChargeResult(ctx, 17); err != nil {
		t.Fatal(err)
	}
	if lease.Used() != 17 {
		t.Fatalf("byte-only lease was not charged: %d", lease.Used())
	}
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := ChargeResult(canceledCtx, 13); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled result charge: %v", err)
	}
	if lease.Used() != 17 {
		t.Fatal("canceled result consumed budget")
	}
}

func TestChargeResultWithoutAdmissionIsNoop(t *testing.T) {
	if err := ChargeResult(context.Background(), ^uint64(0)); err != nil {
		t.Fatal(err)
	}
}
