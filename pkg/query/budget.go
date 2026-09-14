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

	"google.golang.org/protobuf/proto"
)

// Charge accounts bytes against the active query lease before an allocation.
// Calls outside admitted query contexts are intentionally no-ops.
func Charge(ctx context.Context, bytes uint64) error {
	if bytes == 0 {
		return nil
	}
	lease, ok := BudgetLeaseFromContext(ctx)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return lease.Charge(bytes)
}

// ChargeResult accounts one retained result and its bytes before allocation.
// Leases without result accounting retain byte-only charging; unadmitted calls are no-ops.
func ChargeResult(ctx context.Context, bytes uint64) error {
	lease, ok := BudgetLeaseFromContext(ctx)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if resultLease, supportsResults := lease.(interface{ ChargeResult(uint64) error }); supportsResults {
		return resultLease.ChargeResult(bytes)
	}
	return lease.Charge(bytes)
}

// BudgetLease is a query-owned byte reservation that can charge subsequent
// allocations before they occur.
type BudgetLease interface {
	Charge(uint64) error
	Limit() uint64
	Used() uint64
	Owner() any
}

type budgetLeaseContextKey struct{}

// WithBudgetLease attaches a lease to a query context.
func WithBudgetLease(ctx context.Context, lease BudgetLease) context.Context {
	return context.WithValue(ctx, budgetLeaseContextKey{}, lease)
}

// BudgetLeaseFromContext returns the lease attached to ctx, if any.
func BudgetLeaseFromContext(ctx context.Context) (BudgetLease, bool) {
	lease, ok := ctx.Value(budgetLeaseContextKey{}).(BudgetLease)
	return lease, ok
}

// ChargeResponse reserves estimated encoding and transport-copy space before a
// protobuf response is serialized. Non-protobuf replies have no payload charge.
func ChargeResponse(ctx context.Context, response any) error {
	if _, admitted := BudgetLeaseFromContext(ctx); !admitted {
		return nil
	}
	message, isProto := response.(proto.Message)
	if !isProto {
		return nil
	}
	const encodingCopies = 2
	return Charge(ctx, uint64(proto.Size(message))*encodingCopies)
}
