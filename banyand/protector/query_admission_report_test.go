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
	"math"
	"testing"
)

// Reporter patterns: Property limit=MaxUint32; Trace/Stream limit+offset overflow
// or MaxUint32 windows. Admission must reject before any make(..., limit).
//
// Use a 32 GiB mocked budget so the dynamic window ceiling admits 100000 and
// only queryAbsoluteWindow rejects 100001 with ErrQueryTooLarge.
func TestAdmitContextRejectsReporterWindows(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 32 << 30, avail: 32 << 30})
	cases := []struct {
		name          string
		limit, offset uint32
	}{
		{name: "property_or_trace_max_limit", limit: math.MaxUint32},
		{name: "trace_or_stream_limit_offset_overflow", limit: 1, offset: math.MaxUint32},
		{name: "absolute_window_ceiling", limit: 100001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, release, err := budget.AdmitContext(context.Background(), tc.limit, tc.offset, 20)
			if release != nil {
				release()
			}
			if !errors.Is(err, ErrQueryTooLarge) {
				t.Fatalf("expected ErrQueryTooLarge, got %v", err)
			}
			if budget.Reserved() != 0 {
				t.Fatalf("rejected admission leaked %d reserved bytes", budget.Reserved())
			}
		})
	}
}

func TestAdmitContextAcceptsAbsoluteWindowBoundary(t *testing.T) {
	budget := NewQueryBudget(&budgetProtector{limit: 32 << 30, avail: 32 << 30})
	_, release, err := budget.AdmitContext(context.Background(), 100000, 0, 20)
	if err != nil {
		t.Fatalf("limit=100000 must be admitted under ample budget: %v", err)
	}
	release()
	if budget.Reserved() != 0 {
		t.Fatalf("release leaked %d reserved bytes", budget.Reserved())
	}
}
