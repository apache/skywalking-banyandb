// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package querybench

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
)

func TestQLBT15ReadinessChecksEveryDatasetRow(t *testing.T) {
	const cardinality = 16385
	base := time.Unix(1700000000, 0)
	seen := map[string][]bool{}
	specs := map[string]qlbT15Spec{}
	for _, spec := range qlbT15Specs() {
		if !spec.reference && !spec.empty {
			specs[spec.dataset()] = spec
			seen[spec.dataset()] = make([]bool, cardinality)
		}
	}
	client := qlbT07ReadinessClient(func(_ context.Context, r *bydbqlv1.QueryRequest, _ ...grpc.CallOption) (*bydbqlv1.QueryResponse, error) {
		if len(r.Params) != 4 || !strings.Contains(r.Query, "host = ? AND variant = ?") {
			return nil, fmt.Errorf("unbounded readiness")
		}
		host, err := strconv.Atoi(strings.TrimPrefix(r.Params[0].GetStr().GetValue(), "host_"))
		if err != nil {
			return nil, err
		}
		dataset := r.Params[1].GetStr().GetValue()
		spec, ok := specs[dataset]
		if !ok {
			return nil, fmt.Errorf("unknown dataset")
		}
		first := int(r.Params[2].GetTimestamp().AsTime().Sub(base) / time.Millisecond)
		end := int(r.Params[3].GetTimestamp().AsTime().Sub(base) / time.Millisecond)
		if first < 0 || end > cardinality || end-first > 16384 {
			return nil, fmt.Errorf("invalid readiness window")
		}
		_, expected := qlbT15RawChunk(first, end, host, cardinality, spec, base)
		if len(expected.DataPoints) > 1024 {
			return nil, fmt.Errorf("oversized readiness result")
		}
		for _, point := range expected.DataPoints {
			row := int(point.TagFamilies[0].Tags[0].Value.GetInt().GetValue())
			if seen[dataset][row] {
				return nil, fmt.Errorf("row checked twice")
			}
			seen[dataset][row] = true
			// Check the independently generated raw oracle against actual write values.
			write := qlbT15DataPoint(row, cardinality, spec, base)
			if point.TagFamilies[0].Tags[1].Value.GetStr().GetValue() != write.TagFamilies[0].Tags[2].GetStr().GetValue() ||
				point.TagFamilies[0].Tags[2].Value.GetStr().GetValue() != write.TagFamilies[0].Tags[3].GetStr().GetValue() ||
				point.Fields[0].Value.GetInt().GetValue() != int64(row) {
				return nil, fmt.Errorf("raw oracle differs from written fixture")
			}
		}
		return &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: expected}}, nil
	})
	if err := waitForQLBT15Visibility(t.Context(), client, cardinality, base); err != nil {
		t.Fatal(err)
	}
	for dataset, rows := range seen {
		for row, visited := range rows {
			if !visited {
				t.Fatalf("unchecked %s row %d", dataset, row)
			}
		}
	}
}

func TestQLBT15ReadinessStopsAtResourceCeiling(t *testing.T) {
	calls := 0
	client := qlbT07ReadinessClient(func(context.Context, *bydbqlv1.QueryRequest, ...grpc.CallOption) (*bydbqlv1.QueryResponse, error) {
		calls++
		return nil, status.Error(codes.ResourceExhausted, "native decoder ceiling")
	})
	err := waitForQLBT15Visibility(t.Context(), client, 100000, time.Unix(1700000000, 0))
	if status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatalf("readiness must stop, calls=%d error=%v", calls, err)
	}
}
