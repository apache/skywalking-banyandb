// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

// BenchmarkNativeWildcardReuse measures the unchanged native wildcard request
// against a fixed 10,000-term dictionary, including per-query accounting and
// result validation. Index construction and readiness checks are not timed.
func BenchmarkNativeWildcardReuse(b *testing.B) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if closeErr := owner.Close(); closeErr != nil {
			b.Error(closeErr)
		}
	})
	documents := make([]Document, 10_000)
	expected := make(map[string]uint, 16)
	for index := range documents {
		identifier := fmt.Sprintf("row-%05d", index)
		name := fmt.Sprintf("other-%05d", index)
		if index < 16 {
			name = fmt.Sprintf("api-%05d", index)
			expected[identifier] = uint(index)
		}
		documents[index] = Document{
			Identifier: []byte(identifier),
			Fields:     []Field{{Name: "name", Value: []byte(name), Index: true}},
		}
	}
	if err = owner.Batch(context.Background(), Batch{Documents: documents}); err != nil {
		b.Fatal(err)
	}
	view, err := owner.Acquire(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if closeErr := view.Close(); closeErr != nil {
			b.Error(closeErr)
		}
	})
	for _, arm := range []struct {
		name    string
		pattern string
		hits    int
	}{
		{name: "prefix", pattern: "api-*", hits: 16},
		{name: "leading", pattern: "*api*", hits: 16},
		{name: "no_match", pattern: "*absent*", hits: 0},
	} {
		b.Run(arm.name, func(b *testing.B) {
			request := TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte(arm.pattern)}, MaxTerms: 10_000}
			benchmarkWildcardQuery(b, view, request, expected, arm.hits)
			b.ReportAllocs()
			b.ResetTimer()
			var reserved int64
			for range b.N {
				reserved += benchmarkWildcardQuery(b, view, request, expected, arm.hits)
			}
			b.StopTimer()
			b.ReportMetric(float64(reserved)/float64(b.N), "reserved-B/op")
			b.ReportMetric(float64(arm.hits), "hits/op")
		})
	}
}

func benchmarkWildcardQuery(b *testing.B, view *ReadView, request TermSetRequest, expected map[string]uint, count int) int64 {
	b.Helper()
	var used int64
	ctx, scope := query.WithAllocationScope(context.Background(), func(size int64) error {
		if size > 256<<20-used {
			return ErrQueryLimit
		}
		used += size
		return nil
	}, func(size int64) { used -= size })
	hits, err := view.MatchTermsSet(ctx, request)
	reserved := used
	closeErr := scope.Close()
	if err != nil || closeErr != nil || used != 0 {
		b.Fatalf("query: %v; close: %v; retained bytes: %d", err, closeErr, used)
	}
	if len(hits) != count {
		b.Fatalf("expected %d hits, got %d", count, len(hits))
	}
	var seen uint64
	for _, hit := range hits {
		position, present := expected[string(hit.Identifier)]
		if !present {
			b.Fatalf("unexpected identifier %q", hit.Identifier)
		}
		seen |= uint64(1) << position
	}
	if seen != uint64(1)<<count-1 {
		b.Fatalf("unexpected identifier set %016b for %d hits", seen, count)
	}
	return reserved
}
