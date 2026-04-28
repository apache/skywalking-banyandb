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

package panicdiag

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBreadcrumbsFromContextOrdersEntries(t *testing.T) {
	t.Helper()

	originalNow := nowBreadcrumbTime
	defer func() {
		nowBreadcrumbTime = originalNow
	}()

	firstTime := time.Date(2026, time.April, 13, 11, 0, 0, 0, time.UTC)
	secondTime := firstTime.Add(time.Second)
	breadcrumbTimes := []time.Time{firstTime, secondTime}
	nowBreadcrumbTime = func() time.Time {
		next := breadcrumbTimes[0]
		breadcrumbTimes = breadcrumbTimes[1:]
		return next
	}

	ctx := context.Background()
	ctx = WithBreadcrumb(ctx, "start query", "measure", map[string]string{"group": "metrics"})
	ctx = WithBreadcrumb(ctx, "search blocks", "measure", map[string]string{"parts": "4"})

	breadcrumbs := BreadcrumbsFromContext(ctx)
	if len(breadcrumbs) != 2 {
		t.Fatalf("breadcrumb count mismatch: got %d want 2", len(breadcrumbs))
	}
	if breadcrumbs[0].Stage != "start query" {
		t.Fatalf("first breadcrumb mismatch: got %s", breadcrumbs[0].Stage)
	}
	if breadcrumbs[1].Stage != "search blocks" {
		t.Fatalf("second breadcrumb mismatch: got %s", breadcrumbs[1].Stage)
	}
	if breadcrumbs[0].Fields["group"] != "metrics" {
		t.Fatalf("first breadcrumb fields mismatch: got %v", breadcrumbs[0].Fields)
	}
	if breadcrumbs[1].Fields["parts"] != "4" {
		t.Fatalf("second breadcrumb fields mismatch: got %v", breadcrumbs[1].Fields)
	}
}

func TestBreadcrumbsFromContextClonesFieldMaps(t *testing.T) {
	t.Helper()

	fields := map[string]string{"pod": "banyand-0"}
	ctx := WithBreadcrumb(context.Background(), "collect lifecycle", "proxy", fields)
	fields["pod"] = "changed"

	breadcrumbs := BreadcrumbsFromContext(ctx)
	if len(breadcrumbs) != 1 {
		t.Fatalf("breadcrumb count mismatch: got %d want 1", len(breadcrumbs))
	}
	if breadcrumbs[0].Fields["pod"] != "banyand-0" {
		t.Fatalf("field should be cloned, got %s", breadcrumbs[0].Fields["pod"])
	}

	breadcrumbs[0].Fields["pod"] = "mutated"
	again := BreadcrumbsFromContext(ctx)
	if again[0].Fields["pod"] != "banyand-0" {
		t.Fatalf("returned breadcrumbs should be cloned, got %s", again[0].Fields["pod"])
	}
}

func TestMutableBreadcrumbStoreConcurrentAccess(t *testing.T) {
	t.Helper()

	ctx := WithMutableBreadcrumbs(context.Background())
	const goroutineCount = 8
	const breadcrumbsPerGoroutine = 16

	var wg sync.WaitGroup
	wg.Add(goroutineCount + 1)
	for goroutineIdx := 0; goroutineIdx < goroutineCount; goroutineIdx++ {
		go func(workerIdx int) {
			defer wg.Done()
			for breadcrumbIdx := 0; breadcrumbIdx < breadcrumbsPerGoroutine; breadcrumbIdx++ {
				WithBreadcrumb(ctx, fmt.Sprintf("stage-%d-%d", workerIdx, breadcrumbIdx), "worker", map[string]string{
					"worker": fmt.Sprint(workerIdx),
				})
			}
		}(goroutineIdx)
	}
	go func() {
		defer wg.Done()
		for readIdx := 0; readIdx < goroutineCount*breadcrumbsPerGoroutine; readIdx++ {
			_ = BreadcrumbsFromContext(ctx)
		}
	}()
	wg.Wait()

	breadcrumbs := BreadcrumbsFromContext(ctx)
	if len(breadcrumbs) != maxBreadcrumbDepth {
		t.Fatalf("breadcrumb count mismatch: got %d want %d", len(breadcrumbs), maxBreadcrumbDepth)
	}
}
