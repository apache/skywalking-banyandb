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
	"sync"
	"time"
)

type breadcrumbContextKey struct{}

// mutableBreadcrumbContextKey is the context key for a mutable breadcrumb store.
// When present it takes precedence over the immutable linked-list path so that
// gRPC recovery interceptors — which capture the context at interceptor entry,
// before the handler runs — can still read breadcrumbs appended inside the handler.
type mutableBreadcrumbContextKey struct{}

// mutableBreadcrumbStore holds breadcrumbs that can be appended without creating
// a new context value. A pointer to this struct is stored in the context so that
// mutation is visible through the original context reference.
type mutableBreadcrumbStore struct {
	mu          sync.Mutex
	breadcrumbs []Breadcrumb
}

// maxBreadcrumbDepth is the maximum number of breadcrumbs a single context chain
// may hold. Additions beyond this limit are silently dropped, preserving the
// oldest markers so the earliest causal context is never lost.
const maxBreadcrumbDepth = 64

type breadcrumbNode struct {
	parent     *breadcrumbNode
	breadcrumb Breadcrumb
	depth      int
}

var nowBreadcrumbTime = func() time.Time {
	return time.Now().UTC()
}

// WithMutableBreadcrumbs installs a mutable breadcrumb store into ctx and returns
// the enriched context. Subsequent WithBreadcrumb calls on ctx or any context
// derived from it will append to the store in-place rather than creating a new
// immutable node. The same pointer is visible through the original context value,
// so a gRPC recovery interceptor that captures ctx before calling the handler
// will read all breadcrumbs added inside the handler via BreadcrumbsFromContext.
func WithMutableBreadcrumbs(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, mutableBreadcrumbContextKey{}, &mutableBreadcrumbStore{})
}

// WithBreadcrumb appends a semantic breadcrumb to the context.
// If a mutable store was pre-installed via WithMutableBreadcrumbs, the breadcrumb
// is appended to it in-place and the same context is returned unchanged.
// Otherwise the call is O(1) via a single linked-list node allocation.
// When the chain reaches maxBreadcrumbDepth the call is a no-op.
func WithBreadcrumb(ctx context.Context, stage string, component string, fields map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	// Mutable-store path: append in-place so the breadcrumb is visible through
	// any context reference that was captured before WithBreadcrumb was called.
	if store, ok := ctx.Value(mutableBreadcrumbContextKey{}).(*mutableBreadcrumbStore); ok {
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.breadcrumbs) < maxBreadcrumbDepth {
			store.breadcrumbs = append(store.breadcrumbs, Breadcrumb{
				Time:      nowBreadcrumbTime(),
				Stage:     stage,
				Component: component,
				Fields:    cloneStringMap(fields),
			})
		}
		return ctx
	}
	// Immutable linked-list path (default when no mutable store is present).
	node, _ := ctx.Value(breadcrumbContextKey{}).(*breadcrumbNode)
	nextDepth := 0
	if node != nil {
		nextDepth = node.depth + 1
	}
	if nextDepth >= maxBreadcrumbDepth {
		return ctx
	}
	return context.WithValue(ctx, breadcrumbContextKey{}, &breadcrumbNode{
		breadcrumb: Breadcrumb{
			Time:      nowBreadcrumbTime(),
			Stage:     stage,
			Component: component,
			Fields:    cloneStringMap(fields),
		},
		parent: node,
		depth:  nextDepth,
	})
}

// BreadcrumbsFromContext returns breadcrumbs ordered from oldest to newest.
// It checks the mutable store first, then falls back to the immutable linked list.
func BreadcrumbsFromContext(ctx context.Context) []Breadcrumb {
	if ctx == nil {
		return nil
	}
	// Mutable-store path.
	if store, ok := ctx.Value(mutableBreadcrumbContextKey{}).(*mutableBreadcrumbStore); ok {
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.breadcrumbs) == 0 {
			return nil
		}
		result := make([]Breadcrumb, len(store.breadcrumbs))
		copy(result, store.breadcrumbs)
		for idx := range result {
			result[idx].Fields = cloneStringMap(result[idx].Fields)
		}
		return result
	}
	// Immutable linked-list path.
	node, _ := ctx.Value(breadcrumbContextKey{}).(*breadcrumbNode)
	if node == nil {
		return nil
	}
	reversed := make([]Breadcrumb, 0, 8)
	for current := node; current != nil; current = current.parent {
		breadcrumb := current.breadcrumb
		breadcrumb.Fields = cloneStringMap(breadcrumb.Fields)
		reversed = append(reversed, breadcrumb)
	}
	breadcrumbs := make([]Breadcrumb, len(reversed))
	for idx := range reversed {
		breadcrumbs[len(reversed)-1-idx] = reversed[idx]
	}
	return breadcrumbs
}
