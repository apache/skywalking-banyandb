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

// mutableBreadcrumbContextKey stores appendable breadcrumbs in context.
type mutableBreadcrumbContextKey struct{}

// mutableBreadcrumbStore holds breadcrumbs visible through the original context.
type mutableBreadcrumbStore struct {
	breadcrumbs []Breadcrumb
	mu          sync.Mutex
}

// maxBreadcrumbDepth caps breadcrumbs retained per context chain.
const maxBreadcrumbDepth = 64

type breadcrumbNode struct {
	parent     *breadcrumbNode
	breadcrumb Breadcrumb
	depth      int
}

var nowBreadcrumbTime = func() time.Time {
	return time.Now().UTC()
}

// WithMutableBreadcrumbs installs an idempotent mutable breadcrumb store.
// Use it within one call chain; use ForkMutableBreadcrumbs at goroutine
// boundaries.
func WithMutableBreadcrumbs(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(mutableBreadcrumbContextKey{}).(*mutableBreadcrumbStore); ok {
		return ctx
	}
	existingBreadcrumbs := BreadcrumbsFromContext(ctx)
	return context.WithValue(ctx, mutableBreadcrumbContextKey{}, &mutableBreadcrumbStore{
		breadcrumbs: existingBreadcrumbs,
	})
}

// ForkMutableBreadcrumbs returns ctx with a fresh breadcrumb store seeded from
// ctx. Use it at goroutine boundaries to prevent sibling breadcrumb leakage.
func ForkMutableBreadcrumbs(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	existing := BreadcrumbsFromContext(ctx)
	return context.WithValue(ctx, mutableBreadcrumbContextKey{}, &mutableBreadcrumbStore{
		breadcrumbs: existing,
	})
}

// WithBreadcrumb appends a semantic breadcrumb to the context.
// It mutates an installed store, or otherwise adds an immutable node.
func WithBreadcrumb(ctx context.Context, stage string, component string, fields map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
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

// BreadcrumbsFromContext returns breadcrumbs ordered oldest to newest.
func BreadcrumbsFromContext(ctx context.Context) []Breadcrumb {
	if ctx == nil {
		return nil
	}
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
	node, _ := ctx.Value(breadcrumbContextKey{}).(*breadcrumbNode)
	if node == nil {
		return nil
	}
	n := node.depth + 1
	breadcrumbs := make([]Breadcrumb, n)
	for current := node; current != nil; current = current.parent {
		n--
		bc := current.breadcrumb
		bc.Fields = cloneStringMap(bc.Fields)
		breadcrumbs[n] = bc
	}
	return breadcrumbs
}
