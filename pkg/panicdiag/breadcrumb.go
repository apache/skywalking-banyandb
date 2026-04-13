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
	"time"
)

type breadcrumbContextKey struct{}

type breadcrumbNode struct {
	breadcrumb Breadcrumb
	parent     *breadcrumbNode
}

var nowBreadcrumbTime = func() time.Time {
	return time.Now().UTC()
}

// WithBreadcrumb appends a semantic breadcrumb to the context.
func WithBreadcrumb(ctx context.Context, stage string, component string, fields map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	node, _ := ctx.Value(breadcrumbContextKey{}).(*breadcrumbNode)
	return context.WithValue(ctx, breadcrumbContextKey{}, &breadcrumbNode{
		breadcrumb: Breadcrumb{
			Time:      nowBreadcrumbTime(),
			Stage:     stage,
			Component: component,
			Fields:    cloneStringMap(fields),
		},
		parent: node,
	})
}

// BreadcrumbsFromContext returns breadcrumbs ordered from oldest to newest.
func BreadcrumbsFromContext(ctx context.Context) []Breadcrumb {
	if ctx == nil {
		return nil
	}
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
