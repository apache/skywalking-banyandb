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
	"sync"
)

type transportScopeKey struct{}

// TransportScope retains query reservations until a transport has finished
// serializing and sending its response.
type TransportScope struct {
	releases []func()
	mu       sync.Mutex
	closed   bool
}

// NewTransportScope returns a context carrying a scope and the scope itself.
func NewTransportScope(ctx context.Context) (context.Context, *TransportScope) {
	scope := &TransportScope{}
	return context.WithValue(ctx, transportScopeKey{}, scope), scope
}

// RegisterTransportRelease registers release with the transport scope in ctx.
// If no scope is present, release is returned unchanged. When a scope exists,
// the returned function is safe to defer but the scope owns the actual release.
func RegisterTransportRelease(ctx context.Context, release func()) func() {
	if release == nil {
		return nil
	}
	scope, ok := ctx.Value(transportScopeKey{}).(*TransportScope)
	if !ok {
		return release
	}
	scope.mu.Lock()
	if scope.closed {
		scope.mu.Unlock()
		release()
		return func() {}
	}
	scope.releases = append(scope.releases, release)
	scope.mu.Unlock()
	return func() {}
}

// TransportScopeFromContext returns the transport scope attached to ctx.
func TransportScopeFromContext(ctx context.Context) (*TransportScope, bool) {
	scope, ok := ctx.Value(transportScopeKey{}).(*TransportScope)
	return scope, ok
}

// Close releases all reservations registered with the scope exactly once.
func (scope *TransportScope) Close() {
	if scope == nil {
		return
	}
	scope.mu.Lock()
	if scope.closed {
		scope.mu.Unlock()
		return
	}
	scope.closed = true
	releases := scope.releases
	scope.releases = nil
	scope.mu.Unlock()
	for _, release := range releases {
		release()
	}
}
