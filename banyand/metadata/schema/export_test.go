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

package schema

import "context"

// TestableWatcher wraps an unexported watcher for testing.
type TestableWatcher struct {
	inner *watcher
}

// Start starts the watcher.
func (tw *TestableWatcher) Start() { tw.inner.Start() }

// Close closes the watcher.
func (tw *TestableWatcher) Close() { tw.inner.Close() }

// AddHandler adds a handler to the watcher.
func (tw *TestableWatcher) AddHandler(handler watchEventHandler) { tw.inner.AddHandler(handler) }

// NewTestableWatcher creates a watcher from a Registry for testing.
func NewTestableWatcher(reg Registry, name string, kind Kind, revision int64, opts ...WatcherOption) *TestableWatcher {
	return &TestableWatcher{inner: reg.(*etcdSchemaRegistry).NewWatcher(name, kind, revision, opts...)}
}

// CompactRegistry calls Compact on the underlying etcd registry for testing.
func CompactRegistry(ctx context.Context, reg Registry, revision int64) error {
	return reg.(*etcdSchemaRegistry).Compact(ctx, revision)
}

// RegisterNode calls Register on the underlying etcd registry for testing.
func RegisterNode(ctx context.Context, reg Registry, md Metadata, forced bool) error {
	return reg.(*etcdSchemaRegistry).Register(ctx, md, forced)
}
