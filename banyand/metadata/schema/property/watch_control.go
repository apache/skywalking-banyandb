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

package property

import "sync"

// schemaRegistryRoster is the per-process index of every SchemaRegistry
// constructed via NewSchemaRegistryClient. It exists so the test harness
// in pkg/test/setup can attach a watch-control handle to each data node
// it spawns without changing the production SchemaRegistry constructor's
// signature: each data node's metadata.clientService creates exactly one
// SchemaRegistry during PreRun, and the harness captures the index range
// around its CMD() invocation to map gRPC address -> registry.
//
// This is purely a test affordance — production code never reads from the
// roster. It is exposed only because the data-node node lives behind a
// goroutine boundary that test code cannot otherwise reach.
var (
	schemaRegistryRosterMu sync.RWMutex
	schemaRegistryRoster   []*SchemaRegistry
)

// registerForWatchControl appends r to the per-process roster and returns
// its index. Called from NewSchemaRegistryClient. Thread-safe.
func registerForWatchControl(r *SchemaRegistry) {
	schemaRegistryRosterMu.Lock()
	schemaRegistryRoster = append(schemaRegistryRoster, r)
	schemaRegistryRosterMu.Unlock()
}

// CountSchemaRegistries returns the number of SchemaRegistry instances
// constructed in this process so far. The test harness calls this before
// and after a CMD() invocation to discover which registries belong to the
// node it just started.
func CountSchemaRegistries() int {
	schemaRegistryRosterMu.RLock()
	defer schemaRegistryRosterMu.RUnlock()
	return len(schemaRegistryRoster)
}

// SchemaRegistryByIndex returns the i-th SchemaRegistry registered in this
// process, or nil if i is out of range. The test harness uses this to bind
// a freshly-spawned node's address to its registry handle.
func SchemaRegistryByIndex(i int) *SchemaRegistry {
	schemaRegistryRosterMu.RLock()
	defer schemaRegistryRosterMu.RUnlock()
	if i < 0 || i >= len(schemaRegistryRoster) {
		return nil
	}
	return schemaRegistryRoster[i]
}
