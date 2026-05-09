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

package grpc

import (
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

// nodeRepoRegistryProvider is the minimum surface schemaRevisionRegistry
// needs from a metadata.Repo. metadata.Service satisfies it in production;
// declaring it locally keeps the helper independent of the wider Service
// interface and lets unit tests inject a registry without stubbing every
// metadata.Service method.
type nodeRepoRegistryProvider interface {
	NodeRepoRegistry() *registry.NodeRepoRegistry
}

// schemaRevisionRegistry returns the per-node NodeRepoRegistry the repo
// exposes. Returns nil when repo does not provide one (legacy unit-test
// fixtures); callers fall back to the locator-based lookup.
func schemaRevisionRegistry(repo metadata.Repo) *registry.NodeRepoRegistry {
	p, ok := repo.(nodeRepoRegistryProvider)
	if !ok {
		return nil
	}
	return p.NodeRepoRegistry()
}

// resolveSchemaRevision answers the write gate's "what mod_revision is this
// node at for (kind, group, name)?" question.
//
// The registry is authoritative for kinds tracked by at least one per-service
// schemaRepo (Group/Stream/Measure/Trace/IndexRule/IndexRuleBinding) — the
// same caches the executor consults via LoadGroup / LoadResource. Reading
// from the registry instead of the entityRepo locator closes the gap between
// AwaitXXX (which reads the registry view) and the write gate's verdict on
// the same node, so the executor cannot miss a key the gate just certified.
//
// When the registry is authoritative for the kind but does NOT hold the key,
// the function returns 0 so the three-way write-gate split treats the
// request as ahead-of-cache and the bounded await runs.
//
// When the registry does not track the kind (no per-service schemaRepo
// registered for it; e.g., legacy unit tests with a metadata.Repo mock that
// is not a metadata.Service), the caller-provided fallback (typically the
// locator's ModRevision) is returned so existing fixtures keep working.
func resolveSchemaRevision(reg *registry.NodeRepoRegistry, kind schema.Kind, group, name string, fallback int64) int64 {
	if reg == nil || !reg.HasKind(kind) {
		return fallback
	}
	if rev, ok := reg.ResourceRevision(kind, group, name); ok {
		return rev
	}
	return 0
}

// resolveQueryGateRevision answers the query gate's
// `getLocatorRevision(name, group)` callback as a (cacheRev, found) pair.
//
// Routing matches resolveSchemaRevision: registry-tracked kinds read through
// the per-service schemaRepo aggregator; un-tracked kinds fall through to the
// locator. The `found` flag mirrors the locator's existence so the gate's
// STATUS_NOT_FOUND vs SCHEMA_NOT_APPLIED contract is preserved — when the
// registry is authoritative for the kind but does not yet hold the key, the
// gate must return SCHEMA_NOT_APPLIED (after the bounded await), not
// NOT_FOUND, so long as the locator already knows the resource exists.
func resolveQueryGateRevision(reg *registry.NodeRepoRegistry, kind schema.Kind, group, name string, locatorRev int64, locatorExists bool) (int64, bool) {
	if reg == nil || !reg.HasKind(kind) {
		return locatorRev, locatorExists
	}
	if rev, ok := reg.ResourceRevision(kind, group, name); ok {
		return rev, true
	}
	return 0, locatorExists
}
