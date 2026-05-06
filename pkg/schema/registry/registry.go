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

// Package registry holds the per-node aggregator that routes barrier and
// node-status RPC lookups to the same per-service schema-repo caches the
// query executor consults. Living in its own package lets banyand/metadata
// expose the registry via metadata.Service without importing pkg/schema,
// which would create a cycle.
package registry

import (
	"slices"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// RevisionRepository is the read surface a per-service schema repo exposes to
// the registry. The schemaRepo type in pkg/schema implements this interface;
// keeping the interface here (alongside the registry) instead of importing
// pkg/schema avoids a dependency cycle through banyand/metadata.
type RevisionRepository interface {
	LatestModRevision() int64
	ResourceRevision(kind schema.Kind, group, name string) (int64, bool)
	IsAbsent(kind schema.Kind, group, name string) bool
}

// NodeRepoRegistry aggregates per-service RevisionRepository instances on a
// single node (liaison or data). Each banyand service (measure, stream, trace,
// …) registers its schemaRepo during PreRun with a kind bitmask describing the
// kinds the repo tracks. The registry routes barrier and node-status lookups to
// the same caches the executor consults during query plan execution, so a
// positive answer from LatestModRevision implies every registered repo has
// applied the revision and any executor cache the node exposes is at least at
// that point. This is the load-bearing prerequisite for the Phase 2
// single-cache invariant — without it the cluster barrier would certify a
// cache the data-node executor never reads.
//
// Safe for concurrent registration during PreRun and concurrent lookup.
type NodeRepoRegistry struct {
	byKind map[schema.Kind][]RevisionRepository
	seen   map[RevisionRepository]struct{}
	repos  []RevisionRepository
	mu     sync.RWMutex
}

// NewNodeRepoRegistry returns an empty registry ready for service registration.
func NewNodeRepoRegistry() *NodeRepoRegistry {
	return &NodeRepoRegistry{
		byKind: make(map[schema.Kind][]RevisionRepository),
		seen:   make(map[RevisionRepository]struct{}),
	}
}

// Register associates a RevisionRepository with one or more kinds. The kinds
// argument is a bitmask of schema.Kind values; the registry walks each set
// bit and indexes the repo against that kind. Re-registering the same
// (kind, repo) pair is a no-op so PreRun is idempotent. A nil repo or empty
// kind mask is silently dropped.
func (r *NodeRepoRegistry) Register(kinds schema.Kind, repo RevisionRepository) {
	if repo == nil || kinds == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seen[repo]; !ok {
		r.seen[repo] = struct{}{}
		r.repos = append(r.repos, repo)
	}
	for _, k := range schema.AllKinds() {
		if kinds&k == 0 {
			continue
		}
		existing := r.byKind[k]
		if !slices.Contains(existing, repo) {
			r.byKind[k] = append(existing, repo)
		}
	}
}

// LatestModRevision returns the minimum LatestModRevision across every
// registered repo. A registry with no repos returns 0, matching the
// "node not yet caught up" semantic the cluster-barrier loop interprets as
// "keep polling".
func (r *NodeRepoRegistry) LatestModRevision() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.repos) == 0 {
		return 0
	}
	minRev := r.repos[0].LatestModRevision()
	for _, repo := range r.repos[1:] {
		if rev := repo.LatestModRevision(); rev < minRev {
			minRev = rev
		}
	}
	return minRev
}

// ResourceRevision routes the lookup to repos registered against the given
// kind and returns the first match. Returns (0, false) when no repo is
// registered for the kind or no registered repo holds the resource — the same
// shape the per-service schemaRepo.ResourceRevision returns for an unknown
// key, so the barrier and node-status RPC can fall through unchanged.
func (r *NodeRepoRegistry) ResourceRevision(kind schema.Kind, group, name string) (int64, bool) {
	r.mu.RLock()
	repos := r.byKind[kind]
	r.mu.RUnlock()
	for _, repo := range repos {
		if rev, ok := repo.ResourceRevision(kind, group, name); ok {
			return rev, true
		}
	}
	return 0, false
}

// IsAbsent returns true when no repo registered for the kind holds the given
// (group, name). A kind with no registered repo is reported absent — the
// AwaitSchemaDeleted barrier reads this as "the deletion has already been
// observed" only if the kind genuinely has no owner on this node. Wire
// TopN/Property kinds through the schemaCache path; do not infer absence
// from this registry for those.
func (r *NodeRepoRegistry) IsAbsent(kind schema.Kind, group, name string) bool {
	_, ok := r.ResourceRevision(kind, group, name)
	return !ok
}

// HasKind reports whether at least one repo is registered for the given kind.
// The node-status server uses this to route per-key lookups: kinds with a
// registered repo go through the registry; kinds without (TopN, Property)
// fall through to the property schemaCache.
func (r *NodeRepoRegistry) HasKind(kind schema.Kind) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byKind[kind]) > 0
}

// Empty reports whether any repo has been registered. The node-status server
// uses this to skip the registry's min-rev contribution on a node where no
// per-service schemaRepo runs (e.g. a property-only metadata host) — without
// the check, an empty registry's LatestModRevision()=0 would gate every
// barrier verdict to 0.
func (r *NodeRepoRegistry) Empty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.repos) == 0
}

// MaybeRegister registers repo with reg under the given kinds when reg is
// non-nil and repo satisfies RevisionRepository. Service constructors hold the
// schemaRepo as a Repository interface (pkg/schema.Repository); the concrete
// value is *pkg/schema.schemaRepo, which implements RevisionRepository — so a
// type assertion at registration time is safe in production and fails closed
// for in-memory test fakes that only implement the base Repository surface.
func MaybeRegister(reg *NodeRepoRegistry, kinds schema.Kind, repo any) {
	if reg == nil {
		return
	}
	rr, ok := repo.(RevisionRepository)
	if !ok {
		return
	}
	reg.Register(kinds, rr)
}
