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

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

// nodeStatusMaxKeys caps the per-request key count to match SchemaBarrierService
// (barrierMaxKeys in banyand/liaison/grpc/barrier.go). The proto leaves this
// uncapped so chunking is the caller's responsibility, but the server still
// enforces the same bound the SchemaBarrierService does so a misbehaving peer
// cannot allocate unbounded memory.
const nodeStatusMaxKeys = 10000

// NodeSchemaStatusServer implements clusterv1.NodeSchemaStatusServiceServer
// against the per-node schema caches. The same implementation runs on every
// cluster member that holds a schema cache: liaisons (Role_ROLE_LIAISON) and
// data nodes (Role_ROLE_DATA). The "Node" prefix in the service name is a
// misnomer inherited from the original proto draft — treat the service as
// "cluster-member schema status" regardless of the role serving it.
//
// Two underlying caches feed this server:
//
//  1. The per-service NodeRepoRegistry (pkg/schema/registry) — the same
//     pkg/schema.schemaRepo instances the data-node executor's
//     LoadGroup / LoadResource resolve through. Registered against
//     KindGroup, KindStream, KindMeasure, KindTrace, KindIndexRule,
//     KindIndexRuleBinding. Reading from the registry guarantees the
//     barrier and the executor see a consistent answer for the same
//     (group, ModRevision) pair — closing the eventCh-retry leak in
//     pkg/schema.schemaRepo.SendMetadataEvent that produced the §4.6.2
//     second-run flake (commit 47006561). This is the load-bearing
//     invariant of Phase 2 §Step 2.5.
//
//  2. The property schemaCache — the upstream watch cache. Used only for
//     KindTopNAggregation and KindProperty, which schemaRepo does not
//     track (no executor race there because TopN/Property are not
//     consulted in the data-node query path the same way the per-service
//     resources are). The schemaCache.notifiedModRevision watermark is
//     still load-bearing for those kinds.
//
// GetMaxRevision returns the minimum across both sources so the barrier never
// certifies a revision until every cache the node exposes has applied it.
type NodeSchemaStatusServer struct {
	clusterv1.UnimplementedNodeSchemaStatusServiceServer
	cacheProvider    func() *schemaCache
	registryProvider func() *registry.NodeRepoRegistry
}

// NewNodeSchemaStatusServer wires the server to a schemaCache provider only.
// Used by tests that fix a hand-built cache; production wiring uses the
// registry-aware constructors below so the cluster barrier reads the same
// schemaRepo the executor consults.
func NewNodeSchemaStatusServer(cacheProvider func() *schemaCache) *NodeSchemaStatusServer {
	return &NodeSchemaStatusServer{cacheProvider: cacheProvider}
}

// NewNodeSchemaStatusServerWithRegistry wires the server to both a schemaCache
// provider (for TopN/Property kinds) and a NodeRepoRegistry provider (for the
// per-service kinds the executor reads). Either provider may transiently
// return nil while metadata.PreRun is still running; the per-call closures
// tolerate that the same way the standalone fail-closed contract does.
func NewNodeSchemaStatusServerWithRegistry(
	cacheProvider func() *schemaCache,
	registryProvider func() *registry.NodeRepoRegistry,
) *NodeSchemaStatusServer {
	return &NodeSchemaStatusServer{
		cacheProvider:    cacheProvider,
		registryProvider: registryProvider,
	}
}

// NewNodeSchemaStatusServerForRegistry wires the server through a property
// SchemaRegistry provider resolved per request. Construction-time wiring
// (which happens before the metadata service's PreRun has populated the
// registry) would otherwise capture a nil snapshot and skip registration
// permanently. The provider lets the server tolerate a still-initializing
// registry the same way the barrierSVC closure in
// banyand/liaison/grpc/server.go does — a nil registry surfaces through
// cacheProvider as a nil schemaCache and the fail-closed nil-cache contract
// takes over.
func NewNodeSchemaStatusServerForRegistry(provider func() *SchemaRegistry) *NodeSchemaStatusServer {
	return &NodeSchemaStatusServer{
		cacheProvider: func() *schemaCache {
			if provider == nil {
				return nil
			}
			reg := provider()
			if reg == nil {
				return nil
			}
			return reg.cache
		},
	}
}

// NewNodeSchemaStatusServerForRegistryWithNodeRepo wires both the property
// SchemaRegistry provider (for the schemaCache) and a NodeRepoRegistry
// provider (for the per-service schemaRepo aggregator) through per-call
// closures. The metadata Service exposes the NodeRepoRegistry via
// Service.NodeRepoRegistry(); pass the bound method here.
func NewNodeSchemaStatusServerForRegistryWithNodeRepo(
	provider func() *SchemaRegistry,
	registryProvider func() *registry.NodeRepoRegistry,
) *NodeSchemaStatusServer {
	srv := NewNodeSchemaStatusServerForRegistry(provider)
	srv.registryProvider = registryProvider
	return srv
}

func (s *NodeSchemaStatusServer) cache() *schemaCache {
	if s.cacheProvider == nil {
		return nil
	}
	return s.cacheProvider()
}

func (s *NodeSchemaStatusServer) registry() *registry.NodeRepoRegistry {
	if s.registryProvider == nil {
		return nil
	}
	return s.registryProvider()
}

// GetMaxRevision returns the schemaCache's notifiedModRevision watermark.
// The cache observes every catalog's events, so it is the correct global
// watermark — symmetric with the receiving liaison's selfName probe at
// barrier_cluster.go:354-360 which also reads cache-only. Per-key gating
// (executor-cache routing by kind) is handled by GetKeyRevisions /
// GetAbsentKeys via the NodeRepoRegistry.
func (s *NodeSchemaStatusServer) GetMaxRevision(_ context.Context, _ *clusterv1.GetMaxRevisionRequest) (*clusterv1.GetMaxRevisionResponse, error) {
	c := s.cache()
	if c == nil {
		return &clusterv1.GetMaxRevisionResponse{}, nil
	}
	return &clusterv1.GetMaxRevisionResponse{MaxModRevision: c.GetMaxModRevision()}, nil
}

// GetKeyRevisions returns per-key (mod_revision, present) pairs in the same
// order the caller supplied keys. Each key is routed by Kind: kinds tracked
// by some registered schemaRepo are answered from the NodeRepoRegistry (so
// the response matches what the executor's LoadResource would resolve);
// remaining kinds (TopN, Property) fall through to the schemaCache.
//
// A key referencing a group the node has not observed maps to
// mod_revision=0, present=false — the call does not error on missing groups
// so the liaison can treat group-not-yet-registered as a normal laggard
// rather than a server fault. An empty request is valid and produces an
// empty response.
func (s *NodeSchemaStatusServer) GetKeyRevisions(_ context.Context, req *clusterv1.GetKeyRevisionsRequest) (*clusterv1.GetKeyRevisionsResponse, error) {
	keys := req.GetKeys()
	if len(keys) > nodeStatusMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", nodeStatusMaxKeys)
	}
	resp := &clusterv1.GetKeyRevisionsResponse{
		Revisions: make([]*clusterv1.KeyRevision, len(keys)),
	}
	for i, key := range keys {
		resp.Revisions[i] = &clusterv1.KeyRevision{Key: key}
	}
	if len(keys) == 0 {
		return resp, nil
	}
	cacheIndices := s.routeKeysByKind(keys, resp.Revisions)
	s.fillCacheKeyRevisions(keys, cacheIndices, resp.Revisions)
	return resp, nil
}

// GetAbsentKeys partitions the requested keys into "absent" (i.e. not in the
// node's live cache or pending downstream notification) and "still_present".
// A SchemaKey with an unknown kind value is reported in absent_keys without
// erroring so the caller can rely on the partition adding up to the input.
//
// Routing follows GetKeyRevisions: registry-routed kinds consult the
// per-service schemaRepo (so absence here means absence in the executor's
// resolver cache too); TopN/Property fall through to the schemaCache.
func (s *NodeSchemaStatusServer) GetAbsentKeys(_ context.Context, req *clusterv1.GetAbsentKeysRequest) (*clusterv1.GetAbsentKeysResponse, error) {
	keys := req.GetKeys()
	if len(keys) > nodeStatusMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", nodeStatusMaxKeys)
	}
	resp := &clusterv1.GetAbsentKeysResponse{}
	if len(keys) == 0 {
		return resp, nil
	}
	c := s.cache()
	reg := s.registry()
	if c == nil && (reg == nil || reg.Empty()) {
		// Neither source initialized — the node has not observed any
		// schema state, so it cannot claim deletion has been applied.
		// Report every key as still present so the liaison's
		// AwaitSchemaDeleted barrier keeps polling. Mirrors the Phase 1
		// collectPresentKeys nil-cache contract in
		// banyand/liaison/grpc/barrier.go.
		resp.StillPresentKeys = append([]*schemav1.SchemaKey(nil), keys...)
		return resp, nil
	}
	// Reuse the GetKeyRevisions routing to obtain a per-key Present flag,
	// then partition by that flag. The two RPCs share a routing rule by
	// design; keeping the implementation in one place avoids drift.
	revisions := make([]*clusterv1.KeyRevision, len(keys))
	for i, key := range keys {
		revisions[i] = &clusterv1.KeyRevision{Key: key}
	}
	cacheIndices := s.routeKeysByKind(keys, revisions)
	s.fillCacheKeyRevisions(keys, cacheIndices, revisions)
	for i, kr := range revisions {
		if kr.Present {
			resp.StillPresentKeys = append(resp.StillPresentKeys, keys[i])
			continue
		}
		resp.AbsentKeys = append(resp.AbsentKeys, keys[i])
	}
	return resp, nil
}

// routeKeysByKind fills in revisions[i] for every key whose Kind is tracked
// by a registered schemaRepo and returns the indices of the remaining keys
// that need to be answered from the schemaCache. The split is positional so
// the caller can preserve the request key order in its response.
func (s *NodeSchemaStatusServer) routeKeysByKind(keys []*schemav1.SchemaKey, revisions []*clusterv1.KeyRevision) []int {
	reg := s.registry()
	if reg == nil {
		cacheIndices := make([]int, len(keys))
		for i := range keys {
			cacheIndices[i] = i
		}
		return cacheIndices
	}
	cacheIndices := make([]int, 0, len(keys))
	for i, key := range keys {
		kind := kindFromProtoString(key.GetKind())
		if kind == 0 || !reg.HasKind(kind) {
			cacheIndices = append(cacheIndices, i)
			continue
		}
		rev, ok := reg.ResourceRevision(kind, key.GetGroup(), key.GetName())
		revisions[i].ModRevision = rev
		revisions[i].Present = ok
	}
	return cacheIndices
}

// fillCacheKeyRevisions answers the schemaCache-routed indices in a single
// read-lock pass. A nil cache leaves the revisions untouched (Present=false,
// ModRevision=0) which is the correct "node hasn't seen this key" answer.
func (s *NodeSchemaStatusServer) fillCacheKeyRevisions(keys []*schemav1.SchemaKey, cacheIndices []int, revisions []*clusterv1.KeyRevision) {
	if len(cacheIndices) == 0 {
		return
	}
	c := s.cache()
	if c == nil {
		return
	}
	propIDs := make([]string, len(cacheIndices))
	for j, i := range cacheIndices {
		propIDs[j] = BuildPropertyIDFromSchemaKey(keys[i])
	}
	statuses := c.GetKeyRevisions(propIDs)
	for j, keyStatus := range statuses {
		i := cacheIndices[j]
		revisions[i].ModRevision = keyStatus.ModRevision
		revisions[i].Present = keyStatus.Present
	}
}
