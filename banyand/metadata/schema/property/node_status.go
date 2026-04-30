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
)

// nodeStatusMaxKeys caps the per-request key count to match SchemaBarrierService
// (barrierMaxKeys in banyand/liaison/grpc/barrier.go). The proto leaves this
// uncapped so chunking is the caller's responsibility, but the server still
// enforces the same bound the SchemaBarrierService does so a misbehaving peer
// cannot allocate unbounded memory.
const nodeStatusMaxKeys = 10000

// NodeSchemaStatusServer implements clusterv1.NodeSchemaStatusServiceServer
// against a local SchemaRegistry cache. The same implementation runs on every
// cluster member that holds a schema cache: liaisons (Role_ROLE_LIAISON) and
// data nodes (Role_ROLE_DATA). The "Node" prefix in the service name is a
// misnomer inherited from the original proto draft — treat the service as
// "cluster-member schema status" regardless of the role serving it.
//
// The cache the server reads (the SchemaRegistry's schemaCache) is the same
// one downstream consumers (pkg/schema.schemaRepo, groupRepo, entityRepo)
// observe through the property watch loop. The schemaCache.notifiedModRevision
// watermark only advances after every registered handler has processed the
// relevant event, so a positive answer from this server (Present=true,
// rev=R) implies every downstream cache has also observed R. This is the
// load-bearing prerequisite for the Phase 2 cluster barrier — without it,
// the barrier would confirm a cache the data-node query executor never reads.
type NodeSchemaStatusServer struct {
	clusterv1.UnimplementedNodeSchemaStatusServiceServer
	cacheProvider func() *schemaCache
}

// NewNodeSchemaStatusServer wires the server to a cache provider. The
// provider is called per request so the server tolerates SchemaRegistry
// initialisation racing the gRPC server boot — an early call simply gets a
// nil cache and returns zero-valued results, which liaison-side fan-out
// treats as "node not yet ready, retry on the next backoff iteration"
// rather than a hard error.
func NewNodeSchemaStatusServer(cacheProvider func() *schemaCache) *NodeSchemaStatusServer {
	return &NodeSchemaStatusServer{cacheProvider: cacheProvider}
}

// NewNodeSchemaStatusServerForRegistry wires the server through a registry
// provider resolved per request. Construction-time wiring (which happens
// before the metadata service's PreRun has populated the registry) would
// otherwise capture a nil snapshot and skip registration permanently. The
// provider lets the server tolerate a still-initializing registry the same
// way the barrierSVC closure in banyand/liaison/grpc/server.go does — a nil
// registry surfaces through cacheProvider as a nil schemaCache and the
// fail-closed nil-cache contract takes over.
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

// GetMaxRevision returns the highest mod_revision currently observed by the
// node's local schema cache. When the cache is not yet initialized the
// response carries 0, which the liaison's barrier loop interprets as
// "node not yet caught up" and continues polling.
func (s *NodeSchemaStatusServer) GetMaxRevision(_ context.Context, _ *clusterv1.GetMaxRevisionRequest) (*clusterv1.GetMaxRevisionResponse, error) {
	c := s.cacheProvider()
	if c == nil {
		return &clusterv1.GetMaxRevisionResponse{}, nil
	}
	return &clusterv1.GetMaxRevisionResponse{MaxModRevision: c.GetMaxModRevision()}, nil
}

// GetKeyRevisions returns per-key (mod_revision, present) pairs in the same
// order the caller supplied keys. A key referencing a group the node has
// not observed maps to mod_revision=0, present=false — the call does not
// error on missing groups so the liaison can treat group-not-yet-registered
// as a normal laggard rather than a server fault.
//
// An empty request is valid and produces an empty response.
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
	c := s.cacheProvider()
	if c == nil || len(keys) == 0 {
		return resp, nil
	}
	propIDs := schemaKeysToPropIDs(keys)
	statuses := c.GetKeyRevisions(propIDs)
	for i, keyStatus := range statuses {
		resp.Revisions[i].ModRevision = keyStatus.ModRevision
		resp.Revisions[i].Present = keyStatus.Present
	}
	return resp, nil
}

// GetAbsentKeys partitions the requested keys into "absent" (i.e. not in the
// node's live cache or pending downstream notification) and "still_present".
// A SchemaKey with an unknown kind value is reported in absent_keys without
// erroring so the caller can rely on the partition adding up to the input.
func (s *NodeSchemaStatusServer) GetAbsentKeys(_ context.Context, req *clusterv1.GetAbsentKeysRequest) (*clusterv1.GetAbsentKeysResponse, error) {
	keys := req.GetKeys()
	if len(keys) > nodeStatusMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", nodeStatusMaxKeys)
	}
	resp := &clusterv1.GetAbsentKeysResponse{}
	if len(keys) == 0 {
		return resp, nil
	}
	c := s.cacheProvider()
	if c == nil {
		// Cache not yet initialized — the node has not observed any schema
		// state, so it cannot claim deletion has been applied. Report every
		// key as still present so the liaison's AwaitSchemaDeleted barrier
		// keeps polling until the cache is online. Mirrors the Phase 1
		// collectPresentKeys nil-cache contract in
		// banyand/liaison/grpc/barrier.go.
		resp.StillPresentKeys = append([]*schemav1.SchemaKey(nil), keys...)
		return resp, nil
	}
	propIDs := schemaKeysToPropIDs(keys)
	// One read-lock pass over the cache; an empty propID (unknown kind) maps
	// to Present=false in c.GetKeyRevisions because c.entries[""] never
	// matches, which is the same "absent" partition the proto requires.
	statuses := c.GetKeyRevisions(propIDs)
	for i, keyStatus := range statuses {
		if keyStatus.Present {
			resp.StillPresentKeys = append(resp.StillPresentKeys, keys[i])
			continue
		}
		resp.AbsentKeys = append(resp.AbsentKeys, keys[i])
	}
	return resp, nil
}

// schemaKeysToPropIDs converts a slice of SchemaKey messages into the
// internal propID strings used by the schema cache. Keys with unknown kind
// strings produce an empty propID at the corresponding index; callers may
// pass empty propIDs straight to schemaCache.GetKeyRevisions, which reports
// them as Present=false (the cache map never holds an entry under "").
// This matches the proto contract: an unknown kind is "absent from this
// node's perspective" rather than a parse error.
func schemaKeysToPropIDs(keys []*schemav1.SchemaKey) []string {
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = BuildPropertyIDFromSchemaKey(key)
	}
	return out
}
