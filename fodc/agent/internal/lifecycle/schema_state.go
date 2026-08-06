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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
	"github.com/apache/skywalking-banyandb/fodc/internal/timeouts"
)

// groupObjectState is one schema object's cache/runtime fingerprints on the
// local node. The proxy unions these partials across agents and pairs them with
// the registry truth it fetches itself.
type groupObjectState struct {
	kind    string
	name    string
	cache   uint64
	runtime uint64
	hasNode bool
}

// groupSchemaState accumulates one group's per-object evidence, the local node
// id, and any collection errors.
type groupSchemaState struct {
	objects map[consistency.ObjectKey]*groupObjectState
	node    string
	errors  []string
}

func newGroupSchemaState() *groupSchemaState {
	return &groupSchemaState{objects: make(map[consistency.ObjectKey]*groupObjectState)}
}

func (s *groupSchemaState) object(kind, name string) *groupObjectState {
	key := consistency.ObjectKey{Kind: kind, Name: name}
	o, ok := s.objects[key]
	if !ok {
		o = &groupObjectState{kind: kind, name: name}
		s.objects[key] = o
	}
	return o
}

// toSchemaConsistency renders this agent's partial view of a group: an
// object-centric table carrying this node's cache/runtime fingerprints. It sets
// no registry fingerprint (the proxy fetches the registry truth) and leaves
// Status UNSPECIFIED -- the proxy assigns the verdict after unioning every
// agent's partial and comparing against the registry.
func (s *groupSchemaState) toSchemaConsistency() *fodcv1.SchemaConsistency {
	objects := make([]*fodcv1.ObjectConsistency, 0, len(s.objects))
	for _, o := range s.objects {
		oc := &fodcv1.ObjectConsistency{Kind: o.kind, Name: o.name}
		// A node fingerprint without a node id (the trailer never arrived) would
		// merge into a phantom "" node at the proxy; drop it and let the errors
		// this group also carries drive the UNKNOWN verdict.
		if o.hasNode && s.node != "" {
			oc.NodeFingerprints = []*fodcv1.NodeFingerprint{{
				Node: s.node, CacheFingerprint: o.cache, RuntimeFingerprint: o.runtime,
			}}
		}
		objects = append(objects, oc)
	}
	consistency.SortObjectConsistencies(objects)
	return &fodcv1.SchemaConsistency{Objects: objects}
}

// applySchemaStates folds each group's schema evidence into the matching
// GroupLifecycleInfo (its schema_consistency partial and any collection errors),
// creating a bare entry only for a group InspectAll did not return. The proxy
// unions these partials across agents and overwrites schema_consistency with the
// final verdict.
func applySchemaStates(groups []*fodcv1.GroupLifecycleInfo, states map[string]*groupSchemaState) []*fodcv1.GroupLifecycleInfo {
	byName := make(map[string]*fodcv1.GroupLifecycleInfo, len(groups))
	for _, g := range groups {
		byName[g.GetName()] = g
	}
	for name, st := range states {
		g, ok := byName[name]
		if !ok {
			g = &fodcv1.GroupLifecycleInfo{Name: name}
			byName[name] = g
			groups = append(groups, g)
		}
		g.SchemaConsistency = st.toSchemaConsistency()
		if len(st.errors) > 0 {
			g.Errors = append(g.Errors, st.errors...)
		}
	}
	return groups
}

// collectNodeSchemaState streams the local node's schema bodies and fingerprints
// them here (not in the data path), producing one groupSchemaState per group.
// An Unimplemented server (older node, or a role without the service) yields
// (nil, nil): the agent simply contributes no node schema state. A transport
// error is propagated so the caller does not cache a torn result.
func (c *Collector) collectNodeSchemaState(ctx context.Context) (map[string]*groupSchemaState, error) {
	if c.grpcAddr == "" {
		return nil, nil
	}
	conn, err := grpc.NewClient(c.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", c.grpcAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := databasev1.NewNodeSchemaStateServiceClient(conn)
	reqCtx, cancel := context.WithTimeout(ctx, timeouts.AgentInspectAll)
	defer cancel()
	stream, err := client.StreamGroupSchemaState(reqCtx, &databasev1.NodeSchemaStateRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return nil, nil
		}
		return nil, fmt.Errorf("StreamGroupSchemaState on %s: %w", c.grpcAddr, err)
	}

	states := make(map[string]*groupSchemaState)
	received := make(map[string]uint32)
	tables := make(map[string][]*databasev1.IndexRule)
	get := func(group string) *groupSchemaState {
		s, ok := states[group]
		if !ok {
			s = newGroupSchemaState()
			states[group] = s
		}
		return s
	}
	for {
		event, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if status.Code(recvErr) == codes.Unimplemented {
				return nil, nil
			}
			return nil, fmt.Errorf("recv schema state from %s: %w", c.grpcAddr, recvErr)
		}
		switch e := event.GetEvent().(type) {
		case *databasev1.SchemaSnapshotEvent_RuleTable:
			// The node may split a group's rule table across several events;
			// concatenating in arrival order keeps bound_index_rule_refs valid.
			g := e.RuleTable.GetGroup()
			tables[g] = append(tables[g], e.RuleTable.GetRules()...)
		case *databasev1.SchemaSnapshotEvent_Object:
			obj := e.Object
			st := get(obj.GetGroup())
			cacheFP, runtimeFP, fpErr := fingerprintObjectSnapshot(obj, tables[obj.GetGroup()])
			if fpErr != nil {
				st.errors = append(st.errors, fpErr.Error())
				continue
			}
			o := st.object(obj.GetKind(), obj.GetName())
			o.cache, o.runtime, o.hasNode = cacheFP, runtimeFP, true
			received[obj.GetGroup()]++
		case *databasev1.SchemaSnapshotEvent_Trailer:
			t := e.Trailer
			st := get(t.GetGroup())
			st.node = t.GetNode()
			st.errors = append(st.errors, t.GetErrors()...)
			// A count mismatch means the stream tore mid-flight; flag it so the
			// proxy checker degrades this node to UNKNOWN rather than reading the
			// partial set as a real divergence.
			if received[t.GetGroup()] != t.GetObjectCount() {
				st.errors = append(st.errors, fmt.Sprintf(
					"torn stream: received %d objects, trailer reported %d",
					received[t.GetGroup()], t.GetObjectCount()))
			}
		}
	}

	return states, nil
}

// fingerprintObjectSnapshot hashes an object's cache and runtime bodies against
// the group's rule table. Runtime is absent for objects with no derived
// structure (group, indexRule) or when nothing is materialized yet, in which
// case the runtime fingerprint equals the cache one -- matching the node-side
// semantics the checker expects.
func fingerprintObjectSnapshot(obj *databasev1.ObjectSnapshot, table []*databasev1.IndexRule) (cacheFP, runtimeFP uint64, err error) {
	cacheBody, cacheRules, err := schemaBodyPayload(obj.GetCache(), table)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve cache %s %q: %w", obj.GetKind(), obj.GetName(), err)
	}
	cacheFP, err = consistency.Fingerprint(cacheBody, cacheRules)
	if err != nil {
		return 0, 0, fmt.Errorf("fingerprint cache %s %q: %w", obj.GetKind(), obj.GetName(), err)
	}
	runtimeFP = cacheFP
	if runtime := obj.GetRuntime(); runtime != nil {
		runtimeBody, runtimeRules, resolveErr := schemaBodyPayload(runtime, table)
		if resolveErr != nil {
			return 0, 0, fmt.Errorf("resolve runtime %s %q: %w", obj.GetKind(), obj.GetName(), resolveErr)
		}
		runtimeFP, err = consistency.Fingerprint(runtimeBody, runtimeRules)
		if err != nil {
			return 0, 0, fmt.Errorf("fingerprint runtime %s %q: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	return cacheFP, runtimeFP, nil
}

// schemaBodyPayload unwraps the Any payload into its concrete proto and resolves
// its bound_index_rule_refs against the group's rule table. A ref outside the
// table means the stream tore or the table event was lost; that is an error so
// the caller degrades the node to UNKNOWN rather than hashing a partial rule set.
func schemaBodyPayload(body *databasev1.SchemaBody, table []*databasev1.IndexRule) (proto.Message, []*databasev1.IndexRule, error) {
	if body == nil {
		return nil, nil, nil
	}
	payload, err := body.GetPayload().UnmarshalNew()
	if err != nil {
		return nil, nil, fmt.Errorf("unwrap schema body payload: %w", err)
	}
	refs := body.GetBoundIndexRuleRefs()
	if len(refs) == 0 {
		return payload, nil, nil
	}
	rules := make([]*databasev1.IndexRule, 0, len(refs))
	for _, ref := range refs {
		if int(ref) >= len(table) {
			return nil, nil, fmt.Errorf("bound index rule ref %d out of range (table size %d)", ref, len(table))
		}
		rules = append(rules, table[ref])
	}
	return payload, rules, nil
}
