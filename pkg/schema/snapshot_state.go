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

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// ResourceView is one resource's cache and runtime views. Runtime is nil when
// the resource is not materialized.
type ResourceView struct {
	Cached       proto.Message
	Runtime      proto.Message
	Name         string
	RuntimeRules []*databasev1.IndexRule
}

// ResourceViewer extracts the snapshot views of a resource. It returns false
// when the resource is not of the caller's catalog type. This is the only
// catalog-specific part of snapshot collection: IndexListener exposes only
// OnIndexUpdate, so reaching the runtime view needs a type assertion that only
// the owning package can make.
type ResourceViewer func(Resource) (ResourceView, bool)

// CollectSchemaSnapshot returns a node's cache and runtime schema BODIES for
// every object it holds in the group, as ObjectSnapshots. Unlike
// CollectSchemaState it ships no fingerprints: the FODC agent hashes the bodies
// on receive, keeping the hash algorithm and suppression policy out of the data
// path.
//
// The same lock-free rationale as CollectSchemaState applies: the maps are read
// without the write path's mutex, so a concurrent watch event can tear the read;
// the agent's two-round suppression absorbs it. Callers that need a consistent
// view snapshot the resource list under a read lock before calling.
func CollectSchemaSnapshot(
	repo Repository,
	group, resourceKind string,
	view ResourceViewer,
) ([]*databasev1.ObjectSnapshot, []*databasev1.IndexRule, error) {
	tb := newRuleTable()
	var objects []*databasev1.ObjectSnapshot
	if g, ok := repo.LoadGroup(group); ok {
		if gs := g.GetSchema(); gs != nil {
			obj, err := selfConsistentSnapshot(tb, group, schema.KindGroup.String(), group, gs)
			if err != nil {
				return nil, nil, err
			}
			objects = append(objects, obj)
		}
	}
	for _, rule := range repo.LoadAllIndexRules(group) {
		obj, err := selfConsistentSnapshot(tb, group, schema.KindIndexRule.String(), rule.GetMetadata().GetName(), rule)
		if err != nil {
			return nil, nil, err
		}
		objects = append(objects, obj)
	}
	for _, res := range repo.LoadAllResources(group) {
		v, ok := view(res)
		if !ok {
			continue
		}
		cache, err := newSchemaBody(tb, v.Cached, repo.IndexRules(res.Schema()))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build cache body for %s %q: %w", resourceKind, v.Name, err)
		}
		obj := &databasev1.ObjectSnapshot{
			Group: group,
			Kind:  resourceKind,
			Name:  v.Name,
			Cache: cache,
		}
		if v.Runtime != nil {
			runtime, runtimeErr := newSchemaBody(tb, v.Runtime, v.RuntimeRules)
			if runtimeErr != nil {
				return nil, nil, fmt.Errorf("failed to build runtime body for %s %q: %w", resourceKind, v.Name, runtimeErr)
			}
			obj.Runtime = runtime
		}
		objects = append(objects, obj)
	}
	return objects, tb.rules, nil
}

// ruleTable interns index-rule bodies for wire deduplication only (never for
// consistency matching -- the checker keys objects by kind+name). The key is the
// deterministic marshal of the WHOLE rule, i.e. its exact content.
//
// Do NOT key this by rule name or id. A rule's cache body (current) and its
// runtime body (possibly a stale copy) share the same name and id but differ in
// content; that difference is exactly the RUNTIME_NOT_APPLIED signal. A
// content key keeps the two in separate entries so cacheFP != runtimeFP still
// fires; a name/id key would merge them, the runtime ref would resolve to the
// cache body, and the drift would be silently masked. Deterministic marshal
// guarantees "same content -> same key", so identical rules always dedup and
// differing rules never collide -- a miss only ever wastes a table slot, it
// cannot misroute a ref. See TestRuleTable_InternKeepsDivergentBodiesDistinct.
type ruleTable struct {
	index map[string]uint32
	rules []*databasev1.IndexRule
}

func newRuleTable() *ruleTable {
	return &ruleTable{index: make(map[string]uint32)}
}

// intern returns the table indexes of the given rules, appending any not yet
// seen. Order is preserved so the agent's derived rule set matches the node's.
// The key must stay content-based (see ruleTable): never switch it to name/id.
func (t *ruleTable) intern(rules []*databasev1.IndexRule) ([]uint32, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	refs := make([]uint32, 0, len(rules))
	for _, rule := range rules {
		key, err := proto.MarshalOptions{Deterministic: true}.Marshal(rule)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal index rule %q for interning: %w", rule.GetMetadata().GetName(), err)
		}
		idx, ok := t.index[string(key)]
		if !ok {
			idx = uint32(len(t.rules))
			t.rules = append(t.rules, rule)
			t.index[string(key)] = idx
		}
		refs = append(refs, idx)
	}
	return refs, nil
}

// CachedGroupNames lists the names of every group the repository currently
// caches, so an all-groups snapshot request can enumerate without a roster.
func CachedGroupNames(repo Repository) []string {
	groups := repo.LoadAllGroups()
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		if gs := g.GetSchema(); gs != nil {
			names = append(names, gs.GetMetadata().GetName())
		}
	}
	return names
}

// selfConsistentSnapshot builds a snapshot for an object with no derived
// structure (group, indexRule): its cache and runtime bodies are the same, so
// runtime is left unset and the agent takes the runtime fingerprint from cache.
func selfConsistentSnapshot(tb *ruleTable, group, kind, name string, spec proto.Message) (*databasev1.ObjectSnapshot, error) {
	cache, err := newSchemaBody(tb, spec, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build body for %s %q: %w", kind, name, err)
	}
	return &databasev1.ObjectSnapshot{
		Group: group,
		Kind:  kind,
		Name:  name,
		Cache: cache,
	}, nil
}

// newSchemaBody wraps a schema proto as an Any and interns its bound index rules
// into tb, storing only the resulting table refs. FODC needs no per-type
// knowledge, so a single Any replaces the former per-catalog oneof.
func newSchemaBody(tb *ruleTable, payload proto.Message, boundRules []*databasev1.IndexRule) (*databasev1.SchemaBody, error) {
	body, err := anypb.New(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to wrap schema body of type %T: %w", payload, err)
	}
	refs, err := tb.intern(boundRules)
	if err != nil {
		return nil, err
	}
	return &databasev1.SchemaBody{Payload: body, BoundIndexRuleRefs: refs}, nil
}
