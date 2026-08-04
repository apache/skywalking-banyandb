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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/consistency"
)

// ResourceView is one resource's cache and runtime views. Runtime is nil when
// the resource is not materialized, which leaves its fingerprint at zero.
type ResourceView struct {
	Cached       proto.Message
	Runtime      proto.Message
	Name         string
	RuntimeRules []*databasev1.IndexRule
}

// ResourceViewer extracts the fingerprintable views of a resource. It returns
// false when the resource is not of the caller's catalog type.
//
// This is the only catalog-specific part of state collection: IndexListener
// exposes only OnIndexUpdate, so reaching the runtime view needs a type
// assertion that only the owning package can make.
type ResourceViewer func(Resource) (ResourceView, bool)

// CollectSchemaState reports a node's cache and runtime fingerprints for every
// schema object it holds in the group. Registry truth is NOT collected here --
// it is fetched once from a schema-serving node (see the liaison side) and
// reported separately; this node only knows its own two layers. The node
// identity is filled in by the caller onto DataInfo/LiaisonInfo, so it is not
// repeated here.
//
// A fingerprint failure returns an error rather than dropping the object: an
// omitted object would look MISSING_IN_CACHE to the checker (a false positive),
// so the node reports nothing and the caller degrades the group's verdict to
// UNKNOWN instead -- matching how the registry side bails on a fingerprint
// failure rather than emitting a partial truth.
//
// The underlying maps are read without a shared lock: taking the write path's
// mutex here would stall writers for the whole sweep. A concurrent watch event
// can therefore produce a torn read, which the consistency checker's
// consecutive-cycle suppression absorbs -- a torn read does not repeat on the
// same object round after round, while a real divergence does.
func CollectSchemaState(
	repo Repository,
	group, resourceKind string,
	view ResourceViewer,
) ([]*databasev1.ObjectSchemaState, error) {
	var objects []*databasev1.ObjectSchemaState
	if g, ok := repo.LoadGroup(group); ok {
		if gs := g.GetSchema(); gs != nil {
			obj, err := selfConsistent(schema.KindGroup.String(), group, gs)
			if err != nil {
				return nil, err
			}
			objects = append(objects, obj)
		}
	}
	for _, rule := range repo.LoadAllIndexRules(group) {
		obj, err := selfConsistent(schema.KindIndexRule.String(), rule.GetMetadata().GetName(), rule)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	for _, res := range repo.LoadAllResources(group) {
		v, ok := view(res)
		if !ok {
			continue
		}
		// res.Schema() is already a ResourceSchema, so the cached rule set needs no
		// type assertion on v.Cached.
		cacheFP, err := consistency.Fingerprint(v.Cached, repo.IndexRules(res.Schema()))
		if err != nil {
			return nil, fmt.Errorf("failed to fingerprint cached %s %q: %w", resourceKind, v.Name, err)
		}
		obj := &databasev1.ObjectSchemaState{
			Kind:             resourceKind,
			Name:             v.Name,
			CacheFingerprint: cacheFP,
		}
		if v.Runtime != nil {
			runtimeFP, runtimeErr := consistency.Fingerprint(v.Runtime, v.RuntimeRules)
			if runtimeErr != nil {
				return nil, fmt.Errorf("failed to fingerprint runtime %s %q: %w", resourceKind, v.Name, runtimeErr)
			}
			obj.RuntimeFingerprint = runtimeFP
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

// selfConsistent fingerprints an object that has no derived structure, so its
// cache and runtime views are by definition the same value.
func selfConsistent(kind, name string, spec proto.Message) (*databasev1.ObjectSchemaState, error) {
	fp, err := consistency.Fingerprint(spec, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fingerprint %s %q: %w", kind, name, err)
	}
	return &databasev1.ObjectSchemaState{
		Kind:               kind,
		Name:               name,
		CacheFingerprint:   fp,
		RuntimeFingerprint: fp,
	}, nil
}
