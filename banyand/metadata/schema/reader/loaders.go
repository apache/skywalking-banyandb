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

package reader

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// LoadGroups returns the latest non-deleted Group doc for each requested
// group name (catalog and ResourceOpts both live on the Group proto).
// Unparsable Group docs are skipped, matching the schema server's tolerance
// for foreign docs in the catalog.
func LoadGroups(schemaRoot string, names []string) (map[string]*commonv1.Group, error) {
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	out := make(map[string]*commonv1.Group, len(names))
	if err := WalkDocs(schemaRoot, func(d Doc) error {
		if d.SourceJSON == "" {
			return nil
		}
		var g commonv1.Group
		if uErr := protojson.Unmarshal([]byte(d.SourceJSON), &g); uErr != nil {
			return nil
		}
		if len(wanted) > 0 && !wanted[g.GetMetadata().GetName()] {
			return nil
		}
		out[g.GetMetadata().GetName()] = &g
		return nil
	}, schema.KindGroup); err != nil {
		return nil, err
	}
	return out, nil
}

// LoadStreams returns the latest non-deleted stream schemas of the requested
// groups, keyed by group name.
func LoadStreams(schemaRoot string, groups []string) (map[string][]*databasev1.Stream, error) {
	return loadGroupScoped(schemaRoot, schema.KindStream, groups, func() *databasev1.Stream { return &databasev1.Stream{} })
}

// LoadMeasures returns the latest non-deleted measure schemas of the
// requested groups, keyed by group name.
func LoadMeasures(schemaRoot string, groups []string) (map[string][]*databasev1.Measure, error) {
	return loadGroupScoped(schemaRoot, schema.KindMeasure, groups, func() *databasev1.Measure { return &databasev1.Measure{} })
}

// LoadIndexRules returns the latest non-deleted index rules of the requested
// groups, keyed by group name.
func LoadIndexRules(schemaRoot string, groups []string) (map[string][]*databasev1.IndexRule, error) {
	return loadGroupScoped(schemaRoot, schema.KindIndexRule, groups, func() *databasev1.IndexRule { return &databasev1.IndexRule{} })
}

// loadGroupScoped loads one group-scoped Kind (stream / measure) into
// (group -> latest non-deleted protos). A doc missing its source tag is an
// error: unlike Group docs these are the payload the migration depends on.
func loadGroupScoped[T proto.Message](schemaRoot string, kind schema.Kind, groups []string, newT func() T) (map[string][]T, error) {
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	out := make(map[string][]T, len(groups))
	if err := WalkDocs(schemaRoot, func(d Doc) error {
		if len(wanted) > 0 && !wanted[d.Group] {
			return nil
		}
		if d.SourceJSON == "" {
			return fmt.Errorf("schema property %q missing source tag", d.PropID)
		}
		payload := newT()
		if uErr := protojson.Unmarshal([]byte(d.SourceJSON), payload); uErr != nil {
			return fmt.Errorf("unmarshal %s %q source: %w", kind.String(), d.PropID, uErr)
		}
		out[d.Group] = append(out[d.Group], payload)
		return nil
	}, kind); err != nil {
		return nil, err
	}
	return out, nil
}

// GroupedBinding pairs an IndexRuleBinding with the group it belongs to.
type GroupedBinding struct {
	Binding *databasev1.IndexRuleBinding
	Group   string
}

// StreamSchemaContext bundles everything the stream migration needs from the
// catalog: the requested groups' stream schemas plus their index rules and
// bindings. Rules and bindings are group-scoped exactly like the live
// resolution path (metadata's IndexRules lists bindings and fetches rules
// within subject.Group only — cross-group references don't exist), and
// bindings whose subject is not CATALOG_STREAM are dropped. Loaded in a
// single catalog walk; all filters key off each doc's own group tag, so
// visit order across kinds doesn't matter.
type StreamSchemaContext struct {
	Streams  map[string][]*databasev1.Stream
	Rules    map[string]map[string]*databasev1.IndexRule
	Bindings []GroupedBinding
}

// LoadStreamSchemaContext loads streams, index rules and index rule bindings
// in one WalkDocs pass — the per-kind loaders each re-walk every shard, which
// triples the catalog scan cost for callers that need all three.
func LoadStreamSchemaContext(schemaRoot string, groups []string) (*StreamSchemaContext, error) {
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	out := &StreamSchemaContext{
		Streams: make(map[string][]*databasev1.Stream, len(groups)),
		Rules:   map[string]map[string]*databasev1.IndexRule{},
	}
	if err := WalkDocs(schemaRoot, func(d Doc) error {
		switch d.KindName {
		case schema.KindStream.String():
			if len(wanted) > 0 && !wanted[d.Group] {
				return nil
			}
			if d.SourceJSON == "" {
				return fmt.Errorf("schema property %q missing source tag", d.PropID)
			}
			var st databasev1.Stream
			if uErr := protojson.Unmarshal([]byte(d.SourceJSON), &st); uErr != nil {
				return fmt.Errorf("unmarshal stream %q source: %w", d.PropID, uErr)
			}
			out.Streams[d.Group] = append(out.Streams[d.Group], &st)
		case schema.KindIndexRule.String():
			if len(wanted) > 0 && !wanted[d.Group] {
				return nil
			}
			if d.SourceJSON == "" {
				return nil
			}
			var r databasev1.IndexRule
			if uErr := protojson.Unmarshal([]byte(d.SourceJSON), &r); uErr != nil {
				return fmt.Errorf("unmarshal index rule %q: %w", d.PropID, uErr)
			}
			byName := out.Rules[d.Group]
			if byName == nil {
				byName = map[string]*databasev1.IndexRule{}
				out.Rules[d.Group] = byName
			}
			byName[r.GetMetadata().GetName()] = &r
		case schema.KindIndexRuleBinding.String():
			if len(wanted) > 0 && !wanted[d.Group] {
				return nil
			}
			if d.SourceJSON == "" {
				return nil
			}
			var b databasev1.IndexRuleBinding
			if uErr := protojson.Unmarshal([]byte(d.SourceJSON), &b); uErr != nil {
				return fmt.Errorf("unmarshal index rule binding %q: %w", d.PropID, uErr)
			}
			if b.GetSubject().GetCatalog() != commonv1.Catalog_CATALOG_STREAM {
				return nil
			}
			out.Bindings = append(out.Bindings, GroupedBinding{Group: d.Group, Binding: &b})
		}
		return nil
	}, schema.KindStream, schema.KindIndexRule, schema.KindIndexRuleBinding); err != nil {
		return nil, err
	}
	return out, nil
}
