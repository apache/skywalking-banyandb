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

package stream

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/reader"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// streamSchemaInfo is the subset of a stream's schema the migration copy cares about.
type streamSchemaInfo struct {
	Group       string
	Name        string
	TagFamilies []streamTagFamilyInfo
}

type streamTagFamilyInfo struct {
	Name string
	Tags []string
}

// buildStreamSchemas projects every stream schema of the given groups into
// the migration's lookup shape. Returns (group -> stream-name -> schema).
func buildStreamSchemas(streams map[string][]*databasev1.Stream, groups []string) map[string]map[string]*streamSchemaInfo {
	out := make(map[string]map[string]*streamSchemaInfo, len(groups))
	for _, group := range groups {
		byName := make(map[string]*streamSchemaInfo, len(streams[group]))
		for _, s := range streams[group] {
			info := streamSchemaInfoFromProto(group, s)
			byName[info.Name] = info
		}
		out[group] = byName
	}
	return out
}

func streamSchemaInfoFromProto(group string, s *databasev1.Stream) *streamSchemaInfo {
	si := &streamSchemaInfo{
		Group: group,
		Name:  s.GetMetadata().GetName(),
	}
	for _, tf := range s.GetTagFamilies() {
		tags := make([]string, 0, len(tf.GetTags()))
		for _, t := range tf.GetTags() {
			tags = append(tags, t.GetName())
		}
		si.TagFamilies = append(si.TagFamilies, streamTagFamilyInfo{Name: tf.GetName(), Tags: tags})
	}
	return si
}

// streamIndexLocator bundles everything the element-index rebuild needs for one
// stream: the parsed locators (entity set + per-family tag rules) plus the flat
// list of bound INVERTED index rules. It is built offline from schema-property.
type streamIndexLocator struct {
	Locators   partition.IndexRuleLocator
	IndexRules []*databasev1.IndexRule
	Entity     *databasev1.Entity
	Families   []*databasev1.TagFamilySpec
}

// HasEntityIndexRule reports whether any index rule references an entity tag, so
// the slow-path rebuild knows it must resolve seriesID -> EntityValues.
func (l *streamIndexLocator) HasEntityIndexRule() bool {
	for _, ir := range l.IndexRules {
		for _, tagName := range ir.GetTags() {
			if _, ok := l.Locators.EntitySet[tagName]; ok {
				return true
			}
		}
	}
	return false
}

// buildStreamBoundRules builds the (group -> stream -> rule-name-set) index.
// The reader already scopes bindings to the wanted groups and to
// CATALOG_STREAM subjects.
func buildStreamBoundRules(bindings []reader.GroupedBinding) map[string]map[string]map[string]struct{} {
	out := map[string]map[string]map[string]struct{}{}
	for _, b := range bindings {
		subject := b.Binding.GetSubject()
		byStream := out[b.Group]
		if byStream == nil {
			byStream = map[string]map[string]struct{}{}
			out[b.Group] = byStream
		}
		set := byStream[subject.GetName()]
		if set == nil {
			set = map[string]struct{}{}
			byStream[subject.GetName()] = set
		}
		for _, ruleName := range b.Binding.GetRules() {
			set[ruleName] = struct{}{}
		}
	}
	return out
}

// buildStreamIndexLocators builds, for each requested group, one
// streamIndexLocator per stream from the stream protos plus the index rules
// bound to each stream (via IndexRuleBinding whose
// Subject.Catalog==CATALOG_STREAM). Returns (group -> stream-name -> locator).
// This is the offline analog of the indexSchema the write path keeps in memory.
func buildStreamIndexLocators(sc *reader.StreamSchemaContext, groups []string) map[string]map[string]*streamIndexLocator {
	rulesByGroupName := sc.Rules
	boundRules := buildStreamBoundRules(sc.Bindings)

	out := make(map[string]map[string]*streamIndexLocator, len(groups))
	for _, group := range groups {
		out[group] = map[string]*streamIndexLocator{}
		for _, s := range sc.Streams[group] {
			streamName := s.GetMetadata().GetName()
			var indexRules []*databasev1.IndexRule
			if byStream := boundRules[group]; byStream != nil {
				if ruleNames := byStream[streamName]; ruleNames != nil {
					byName := rulesByGroupName[group]
					for ruleName := range ruleNames {
						if r, ok := byName[ruleName]; ok {
							indexRules = append(indexRules, r)
						}
					}
				}
			}
			locators, _ := partition.ParseIndexRuleLocators(s.GetEntity(), s.GetTagFamilies(), indexRules, false)
			out[group][streamName] = &streamIndexLocator{
				Locators:   locators,
				IndexRules: indexRules,
				Entity:     s.GetEntity(),
				Families:   s.GetTagFamilies(),
			}
		}
	}
	return out
}
