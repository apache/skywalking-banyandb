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

package main

import (
	"fmt"
	"slices"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// exportedSchema is every schema/<kind>.jsonl of an artifact.
type exportedSchema struct {
	groups   []*commonv1.Group
	streams  []*databasev1.Stream
	measures []*databasev1.Measure
	traces   []*databasev1.Trace
	props    []*databasev1.Property
	rules    []*databasev1.IndexRule
	bindings []*databasev1.IndexRuleBinding
	topNs    []*databasev1.TopNAggregation
}

func readExportedSchema(dir string) (*exportedSchema, error) {
	s := &exportedSchema{}
	var err error
	if s.groups, err = readSchema(dir, schemasync.KindGroup, func() *commonv1.Group { return &commonv1.Group{} }); err != nil {
		return nil, err
	}
	if s.streams, err = readSchema(dir, schemasync.KindStream, func() *databasev1.Stream { return &databasev1.Stream{} }); err != nil {
		return nil, err
	}
	if s.measures, err = readSchema(dir, schemasync.KindMeasure, func() *databasev1.Measure { return &databasev1.Measure{} }); err != nil {
		return nil, err
	}
	if s.traces, err = readSchema(dir, schemasync.KindTrace, func() *databasev1.Trace { return &databasev1.Trace{} }); err != nil {
		return nil, err
	}
	if s.props, err = readSchema(dir, schemasync.KindProperty, func() *databasev1.Property { return &databasev1.Property{} }); err != nil {
		return nil, err
	}
	if s.rules, err = readSchema(dir, schemasync.KindIndexRule, func() *databasev1.IndexRule { return &databasev1.IndexRule{} }); err != nil {
		return nil, err
	}
	if s.bindings, err = readSchema(dir, schemasync.KindIndexRuleBinding, func() *databasev1.IndexRuleBinding { return &databasev1.IndexRuleBinding{} }); err != nil {
		return nil, err
	}
	if s.topNs, err = readSchema(dir, schemasync.KindTopNAggregation, func() *databasev1.TopNAggregation { return &databasev1.TopNAggregation{} }); err != nil {
		return nil, err
	}
	return s, nil
}

// knownResource reports whether a resource is one whose rows are printed under data, so its
// schema is printed too.
func knownResource(group, name string) bool {
	switch group {
	case "sw_records":
		return name == "event"
	case "sw_metadata":
		return name == "service_traffic_minute"
	case "sw_metricsMinute", "sw_metricsHour", "sw_metricsDay":
		return strings.HasPrefix(name, "service_cpm_") || strings.HasPrefix(name, "service_resp_time_")
	case "sw_trace":
		return name == "segment"
	case "sw_property":
		return name == uiTemplate
	default:
		return false
	}
}

// checkSchema prints the exported schema under schema: the entries per kind, every group with
// its resource options and the known resources with their tags, fields and index rules. It
// returns the problems of the references between the entries: every resource, index rule,
// binding and TopN aggregation belongs to an exported group of its catalog, every binding names
// an exported subject and index rules, and every TopN aggregation an exported source measure.
func checkSchema(dir string) []string {
	s, err := readExportedSchema(dir)
	if err != nil {
		fmt.Println("schema: {}")
		return []string{err.Error()}
	}
	fmt.Println("schema:")
	fmt.Println("  counts:")
	counts := []int{
		len(s.groups), len(s.streams), len(s.measures), len(s.traces), len(s.props),
		len(s.rules), len(s.bindings), len(s.topNs),
	}
	for i, kind := range schemasync.Kinds {
		fmt.Printf("    %s: %d\n", kind, counts[i])
	}
	s.printGroups()
	s.printResources()
	return s.refProblems()
}

func (s *exportedSchema) printGroups() {
	fmt.Println("  groups:")
	for _, g := range slices.SortedFunc(slices.Values(s.groups), func(a, b *commonv1.Group) int {
		return strings.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	}) {
		o := g.GetResourceOpts()
		fmt.Printf("    - name: %s\n      catalog: %s\n", g.GetMetadata().GetName(), transfer.CatalogName(g.GetCatalog()))
		if o == nil {
			continue
		}
		fmt.Printf("      shardNum: %d\n      replicas: %d\n      segmentInterval: %s\n      ttl: %s\n",
			o.GetShardNum(), o.GetReplicas(), interval(o.GetSegmentInterval()), interval(o.GetTtl()))
		fmt.Printf("      defaultStages: %s\n", yamlStrings(o.GetDefaultStages()))
		if len(o.GetStages()) == 0 {
			fmt.Println("      stages: []")
			continue
		}
		fmt.Println("      stages:")
		for _, st := range o.GetStages() {
			fmt.Printf("        - name: %s\n          shardNum: %d\n          replicas: %d\n          segmentInterval: %s\n          ttl: %s\n",
				st.GetName(), st.GetShardNum(), st.GetReplicas(), interval(st.GetSegmentInterval()), interval(st.GetTtl()))
			fmt.Printf("          nodeSelector: %q\n          close: %t\n", st.GetNodeSelector(), st.GetClose())
		}
	}
}

func (s *exportedSchema) printResources() {
	var out []string
	add := func(kind string, m *commonv1.Metadata, body string) {
		if !knownResource(m.GetGroup(), m.GetName()) {
			return
		}
		item := fmt.Sprintf("    - kind: %s\n      group: %s\n      name: %s\n%s", kind, m.GetGroup(), m.GetName(), body)
		out = append(out, item+fmt.Sprintf("      indexRules: %s\n", yamlStrings(s.boundRules(m))))
	}
	for _, x := range s.streams {
		add(schemasync.KindStream, x.GetMetadata(), fmt.Sprintf("      entity: %s\n      tags: %s\n",
			yamlStrings(x.GetEntity().GetTagNames()), yamlStrings(familyTags(x.GetTagFamilies()))))
	}
	for _, x := range s.measures {
		var fields []string
		for _, f := range x.GetFields() {
			fields = append(fields, f.GetName()+":"+strings.TrimPrefix(f.GetFieldType().String(), "FIELD_TYPE_"))
		}
		add(schemasync.KindMeasure, x.GetMetadata(), fmt.Sprintf("      entity: %s\n      interval: %s\n      indexMode: %t\n      tags: %s\n      fields: %s\n",
			yamlStrings(x.GetEntity().GetTagNames()), x.GetInterval(), x.GetIndexMode(), yamlStrings(familyTags(x.GetTagFamilies())), yamlStrings(fields)))
	}
	for _, x := range s.traces {
		add(schemasync.KindTrace, x.GetMetadata(), fmt.Sprintf("      traceIdTag: %s\n      spanIdTag: %s\n      timestampTag: %s\n      tags: %s\n",
			x.GetTraceIdTagName(), x.GetSpanIdTagName(), x.GetTimestampTagName(), yamlStrings(tagSpecs(x.GetTags()))))
	}
	for _, x := range s.props {
		add(schemasync.KindProperty, x.GetMetadata(), fmt.Sprintf("      tags: %s\n", yamlStrings(tagSpecs(x.GetTags()))))
	}
	if len(out) == 0 {
		fmt.Println("  resources: []")
		return
	}
	fmt.Println("  resources:")
	slices.Sort(out)
	for _, item := range out {
		fmt.Print(item)
	}
}

// boundRules returns the sorted index rules the bindings of a resource name.
func (s *exportedSchema) boundRules(m *commonv1.Metadata) []string {
	var out []string
	for _, b := range s.bindings {
		if b.GetMetadata().GetGroup() == m.GetGroup() && b.GetSubject().GetName() == m.GetName() {
			out = append(out, b.GetRules()...)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (s *exportedSchema) refProblems() []string {
	catalogs := map[string]commonv1.Catalog{}
	for _, g := range s.groups {
		catalogs[g.GetMetadata().GetName()] = g.GetCatalog()
	}
	var problems []string
	resources := map[string]bool{}
	member := func(kind string, m *commonv1.Metadata, catalog commonv1.Catalog) {
		got, ok := catalogs[m.GetGroup()]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s %s/%s: group not exported", kind, m.GetGroup(), m.GetName()))
		case catalog != commonv1.Catalog_CATALOG_UNSPECIFIED && got != catalog:
			problems = append(problems, fmt.Sprintf("%s %s/%s: group of catalog %s", kind, m.GetGroup(), m.GetName(), transfer.CatalogName(got)))
		}
	}
	for _, x := range s.streams {
		member(schemasync.KindStream, x.GetMetadata(), commonv1.Catalog_CATALOG_STREAM)
		resources[resourceKey(commonv1.Catalog_CATALOG_STREAM, x.GetMetadata().GetGroup(), x.GetMetadata().GetName())] = true
	}
	for _, x := range s.measures {
		member(schemasync.KindMeasure, x.GetMetadata(), commonv1.Catalog_CATALOG_MEASURE)
		resources[resourceKey(commonv1.Catalog_CATALOG_MEASURE, x.GetMetadata().GetGroup(), x.GetMetadata().GetName())] = true
	}
	for _, x := range s.traces {
		member(schemasync.KindTrace, x.GetMetadata(), commonv1.Catalog_CATALOG_TRACE)
		resources[resourceKey(commonv1.Catalog_CATALOG_TRACE, x.GetMetadata().GetGroup(), x.GetMetadata().GetName())] = true
	}
	for _, x := range s.props {
		member(schemasync.KindProperty, x.GetMetadata(), commonv1.Catalog_CATALOG_PROPERTY)
		resources[resourceKey(commonv1.Catalog_CATALOG_PROPERTY, x.GetMetadata().GetGroup(), x.GetMetadata().GetName())] = true
	}
	rules := map[string]bool{}
	for _, x := range s.rules {
		member(schemasync.KindIndexRule, x.GetMetadata(), commonv1.Catalog_CATALOG_UNSPECIFIED)
		rules[x.GetMetadata().GetGroup()+"/"+x.GetMetadata().GetName()] = true
	}
	for _, b := range s.bindings {
		m := b.GetMetadata()
		member(schemasync.KindIndexRuleBinding, m, commonv1.Catalog_CATALOG_UNSPECIFIED)
		if !resources[resourceKey(b.GetSubject().GetCatalog(), m.GetGroup(), b.GetSubject().GetName())] {
			problems = append(problems, fmt.Sprintf("indexRuleBinding %s/%s: subject %s %s not exported",
				m.GetGroup(), m.GetName(), transfer.CatalogName(b.GetSubject().GetCatalog()), b.GetSubject().GetName()))
		}
		for _, r := range b.GetRules() {
			if !rules[m.GetGroup()+"/"+r] {
				problems = append(problems, fmt.Sprintf("indexRuleBinding %s/%s: index rule %s not exported", m.GetGroup(), m.GetName(), r))
			}
		}
	}
	for _, t := range s.topNs {
		m, src := t.GetMetadata(), t.GetSourceMeasure()
		member(schemasync.KindTopNAggregation, m, commonv1.Catalog_CATALOG_MEASURE)
		if !resources[resourceKey(commonv1.Catalog_CATALOG_MEASURE, src.GetGroup(), src.GetName())] {
			problems = append(problems, fmt.Sprintf("topNAggregation %s/%s: source measure %s/%s not exported", m.GetGroup(), m.GetName(), src.GetGroup(), src.GetName()))
		}
	}
	return problems
}

func resourceKey(catalog commonv1.Catalog, group, name string) string {
	return fmt.Sprintf("%d/%s/%s", catalog, group, name)
}

// interval prints an interval rule as its number and unit, such as 7d, or none.
func interval(r *commonv1.IntervalRule) string {
	switch r.GetUnit() {
	case commonv1.IntervalRule_UNIT_UNSPECIFIED:
		return "none"
	case commonv1.IntervalRule_UNIT_DAY:
		return fmt.Sprintf("%dd", r.GetNum())
	case commonv1.IntervalRule_UNIT_HOUR:
		return fmt.Sprintf("%dh", r.GetNum())
	default:
		return fmt.Sprintf("%d %s", r.GetNum(), r.GetUnit())
	}
}

// familyTags names every tag of the families as family.tag:TYPE.
func familyTags(families []*databasev1.TagFamilySpec) []string {
	var out []string
	for _, f := range families {
		for _, t := range tagSpecs(f.GetTags()) {
			out = append(out, f.GetName()+"."+t)
		}
	}
	return out
}

// tagSpec is a stream, measure, property or trace tag.
type tagSpec interface {
	GetName() string
	GetType() databasev1.TagType
}

// tagSpecs names every tag as tag:TYPE.
func tagSpecs[T tagSpec](tags []T) []string {
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		out = append(out, t.GetName()+":"+strings.TrimPrefix(t.GetType().String(), "TAG_TYPE_"))
	}
	return out
}

// yamlStrings prints a flow list of quoted strings.
func yamlStrings(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
