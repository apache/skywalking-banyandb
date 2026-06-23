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

package measure

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/reader"
)

// loadMeasureSchemas reads every measure schema under the given groups from
// the schema-property catalog. Returns (group -> measure-name -> schema).
func loadMeasureSchemas(schemaRoot string, groups []string) (map[string]map[string]*measureSchemaInfo, error) {
	measures, err := reader.LoadMeasures(schemaRoot, groups)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]*measureSchemaInfo, len(groups))
	for _, group := range groups {
		byName := make(map[string]*measureSchemaInfo, len(measures[group]))
		for _, m := range measures[group] {
			info := measureSchemaInfoFromProto(group, m)
			byName[info.Name] = info
		}
		out[group] = byName
	}
	return out, nil
}

// measureSchemaInfoFromProto builds the lookup-table view of a measure
// schema directly from its proto definition.
func measureSchemaInfoFromProto(group string, m *databasev1.Measure) *measureSchemaInfo {
	si := &measureSchemaInfo{
		Group:          group,
		Name:           m.GetMetadata().GetName(),
		IndexMode:      m.GetIndexMode(),
		EntityTagNames: m.GetEntity().GetTagNames(),
		TagType:        make(map[string]databasev1.TagType),
	}
	for _, tf := range m.GetTagFamilies() {
		tags := make([]string, 0, len(tf.GetTags()))
		for _, t := range tf.GetTags() {
			tags = append(tags, t.GetName())
			si.TagType[t.GetName()] = t.GetType()
		}
		si.TagFamilies = append(si.TagFamilies, tagFamilyInfo{Name: tf.GetName(), Tags: tags})
	}
	return si
}

// loadIndexRuleInfoByID returns IndexRuleID -> {Analyzer, NoSort} across the
// given groups. The slow-path rebuild needs it because FieldKey.Marshal only
// encodes TagName or IndexRuleID and drops Analyzer/NoSort.
func loadIndexRuleInfoByID(schemaRoot string, groups []string) (map[uint32]indexRuleInfo, error) {
	rules, err := reader.LoadIndexRules(schemaRoot, groups)
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]indexRuleInfo)
	for _, byGroup := range rules {
		for _, r := range byGroup {
			out[r.GetMetadata().GetId()] = indexRuleInfo{Analyzer: r.GetAnalyzer(), NoSort: r.GetNoSort()}
		}
	}
	return out, nil
}

// measureSchemaInfo is the subset of a measure's schema the migration copy
// cares about: enough to drive tag-family projection and reject IndexMode
// groups (their field values live inside sidx, not in part data, and
// broadcasting a union sidx would break query correctness).
type measureSchemaInfo struct {
	TagType           map[string]databasev1.TagType
	IndexedEntityTags map[string]struct{}
	Group             string
	Name              string
	TagFamilies       []tagFamilyInfo
	EntityTagNames    []string
	IndexMode         bool
}

type tagFamilyInfo struct {
	Name string
	Tags []string
}

// indexRuleInfo is the minimal index-rule view the slow-path rebuild needs: it
// restores the Analyzer and NoSort flags that FieldKey.Marshal does not encode.
type indexRuleInfo struct {
	Analyzer string
	NoSort   bool
}
