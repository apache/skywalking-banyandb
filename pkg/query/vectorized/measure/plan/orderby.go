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

package plan

import (
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// resolvedOrderByTag identifies the (family, tag) that an OrderBy IndexRule
// resolves to on a given measure schema. It is the input to both the
// hidden-projection augmentation (when the tag is not in the visible
// projection) and the merge-time sort-column lookup.
type resolvedOrderByTag struct {
	family string
	tag    string
}

// resolveOrderByTag walks the supplied index rules for an entry whose name
// matches indexRuleName, then looks up the single tag the rule indexes on
// the measure schema. Error text matches the row-path distributed resolver
// in pkg/query/logical/measure/cross_group_merge.go byte-for-byte so
// distributed fixtures with WantErr land on identical operator-facing
// messages across paths. It deliberately diverges from
// pkg/query/logical.ParseOrderBy (which wraps logical.ErrIndexNotDefined
// / errIndexSortingUnsupported) — that surface is for the non-distributed
// query analyzer and is not reachable from this code path.
//
// Errors:
//   - indexRuleName not present in indexRules: "index rule %s not found"
//   - rule.NoSort: "index rule %s does not support sorting"
//   - rule indexes more than one tag: "index rule %s should have one tag"
//   - tag not present in any tag family of the measure schema:
//     "tag %s not found"
func resolveOrderByTag(measureSchema *databasev1.Measure, indexRules []*databasev1.IndexRule, indexRuleName string) (resolvedOrderByTag, error) {
	if indexRuleName == "" {
		return resolvedOrderByTag{}, fmt.Errorf("index rule name is empty")
	}
	var indexRule *databasev1.IndexRule
	for _, rule := range indexRules {
		if rule.GetMetadata().GetName() == indexRuleName {
			indexRule = rule
			break
		}
	}
	if indexRule == nil {
		return resolvedOrderByTag{}, fmt.Errorf("index rule %s not found", indexRuleName)
	}
	if indexRule.GetNoSort() {
		return resolvedOrderByTag{}, fmt.Errorf("index rule %s does not support sorting", indexRuleName)
	}
	if len(indexRule.GetTags()) != 1 {
		return resolvedOrderByTag{}, fmt.Errorf("index rule %s should have one tag", indexRuleName)
	}
	tagName := indexRule.GetTags()[0]
	for _, tf := range measureSchema.GetTagFamilies() {
		for _, tagSpec := range tf.GetTags() {
			if tagSpec.GetName() == tagName {
				return resolvedOrderByTag{family: tf.GetName(), tag: tagName}, nil
			}
		}
	}
	return resolvedOrderByTag{}, fmt.Errorf("tag %s not found", tagName)
}
