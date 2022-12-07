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

package partition

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IndexRuleLocator combines several TagLocators that help find the index value.
type IndexRuleLocator struct {
	Rule       *databasev1.IndexRule
	TagIndices []TagLocator
}

// ParseIndexRuleLocators returns a IndexRuleLocator based on the tag family spec and index rules.
func ParseIndexRuleLocators(families []*databasev1.TagFamilySpec, indexRules []*databasev1.IndexRule) (locators []*IndexRuleLocator) {
	for _, rule := range indexRules {
		tagIndices := make([]TagLocator, 0, len(rule.GetTags()))
		for _, tagInIndex := range rule.GetTags() {
			fIndex, tIndex, tag := pbv1.FindTagByName(families, tagInIndex)
			if tag != nil {
				tagIndices = append(tagIndices, TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
			}
		}
		if len(tagIndices) > 0 {
			locators = append(locators, &IndexRuleLocator{Rule: rule, TagIndices: tagIndices})
		}
	}
	return locators
}
