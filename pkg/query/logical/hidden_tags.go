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

package logical

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// HiddenTagSet is a set of tag names that should be stripped from query results.
// These are tags that were used in criteria but not requested in the projection.
type HiddenTagSet map[string]struct{}

// NewHiddenTagSet creates a new HiddenTagSet.
func NewHiddenTagSet() HiddenTagSet {
	return make(map[string]struct{})
}

// Add adds a tag name to the hidden set.
func (h HiddenTagSet) Add(tagName string) {
	h[tagName] = struct{}{}
}

// Contains checks if a tag name is in the hidden set.
func (h HiddenTagSet) Contains(tagName string) bool {
	_, ok := h[tagName]
	return ok
}

// IsEmpty returns true if the hidden set has no entries.
func (h HiddenTagSet) IsEmpty() bool {
	return len(h) == 0
}

// StripHiddenTags removes hidden tags from a slice of tag families.
// It modifies the slice in place and returns the filtered result.
// Empty tag families are removed from the result.
func (h HiddenTagSet) StripHiddenTags(tagFamilies []*modelv1.TagFamily) []*modelv1.TagFamily {
	if h.IsEmpty() || len(tagFamilies) == 0 {
		return tagFamilies
	}

	families := tagFamilies[:0]
	for _, tf := range tagFamilies {
		if tf == nil {
			continue
		}
		tags := tf.Tags[:0]
		for _, tag := range tf.Tags {
			if tag == nil {
				continue
			}
			if h.Contains(tag.GetKey()) {
				continue
			}
			tags = append(tags, tag)
		}
		if len(tags) == 0 {
			continue
		}
		tf.Tags = tags
		families = append(families, tf)
	}
	return families
}

// CollectHiddenCriteriaTags identifies tags used in criteria that are not in the projection.
// It returns a HiddenTagSet containing those tag names and an optional slice of Tag
// structures grouped by family for adding to the projection.
//
// Parameters:
//   - criteria: The query criteria containing filter conditions
//   - projectedTagNames: Set of tag names already in the projection
//   - entityDict: Map of entity tag names (these are skipped)
//   - schema: The schema to look up tag family information
//   - tagFamilyGetter: Function to get tag families from the schema
//
// Returns:
//   - HiddenTagSet: Set of hidden tag names
//   - [][]*Tag: Tags grouped by family to add to projection (nil if empty)
func CollectHiddenCriteriaTags(
	criteria *modelv1.Criteria,
	projectedTagNames map[string]struct{},
	entityDict map[string]int,
	schema Schema,
	tagFamilyGetter func() []string,
) (HiddenTagSet, [][]*Tag) {
	hidden := NewHiddenTagSet()
	if criteria == nil {
		return hidden, nil
	}

	// Collect all tag names from criteria
	tagNames := make(map[string]struct{})
	CollectCriteriaTagNames(criteria, tagNames)

	// Group tags by family
	familyTags := make(map[string][]*Tag)
	var familyOrder []string

	tagFamilies := tagFamilyGetter()

	for tagName := range tagNames {
		// Skip if already in projection
		if _, ok := projectedTagNames[tagName]; ok {
			continue
		}
		// Skip entity tags
		if _, isEntity := entityDict[tagName]; isEntity {
			continue
		}

		tagSpec := schema.FindTagSpecByName(tagName)
		if tagSpec == nil {
			continue
		}

		// Get the actual family name from the schema
		if tagSpec.TagFamilyIdx < 0 || tagSpec.TagFamilyIdx >= len(tagFamilies) {
			continue
		}
		familyName := tagFamilies[tagSpec.TagFamilyIdx]

		// Mark as hidden
		hidden.Add(tagName)

		// Group tags by family
		if _, exists := familyTags[familyName]; !exists {
			familyOrder = append(familyOrder, familyName)
			familyTags[familyName] = make([]*Tag, 0)
		}
		familyTags[familyName] = append(familyTags[familyName], NewTag(familyName, tagName))
	}

	// Convert to [][]*Tag maintaining family grouping
	if len(familyOrder) == 0 {
		return hidden, nil
	}
	extras := make([][]*Tag, 0, len(familyOrder))
	for _, family := range familyOrder {
		extras = append(extras, familyTags[family])
	}
	return hidden, extras
}
