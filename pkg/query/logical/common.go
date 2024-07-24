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
	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var (
	// ErrUnsupportedConditionOp indicates an unsupported condition operation.
	ErrUnsupportedConditionOp = errors.New("unsupported condition operation")
	// ErrUnsupportedConditionValue indicates an unsupported condition value type.
	ErrUnsupportedConditionValue = errors.New("unsupported condition value type")
	// ErrInvalidCriteriaType indicates an invalid criteria type.
	ErrInvalidCriteriaType     = errors.New("invalid criteria type")
	errTagNotDefined           = errors.New("tag is not defined")
	errIndexNotDefined         = errors.New("index is not define for the tag")
	errIndexSortingUnsupported = errors.New("index does not support sorting")
)

// Tag represents the combination of  tag family and tag name.
// It's a tag's identity.
type Tag struct {
	familyName, name string
}

// NewTag return a new Tag.
func NewTag(family, name string) *Tag {
	return &Tag{
		familyName: family,
		name:       name,
	}
}

// NewTags create an array of Tag within a TagFamily.
func NewTags(family string, tagNames ...string) []*Tag {
	tags := make([]*Tag, len(tagNames))
	for i, name := range tagNames {
		tags[i] = NewTag(family, name)
	}
	return tags
}

// GetCompoundName is only used for error message.
func (t *Tag) GetCompoundName() string {
	return t.familyName + ":" + t.name
}

// GetTagName returns the tag name.
func (t *Tag) GetTagName() string {
	return t.name
}

// GetFamilyName returns the tag family name.
func (t *Tag) GetFamilyName() string {
	return t.familyName
}

// ToTags converts a projection spec to Tag sets.
func ToTags(projection *modelv1.TagProjection) [][]*Tag {
	projTags := make([][]*Tag, len(projection.GetTagFamilies()))
	for i, tagFamily := range projection.GetTagFamilies() {
		var projTagInFamily []*Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}
	return projTags
}

// Field identity a field in a measure.
type Field struct {
	Name string
}

// NewField return a new Field.
func NewField(name string) *Field {
	return &Field{Name: name}
}

// StringSlicesEqual reports whether a and b are the same length and contain the same strings.
// A nil argument is equivalent to an empty slice.
func StringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
