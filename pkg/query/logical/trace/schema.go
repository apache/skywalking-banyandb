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

// Package trace implements execution operations for querying trace data.
package trace

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// BuildSchema returns Schema loaded from the metadata repository.
func BuildSchema(tr *databasev1.Trace, indexRules []*databasev1.IndexRule) (logical.Schema, error) {
	// Derive entity list from index rules (all tags except the last one)
	var entityList []string
	if len(indexRules) > 0 && len(indexRules[0].GetTags()) > 1 {
		entityList = indexRules[0].GetTags()[:len(indexRules[0].GetTags())-1]
	}

	s := &schema{
		common: &logical.CommonSchema{
			IndexRules: indexRules,
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: entityList,
		},
		trace: tr,
	}

	// Register trace tags directly (no tag families)
	for i, tag := range tr.GetTags() {
		// Convert TraceTagSpec to TagSpec for compatibility
		dbTagSpec := &databasev1.TagSpec{
			Name: tag.GetName(),
			Type: tag.GetType(),
		}
		tagSpec := &logical.TagSpec{
			Spec:         dbTagSpec,
			TagFamilyIdx: 0, // Trace has no tag families
			TagIdx:       i,
		}
		s.common.TagSpecMap[tag.GetName()] = tagSpec
	}

	return s, nil
}

var _ logical.Schema = (*schema)(nil)

type schema struct {
	trace    *databasev1.Trace
	common   *logical.CommonSchema
	children []logical.Schema
}

func (s *schema) FindTagSpecByName(name string) *logical.TagSpec {
	return s.common.FindTagSpecByName(name)
}

func (s *schema) CreateFieldRef(_ ...*logical.Field) ([]*logical.FieldRef, error) {
	panic("no field for trace")
}

func (s *schema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	panic("trace does not support index rule")
}

func (s *schema) EntityList() []string {
	return s.common.EntityList
}

// IndexDefined checks whether the field given is indexed.
// For trace, we check if tagName matches the last tag in any index rule.
func (s *schema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	for _, rule := range s.common.IndexRules {
		tags := rule.GetTags()
		if len(tags) > 0 && tags[len(tags)-1] == tagName {
			return true, rule
		}
	}
	return false, nil
}

// CreateTagRef create TagRef to the given tags.
// The uniqueness of the tag names can be guaranteed.
func (s *schema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return s.common.CreateRef(tags...)
}

// ProjTags creates a projection view from the present traceSchema
// with a given list of projections.
func (s *schema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &schema{
		trace:  s.trace,
		common: s.common.ProjTags(refs...),
	}
	return newSchema
}

func (s *schema) ProjFields(...*logical.FieldRef) logical.Schema {
	panic("trace does not support field")
}

func (s *schema) Children() []logical.Schema {
	return s.children
}

func mergeSchema(schemas []logical.Schema) (logical.Schema, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	if len(schemas) == 1 {
		return schemas[0], nil
	}
	var commonSchemas []*logical.CommonSchema
	for _, sm := range schemas {
		if sm == nil {
			continue
		}
		s := sm.(*schema)
		if s == nil {
			continue
		}
		commonSchemas = append(commonSchemas, s.common)
	}
	merged, err := logical.MergeSchemas(commonSchemas)
	if err != nil {
		return nil, err
	}
	ret := &schema{
		common:   merged,
		children: schemas,
	}
	return ret, nil
}
