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
	"github.com/google/go-cmp/cmp"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ logical.Schema = (*schema)(nil)

type schema struct {
	stream *databasev1.Stream
	common *logical.CommonSchema
}

func (s *schema) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	panic("no field for stream")
}

func (s *schema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return s.common.IndexRuleDefined(indexRuleName)
}

func (s *schema) EntityList() []string {
	return s.common.EntityList
}

// IndexDefined checks whether the field given is indexed
func (s *schema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return s.common.IndexDefined(tagName)
}

func (s *schema) Equal(s2 logical.Schema) bool {
	if other, ok := s2.(*schema); ok {
		return cmp.Equal(other.common.TagMap, s.common.TagMap)
	}
	return false
}

// registerTag registers the tag spec with given tagFamilyName, tagName and indexes.
func (s *schema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	s.common.RegisterTag(tagFamilyIdx, tagIdx, spec)
}

// CreateTagRef create TagRef to the given tags.
// The family name of the tag is actually not used
// since the uniqueness of the tag names can be guaranteed across families.
func (s *schema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return s.common.CreateRef(tags...)
}

// ProjTags creates a projection view from the present streamSchema
// with a given list of projections
func (s *schema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &schema{
		stream: s.stream,
		common: s.common.ProjTags(refs...),
	}
	return newSchema
}

func (s *schema) ProjFields(...*logical.FieldRef) logical.Schema {
	panic("stream does not support field")
}

func (s *schema) ShardNumber() uint32 {
	return s.common.ShardNumber()
}

func (s *schema) Scope() tsdb.Entry {
	return tsdb.Entry(s.stream.Metadata.Name)
}
