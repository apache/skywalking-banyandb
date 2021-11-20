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
	"context"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// a chunk is 1MB
const chunkSize = 1 << 20

type indexRule struct {
	rule       *databasev1.IndexRule
	tagIndices []partition.TagLocator
}

type stream struct {
	name           string
	group          string
	l              *logger.Logger
	schema         *databasev1.Stream
	db             tsdb.Database
	entityLocator  partition.EntityLocator
	indexRules     []*databasev1.IndexRule
	indexRuleIndex []indexRule

	indexCh chan indexMessage
}

func (s *stream) Close() error {
	close(s.indexCh)
	return s.db.Close()
}

func (s *stream) parseSchema() {
	sm := s.schema
	meta := sm.GetMetadata()
	s.name, s.group = meta.GetName(), meta.GetGroup()
	for _, tagInEntity := range sm.Entity.GetTagNames() {
		fIndex, tIndex, tag := s.findTagByName(tagInEntity)
		if tag != nil {
			s.entityLocator = append(s.entityLocator, partition.TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
		}
	}
	for _, rule := range s.indexRules {
		tagIndices := make([]partition.TagLocator, 0, len(rule.GetTags()))
		for _, tagInIndex := range rule.GetTags() {
			fIndex, tIndex, tag := s.findTagByName(tagInIndex)
			if tag != nil {
				tagIndices = append(tagIndices, partition.TagLocator{FamilyOffset: fIndex, TagOffset: tIndex})
			}
		}
		s.indexRuleIndex = append(s.indexRuleIndex, indexRule{rule: rule, tagIndices: tagIndices})
	}
}

func (s *stream) findTagByName(tagName string) (int, int, *databasev1.TagSpec) {
	for fi, family := range s.schema.GetTagFamilies() {
		for ti, tag := range family.Tags {
			if tagName == tag.GetName() {
				return fi, ti, tag
			}
		}
	}
	return 0, 0, nil
}

type streamSpec struct {
	schema     *databasev1.Stream
	indexRules []*databasev1.IndexRule
}

func openStream(root string, spec streamSpec, l *logger.Logger) (*stream, error) {
	sm := &stream{
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
		indexCh:    make(chan indexMessage),
	}
	sm.parseSchema()
	db, err := tsdb.OpenDatabase(
		context.WithValue(context.Background(), logger.ContextKey, l),
		tsdb.DatabaseOpts{
			Location:   root,
			ShardNum:   sm.schema.GetOpts().GetShardNum(),
			IndexRules: spec.indexRules,
			EncodingMethod: tsdb.EncodingMethod{
				EncoderPool: encoding.NewPlainEncoderPool(chunkSize),
				DecoderPool: encoding.NewPlainDecoderPool(chunkSize),
			},
		})
	if err != nil {
		return nil, err
	}
	sm.db = db
	sm.bootIndexGenerator()
	return sm, nil
}

func formatStreamID(name, group string) string {
	return name + ":" + group
}

func tagValueTypeConv(tagValue *modelv1.TagValue) (tagType databasev1.TagType, isNull bool) {
	switch tagValue.GetValue().(type) {
	case *modelv1.TagValue_Int:
		return databasev1.TagType_TAG_TYPE_INT, false
	case *modelv1.TagValue_Str:
		return databasev1.TagType_TAG_TYPE_STRING, false
	case *modelv1.TagValue_IntArray:
		return databasev1.TagType_TAG_TYPE_INT_ARRAY, false
	case *modelv1.TagValue_StrArray:
		return databasev1.TagType_TAG_TYPE_STRING_ARRAY, false
	case *modelv1.TagValue_BinaryData:
		return databasev1.TagType_TAG_TYPE_DATA_BINARY, false
	case *modelv1.TagValue_Null:
		return databasev1.TagType_TAG_TYPE_UNSPECIFIED, true
	}
	return databasev1.TagType_TAG_TYPE_UNSPECIFIED, false
}
