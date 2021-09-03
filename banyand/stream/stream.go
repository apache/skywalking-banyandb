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

	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type tagIndex struct {
	family int
	tag    int
}

type indexRule struct {
	rule       *databasev2.IndexRule
	tagIndices []tagIndex
}
type stream struct {
	name           string
	group          string
	l              *logger.Logger
	schema         *databasev2.Stream
	db             tsdb.Database
	entityIndex    []tagIndex
	indexRules     []*databasev2.IndexRule
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
			s.entityIndex = append(s.entityIndex, tagIndex{family: fIndex, tag: tIndex})
		}
	}
	for _, rule := range s.indexRules {
		tagIndices := make([]tagIndex, 0, len(rule.GetTags()))
		for _, tagInIndex := range rule.GetTags() {
			fIndex, tIndex, tag := s.findTagByName(tagInIndex)
			if tag != nil {
				tagIndices = append(tagIndices, tagIndex{family: fIndex, tag: tIndex})
			}
		}
		s.indexRuleIndex = append(s.indexRuleIndex, indexRule{rule: rule, tagIndices: tagIndices})
	}
}

func (s *stream) findTagByName(tagName string) (int, int, *databasev2.TagSpec) {
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
	schema     *databasev2.Stream
	indexRules []*databasev2.IndexRule
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
			Location: root,
			ShardNum: sm.schema.GetShardNum(),
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

func tagValueTypeConv(tag *modelv2.Tag) (tagType databasev2.TagType, isNull bool) {
	switch tag.GetValueType().(type) {
	case *modelv2.Tag_Int:
		return databasev2.TagType_TAG_TYPE_INT, false
	case *modelv2.Tag_Str:
		return databasev2.TagType_TAG_TYPE_STRING, false
	case *modelv2.Tag_IntArray:
		return databasev2.TagType_TAG_TYPE_INT_ARRAY, false
	case *modelv2.Tag_StrArray:
		return databasev2.TagType_TAG_TYPE_STRING_ARRAY, false
	case *modelv2.Tag_BinaryData:
		return databasev2.TagType_TAG_TYPE_DATA_BINARY, false
	case *modelv2.Tag_Null:
		return databasev2.TagType_TAG_TYPE_UNSPECIFIED, true
	}
	return databasev2.TagType_TAG_TYPE_UNSPECIFIED, false
}
