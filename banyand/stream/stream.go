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

type stream struct {
	name        string
	group       string
	l           *logger.Logger
	schema      *databasev2.Stream
	db          tsdb.Database
	entityIndex []struct {
		family int
		tag    int
	}
}

func (s *stream) Close() error {
	return s.db.Close()
}

func (s *stream) parseSchema() {
	sm := s.schema
	meta := sm.GetMetadata()
	s.name, s.group = meta.GetName(), meta.GetGroup()
	for _, tagInEntity := range sm.Entity.GetTagNames() {
	nextEntityTag:
		for fi, family := range sm.GetTagFamilies() {
			for ti, tag := range family.Tags {
				if tagInEntity == tag.GetName() {
					s.entityIndex = append(s.entityIndex, struct {
						family int
						tag    int
					}{family: fi, tag: ti})
					break nextEntityTag
				}
			}
		}
	}
}

func openStream(root string, schema *databasev2.Stream, l *logger.Logger) (*stream, error) {
	sm := &stream{
		schema: schema,
		l:      l,
	}
	sm.parseSchema()
	db, err := tsdb.OpenDatabase(
		context.WithValue(context.Background(), logger.ContextKey, l),
		tsdb.DatabaseOpts{
			Location: root,
			ShardNum: uint(schema.GetShardNum()),
		})
	if err != nil {
		return nil, err
	}
	sm.db = db
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
