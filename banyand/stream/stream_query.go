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
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var (
	ErrTagFamilyNotExist = errors.New("tag family doesn't exist")
)

type Query interface {
	Stream(stream *commonv2.Metadata) (Stream, error)
}

type Stream interface {
	io.Closer
	Write(value *streamv2.ElementValue) error
	Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	Shard(id common.ShardID) (tsdb.Shard, error)
	ParseTagFamily(family string, item tsdb.Item) (*modelv2.TagFamily, error)
	ParseElementID(item tsdb.Item) (string, error)
}

var _ Stream = (*stream)(nil)

func (s *stream) Shards(entity tsdb.Entity) ([]tsdb.Shard, error) {
	if len(entity) < 1 {
		return s.db.Shards(), nil
	}
	for _, e := range entity {
		if e == nil {
			return s.db.Shards(), nil
		}
	}
	shardID, err := partition.ShardID(entity.Marshal(), s.schema.GetShardNum())
	if err != nil {
		return nil, err
	}
	shard, err := s.db.Shard(common.ShardID(shardID))
	if err != nil {
		return nil, err
	}
	return []tsdb.Shard{shard}, nil
}

func (s *stream) Shard(id common.ShardID) (tsdb.Shard, error) {
	return s.db.Shard(id)
}

func (s *stream) ParseTagFamily(family string, item tsdb.Item) (*modelv2.TagFamily, error) {
	familyRawBytes, err := item.Family(family)
	if err != nil {
		return nil, err
	}
	tagFamily := &modelv2.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv2.Tag, len(tagFamily.GetTags()))
	var tagSpec []*databasev2.TagSpec
	for _, tf := range s.schema.GetTagFamilies() {
		if tf.GetName() == family {
			tagSpec = tf.GetTags()
		}
	}
	if tagSpec == nil {
		return nil, ErrTagFamilyNotExist
	}
	for i, tag := range tagFamily.GetTags() {
		tags[i] = &modelv2.Tag{
			Key: tagSpec[i].GetName(),
			Value: &modelv2.TagValue{
				Value: tag.GetValue(),
			},
		}
	}
	return &modelv2.TagFamily{
		Name: family,
		Tags: tags,
	}, err
}

func (s *stream) ParseElementID(item tsdb.Item) (string, error) {
	rawBytes, err := item.Val()
	if err != nil {
		return "", err
	}
	return string(rawBytes), nil
}
