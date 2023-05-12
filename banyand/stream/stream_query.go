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

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

var errTagFamilyNotExist = errors.New("tag family doesn't exist")

// Query allow to retrieve elements in a series of streams.
type Query interface {
	Stream(stream *commonv1.Metadata) (Stream, error)
}

// Stream allows inspecting elements' details.
type Stream interface {
	io.Closer
	Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	Shard(id common.ShardID) (tsdb.Shard, error)
	ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
	ParseElementID(item tsdb.Item) (string, error)
	GetSchema() *databasev1.Stream
	GetIndexRules() []*databasev1.IndexRule
}

var _ Stream = (*stream)(nil)

func (s *stream) Shards(entity tsdb.Entity) ([]tsdb.Shard, error) {
	wrap := func(shards []tsdb.Shard) []tsdb.Shard {
		result := make([]tsdb.Shard, len(shards))
		for i := 0; i < len(shards); i++ {
			result[i] = tsdb.NewScopedShard(tsdb.Entry(s.name), shards[i])
		}
		return result
	}
	db := s.db.SupplyTSDB()
	if len(entity) < 1 {
		return wrap(db.Shards()), nil
	}
	for _, e := range entity {
		if e == nil {
			return wrap(db.Shards()), nil
		}
	}
	shardID, err := partition.ShardID(entity.Prepend(tsdb.Entry(s.name)).Marshal(), s.shardNum)
	if err != nil {
		return nil, err
	}
	shard, err := db.Shard(common.ShardID(shardID))
	if err != nil {
		if errors.Is(err, tsdb.ErrUnknownShard) {
			return []tsdb.Shard{}, nil
		}
		return nil, err
	}
	return []tsdb.Shard{tsdb.NewScopedShard(tsdb.Entry(s.name), shard)}, nil
}

func (s *stream) Shard(id common.ShardID) (tsdb.Shard, error) {
	shard, err := s.db.SupplyTSDB().Shard(id)
	if err != nil {
		return nil, err
	}
	return tsdb.NewScopedShard(tsdb.Entry(s.name), shard), nil
}

func (s *stream) ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error) {
	familyRawBytes, err := item.Family(tsdb.Hash([]byte(family)))
	if err != nil {
		return nil, errors.Wrapf(err, "stream %s.%s parse family %s", s.name, s.group, family)
	}
	tagFamily := &modelv1.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv1.Tag, len(tagFamily.GetTags()))
	var tagSpec []*databasev1.TagSpec
	for _, tf := range s.schema.GetTagFamilies() {
		if tf.GetName() == family {
			tagSpec = tf.GetTags()
		}
	}
	if tagSpec == nil {
		return nil, errTagFamilyNotExist
	}
	for i, tag := range tagFamily.GetTags() {
		tags[i] = &modelv1.Tag{
			Key: tagSpec[i].GetName(),
			Value: &modelv1.TagValue{
				Value: tag.GetValue(),
			},
		}
	}
	return &modelv1.TagFamily{
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
