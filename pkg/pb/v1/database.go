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

package v1

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type ShardEventBuilder struct {
	se *databasev1.ShardEvent
}

func NewShardEventBuilder() *ShardEventBuilder {
	return &ShardEventBuilder{se: &databasev1.ShardEvent{}}
}

func (seb *ShardEventBuilder) Action(action databasev1.Action) *ShardEventBuilder {
	seb.se.Action = action
	return seb
}

func (seb *ShardEventBuilder) Time(t time.Time) *ShardEventBuilder {
	seb.se.Time = timestamppb.New(t)
	return seb
}

func (seb *ShardEventBuilder) Shard(shard *databasev1.Shard) *ShardEventBuilder {
	seb.se.Shard = shard
	return seb
}

func (seb *ShardEventBuilder) Build() *databasev1.ShardEvent {
	return seb.se
}

type ShardBuilder struct {
	s *databasev1.Shard
}

func NewShardBuilder() *ShardBuilder {
	return &ShardBuilder{s: &databasev1.Shard{}}
}

func (sb *ShardBuilder) ID(shardID uint64) *ShardBuilder {
	sb.s.Id = shardID
	return sb
}

func (sb *ShardBuilder) SeriesMetadata(group, name string) *ShardBuilder {
	sb.s.Series = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return sb
}

func (sb *ShardBuilder) Node(node *databasev1.Node) *ShardBuilder {
	sb.s.Node = node
	return sb
}

func (sb *ShardBuilder) Total(total uint32) *ShardBuilder {
	sb.s.Total = total
	return sb
}

func (sb *ShardBuilder) CreatedAt(t time.Time) *ShardBuilder {
	sb.s.CreatedAt = timestamppb.New(t)
	return sb
}

func (sb *ShardBuilder) UpdatedAt(t time.Time) *ShardBuilder {
	sb.s.UpdatedAt = timestamppb.New(t)
	return sb
}

func (sb *ShardBuilder) Build() *databasev1.Shard {
	return sb.s
}

type NodeBuilder struct {
	n *databasev1.Node
}

func NewNodeBuilder() *NodeBuilder {
	return &NodeBuilder{n: &databasev1.Node{}}
}

func (nb *NodeBuilder) ID(id string) *NodeBuilder {
	nb.n.Id = id
	return nb
}

func (nb *NodeBuilder) Addr(addr string) *NodeBuilder {
	nb.n.Addr = addr
	return nb
}

func (nb *NodeBuilder) UpdatedAt(t time.Time) *NodeBuilder {
	nb.n.UpdatedAt = timestamppb.New(t)
	return nb
}

func (nb *NodeBuilder) CreatedAt(t time.Time) *NodeBuilder {
	nb.n.CreatedAt = timestamppb.New(t)
	return nb
}

func (nb *NodeBuilder) Build() *databasev1.Node {
	return nb.n
}

type SeriesEventBuilder struct {
	se *databasev1.SeriesEvent
}

func NewSeriesEventBuilder() *SeriesEventBuilder {
	return &SeriesEventBuilder{se: &databasev1.SeriesEvent{}}
}

func (seb *SeriesEventBuilder) SeriesMetadata(group, name string) *SeriesEventBuilder {
	seb.se.Series = &commonv1.Metadata{
		Group: group,
		Name:  name,
	}
	return seb
}

func (seb *SeriesEventBuilder) FieldNames(names ...string) *SeriesEventBuilder {
	seb.se.FieldNamesCompositeSeriesId = names
	return seb
}

func (seb *SeriesEventBuilder) Action(action databasev1.Action) *SeriesEventBuilder {
	seb.se.Action = action
	return seb
}

func (seb *SeriesEventBuilder) Time(t time.Time) *SeriesEventBuilder {
	seb.se.Time = timestamppb.New(t)
	return seb
}

func (seb *SeriesEventBuilder) Build() *databasev1.SeriesEvent {
	return seb.se
}
