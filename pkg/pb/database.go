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

package pb

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
)

type shardEventBuilder struct {
	se *v1.ShardEvent
}

func NewShardEventBuilder() *shardEventBuilder {
	return &shardEventBuilder{se: &v1.ShardEvent{}}
}

func (seb *shardEventBuilder) Action(action v1.Action) *shardEventBuilder {
	seb.se.Action = action
	return seb
}

func (seb *shardEventBuilder) Time(t time.Time) *shardEventBuilder {
	seb.se.Time = timestamppb.New(t)
	return seb
}

func (seb *shardEventBuilder) Shard(shard *v1.Shard) *shardEventBuilder {
	seb.se.Shard = shard
	return seb
}

func (seb *shardEventBuilder) Build() *v1.ShardEvent {
	return seb.se
}

type shardBuilder struct {
	s *v1.Shard
}

func NewShardBuilder() *shardBuilder {
	return &shardBuilder{s: &v1.Shard{}}
}

func (sb *shardBuilder) Id(shardId uint64) *shardBuilder {
	sb.s.Id = shardId
	return sb
}

func (sb *shardBuilder) SeriesMetadata(group, name string) *shardBuilder {
	sb.s.Series = &v1.Metadata{
		Group: group,
		Name:  name,
	}
	return sb
}

func (sb *shardBuilder) Node(node *v1.Node) *shardBuilder {
	sb.s.Node = node
	return sb
}

func (sb *shardBuilder) Total(total uint32) *shardBuilder {
	sb.s.Total = total
	return sb
}

func (sb *shardBuilder) CreatedAt(t time.Time) *shardBuilder {
	sb.s.CreatedAt = timestamppb.New(t)
	return sb
}

func (sb *shardBuilder) UpdatedAt(t time.Time) *shardBuilder {
	sb.s.UpdatedAt = timestamppb.New(t)
	return sb
}

func (sb *shardBuilder) Build() *v1.Shard {
	return sb.s
}

type nodeBuilder struct {
	n *v1.Node
}

func NewNodeBuilder() *nodeBuilder {
	return &nodeBuilder{n: &v1.Node{}}
}

func (nb *nodeBuilder) Id(id string) *nodeBuilder {
	nb.n.Id = id
	return nb
}

func (nb *nodeBuilder) Addr(addr string) *nodeBuilder {
	nb.n.Addr = addr
	return nb
}

func (nb *nodeBuilder) UpdatedAt(t time.Time) *nodeBuilder {
	nb.n.UpdatedAt = timestamppb.New(t)
	return nb
}

func (nb *nodeBuilder) CreatedAt(t time.Time) *nodeBuilder {
	nb.n.CreatedAt = timestamppb.New(t)
	return nb
}

func (nb *nodeBuilder) Build() *v1.Node {
	return nb.n
}

type seriesEventBuilder struct {
	se *v1.SeriesEvent
}

func NewSeriesEventBuilder() *seriesEventBuilder {
	return &seriesEventBuilder{se: &v1.SeriesEvent{}}
}

func (seb *seriesEventBuilder) SeriesMetadata(group, name string) *seriesEventBuilder {
	seb.se.Series = &v1.Metadata{
		Group: group,
		Name:  name,
	}
	return seb
}

func (seb *seriesEventBuilder) FieldNames(names ...string) *seriesEventBuilder {
	seb.se.FieldNamesCompositeSeriesId = names
	return seb
}

func (seb *seriesEventBuilder) Action(action v1.Action) *seriesEventBuilder {
	seb.se.Action = action
	return seb
}

func (seb *seriesEventBuilder) Time(t time.Time) *seriesEventBuilder {
	seb.se.Time = timestamppb.New(t)
	return seb
}

func (seb *seriesEventBuilder) Build() *v1.SeriesEvent {
	return seb.se
}
