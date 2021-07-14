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

package fb

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func BuildShardEvent(nodeID, seriesName, seriesGroup string, shardID, totalShards uint) []byte {
	now := time.Now().UnixNano()
	b := flatbuffers.NewBuilder(0)
	nID := b.CreateString(nodeID)
	addr := b.CreateString("localhost")
	v1.NodeStart(b)
	v1.NodeAddId(b, nID)
	v1.NodeAddAddr(b, addr)
	v1.NodeAddCreateTime(b, now)
	v1.NodeAddUpdateTime(b, now)
	node := v1.NodeEnd(b)
	name := b.CreateString(seriesName)
	group := b.CreateString(seriesGroup)
	v1.MetadataStart(b)
	v1.MetadataAddName(b, name)
	v1.MetadataAddGroup(b, group)
	md := v1.MetadataEnd(b)
	v1.ShardStart(b)
	v1.ShardAddId(b, uint64(shardID))
	v1.ShardAddNode(b, node)
	v1.ShardAddSeries(b, md)
	v1.ShardAddTotal(b, uint8(totalShards))
	v1.ShardAddCreateTime(b, now)
	v1.ShardAddUpdateTime(b, now)
	shard := v1.ShardEnd(b)
	v1.ShardEventStart(b)
	v1.ShardEventAddShard(b, shard)
	v1.ShardEventAddTime(b, time.Now().UnixNano())
	v1.ShardEventAddAction(b, v1.ActionPut)
	b.Finish(v1.ShardEventEnd(b))
	return b.FinishedBytes()
}

func BuildSeriesEvent(seriesName, seriesGroup string, fieldNames [][]byte) []byte {
	now := time.Now().UnixNano()
	b := flatbuffers.NewBuilder(0)
	name := b.CreateString(seriesName)
	group := b.CreateString(seriesGroup)
	v1.MetadataStart(b)
	v1.MetadataAddName(b, name)
	v1.MetadataAddGroup(b, group)
	md := v1.MetadataEnd(b)
	var nameOffsets []flatbuffers.UOffsetT
	for _, fieldName := range fieldNames {
		nameOffsets = append(nameOffsets, b.CreateByteString(fieldName))
	}
	v1.SeriesEventStartFieldNamesCompositeSeriesIdVector(b, len(fieldNames))
	for i := len(nameOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(nameOffsets[i])
	}
	names := b.EndVector(len(fieldNames))
	v1.SeriesEventStart(b)
	v1.SeriesEventAddSeries(b, md)
	v1.SeriesEventAddFieldNamesCompositeSeriesId(b, names)
	v1.SeriesEventAddAction(b, v1.ActionPut)
	v1.SeriesEventAddTime(b, now)
	b.Finish(v1.SeriesEventEnd(b))
	return b.FinishedBytes()
}
