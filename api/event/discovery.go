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

package event

import (
	"github.com/apache/skywalking-banyandb/api/common"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var (
	ShardEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "event-shard",
	}
	TopicShardEvent        = bus.UniTopic(ShardEventKindVersion.String())

	SeriesEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "event-series",
	}
	TopicSeriesEvent = bus.UniTopic(SeriesEventKindVersion.String())

	WriteEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "event-write",
	}
	TopicWriteEvent = bus.UniTopic(WriteEventKindVersion.String())

	IndexRuleKindVersion = common.KindVersion{Version: "v1", Kind: "index-rule"}
	TopicIndexRule       = bus.UniTopic(IndexRuleKindVersion.String())
)

type TraceWriteDate struct {
	ShardID uint
	SeriesID uint64
	WriteRequest *v1.WriteRequest
}

type Shard struct {
	common.KindVersion
	Payload v1.ShardEvent
}

type Series struct {
	common.KindVersion
	Payload v1.SeriesEvent
}

type Write struct {
	common.KindVersion
	Payload *TraceWriteDate
}
type IndexRule struct {
	common.KindVersion
	Payload *v1.IndexRuleEvent
}
