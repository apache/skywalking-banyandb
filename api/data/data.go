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

// Package data contains data transmission topics.
package data

import (
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// TopicMap is the map of topic name to topic.
var TopicMap = map[string]bus.Topic{
	TopicStreamWrite.String():  TopicStreamWrite,
	TopicStreamQuery.String():  TopicStreamQuery,
	TopicMeasureWrite.String(): TopicMeasureWrite,
	TopicMeasureQuery.String(): TopicMeasureQuery,
	TopicTopNQuery.String():    TopicTopNQuery,
}

// TopicRequestMap is the map of topic name to request message.
var TopicRequestMap = map[bus.Topic]func() proto.Message{
	TopicStreamWrite: func() proto.Message {
		return &streamv1.InternalWriteRequest{}
	},
	TopicStreamQuery: func() proto.Message {
		return &streamv1.QueryRequest{}
	},
	TopicMeasureWrite: func() proto.Message {
		return &measurev1.InternalWriteRequest{}
	},
	TopicMeasureQuery: func() proto.Message {
		return &measurev1.QueryRequest{}
	},
	TopicTopNQuery: func() proto.Message {
		return &measurev1.TopNRequest{}
	},
}

// TopicResponseMap is the map of topic name to response message.
var TopicResponseMap = map[bus.Topic]func() proto.Message{
	TopicStreamQuery: func() proto.Message {
		return &streamv1.QueryResponse{}
	},
	TopicMeasureQuery: func() proto.Message {
		return &measurev1.QueryResponse{}
	},
	TopicTopNQuery: func() proto.Message {
		return &measurev1.TopNResponse{}
	},
}
