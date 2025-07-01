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
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var (
	// TopicMap is the map of topic name to topic.
	TopicMap = map[string]bus.Topic{
		TopicStreamWrite.String():    TopicStreamWrite,
		TopicStreamQuery.String():    TopicStreamQuery,
		TopicMeasureWrite.String():   TopicMeasureWrite,
		TopicMeasureQuery.String():   TopicMeasureQuery,
		TopicTopNQuery.String():      TopicTopNQuery,
		TopicPropertyDelete.String(): TopicPropertyDelete,
		TopicPropertyQuery.String():  TopicPropertyQuery,
		TopicPropertyUpdate.String(): TopicPropertyUpdate,
		TopicPropertyRepair.String(): TopicPropertyRepair,
	}

	// TopicRequestMap is the map of topic name to request message.
	// nolint: exhaustruct
	TopicRequestMap = map[bus.Topic]func() proto.Message{
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
		TopicPropertyUpdate: func() proto.Message {
			return &propertyv1.InternalUpdateRequest{}
		},
		TopicPropertyQuery: func() proto.Message {
			return &propertyv1.QueryRequest{}
		},
		TopicPropertyDelete: func() proto.Message {
			return &propertyv1.InternalDeleteRequest{}
		},
		TopicPropertyRepair: func() proto.Message {
			return &propertyv1.InternalRepairRequest{}
		},
	}

	// TopicResponseMap is the map of topic name to response message.
	// nolint: exhaustruct
	TopicResponseMap = map[bus.Topic]func() proto.Message{
		TopicStreamQuery: func() proto.Message {
			return &streamv1.QueryResponse{}
		},
		TopicMeasureQuery: func() proto.Message {
			return &measurev1.QueryResponse{}
		},
		TopicTopNQuery: func() proto.Message {
			return &measurev1.TopNResponse{}
		},
		TopicPropertyQuery: func() proto.Message {
			return &propertyv1.InternalQueryResponse{}
		},
		TopicPropertyDelete: func() proto.Message {
			return &propertyv1.DeleteResponse{}
		},
		TopicPropertyUpdate: func() proto.Message {
			return &propertyv1.ApplyResponse{}
		},
		TopicPropertyRepair: func() proto.Message {
			return &propertyv1.InternalRepairResponse{}
		},
	}

	// TopicCommon is the common topic for data transmission.
	TopicCommon = bus.Topic{}
)
