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
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var (
	// MeasureShardEventKindVersion is the version tag of measure shard event kind.
	MeasureShardEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "measure-event-shard",
	}

	// MeasureTopicShardEvent is the measure shard event publishing topic.
	MeasureTopicShardEvent = bus.UniTopic(MeasureShardEventKindVersion.String())

	// MeasureEntityEventKindVersion is the version tag of measure entity kind.
	MeasureEntityEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "measure-event-entity",
	}

	// MeasureTopicEntityEvent is the measure entity event publishing topic.
	MeasureTopicEntityEvent = bus.UniTopic(MeasureEntityEventKindVersion.String())
)
