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

package data

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// StreamWriteKindVersion is the version tag of stream write kind.
var StreamWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "stream-write",
}

// TopicStreamWrite is the stream write topic.
var TopicStreamWrite = bus.BiTopic(StreamWriteKindVersion.String())

// StreamQueryKindVersion is the version tag of stream query kind.
var StreamQueryKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "stream-query",
}

// TopicStreamQuery is the stream query topic.
var TopicStreamQuery = bus.BiTopic(StreamQueryKindVersion.String())

// StreamDeleteExpiredSegmentsKindVersion is the version tag of stream delete segments kind.
var StreamDeleteExpiredSegmentsKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "stream-delete-expired-segments",
}

// TopicDeleteExpiredStreamSegments is the delete stream segments topic.
var TopicDeleteExpiredStreamSegments = bus.BiTopic(StreamDeleteExpiredSegmentsKindVersion.String())

// StreamPartSyncKindVersion is the version tag of part sync kind.
var StreamPartSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "part-sync",
}

// TopicStreamPartSync is the part sync topic.
var TopicStreamPartSync = bus.BiTopic(StreamPartSyncKindVersion.String())

// StreamSeriesIndexWriteKindVersion is the version tag of stream series index write kind.
var StreamSeriesIndexWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "stream-series-index-write",
}

// TopicStreamSeriesIndexWrite is the stream series index write topic.
var TopicStreamSeriesIndexWrite = bus.BiTopic(StreamSeriesIndexWriteKindVersion.String())

// StreamLocalIndexWriteKindVersion is the version tag of stream local index write kind.
var StreamLocalIndexWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "stream-local-index-write",
}

// TopicStreamLocalIndexWrite is the stream local index write topic.
var TopicStreamLocalIndexWrite = bus.BiTopic(StreamLocalIndexWriteKindVersion.String())

// StreamSeriesSyncKindVersion is the version tag of series sync kind.
var StreamSeriesSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "series-sync",
}

// TopicStreamSeriesSync is the series sync topic.
var TopicStreamSeriesSync = bus.BiTopic(StreamSeriesSyncKindVersion.String())

// StreamElementIndexSyncKindVersion is the version tag of element index sync kind.
var StreamElementIndexSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "element-index-sync",
}

// TopicStreamElementIndexSync is the element index sync topic.
var TopicStreamElementIndexSync = bus.BiTopic(StreamElementIndexSyncKindVersion.String())
