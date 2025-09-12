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

// TraceWriteKindVersion is the version tag of trace write kind.
var TraceWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-write",
}

// TopicTraceWrite is the trace write topic.
var TopicTraceWrite = bus.BiTopic(TraceWriteKindVersion.String())

// TraceQueryKindVersion is the version tag of trace query kind.
var TraceQueryKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-query",
}

// TopicTraceQuery is the trace query topic.
var TopicTraceQuery = bus.BiTopic(TraceQueryKindVersion.String())

// TraceDeleteExpiredSegmentsKindVersion is the version tag of trace delete segments kind.
var TraceDeleteExpiredSegmentsKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-delete-expired-segments",
}

// TopicDeleteExpiredTraceSegments is the delete trace segments topic.
var TopicDeleteExpiredTraceSegments = bus.BiTopic(TraceDeleteExpiredSegmentsKindVersion.String())

// TracePartSyncKindVersion is the version tag of part sync kind.
var TracePartSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-part-sync",
}

// TopicTracePartSync is the part sync topic.
var TopicTracePartSync = bus.BiTopic(TracePartSyncKindVersion.String())

// TraceSidxPartSyncKindVersion is the version tag of trace sidx part sync kind.
var TraceSidxPartSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-sidx-part-sync",
}

// TopicTraceSidxPartSync is the trace sidx part sync topic.
var TopicTraceSidxPartSync = bus.BiTopic(TraceSidxPartSyncKindVersion.String())

// TraceSidxSeriesWriteKindVersion is the version tag of trace sidx series write kind.
var TraceSidxSeriesWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "trace-sidx-series-write",
}

// TopicTraceSidxSeriesWrite is the trace sidx series write topic.
var TopicTraceSidxSeriesWrite = bus.BiTopic(TraceSidxSeriesWriteKindVersion.String())
