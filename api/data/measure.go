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

// MeasureWriteKindVersion is the version tag of measure write kind.
var MeasureWriteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-write",
}

// TopicMeasureWrite is the measure write topic.
var TopicMeasureWrite = bus.BiTopic(MeasureWriteKindVersion.String())

// MeasureQueryKindVersion is the version tag of measure query kind.
var MeasureQueryKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-query",
}

// TopicMeasureQuery is the measure query topic.
var TopicMeasureQuery = bus.BiTopic(MeasureQueryKindVersion.String())

// TopNQueryKindVersion is the version tag of top-n query kind.
var TopNQueryKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "topN-query",
}

// TopicTopNQuery is the top-n query topic.
var TopicTopNQuery = bus.BiTopic(TopNQueryKindVersion.String())

// MeasureDeleteExpiredSegmentsKindVersion is the version tag of measure delete kind.
var MeasureDeleteExpiredSegmentsKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-delete-expired-segments",
}

// TopicMeasureDeleteExpiredSegments is the measure delete topic.
var TopicMeasureDeleteExpiredSegments = bus.BiTopic(MeasureDeleteExpiredSegmentsKindVersion.String())

// MeasurePartSyncKindVersion is the version tag of measure part sync kind.
var MeasurePartSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-part-sync",
}

// TopicMeasurePartSync is the measure part sync topic.
var TopicMeasurePartSync = bus.BiTopic(MeasurePartSyncKindVersion.String())

// MeasureSeriesIndexInsertKindVersion is the version tag of measure series index insert kind.
var MeasureSeriesIndexInsertKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-series-index-insert",
}

// TopicMeasureSeriesIndexInsert is the measure series index insert topic.
var TopicMeasureSeriesIndexInsert = bus.BiTopic(MeasureSeriesIndexInsertKindVersion.String())

// MeasureSeriesIndexUpdateKindVersion is the version tag of measure series index update kind.
var MeasureSeriesIndexUpdateKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-series-index-update",
}

// TopicMeasureSeriesIndexUpdate is the measure series index update topic.
var TopicMeasureSeriesIndexUpdate = bus.BiTopic(MeasureSeriesIndexUpdateKindVersion.String())

// MeasureSeriesSyncKindVersion is the version tag of measure series sync kind.
var MeasureSeriesSyncKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "measure-series-sync",
}

// TopicMeasureSeriesSync is the measure series sync topic.
var TopicMeasureSeriesSync = bus.BiTopic(MeasureSeriesSyncKindVersion.String())
