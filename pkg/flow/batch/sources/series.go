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

package sources

import (
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	batchApi "github.com/apache/skywalking-banyandb/pkg/flow/batch/api"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ batchApi.Source = (*measureSource)(nil)

type measureSource struct {
	storage   measure.Measure
	timeRange timestamp.TimeRange
}

func NewMeasure(m measure.Measure, timeRange timestamp.TimeRange) batchApi.Source {
	return &measureSource{
		storage:   m,
		timeRange: timeRange,
	}
}

func (ms *measureSource) TimeRange() timestamp.TimeRange {
	return ms.timeRange
}

func (ms *measureSource) Metadata() *commonv1.Metadata {
	return ms.storage.GetSchema().GetMetadata()
}

func (ms *measureSource) Shards(shardingKeys tsdb.Entity) iter.Iterator[tsdb.Series] {
	shards, err := ms.storage.Shards(shardingKeys)
	if err != nil {
		// TODO: how to throw error?
		panic("drain error")
	}
	var series []tsdb.Series
	for _, shard := range shards {
		seriesList, innerErr := shard.Series().List(tsdb.NewPath(shardingKeys))
		if innerErr != nil {
			// TODO: log innerError
			panic("drain error")
		}
		series = append(series, seriesList...)
	}
	return iter.FromSlice(series)
}
