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

package logical

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ Plan = (*localIndexScan)(nil)

type localIndexScan struct {
	*orderBy
	timeRange         timestamp.TimeRange
	schema            Schema
	metadata          *commonv1.Metadata
	projectionTagRefs [][]*TagRef
	entities          []tsdb.Entity
	filter            index.Filter
}

func (i *localIndexScan) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	var seriesList tsdb.SeriesList
	for _, e := range i.entities {
		shards, err := ec.Shards(e)
		if err != nil {
			return nil, err
		}
		for _, shard := range shards {
			sl, err := shard.Series().List(tsdb.NewPath(e))
			if err != nil {
				return nil, err
			}
			seriesList = append(seriesList, sl...)
		}
	}
	if len(seriesList) == 0 {
		return nil, nil
	}
	var builders []seekerBuilder
	if i.index != nil {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByIndex(i.index, i.sort)
		})
	} else {
		builders = append(builders, func(builder tsdb.SeekerBuilder) {
			builder.OrderByTime(i.sort)
		})
	}
	if i.filter != nil {
		builders = append(builders, func(b tsdb.SeekerBuilder) {
			b.Filter(i.filter)
		})
	}
	iters, innerErr := executeForShard(seriesList, i.timeRange, builders...)
	if innerErr != nil {
		return nil, innerErr
	}

	var elems []*streamv1.Element

	if len(iters) == 0 {
		return elems, nil
	}

	c := createComparator(i.sort)
	it := NewItemIter(iters, c)
	for it.HasNext() {
		nextItem := it.Next()
		tagFamilies, innerErr := projectItem(ec, nextItem, i.projectionTagRefs)
		if innerErr != nil {
			return nil, innerErr
		}
		elementID, innerErr := ec.ParseElementID(nextItem)
		if innerErr != nil {
			return nil, innerErr
		}
		elems = append(elems, &streamv1.Element{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(time.Unix(0, int64(nextItem.Time()))),
			TagFamilies: tagFamilies,
		})
	}
	return elems, nil
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, formatTagRefs(", ", i.projectionTagRefs...))
}

func (i *localIndexScan) Children() []Plan {
	return []Plan{}
}

func (i *localIndexScan) Schema() Schema {
	if i.projectionTagRefs == nil || len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}
