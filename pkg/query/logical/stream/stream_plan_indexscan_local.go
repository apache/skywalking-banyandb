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

package stream

import (
	"context"
	"fmt"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_ logical.Plan          = (*localIndexScan)(nil)
	_ logical.Sorter        = (*localIndexScan)(nil)
	_ logical.VolumeLimiter = (*localIndexScan)(nil)
)

type localIndexScan struct {
	schema            logical.Schema
	filter            index.Filter
	order             *logical.OrderBy
	metadata          *commonv1.Metadata
	l                 *logger.Logger
	timeRange         timestamp.TimeRange
	projectionTagRefs [][]*logical.TagRef
	// entitiesDeprecated []tsdb.Entity
	projectionTags []pbv1.TagProjection
	entities       [][]*modelv1.TagValue
	maxElementSize int
}

func (i *localIndexScan) Limit(max int) {
	i.maxElementSize = max
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) (elements []*streamv1.Element, err error) {
	var orderBy *pbv1.OrderBy
	if i.order != nil {
		orderBy = &pbv1.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	ec := executor.FromStreamExecutionContext(ctx)
	var results []pbv1.StreamQueryResult
	for _, e := range i.entities {
		result, err := ec.Query(ctx, pbv1.StreamQueryOptions{
			Name:          i.metadata.GetName(),
			TimeRange:     &i.timeRange,
			Entity:        e,
			Filter:        i.filter,
			Order:         orderBy,
			TagProjection: i.projectionTags,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query measure: %w", err)
		}

		results = append(results, result)
	}

	for _, result := range results {
		r := result.Pull()
		if r == nil {
			continue
		}
		for i := range r.Timestamps {
			e := &streamv1.Element{
				Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[i])),
				ElementId: r.ElementIDs[i],
			}

			for _, tf := range r.TagFamilies {
				tagFamily := &modelv1.TagFamily{
					Name: tf.Name,
				}
				// if tf.Name != stream.DefaultTagFamily {
				e.TagFamilies = append(e.TagFamilies, tagFamily)
				// }
				for _, t := range tf.Tags {
					// if tf.Name == stream.DefaultTagFamily && t.Name == stream.ElementIDTag {
					// 	e.ElementId = t.Values[i].GetStr().GetValue()
					// 	continue
					// }
					tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
						Key:   t.Name,
						Value: t.Values[i],
					})
				}
			}
			elements = append(elements, e)
		}
	}
	return elements, nil
}

// func (i *localIndexScan) ExecuteDeprecated(ctx context.Context) (elements []*streamv1.Element, err error) {
// 	var seriesList tsdb.SeriesList
// 	ec := executor.FromStreamExecutionContext(ctx)
// 	for _, e := range i.entitiesDeprecated {
// 		shards, errInternal := ec.Shards(e)
// 		if errInternal != nil {
// 			return nil, errInternal
// 		}
// 		for _, shard := range shards {
// 			sl, errInternal := shard.Series().List(context.WithValue(
// 				ctx,
// 				logger.ContextKey,
// 				i.l,
// 			), tsdb.NewPath(e))
// 			if errInternal != nil {
// 				return nil, errInternal
// 			}
// 			seriesList = seriesList.Merge(sl)
// 		}
// 	}
// 	if len(seriesList) == 0 {
// 		return nil, nil
// 	}
// 	var builders []logical.SeekerBuilder
// 	if i.order.Index != nil {
// 		builders = append(builders, func(builder tsdb.SeekerBuilder) {
// 			builder.OrderByIndex(i.order.Index, i.order.Sort)
// 		})
// 	} else {
// 		builders = append(builders, func(builder tsdb.SeekerBuilder) {
// 			builder.OrderByTime(i.order.Sort)
// 		})
// 	}
// 	if i.filter != nil {
// 		builders = append(builders, func(b tsdb.SeekerBuilder) {
// 			b.Filter(i.filter)
// 		})
// 	}
// 	iters, closers, err := logical.ExecuteForShard(ctx, i.l, seriesList, i.timeRange, builders...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if len(closers) > 0 {
// 		defer func(closers []io.Closer) {
// 			for _, c := range closers {
// 				err = multierr.Append(err, c.Close())
// 			}
// 		}(closers)
// 	}

// 	var elems []*streamv1.Element

// 	if len(iters) == 0 {
// 		return elems, nil
// 	}

// 	it := logical.NewItemIter(iters, i.order.Sort)
// 	defer func() {
// 		err = multierr.Append(err, it.Close())
// 	}()
// 	for it.Next() {
// 		nextItem := it.Val()
// 		tagFamilies, innerErr := logical.ProjectItem(ec, nextItem, i.projectionTagRefs)
// 		if innerErr != nil {
// 			return nil, innerErr
// 		}
// 		elementID, innerErr := ec.ParseElementIDDeprecated(nextItem)
// 		if innerErr != nil {
// 			return nil, innerErr
// 		}
// 		elems = append(elems, &streamv1.Element{
// 			ElementId:   elementID,
// 			Timestamp:   timestamppb.New(time.Unix(0, int64(nextItem.Time()))),
// 			TagFamilies: tagFamilies,
// 		})
// 		if len(elems) > i.maxElementSize {
// 			break
// 		}
// 	}
// 	return elems, nil
// }

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; orderBy=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, logical.FormatTagRefs(", ", i.projectionTagRefs...), i.order, i.maxElementSize)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	if i.projectionTagRefs == nil || len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}
