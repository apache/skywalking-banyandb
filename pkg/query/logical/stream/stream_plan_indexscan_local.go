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
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_ logical.Plan              = (*localIndexScan)(nil)
	_ logical.Sorter            = (*localIndexScan)(nil)
	_ logical.VolumeLimiter     = (*localIndexScan)(nil)
	_ executor.StreamExecutable = (*localIndexScan)(nil)
)

type localIndexScan struct {
	schema            logical.Schema
	filter            index.Filter
	result            model.StreamQueryResult
	order             *logical.OrderBy
	metadata          *commonv1.Metadata
	l                 *logger.Logger
	timeRange         timestamp.TimeRange
	projectionTagRefs [][]*logical.TagRef
	projectionTags    []model.TagProjection
	entities          [][]*modelv1.TagValue
	maxElementSize    int
}

func (i *localIndexScan) Close() {
	if i.result != nil {
		i.result.Release()
	}
}

func (i *localIndexScan) Limit(maxVal int) {
	i.maxElementSize = maxVal
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) Execute(ctx context.Context) ([]*streamv1.Element, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if i.result != nil {
		return BuildElementsFromStreamResult(ctx, i.result), nil
	}
	var orderBy *model.OrderBy
	if i.order != nil {
		orderBy = &model.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	ec := executor.FromStreamExecutionContext(ctx)
	var err error
	if i.result, err = ec.Query(ctx, model.StreamQueryOptions{
		Name:           i.metadata.GetName(),
		TimeRange:      &i.timeRange,
		Entities:       i.entities,
		Filter:         i.filter,
		Order:          orderBy,
		TagProjection:  i.projectionTags,
		MaxElementSize: i.maxElementSize,
	}); err != nil {
		return nil, err
	}
	if i.result == nil {
		return nil, nil
	}
	return BuildElementsFromStreamResult(ctx, i.result), nil
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; orderBy=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.filter, logical.FormatTagRefs(", ", i.projectionTagRefs...), i.order, i.maxElementSize)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	if len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}

// BuildElementsFromStreamResult builds a slice of elements from the given stream query result.
func BuildElementsFromStreamResult(ctx context.Context, result model.StreamQueryResult) (elements []*streamv1.Element) {
	r := result.Pull(ctx)
	if r == nil {
		return nil
	}
	for i := range r.Timestamps {
		e := &streamv1.Element{
			Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[i])),
			ElementId: hex.EncodeToString(convert.Uint64ToBytes(r.ElementIDs[i])),
		}

		for _, tf := range r.TagFamilies {
			tagFamily := &modelv1.TagFamily{
				Name: tf.Name,
			}
			e.TagFamilies = append(e.TagFamilies, tagFamily)
			for _, t := range tf.Tags {
				tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
					Key:   t.Name,
					Value: t.Values[i],
				})
			}
		}
		elements = append(elements, e)
	}
	return elements
}
