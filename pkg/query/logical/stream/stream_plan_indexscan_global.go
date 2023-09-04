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
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ logical.Plan = (*globalIndexScan)(nil)

type globalIndexScan struct {
	schema            logical.Schema
	metadata          *commonv1.Metadata
	globalIndexRule   *databasev1.IndexRule
	expr              logical.LiteralExpr
	projectionTagRefs [][]*logical.TagRef
}

func (t *globalIndexScan) String() string {
	return fmt.Sprintf("GlobalIndexScan: Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		t.metadata.GetGroup(), t.metadata.GetName(),
		t.expr.String(), logical.FormatTagRefs(", ", t.projectionTagRefs...))
}

func (t *globalIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (t *globalIndexScan) Schema() logical.Schema {
	return t.schema
}

func (t *globalIndexScan) Execute(ctx context.Context) ([]*streamv1.Element, error) {
	ec := executor.FromStreamExecutionContext(ctx)
	shards, err := ec.Shards(nil)
	if err != nil {
		return nil, err
	}
	var elements []*streamv1.Element
	for _, shard := range shards {
		elementsInShard, shardErr := t.executeForShard(ctx, ec, shard)
		if shardErr != nil {
			return elements, shardErr
		}
		elements = append(elements, elementsInShard...)
	}
	return elements, nil
}

func (t *globalIndexScan) executeForShard(ctx context.Context, ec executor.StreamExecutionContext, shard tsdb.Shard) ([]*streamv1.Element, error) {
	var elementsInShard []*streamv1.Element
	for _, term := range t.expr.Bytes() {
		itemIDs, err := shard.Index().Seek(index.Field{
			Key: index.FieldKey{
				SeriesID:    tsdb.GlobalSeriesID(t.schema.Scope()),
				IndexRuleID: t.globalIndexRule.GetMetadata().GetId(),
			},
			Term: term,
		})
		if err != nil {
			return nil, err
		}
		for _, itemID := range itemIDs {
			segShard, err := ec.Shard(itemID.ShardID)
			if err != nil {
				return elementsInShard, errors.WithStack(err)
			}
			series, err := segShard.Series().GetByID(itemID.SeriesID)
			if err != nil {
				return elementsInShard, errors.WithStack(err)
			}
			err = func() error {
				ctxSeries, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				item, closer, errInner := series.Get(ctxSeries, itemID)
				defer func(closer io.Closer) {
					if closer != nil {
						_ = closer.Close()
					}
				}(closer)
				if errInner != nil {
					return errors.WithStack(errInner)
				}
				tagFamilies, errInner := logical.ProjectItem(ec, item, t.projectionTagRefs)
				if errInner != nil {
					return errors.WithStack(errInner)
				}
				elementID, errInner := ec.ParseElementID(item)
				if errInner != nil {
					return errors.WithStack(errInner)
				}
				elementsInShard = append(elementsInShard, &streamv1.Element{
					ElementId:   elementID,
					Timestamp:   timestamppb.New(time.Unix(0, int64(item.Time()))),
					TagFamilies: tagFamilies,
				})
				return nil
			}()
			if err != nil {
				return nil, err
			}
		}
	}

	return elementsInShard, nil
}
