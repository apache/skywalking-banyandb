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
	"io"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var _ Plan = (*globalIndexScan)(nil)

type globalIndexScan struct {
	schema              Schema
	metadata            *commonv1.Metadata
	globalIndexRule     *databasev1.IndexRule
	expr                Expr
	projectionFieldRefs [][]*TagRef
}

func (t *globalIndexScan) String() string {
	if len(t.projectionFieldRefs) == 0 {
		return fmt.Sprintf("GlobalIndexScan: Metadata{group=%s,name=%s},condition=%s; projection=None",
			t.metadata.GetGroup(), t.metadata.GetName(), t.expr.String())
	}
	return fmt.Sprintf("GlobalIndexScan: Metadata{group=%s,name=%s},conditions=%s; projection=%s",
		t.metadata.GetGroup(), t.metadata.GetName(),
		t.expr.String(), formatTagRefs(", ", t.projectionFieldRefs...))
}

func (t *globalIndexScan) Children() []Plan {
	return []Plan{}
}

func (t *globalIndexScan) Type() PlanType {
	return PlanGlobalIndexScan
}

func (t *globalIndexScan) Schema() Schema {
	return t.schema
}

func (t *globalIndexScan) Equal(plan Plan) bool {
	if plan.Type() != PlanGlobalIndexScan {
		return false
	}
	other := plan.(*globalIndexScan)
	return t.metadata.GetGroup() == other.metadata.GetGroup() &&
		t.metadata.GetName() == other.metadata.GetName() &&
		cmp.Equal(t.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(t.schema, other.schema) &&
		cmp.Equal(t.globalIndexRule.GetMetadata().GetId(), other.globalIndexRule.GetMetadata().GetId()) &&
		cmp.Equal(t.expr, other.expr)
}

func (t *globalIndexScan) Execute(ec executor.StreamExecutionContext) ([]*streamv1.Element, error) {
	shards, err := ec.Shards(nil)
	if err != nil {
		return nil, err
	}
	var elements []*streamv1.Element
	for _, shard := range shards {
		elementsInShard, shardErr := t.executeForShard(ec, shard)
		if shardErr != nil {
			return elements, shardErr
		}
		elements = append(elements, elementsInShard...)
	}
	return elements, nil
}

func (t *globalIndexScan) executeForShard(ec executor.StreamExecutionContext, shard tsdb.Shard) ([]*streamv1.Element, error) {
	var elementsInShard []*streamv1.Element
	itemIDs, err := shard.Index().Seek(index.Field{
		Key: index.FieldKey{
			SeriesID:    tsdb.GlobalSeriesID(t.schema.Scope()),
			IndexRuleID: t.globalIndexRule.GetMetadata().GetId(),
		},
		Term: t.expr.(*binaryExpr).r.(LiteralExpr).Bytes()[0],
	})
	if err != nil || len(itemIDs) < 1 {
		return elementsInShard, nil
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
			item, closer, errInner := series.Get(itemID)
			defer func(closer io.Closer) {
				if closer != nil {
					_ = closer.Close()
				}
			}(closer)
			if errInner != nil {
				return errors.WithStack(errInner)
			}
			tagFamilies, errInner := projectItem(ec, item, t.projectionFieldRefs)
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
	return elementsInShard, nil
}
