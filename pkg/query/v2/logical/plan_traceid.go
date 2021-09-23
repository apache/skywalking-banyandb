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

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/executor"
)

var _ UnresolvedPlan = (*unresolvedTraceIDFetch)(nil)
var _ Plan = (*traceIDFetch)(nil)

type unresolvedTraceIDFetch struct {
	metadata         *commonv2.Metadata
	traceID          string
	projectionFields [][]*Tag
}

func (t *unresolvedTraceIDFetch) Analyze(s Schema) (Plan, error) {
	defined, idxRule := s.IndexDefined(NewTag("", s.TraceIDFieldName()))
	if !defined {
		return nil, errors.Wrap(ErrIndexNotDefined, "trace_id")
	}

	if t.projectionFields == nil || len(t.projectionFields) == 0 {
		return &traceIDFetch{
			metadata:           t.metadata,
			schema:             s,
			traceID:            t.traceID,
			traceIDIndexRuleID: idxRule.GetMetadata().GetId(),
		}, nil
	}

	if s == nil {
		return nil, errors.Wrap(ErrInvalidSchema, "nil")
	}

	fieldRefs, err := s.CreateRef(t.projectionFields...)
	if err != nil {
		return nil, err
	}
	return &traceIDFetch{
		projectionFieldRefs: fieldRefs,
		schema:              s,
		traceID:             t.traceID,
		metadata:            t.metadata,
		traceIDIndexRuleID:  idxRule.GetMetadata().GetId(),
	}, nil
}

func (t *unresolvedTraceIDFetch) Type() PlanType {
	return PlanTraceIDFetch
}

type traceIDFetch struct {
	metadata            *commonv2.Metadata
	traceID             string
	projectionFieldRefs [][]*FieldRef
	schema              Schema
	traceIDIndexRuleID  uint32
}

func (t *traceIDFetch) String() string {
	return fmt.Sprintf("TraceIDFetch: traceID=%s,Metadata{group=%s,name=%s}",
		t.traceID,
		t.metadata.GetGroup(),
		t.metadata.GetName(),
	)
}

func (t *traceIDFetch) Children() []Plan {
	return []Plan{}
}

func (t *traceIDFetch) Type() PlanType {
	return PlanTraceIDFetch
}

func (t *traceIDFetch) Schema() Schema {
	return t.schema
}

func (t *traceIDFetch) Equal(plan Plan) bool {
	if plan.Type() != PlanTraceIDFetch {
		return false
	}
	other := plan.(*traceIDFetch)
	return t.traceID == other.traceID &&
		cmp.Equal(t.projectionFieldRefs, other.projectionFieldRefs) &&
		cmp.Equal(t.schema, other.schema) &&
		t.metadata.GetGroup() == other.metadata.GetGroup() &&
		t.metadata.GetName() == other.metadata.GetName()
}

func (t *traceIDFetch) Execute(ec executor.ExecutionContext) ([]*streamv2.Element, error) {
	shards, err := ec.Shards(nil)
	if err != nil {
		return nil, err
	}
	var elements []*streamv2.Element
	for _, shard := range shards {
		elementsInShard, err := t.executeForShard(ec, shard)
		if err != nil {
			return elements, err
		}
		elements = append(elements, elementsInShard...)
	}
	return elements, nil
}

func (t *traceIDFetch) executeForShard(ec executor.ExecutionContext, shard tsdb.Shard) ([]*streamv2.Element, error) {
	var elementsInShard []*streamv2.Element
	itemIDs, err := shard.Index().Seek(index.Field{
		Key: index.FieldKey{
			IndexRuleID: t.traceIDIndexRuleID,
		},
		Term: []byte(t.traceID),
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
				_ = closer.Close()
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
			elementsInShard = append(elementsInShard, &streamv2.Element{
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

func TraceIDFetch(traceID string, metadata *commonv2.Metadata, projection ...[]*Tag) UnresolvedPlan {
	return &unresolvedTraceIDFetch{
		metadata:         metadata,
		traceID:          traceID,
		projectionFields: projection,
	}
}
