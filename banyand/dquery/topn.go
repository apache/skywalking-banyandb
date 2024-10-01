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

package dquery

import (
	"context"
	"errors"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	pkgquery "github.com/apache/skywalking-banyandb/pkg/query"
)

const defaultTopNQueryTimeout = 10 * time.Second

type topNQueryProcessor struct {
	broadcaster bus.Broadcaster
	*queryService
}

func (t *topNQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	n := time.Now()
	now := bus.MessageID(request.TimeRange.Begin.Nanos)
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		resp = bus.NewMessage(now, common.NewError("unspecified requested sort direction"))
		return
	}
	if request.GetAgg() == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		resp = bus.NewMessage(now, common.NewError("unspecified requested aggregation function"))
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Msg("received a topN query event")
	}
	if request.Trace {
		var tracer *pkgquery.Tracer
		tracer, ctx = pkgquery.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, _ := tracer.StartSpan(ctx, "distributed-client")
		span.Tag("request", convert.BytesToString(logger.Proto(request)))
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.TopNResponse:
				d.Trace = tracer.ToProto()
			case common.Error:
				span.Error(errors.New(d.Msg()))
				resp = bus.NewMessage(now, &measurev1.TopNResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	agg := request.Agg
	request.Agg = modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED
	ff, err := t.broadcaster.Broadcast(defaultTopNQueryTimeout, data.TopicTopNQuery, bus.NewMessage(now, request))
	if err != nil {
		resp = bus.NewMessage(now, common.NewError("execute the query %s: %v", request.GetName(), err))
		return
	}
	var allErr error
	aggregator := query.CreateTopNPostAggregator(request.GetTopN(),
		agg, request.GetFieldValueSort())
	var tags []string
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			allErr = multierr.Append(allErr, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			topNResp := d.(*measurev1.TopNResponse)
			for _, l := range topNResp.Lists {
				for _, tn := range l.Items {
					if tags == nil {
						tags = make([]string, 0, len(tn.Entity))
						for _, e := range tn.Entity {
							tags = append(tags, e.Key)
						}
					}
					entityValues := make(pbv1.EntityValues, 0, len(tn.Entity))
					for _, e := range tn.Entity {
						entityValues = append(entityValues, e.Value)
					}
					_ = aggregator.Put(entityValues, tn.Value.GetInt().GetValue(), uint64(l.Timestamp.AsTime().UnixMilli()))
				}
			}
		}
	}
	if allErr != nil {
		resp = bus.NewMessage(now, common.NewError("execute the query %s: %v", request.GetName(), allErr))
		return
	}
	if tags == nil {
		resp = bus.NewMessage(now, &measurev1.TopNResponse{})
		return
	}
	lists := aggregator.Val(tags)
	resp = bus.NewMessage(now, &measurev1.TopNResponse{
		Lists: lists,
	})
	if !request.Trace && t.slowQuery > 0 {
		latency := time.Since(n)
		if latency > t.slowQuery {
			t.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(request)).Int("resp_count", len(lists)).Msg("top_n slow query")
		}
	}
	return
}

var _ sort.Comparable = (*comparableTopNItem)(nil)

type comparableTopNItem struct {
	*measurev1.TopNList_Item
}

func (c *comparableTopNItem) SortedField() []byte {
	return convert.Int64ToBytes(c.Value.GetInt().Value)
}

var _ sort.Iterator[*comparableTopNItem] = (*sortedTopNList)(nil)

type sortedTopNList struct {
	*measurev1.TopNList
	index int
}

func (*sortedTopNList) Close() error {
	return nil
}

func (s *sortedTopNList) Next() bool {
	if s.index >= len(s.Items) {
		return false
	}
	s.index++
	return s.index < len(s.Items)
}

func (s *sortedTopNList) Val() *comparableTopNItem {
	return &comparableTopNItem{s.Items[s.index-1]}
}
