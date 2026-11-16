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
	"fmt"
	"slices"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pkgquery "github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

const defaultTopNQueryTimeout = 10 * time.Second

type topNQueryProcessor struct {
	measureService measure.Service
	broadcaster    bus.Broadcaster
	*queryService
	*bus.UnImplementedHealthyListener
}

func (t *topNQueryProcessor) getTopNFieldType(ctx context.Context, group, topNName string) (databasev1.FieldType, error) {
	topNAgg, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(ctx, &commonv1.Metadata{
		Group: group,
		Name:  topNName,
	})
	if err != nil {
		return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("failed to get topN aggregation %s/%s: %w", group, topNName, err)
	}
	sourceMeasure := topNAgg.GetSourceMeasure()
	measureSchema, err := t.metaService.MeasureRegistry().GetMeasure(ctx, sourceMeasure)
	if err != nil {
		return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("failed to get source measure %s: %w", sourceMeasure.GetName(), err)
	}
	fieldName := topNAgg.GetFieldName()
	for _, field := range measureSchema.GetFields() {
		if field.GetName() == fieldName {
			ft := field.GetFieldType()
			if ft != databasev1.FieldType_FIELD_TYPE_INT && ft != databasev1.FieldType_FIELD_TYPE_FLOAT {
				return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("unsupported field type %s for field %s in topN aggregation", ft.String(), fieldName)
			}
			return ft, nil
		}
	}
	return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("field %s not found in measure %s", fieldName, sourceMeasure.GetName())
}

func (t *topNQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	n := time.Now()
	now := bus.MessageID(request.TimeRange.Begin.Nanos)
	if err := t.validateRequest(request); err != nil {
		resp = bus.NewMessage(now, common.NewError("%s", err.Error()))
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Msg("received a topN query event")
	}
	nodeSelectors := make(map[string][]string)
	for _, g := range request.Groups {
		if gs, ok := t.measureService.LoadGroup(g); ok {
			if ns, exist := t.parseNodeSelector(request.Stages, gs.GetSchema().ResourceOpts); exist {
				nodeSelectors[g] = ns
			} else if len(gs.GetSchema().ResourceOpts.Stages) > 0 {
				t.log.Error().Strs("req_stages", request.Stages).Strs("default_stages", gs.GetSchema().GetResourceOpts().GetDefaultStages()).Msg("no stage found")
				resp = bus.NewMessage(now, common.NewError("no stage found in request or default stages in resource opts"))
				return
			}
		} else {
			t.log.Error().Str("group", g).Msg("failed to load group")
			resp = bus.NewMessage(now, common.NewError("failed to load group %s", g))
			return
		}
	}
	if len(request.Stages) > 0 && len(nodeSelectors) == 0 {
		t.log.Error().RawJSON("req", logger.Proto(request)).Msg("no stage found")
		resp = bus.NewMessage(now, common.NewError("no stage found"))
		return
	}
	var span *pkgquery.Span
	if request.Trace {
		var tracer *pkgquery.Tracer
		tracer, ctx = pkgquery.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "distributed-client")
		span.Tag("request", convert.BytesToString(logger.Proto(request)))
		span.Tagf("nodeSelectors", "%v", nodeSelectors)
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.TopNResponse:
				d.Trace = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				resp = bus.NewMessage(now, &measurev1.TopNResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
			span.Stop()
		}()
	}
	fieldType, fieldTypeErr := t.getTopNFieldType(ctx, request.Groups[0], request.GetName())
	if fieldTypeErr != nil {
		resp = bus.NewMessage(now, protector.QueryFailure(fieldTypeErr, "failed to determine field type for topN query %s: %v", request.GetName(), fieldTypeErr))
		return
	}
	targets := topNTargets(request.GetGroups(), nodeSelectors)
	partialLimit, limitErr := t.partialLimit(ctx, request, len(targets))
	if limitErr != nil {
		resp = bus.NewMessage(now, protector.QueryFailure(limitErr, "execute the query %s: %v", request.GetName(), limitErr))
		return
	}
	if span != nil {
		span.Tagf("targets", "%d", len(targets))
		span.Tagf("partial_limit", "%d", partialLimit)
	}
	answers, broadcastErr := t.broadcastTargets(now, request, targets, partialLimit)
	if broadcastErr != nil {
		resp = bus.NewMessage(now, protector.QueryFailure(broadcastErr, "execute the query %s: %v", request.GetName(), broadcastErr))
		return
	}
	var allErr error
	var lists []*measurev1.TopNList
	var responseCount int
	if fieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
		lists, responseCount, allErr = mergeTopNPartials[float64](answers, request.GetTopN(), request.GetFieldValueSort(), request.GetAgg())
	} else {
		lists, responseCount, allErr = mergeTopNPartials[int64](answers, request.GetTopN(), request.GetFieldValueSort(), request.GetAgg())
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
	}
	if allErr != nil {
		resp = bus.NewMessage(now, protector.QueryFailure(allErr, "execute the query %s: %v", request.GetName(), allErr))
		return
	}
	if len(lists) == 0 {
		resp = bus.NewMessage(now, &measurev1.TopNResponse{})
		return
	}

	if span != nil {
		span.Tagf("list_count", "%d", len(lists))
	}
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

// broadcastTargets sends every target its internal TopN request: the query narrowed to the target's group, with
// the partial limit, to the nodes of the target's stage only. It returns the answers of every target in order.
func (t *topNQueryProcessor) broadcastTargets(now bus.MessageID, request *measurev1.TopNRequest, targets []topNTarget,
	partialLimit int32,
) ([][]bus.Future, error) {
	answers := make([][]bus.Future, 0, len(targets))
	for _, target := range targets {
		internal := &measurev1.InternalTopNRequest{Request: target.request(request), PartialLimit: partialLimit}
		ff, err := t.broadcaster.Broadcast(defaultTopNQueryTimeout, data.TopicInternalTopNQuery,
			bus.NewMessageWithNodeSelectors(now, target.nodeSelectors(), request.TimeRange, internal))
		if err != nil {
			return nil, err
		}
		answers = append(answers, ff)
	}
	return answers, nil
}

// topNTarget is the part of a TopN query one broadcast answers: one group, and the nodes of one lifecycle stage
// of it. A shard ID identifies a shard only within a group and a stage, so the answers of different targets are
// never taken for replicas of each other.
type topNTarget struct {
	group    string
	selector string
}

// topNTargets splits a query into one target per group and selected lifecycle stage. A group without stages is
// one target that reaches every data node.
func topNTargets(groups []string, nodeSelectors map[string][]string) []topNTarget {
	var targets []topNTarget
	for _, g := range groups {
		selectors := nodeSelectors[g]
		if len(selectors) == 0 {
			targets = append(targets, topNTarget{group: g})
			continue
		}
		for _, sel := range selectors {
			targets = append(targets, topNTarget{group: g, selector: sel})
		}
	}
	return targets
}

func (tt topNTarget) request(request *measurev1.TopNRequest) *measurev1.TopNRequest {
	r := proto.Clone(request).(*measurev1.TopNRequest)
	r.Groups = []string{tt.group}
	return r
}

func (tt topNTarget) nodeSelectors() map[string][]string {
	if tt.selector == "" {
		return nil
	}
	return map[string][]string{tt.group: {tt.selector}}
}

// partialLimit decides how many items each shard returns.
//
// A query aggregates within one TopN group. When it reads one lifecycle stage of one group and one TopN group
// (its conditions pin the value of every group-by tag, or the TopN has no group-by tags), all the windows of
// that TopN group are stored on one shard (the TopN processor stores each result row on the shard of its own
// series). A shard's aggregate of an item is then complete, and its local top N contains every item of the
// global top N that it holds, so N is enough and the answer is exact.
//
// Otherwise the windows of an item are spread over several shards and each shard only aggregates a part of
// them: over one shard per target when the query spans lifecycle stages or groups, and over the shards of the
// TopN groups the item is reported under when the query reads every group-by value. An item can then rank below
// N on every shard and still belong to the global top N. Each shard returns its best K items, "more than the
// request asks": K is the counters number of the TopN aggregation (the size of the list the TopN keeps for
// every window), and at least N. Every window keeps at most K items of a TopN group, so a shard returns all of
// them unless the items change over the range; the merged result is exact then, and approximate otherwise,
// which is consistent with the pre-built TopN being approximate itself.
func (t *topNQueryProcessor) partialLimit(ctx context.Context, request *measurev1.TopNRequest, targets int) (int32, error) {
	topN := request.GetTopN()
	limit := topN
	spread := targets > 1
	for _, g := range request.GetGroups() {
		topNAgg, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(ctx, &commonv1.Metadata{Group: g, Name: request.GetName()})
		if err != nil {
			return 0, fmt.Errorf("failed to get topN aggregation %s/%s: %w", g, request.GetName(), err)
		}
		if len(request.GetConditions()) == 0 && len(topNAgg.GetGroupByTagNames()) > 0 {
			spread = true
		}
		if counters := topNAgg.GetCountersNumber(); counters > limit {
			limit = counters
		}
	}
	if !spread {
		return topN, nil
	}
	return limit, nil
}

var _ sort.Comparable = (*comparableTopNItem)(nil)

type comparableTopNItem struct {
	*measurev1.TopNList_Item
}

func (c *comparableTopNItem) SortedField() []byte {
	if c.Value.GetFloat() != nil {
		return convert.Float64ToOrderedBytes(c.Value.GetFloat().GetValue())
	}
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

// topNShardKey identifies the answer of one shard of one target. Replicas of a shard answer with the same key.
type topNShardKey struct {
	target  int
	shardID uint32
}

// mergeTopNPartials merges the per-shard partial aggregates that the data nodes answer for every target. The
// answer of each shard is counted once: the replicas of a shard answer with the same shard ID, and only the first
// answer that arrives is used, whatever its values. The partials of one item from different shards are combined
// (sums and counts are added, the maximum of the maxima and the minimum of the minima are taken, and MEAN is the
// sum of the sums divided by the sum of the counts), then the top N items are returned in the requested order.
// Items are keyed and ranked as the data nodes key and rank them (logical_measure.TopNItemKey and CompareTopNItems).
func mergeTopNPartials[K streaming.TopSortKey](
	answers [][]bus.Future,
	topN int32,
	sortDirection modelv1.Sort,
	agg modelv1.AggregationFunction,
) ([]*measurev1.TopNList, int, error) {
	type mergedItem struct {
		reduce aggregation.Reduce[K]
		val    K
		key    string
		entity []*modelv1.Tag
	}
	desc := sortDirection == modelv1.Sort_SORT_DESC
	var allErr error
	var responseCount int
	answered := make(map[topNShardKey]struct{})
	byKey := make(map[string]*mergedItem)
	var items []*mergedItem
	for target, ff := range answers {
		for _, f := range ff {
			m, getErr := f.Get()
			if getErr != nil {
				allErr = multierr.Append(allErr, getErr)
				continue
			}
			d := m.Data()
			if d == nil {
				continue
			}
			nodeResp, ok := d.(*measurev1.InternalTopNResponse)
			if !ok {
				allErr = multierr.Append(allErr, fmt.Errorf("unexpected topN answer %T from node %s", d, m.Node()))
				continue
			}
			responseCount++
			for _, shard := range nodeResp.GetShards() {
				key := topNShardKey{target: target, shardID: shard.GetShardId()}
				if _, replica := answered[key]; replica {
					continue
				}
				answered[key] = struct{}{}
				for _, p := range shard.GetPartials() {
					partial, partialErr := aggregation.FieldValuesToPartial[K](agg, p.GetPartial())
					if partialErr != nil {
						allErr = multierr.Append(allErr, partialErr)
						continue
					}
					itemKey := logical_measure.TopNItemKey(p.GetEntity())
					item, exist := byKey[itemKey]
					if !exist {
						reduce, reduceErr := aggregation.NewReduce[K](agg)
						if reduceErr != nil {
							return nil, responseCount, multierr.Append(allErr, reduceErr)
						}
						item = &mergedItem{key: itemKey, entity: p.GetEntity(), reduce: reduce}
						byKey[itemKey] = item
						items = append(items, item)
					}
					item.reduce.Combine(partial)
				}
			}
		}
	}
	if len(items) == 0 {
		return nil, responseCount, allErr
	}
	for _, item := range items {
		item.val = item.reduce.Val()
	}
	slices.SortFunc(items, func(a, b *mergedItem) int {
		return logical_measure.CompareTopNItems(a.val, a.key, b.val, b.key, desc)
	})
	if topN >= 0 && len(items) > int(topN) {
		items = items[:topN]
	}
	result := make([]*measurev1.TopNList_Item, len(items))
	for i, item := range items {
		value, valueErr := aggregation.ToFieldValue(item.val)
		if valueErr != nil {
			return nil, responseCount, multierr.Append(allErr, valueErr)
		}
		result[i] = &measurev1.TopNList_Item{Entity: item.entity, Value: value}
	}
	return []*measurev1.TopNList{{Timestamp: timestamppb.Now(), Items: result}}, responseCount, allErr
}

func (t *topNQueryProcessor) validateRequest(request *measurev1.TopNRequest) error {
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		return errors.New("unspecified requested sort direction")
	}
	if request.GetAgg() == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		return errors.New("unspecified requested aggregation function")
	}
	if len(request.GetGroups()) == 0 {
		return errors.New("at least one group is required")
	}
	if request.GetTopN() <= 0 {
		return errors.New("topN must be positive")
	}
	return nil
}
