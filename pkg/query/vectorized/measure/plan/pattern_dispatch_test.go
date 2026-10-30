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

package plan

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func TestPatternDispatchPreservesNativeOptions(t *testing.T) {
	schema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)
	patterns := []native.TermSetRequest{{
		Field: "name-index", WildcardPatterns: []native.WildcardPattern{{Pattern: "%a%", Escape: "\\", Syntax: native.WildcardSyntaxSQL}},
		PatternBudget: native.NewWildcardBudget(native.WildcardLimits{}),
	}}
	result, _, err := DispatchBatches(t.Context(), bareReq(), metadata, schema, logicalSchema, ec, dispatchCfg(), false, false, patterns...)
	require.NoError(t, err)
	require.NoError(t, result.Close())
	require.True(t, ec.called)
	require.Equal(t, patterns, ec.lastOpts.PatternRequests)
}

type patternCaptureCluster struct {
	messages []bus.Message
	message  bus.Message
	topic    bus.Topic
	fakeCluster
}

func (c *patternCaptureCluster) Broadcast(_ time.Duration, topic bus.Topic, message bus.Message) ([]bus.Future, error) {
	c.topic = topic
	c.message = message
	c.messages = append(c.messages, message)
	return nil, nil
}

func TestPatternDistributedRequestEnvelopeAndIdentity(t *testing.T) {
	req := &measurev1.InternalQueryRequest{Request: bareReq()}
	patterns := []model.IndexedPattern{{TagFamily: "default", TagName: "svc", Pattern: "%a%", Escape: "\\", IgnoreCase: true}}
	plan := &DistributedPlan{patterns: patterns, cfg: dispatchCfg()}
	cluster := &patternCaptureCluster{}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	_, release, err := plan.broadcastRequest(ctx, cluster, req)
	defer release()
	require.NoError(t, err)
	require.Positive(t, tracker.Used())
	release()
	require.Zero(t, tracker.Used())
	require.Equal(t, data.TopicInternalMeasurePatternQuery, cluster.topic)
	require.Equal(t, req.Request.Groups[0], cluster.message.Group())
	decoded, got, err := vmeasure.DecodeMeasurePatternRequest(cluster.message.Data().([]byte))
	require.NoError(t, err)
	require.True(t, proto.Equal(req, decoded))
	require.Equal(t, patterns, got)
	plan.patterns = nil
	_, release, err = plan.broadcastRequest(ctx, cluster, req)
	defer release()
	require.NoError(t, err)
	require.Equal(t, data.TopicInternalMeasureQuery, cluster.topic)
	require.Same(t, req, cluster.message.Data())
}

func TestPatternDistributedRequestBudgetAndCleanup(t *testing.T) {
	req := &measurev1.InternalQueryRequest{Request: bareReq()}
	plan := &DistributedPlan{
		patterns: []model.IndexedPattern{{TagFamily: "default", TagName: "svc", Pattern: "trailing!", Escape: "!"}},
		cfg:      dispatchCfg(),
	}
	cluster := &patternCaptureCluster{}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1)
	_, release, err := plan.broadcastRequest(ctx, cluster, req)
	require.ErrorIs(t, err, vectorized.ErrMemoryBudgetExceeded, "memory admission precedes encoding and validation")
	release()
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Empty(t, cluster.messages)

	ctx = vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	_, release, err = plan.broadcastRequest(ctx, cluster, req)
	require.ErrorIs(t, err, native.ErrInvalidQuery)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	require.Positive(t, tracker.Used())
	release()
	release()
	require.Zero(t, tracker.Used(), "the caller releases the reservation after encoding fails")
	require.Empty(t, cluster.messages)
}

func TestPatternDistributedMultipleGroups(t *testing.T) {
	original := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	defer data.SetMeasureWireModeRaw(original)
	req := bareReq()
	req.Groups = []string{"first", "second"}
	req.Limit = 10
	schemas := []*databasev1.Measure{testMeasureSchema(), testMeasureSchema()}
	for i, schema := range schemas {
		schema.Metadata.Group = req.Groups[i]
		schema.TagFamilies = schema.TagFamilies[:1]
	}
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Name: "region", Id: 7}, Tags: []string{"region"}, Type: databasev1.IndexRule_TYPE_INVERTED}
	rules := [][]*databasev1.IndexRule{{rule}, {rule}}
	patterns := []model.IndexedPattern{{TagFamily: "default", TagName: "region", Pattern: "%a%", Escape: "\\", IgnoreCase: true}}
	plan, err := AnalyzeDistributed(req, schemas, rules, dispatchCfg(), patterns...)
	require.NoError(t, err)
	cluster := &patternCaptureCluster{}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 16<<20)
	result, err := plan.ExecuteBatches(executor.WithDistributedExecutionContext(ctx, cluster))
	require.NoError(t, err)
	require.NoError(t, result.Close())
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	require.Len(t, cluster.messages, 2)
	for i, message := range cluster.messages {
		require.Equal(t, req.Groups[i], message.Group())
		decoded, got, decodeErr := vmeasure.DecodeMeasurePatternRequest(message.Data().([]byte))
		require.NoError(t, decodeErr)
		require.Equal(t, []string{req.Groups[i]}, decoded.GetRequest().GetGroups())
		require.Equal(t, patterns, got)
	}
	rules[1] = nil
	_, err = AnalyzeDistributed(req, schemas, rules, dispatchCfg(), patterns...)
	require.ErrorContains(t, err, "whole-value inverted index")
	rules[1] = []*databasev1.IndexRule{proto.Clone(rule).(*databasev1.IndexRule)}
	rules[1][0].Analyzer = "simple"
	_, err = AnalyzeDistributed(req, schemas, rules, dispatchCfg(), patterns...)
	require.ErrorContains(t, err, "whole-value inverted index")
}
