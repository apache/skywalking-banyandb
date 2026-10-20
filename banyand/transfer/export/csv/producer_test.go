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

package csv

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// recordingRun returns a unit run whose frames are recorded.
func recordingRun(repo metadata.Repo) (*unitRun, *[]*transferv1.ExportResponse) {
	var sent []*transferv1.ExportResponse
	settings := &streamSettings{send: func(r *transferv1.ExportResponse) error {
		sent = append(sent, r)
		return nil
	}}
	return newUnitRun(context.Background(), repo, settings), &sent
}

func traceTagsOf(names ...string) []*databasev1.TraceTagSpec {
	out := []*databasev1.TraceTagSpec{{Name: "tid", Type: databasev1.TagType_TAG_TYPE_STRING}, {Name: "sid", Type: databasev1.TagType_TAG_TYPE_STRING}}
	for _, n := range names {
		out = append(out, &databasev1.TraceTagSpec{Name: n, Type: databasev1.TagType_TAG_TYPE_STRING})
	}
	return out
}

func TestTraceCSV_SignatureCollisionIsRefused(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := metadata.NewMockRepo(ctrl)
	traceReg := schema.NewMockTrace(ctrl)
	repo.EXPECT().TraceRegistry().Return(traceReg).AnyTimes()
	newTrace := func(name, idTag string, tags ...string) *databasev1.Trace {
		return &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: name, Group: "g"}, Tags: traceTagsOf(tags...),
			TraceIdTagName: "tid", SpanIdTagName: "sid", TimestampTagName: idTag,
		}
	}
	// The id tags do not count: both traces store exactly {a, b}.
	traceReg.EXPECT().ListTrace(gomock.Any(), gomock.Any()).Return([]*databasev1.Trace{
		newTrace("t1", "a", "a", "b"), newTrace("t2", "b", "b", "a"),
	}, nil)
	run, sent := recordingRun(repo)
	err := produceTraceCSV(run, &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_TRACE, Group: "g", SegmentSuffix: "20260101"}, t.TempDir())
	require.Equal(t, codes.FailedPrecondition, status.Code(err), err)
	require.Contains(t, err.Error(), "traces t1 and t2 in group g have the same tag set; export it in native format")
	require.Empty(t, *sent)
}

func TestTraceResolver_SupersetFallback(t *testing.T) {
	newTrace := func(name string, tags ...string) *databasev1.Trace {
		return &databasev1.Trace{
			Metadata: &commonv1.Metadata{Name: name, Group: "g"}, Tags: traceTagsOf(tags...), TraceIdTagName: "tid", SpanIdTagName: "sid",
		}
	}
	traces := []*databasev1.Trace{newTrace("segment", "a", "b", "c"), newTrace("zipkin", "a", "d")}
	layout, err := csvfmt.TraceLayout(traces)
	require.NoError(t, err)
	resolver, _, err := newTraceResolver("g", traces, layout)
	require.NoError(t, err)
	row := func(names ...string) map[string][]byte {
		out := map[string][]byte{}
		for _, n := range names {
			out[n] = nil
		}
		return out
	}
	nameOf := func(res *traceResource) string {
		if res == nil {
			return ""
		}
		return res.name
	}
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b", "c"))), "exact match")
	require.Equal(t, "zipkin", nameOf(resolver.resolve(row("a", "d"))), "exact match")
	// A row written before c was added to segment: only segment holds {a, b}.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b"))), "unique superset")
	require.Equal(t, "segment", nameOf(resolver.resolve(row("b", "a"))), "cached superset")
	require.Equal(t, "zipkin", nameOf(resolver.resolve(row("d"))), "unique superset")
	// Both traces hold {a}; no trace holds x.
	require.Nil(t, resolver.resolve(row("a")), "ambiguous superset")
	require.Nil(t, resolver.resolve(row("b", "d")), "no superset")
	// x was deleted from the schema: it does not keep the row from its trace.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b", "c", "x"))), "deleted tag ignored")
	require.Nil(t, resolver.resolve(row("a", "x")), "deleted tag ignored, ambiguous superset")
	// A merge renamed b to b#str and b#int: both still hold tag b.
	require.Equal(t, "segment", nameOf(resolver.resolve(row("a", "b#str", "b#int", "c"))), "typed columns")
}

func TestTraceSignature(t *testing.T) {
	tr := &databasev1.Trace{Tags: traceTagsOf("z", "a"), TraceIdTagName: "tid", SpanIdTagName: "sid"}
	declared := map[string]struct{}{"a": {}, "z": {}}
	sig, _ := rowSignature(map[string][]byte{"a": nil, "z": nil}, declared, nil)
	require.Equal(t, traceSignature(tr), sig)
	other, _ := rowSignature(map[string][]byte{"a": nil}, declared, nil)
	require.NotEqual(t, traceSignature(tr), other)
}

// measureRepo mocks the registries the measure producer reads.
func measureRepo(t *testing.T, measures ...*databasev1.Measure) metadata.Repo {
	ctrl := gomock.NewController(t)
	repo := metadata.NewMockRepo(ctrl)
	measureReg := schema.NewMockMeasure(ctrl)
	repo.EXPECT().MeasureRegistry().Return(measureReg).AnyTimes()
	repo.EXPECT().IndexRuleRegistry().Return(emptyRules{}).AnyTimes()
	repo.EXPECT().IndexRuleBindingRegistry().Return(emptyBindings{}).AnyTimes()
	measureReg.EXPECT().ListMeasure(gomock.Any(), gomock.Any()).Return(measures, nil).AnyTimes()
	return repo
}

// emptyRules and emptyBindings are registries without any index rule or binding.
type emptyRules struct{ schema.IndexRule }

func (emptyRules) ListIndexRule(context.Context, schema.ListOpt) ([]*databasev1.IndexRule, error) {
	return nil, nil
}

type emptyBindings struct{ schema.IndexRuleBinding }

func (emptyBindings) ListIndexRuleBinding(context.Context, schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	return nil, nil
}

func simpleMeasure(name string, indexMode bool) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: name, Group: "g"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
		Entity:    &databasev1.Entity{TagNames: []string{"id"}},
		IndexMode: indexMode,
	}
}

// TestMeasureCSV_EmptyUnit covers units without rows: one without shards (a segment created
// ahead of time) is silent, one whose shards yield no row warns. TestProducer_RealEngineData
// covers the skipped Top-N results with real parts.
func TestMeasureCSV_EmptyUnit(t *testing.T) {
	repo := measureRepo(t, simpleMeasure(csvfmt.TopNResultMeasure, false), simpleMeasure("cpm", false))
	run, sent := recordingRun(repo)
	unit := &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "g", SegmentSuffix: "20260101"}
	require.NoError(t, produceMeasureCSV(run, unit, t.TempDir(), nil))
	require.Empty(t, *sent)
	require.Empty(t, run.finish(false), "a unit without shards has nothing to export")

	segDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(segDir, shardDirPrefix+"0"), 0o700))
	run, sent = recordingRun(repo)
	unit.ShardIds = []uint32{0}
	require.NoError(t, produceMeasureCSV(run, unit, segDir, nil))
	require.Empty(t, *sent)
	require.Equal(t, []string{"unit has no rows"}, run.finish(true))
}

func TestMeasureCSV_IndexModeNeedsTheReader(t *testing.T) {
	run, _ := recordingRun(measureRepo(t, simpleMeasure("traffic", true)))
	unit := &transferv1.SegmentUnit{Catalog: commonv1.Catalog_CATALOG_MEASURE, Group: "g", SegmentSuffix: "20260101"}
	err := produceMeasureCSV(run, unit, t.TempDir(), nil)
	require.Equal(t, codes.Internal, status.Code(err), err)
}

func TestRuleToTagByMeasure(t *testing.T) {
	m := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "cpm"},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "layer", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "codes", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
		}}},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Name: "r1", Id: 11}, Tags: []string{"layer"}},
		{Metadata: &commonv1.Metadata{Name: "r2", Id: 12}, Tags: []string{"codes"}},
		{Metadata: &commonv1.Metadata{Name: "multi", Id: 13}, Tags: []string{"layer", "codes"}},
	}
	bindings := []*databasev1.IndexRuleBinding{{
		Subject: &databasev1.Subject{Name: "cpm", Catalog: commonv1.Catalog_CATALOG_MEASURE},
		Rules:   []string{"r1", "r2", "multi", "missing"},
	}}
	got := ruleToTagByMeasure([]*databasev1.Measure{m}, rules, bindings)["cpm"]
	require.Len(t, got, 2, "multi-tag and unknown rules are skipped")
	require.Equal(t, "default", got[11].Family)
	require.Equal(t, "layer", got[11].Name)
	require.Equal(t, "codes", got[12].Name)
}

func TestMultiTagRuleWarnings(t *testing.T) {
	m := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "cpm"},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "layer", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "rack", Type: databasev1.TagType_TAG_TYPE_STRING},
		}}},
	}
	rules := []*databasev1.IndexRule{
		{Metadata: &commonv1.Metadata{Name: "layer", Id: 11}, Tags: []string{"layer"}},
		{Metadata: &commonv1.Metadata{Name: "svc_zone", Id: 12}, Tags: []string{"svc", "zone"}},
		{Metadata: &commonv1.Metadata{Name: "layer_rack", Id: 13}, Tags: []string{"layer", "rack"}},
		{Metadata: &commonv1.Metadata{Name: "svc_layer", Id: 14}, Tags: []string{"svc", "layer"}},
		{Metadata: &commonv1.Metadata{Name: "unbound", Id: 15}, Tags: []string{"zone", "rack"}},
		{Metadata: &commonv1.Metadata{Name: "zone_gone", Id: 16}, Tags: []string{"zone", "gone"}},
	}
	bindings := []*databasev1.IndexRuleBinding{{
		Subject: &databasev1.Subject{Name: "cpm", Catalog: commonv1.Catalog_CATALOG_MEASURE},
		Rules:   []string{"layer", "svc_zone", "layer_rack", "svc_layer", "zone_gone"},
	}}
	// The entity tag always comes from the series. layer has a single-tag rule, but the write
	// path may store it under a multi-tag rule bound before it, so it may be lost too. The
	// write path ignores zone_gone, whose tag gone the schema no longer declares.
	require.Equal(t, map[string][]string{"cpm": {
		"measure cpm: tags [zone] are indexed by multi-tag rule svc_zone, which may hold their values; such values cannot be exported and their cells are null",
		"measure cpm: tags [layer rack] are indexed by multi-tag rule layer_rack, which may hold their values; such values cannot be exported and their cells are null",
		"measure cpm: tags [layer] are indexed by multi-tag rule svc_layer, which may hold their values; such values cannot be exported and their cells are null",
	}}, multiTagRuleWarnings([]*databasev1.Measure{m}, rules, bindings))
}

func TestReadError(t *testing.T) {
	require.NoError(t, readError("x", nil))
	require.Equal(t, codes.FailedPrecondition, status.Code(readError("x", fmt.Errorf("open: %w", fs.ErrNotExist))))
	require.Equal(t, codes.Canceled, status.Code(readError("x", context.Canceled)))
	require.Equal(t, codes.Unavailable, status.Code(readError("x", status.Error(codes.Unavailable, "gone"))))
	require.Equal(t, codes.Internal, status.Code(readError("x", errors.New("boom"))))
}

func TestReadOnlyFSReportsMissingDirectory(t *testing.T) {
	rofs := newReadOnlyFS()
	require.NotPanics(t, func() { rofs.ReadDir(filepath.Join(t.TempDir(), "gone")) })
	require.ErrorIs(t, rofs.err, fs.ErrNotExist)
}

func TestLimiters(t *testing.T) {
	require.Nil(t, newByteLimiter(0))
	require.Nil(t, newRowLimiter(0))
	require.Equal(t, 1, newByteLimiter(1).Burst(), "one second's worth")
	require.Equal(t, 4*chunkSize, newByteLimiter(4*chunkSize).Burst())
	require.Equal(t, 1, newRowLimiter(1).Burst())
	// A chunk always fits the burst: a rate below chunkSize sends one second's worth at a time.
	require.Equal(t, chunkSize, chunkLimit(nil))
	require.Equal(t, 1, chunkLimit(newByteLimiter(1)))
	require.Equal(t, 64<<10, chunkLimit(newByteLimiter(64<<10)))
	require.Equal(t, chunkSize, chunkLimit(newByteLimiter(4*chunkSize)))
	require.NoError(t, waitN(context.Background(), newByteLimiter(1), chunkLimit(newByteLimiter(1))))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, codes.Canceled, status.Code(waitN(ctx, newRowLimiter(1), 1)))

	// A full row bucket does not wait; an empty one at 1 row/s waits about a second.
	rows := newRowLimiter(1)
	require.Zero(t, rowWait(rows))
	require.NoError(t, waitN(context.Background(), rows, 1))
	require.InDelta(t, time.Second, rowWait(rows), float64(100*time.Millisecond))
}
