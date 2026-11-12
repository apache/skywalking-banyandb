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

package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

const (
	// queryTimeout bounds one query.
	queryTimeout = time.Minute
	// queryLimit is the limit of every compared query, far above what the e2e data holds.
	queryLimit = 10000
	// maxSamples bounds the (unit, resource) candidates tried for a sample row.
	maxSamples = 30
)

// errNoSample reports that no candidate produced a sample on the source cluster.
var errNoSample = errors.New("no candidate unit answered a sample on the source cluster")

// queryResult is one query-level assertion: the same query on both clusters, compared after
// normalization (every returned row encoded deterministically, then sorted), or, when compared
// is false, only run on the destination.
type queryResult struct {
	err      error
	name     string
	resource string
	srcRows  int
	dstRows  int
	equal    bool
	compared bool
}

// clusters holds the clients of the source and destination liaisons.
type clusters struct {
	src, dst *grpc.ClientConn
}

// queryPlan is what the queries need from artifact A: its manifest and its schema.
type queryPlan struct {
	m          *manifest.Manifest
	indexModes map[string]bool
	schemas    importer.Schemas
}

// runQueries runs the query-level assertions of the round trip against the source
// and destination liaisons, choosing the queried resources and time windows from artifact A:
//
//   - measureByEntity: a measure's points of one entity in one unit's stage and time range;
//   - measureIndexMode: the points of an index-mode measure on the destination (only non-zero
//     is asserted: the source keeps upserting them after the export);
//   - streamByEntity / streamByIndexedTag: a stream's elements of one entity, and of one value
//     of an indexed non-entity tag;
//   - traceOrdered / traceByID: every trace of one unit's window ordered by the timestamp
//     index, and the spans of one of them by trace id;
//   - topNResult: the _top_n_result points of one unit on the destination (only non-zero is
//     asserted: their rows are compared with artifact A by checker compare, while a plain query
//     on the source merges unmerged versions with other parameters than the export did).
//
// The sample (entity, tag value, trace id) is the first one the source answers, trying units
// from the newest; the compared query then runs on both clusters. Every result prints under
// queries.<name> with resource, srcRows, dstRows and, for compared ones, equal. The exit code is
// 1 when a query failed, differs, or found no rows on the destination.
func runQueries(args []string) int {
	if len(args) != 3 {
		usage()
	}
	p, err := loadQueryPlan(args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("queries: {}")
		return 1
	}
	var c clusters
	if c.src, err = grpc.NewClient(args[0], grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.src.Close()
	if c.dst, err = grpc.NewClient(args[1], grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer c.dst.Close()

	results := []queryResult{p.measureByEntity(c), p.measureIndexMode(c)}
	results = append(results, p.streamQueries(c)...)
	results = append(results, p.traceQueries(c)...)
	results = append(results, p.topNResult(c))
	code := 0
	fmt.Println("queries:")
	for _, r := range results {
		fmt.Printf("  %s:\n    resource: %q\n    srcRows: %d\n    dstRows: %d\n", r.name, r.resource, r.srcRows, r.dstRows)
		if r.compared {
			fmt.Printf("    equal: %t\n", r.equal)
		}
		if r.err != nil {
			fmt.Printf("    error: %s\n", yamlScalar(r.err.Error()))
			fmt.Fprintf(os.Stderr, "query %s (%s): %v\n", r.name, r.resource, r.err)
		}
		if r.err != nil || r.dstRows == 0 || (r.compared && !r.equal) {
			code = 1
		}
	}
	return code
}

func loadQueryPlan(dir string) (*queryPlan, error) {
	art, err := importer.OpenArtifact(context.Background(), dir, false)
	if err != nil {
		return nil, err
	}
	p := &queryPlan{m: art.Manifest, schemas: art.Schemas, indexModes: map[string]bool{}}
	for _, g := range art.Manifest.Groups {
		p.indexModes[g.Name] = g.IndexMode
	}
	return p, nil
}

// candidate is one resource with rows in one unit of artifact A.
type candidate struct {
	unit     *manifest.Unit
	resource string
}

func (c candidate) id() string {
	return c.unit.Group + "/" + c.resource
}

func (c candidate) key() importer.ResourceKey {
	return importer.ResourceKey{Group: c.unit.Group, Name: c.resource}
}

func (c candidate) stages() []string {
	if c.unit.Stage == "" {
		return nil
	}
	return []string{c.unit.Stage}
}

// window is the unit's time range, the end made inclusive.
func (c candidate) window() *modelv1.TimeRange {
	return &modelv1.TimeRange{
		Begin: timestamppb.New(time.Unix(0, c.unit.MinTimestamp)),
		End:   timestamppb.New(time.Unix(0, c.unit.MaxTimestamp+int64(time.Millisecond))),
	}
}

// candidates lists the resources with rows of the units of a catalog that accept accepts,
// newest unit first, at most maxSamples of them.
func (p *queryPlan) candidates(catalog string, accept func(u *manifest.Unit, resource string) bool) []candidate {
	var out []candidate
	for i := range p.m.Units {
		u := &p.m.Units[i]
		if u.Catalog != catalog {
			continue
		}
		rows := map[string]uint64{}
		for _, s := range u.Sources {
			for r, n := range s.RowCounts {
				rows[r] += n
			}
			for _, sh := range s.Shards {
				for r, n := range sh.RowCounts {
					rows[r] += n
				}
			}
		}
		for r, n := range rows {
			if n > 0 && accept(u, r) {
				out = append(out, candidate{unit: u, resource: r})
			}
		}
	}
	slices.SortFunc(out, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(b.unit.MaxTimestamp, a.unit.MaxTimestamp), cmp.Compare(a.id(), b.id()),
			cmp.Compare(a.unit.Stage, b.unit.Stage), cmp.Compare(a.unit.Segment, b.unit.Segment))
	})
	return out[:min(len(out), maxSamples)]
}

// compare runs one query on both clusters and fills the counts and the verdict.
func compare(r *queryResult, run func(conn *grpc.ClientConn) ([]string, error), c clusters) {
	r.compared = true
	src, err := run(c.src)
	if err != nil {
		r.err = fmt.Errorf("source: %w", err)
		return
	}
	dst, err := run(c.dst)
	if err != nil {
		r.err = fmt.Errorf("destination: %w", err)
		return
	}
	slices.Sort(src)
	slices.Sort(dst)
	r.srcRows, r.dstRows, r.equal = len(src), len(dst), slices.Equal(src, dst)
}

// encodeAll encodes every message deterministically so that equal rows compare equal.
func encodeAll[T proto.Message](msgs []T) ([]string, error) {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		if err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}

func eq(name string, v *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: name, Op: modelv1.Condition_BINARY_OP_EQ, Value: v}}}
}

func and(cs []*modelv1.Criteria) *modelv1.Criteria {
	out := cs[0]
	for _, c := range cs[1:] {
		out = &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{Op: modelv1.LogicalExpression_LOGICAL_OP_AND, Left: out, Right: c}}}
	}
	return out
}

// tagValue finds a tag in returned tag families; nil when it is absent or null.
func tagValue(families []*modelv1.TagFamily, name string) *modelv1.TagValue {
	for _, f := range families {
		for _, t := range f.GetTags() {
			if t.GetKey() != name || t.GetValue().GetValue() == nil {
				continue
			}
			if _, isNull := t.GetValue().GetValue().(*modelv1.TagValue_Null); !isNull {
				return t.GetValue()
			}
		}
	}
	return nil
}

// entityCriteria builds the conjunction of the entity tags' values of a sample row; false when
// one of them is missing.
func entityCriteria(names []string, families []*modelv1.TagFamily) (*modelv1.Criteria, bool) {
	conds := make([]*modelv1.Criteria, 0, len(names))
	for _, n := range names {
		v := tagValue(families, n)
		if v == nil {
			return nil, false
		}
		conds = append(conds, eq(n, v))
	}
	return and(conds), len(conds) > 0
}

func tagProjection(families []*databasev1.TagFamilySpec) *modelv1.TagProjection {
	tp := &modelv1.TagProjection{}
	for _, f := range families {
		names := make([]string, 0, len(f.GetTags()))
		for _, t := range f.GetTags() {
			names = append(names, t.GetName())
		}
		tp.TagFamilies = append(tp.TagFamilies, &modelv1.TagProjection_TagFamily{Name: f.GetName(), Tags: names})
	}
	return tp
}

func measurePoints(conn *grpc.ClientConn, spec *databasev1.Measure, c candidate, window *modelv1.TimeRange,
	criteria *modelv1.Criteria, limit uint32,
) ([]*measurev1.DataPoint, error) {
	req := &measurev1.QueryRequest{
		Groups: []string{c.unit.Group}, Name: c.resource, Stages: c.stages(), TimeRange: window, Criteria: criteria, Limit: limit,
		TagProjection: tagProjection(spec.GetTagFamilies()),
	}
	names := make([]string, 0, len(spec.GetFields()))
	for _, f := range spec.GetFields() {
		names = append(names, f.GetName())
	}
	if len(names) > 0 {
		req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: names}
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, req)
	return resp.GetDataPoints(), err
}

func (p *queryPlan) measureByEntity(c clusters) queryResult {
	r := queryResult{name: "measureByEntity", err: errNoSample}
	for _, cand := range p.candidates("MEASURE", func(u *manifest.Unit, res string) bool {
		return !p.indexModes[u.Group] && res != csvfmt.TopNResultMeasure
	}) {
		spec := p.schemas.Measures[cand.key()]
		if spec == nil || cand.unit.MaxTimestamp == 0 {
			continue
		}
		sample, err := measurePoints(c.src, spec, cand, cand.window(), nil, 1)
		if err != nil || len(sample) == 0 {
			continue
		}
		criteria, ok := entityCriteria(spec.GetEntity().GetTagNames(), sample[0].GetTagFamilies())
		if !ok {
			continue
		}
		r = queryResult{name: r.name, resource: cand.id()}
		compare(&r, func(conn *grpc.ClientConn) ([]string, error) {
			points, err := measurePoints(conn, spec, cand, cand.window(), criteria, queryLimit)
			if err != nil {
				return nil, err
			}
			return encodeAll(points)
		}, c)
		return r
	}
	return r
}

// measureIndexMode counts the points of the first index-mode measure with rows on the
// destination, from the start of its segment to now. Index-mode units carry no time range.
func (p *queryPlan) measureIndexMode(c clusters) queryResult {
	r := queryResult{name: "measureIndexMode", err: errNoSample}
	for _, cand := range p.candidates("MEASURE", func(u *manifest.Unit, _ string) bool { return p.indexModes[u.Group] }) {
		spec := p.schemas.Measures[cand.key()]
		start, err := segmentStart(p.m, cand.unit)
		if spec == nil || err != nil {
			continue
		}
		window := &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, start)), End: timestamppb.New(time.Now().Add(time.Hour).Truncate(time.Millisecond))}
		r = countOnBoth(r.name, spec, cand, window, c)
		if r.dstRows > 0 || r.err != nil {
			return r
		}
	}
	return r
}

// countOnBoth counts the points of one query on both clusters without comparing them.
func countOnBoth(name string, spec *databasev1.Measure, cand candidate, window *modelv1.TimeRange, c clusters) queryResult {
	src, srcErr := measurePoints(c.src, spec, cand, window, nil, queryLimit)
	dst, dstErr := measurePoints(c.dst, spec, cand, window, nil, queryLimit)
	return queryResult{name: name, resource: cand.id(), srcRows: len(src), dstRows: len(dst), err: errors.Join(srcErr, dstErr)}
}

// topNResult counts the _top_n_result points of the first unit holding them on the destination,
// and on the source for information. The values are not compared between the clusters: a plain
// query on the source merges several stored versions of a point without the entity's Top-N
// parameters, while the destination holds the one row the export merged with them. checker
// compare checks those rows against artifact A instead.
func (p *queryPlan) topNResult(c clusters) queryResult {
	r := queryResult{name: "topNResult", err: errNoSample}
	for _, cand := range p.candidates("MEASURE", func(_ *manifest.Unit, res string) bool { return res == csvfmt.TopNResultMeasure }) {
		spec := p.schemas.Measures[cand.key()]
		if spec == nil || cand.unit.MaxTimestamp == 0 {
			continue
		}
		r = countOnBoth(r.name, spec, cand, cand.window(), c)
		if r.dstRows > 0 || r.err != nil {
			return r
		}
	}
	return r
}

func streamElements(conn *grpc.ClientConn, spec *databasev1.Stream, c candidate, criteria *modelv1.Criteria, limit uint32) ([]*streamv1.Element, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups: []string{c.unit.Group}, Name: c.resource, Stages: c.stages(), TimeRange: c.window(), Criteria: criteria, Limit: limit,
		Projection: tagProjection(spec.GetTagFamilies()),
	})
	return resp.GetElements(), err
}

// boundRules lists the index rules bound to a resource, in the sorted order of the bindings.
func (p *queryPlan) boundRules(catalog commonv1.Catalog, group, name string) []string {
	var out []string
	for _, k := range slices.SortedFunc(maps.Keys(p.schemas.IndexRuleBindings), compareKeys) {
		b := p.schemas.IndexRuleBindings[k]
		if k.Group == group && b.GetSubject().GetCatalog() == catalog && b.GetSubject().GetName() == name {
			out = append(out, b.GetRules()...)
		}
	}
	return out
}

// boundTags returns the sorted tags of the single-tag index rules bound to a resource.
func (p *queryPlan) boundTags(catalog commonv1.Catalog, group, name string) []string {
	set := map[string]struct{}{}
	for _, rule := range p.boundRules(catalog, group, name) {
		ir := p.schemas.IndexRules[importer.ResourceKey{Group: group, Name: rule}]
		if ir != nil && len(ir.GetTags()) == 1 {
			set[ir.GetTags()[0]] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// orderRule returns an index rule bound to the trace that ends with its timestamp tag, the
// shape of the tree rules a trace is ordered by (e.g. service_id, is_error, start_time).
func (p *queryPlan) orderRule(group, name, timestampTag string) (string, bool) {
	for _, rule := range p.boundRules(commonv1.Catalog_CATALOG_TRACE, group, name) {
		ir := p.schemas.IndexRules[importer.ResourceKey{Group: group, Name: rule}]
		if tags := ir.GetTags(); len(tags) > 0 && tags[len(tags)-1] == timestampTag {
			return rule, true
		}
	}
	return "", false
}

func compareKeys(a, b importer.ResourceKey) int {
	return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Name, b.Name))
}

// streamQueries fills streamByEntity from the first candidate that answers a sample, and
// streamByIndexedTag from the first candidate whose sample has a value for an indexed non-entity
// tag, which may be a later one.
func (p *queryPlan) streamQueries(c clusters) []queryResult {
	byEntity := queryResult{name: "streamByEntity", err: errNoSample}
	byTag := queryResult{name: "streamByIndexedTag", err: errNoSample}
	sampled := false
	for _, cand := range p.candidates("STREAM", func(*manifest.Unit, string) bool { return true }) {
		spec := p.schemas.Streams[cand.key()]
		if spec == nil || cand.unit.MaxTimestamp == 0 {
			continue
		}
		sample, err := streamElements(c.src, spec, cand, nil, 1)
		if err != nil || len(sample) == 0 {
			continue
		}
		entity := spec.GetEntity().GetTagNames()
		criteria, ok := entityCriteria(entity, sample[0].GetTagFamilies())
		if !ok {
			continue
		}
		run := func(crit *modelv1.Criteria) func(conn *grpc.ClientConn) ([]string, error) {
			return func(conn *grpc.ClientConn) ([]string, error) {
				elements, err := streamElements(conn, spec, cand, crit, queryLimit)
				if err != nil {
					return nil, err
				}
				return encodeAll(elements)
			}
		}
		sampled = true
		if !byEntity.compared {
			byEntity = queryResult{name: byEntity.name, resource: cand.id()}
			compare(&byEntity, run(criteria), c)
		}
		for _, t := range p.boundTags(commonv1.Catalog_CATALOG_STREAM, cand.unit.Group, cand.resource) {
			v := tagValue(sample[0].GetTagFamilies(), t)
			if slices.Contains(entity, t) || v == nil {
				continue
			}
			byTag = queryResult{name: byTag.name, resource: cand.id() + " " + t}
			compare(&byTag, run(eq(t, v)), c)
			return []queryResult{byEntity, byTag}
		}
	}
	if sampled {
		byTag.err = errors.New("no sampled stream has a non-null indexed non-entity tag")
	}
	return []queryResult{byEntity, byTag}
}

func traceSpans(conn *grpc.ClientConn, spec *databasev1.Trace, c candidate, criteria *modelv1.Criteria, order *modelv1.QueryOrder,
	limit uint32,
) ([]*tracev1.Trace, error) {
	names := make([]string, 0, len(spec.GetTags()))
	for _, t := range spec.GetTags() {
		names = append(names, t.GetName())
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	resp, err := tracev1.NewTraceServiceClient(conn).Query(ctx, &tracev1.QueryRequest{
		Groups: []string{c.unit.Group}, Name: c.resource, Stages: c.stages(), TimeRange: c.window(), Criteria: criteria,
		OrderBy: order, Limit: limit, TagProjection: names,
	})
	return resp.GetTraces(), err
}

// encodeTraces encodes each trace as its id and its spans sorted by encoding, so that the
// order spans come back in does not matter.
func encodeTraces(traces []*tracev1.Trace) ([]string, error) {
	out := make([]string, 0, len(traces))
	for _, t := range traces {
		spans, err := encodeAll(t.GetSpans())
		if err != nil {
			return nil, err
		}
		slices.Sort(spans)
		out = append(out, t.GetTraceId()+"\x00"+strings.Join(spans, "\x00"))
	}
	return out, nil
}

func (p *queryPlan) traceQueries(c clusters) []queryResult {
	ordered := queryResult{name: "traceOrdered", err: errNoSample}
	byID := queryResult{name: "traceByID", err: errNoSample}
	for _, cand := range p.candidates("TRACE", func(*manifest.Unit, string) bool { return true }) {
		spec := p.schemas.Traces[cand.key()]
		if spec == nil || cand.unit.MaxTimestamp == 0 {
			continue
		}
		rule, ok := p.orderRule(cand.unit.Group, cand.resource, spec.GetTimestampTagName())
		if !ok {
			ordered.err = fmt.Errorf("%s has no index rule on its timestamp tag %s", cand.id(), spec.GetTimestampTagName())
			continue
		}
		order := &modelv1.QueryOrder{IndexRuleName: rule, Sort: modelv1.Sort_SORT_DESC}
		sample, err := traceSpans(c.src, spec, cand, nil, order, 1)
		if err != nil || len(sample) == 0 {
			continue
		}
		ordered = queryResult{name: ordered.name, resource: cand.id() + " by " + rule}
		// The query covers the whole window (queryLimit is far above the traces it holds), so no
		// limit boundary is left for traces with equal timestamps to fall on; ties may still come
		// back in either order, so the ids are compared as a set.
		compare(&ordered, func(conn *grpc.ClientConn) ([]string, error) {
			traces, err := traceSpans(conn, spec, cand, nil, order, queryLimit)
			ids := make([]string, 0, len(traces))
			for _, t := range traces {
				ids = append(ids, t.GetTraceId())
			}
			return ids, err
		}, c)
		byID = queryResult{name: byID.name, resource: cand.id()}
		criteria := eq(spec.GetTraceIdTagName(), &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: sample[0].GetTraceId()}}})
		compare(&byID, func(conn *grpc.ClientConn) ([]string, error) {
			traces, err := traceSpans(conn, spec, cand, criteria, nil, queryLimit)
			if err != nil {
				return nil, err
			}
			return encodeTraces(traces)
		}, c)
		return []queryResult{ordered, byID}
	}
	return []queryResult{ordered, byID}
}
