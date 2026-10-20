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
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// countQueryTimeout bounds one count query.
const countQueryTimeout = time.Minute

// unitRows are the rows a unit exported for one resource and the span of their timestamps.
type unitRows struct {
	n            uint64
	minTS, maxTS int64
}

// track counts an exported row of a resource in its unit; a row without a valid time was
// reported already and is left out.
func (g *groupResult) track(u *manifest.Unit, resource, tsCell string) {
	ts, err := strconv.ParseInt(tsCell, 10, 64)
	if err != nil {
		return
	}
	byResource := g.units[u]
	if byResource == nil {
		byResource = map[string]*unitRows{}
		g.units[u] = byResource
	}
	r := byResource[resource]
	if r == nil {
		r = &unitRows{minTS: math.MaxInt64, maxTS: math.MinInt64}
		byResource[resource] = r
	}
	r.n++
	r.minTS, r.maxTS = min(r.minTS, ts), max(r.maxTS, ts)
}

// queryCountMismatches asks the cluster, through the liaison at addr, how many points every
// measure of every exported unit holds, with a COUNT over the measure's first entity tag (set
// on every point) in the unit's stage and time range, and compares it with the rows exported.
// A query answers the latest version of a point, as the export writes it, so the two are
// equal. An index-mode unit records no time range and is counted over its rows' span. The
// manifest's time range is that of the data the snapshot held, so points written later at the
// current time stay out of it.
func queryCountMismatches(addr, dir string, results map[string]*groupResult) []string {
	measures, err := readSchema(dir, schemasync.KindMeasure, func() *databasev1.Measure { return &databasev1.Measure{} })
	if err != nil {
		return []string{"read the measure schema: " + err.Error()}
	}
	specs := map[string]*databasev1.Measure{}
	for _, ms := range measures {
		specs[ms.GetMetadata().GetGroup()+"/"+ms.GetMetadata().GetName()] = ms
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return []string{fmt.Sprintf("connect to %s: %v", addr, err)}
	}
	defer conn.Close()
	client := measurev1.NewMeasureServiceClient(conn)
	var out []string
	for _, group := range slices.Sorted(maps.Keys(results)) {
		g := results[group]
		units := slices.SortedFunc(maps.Keys(g.units), func(a, b *manifest.Unit) int {
			return cmp.Or(cmp.Compare(a.Segment, b.Segment), cmp.Compare(a.Stage, b.Stage))
		})
		for _, u := range units {
			for _, resource := range slices.Sorted(maps.Keys(g.units[u])) {
				rows := g.units[u][resource]
				label := fmt.Sprintf("measure %s/%s segment %s stage %s", group, resource, u.Segment, u.Stage)
				begin, end := u.MinTimestamp, u.MaxTimestamp
				if end == 0 {
					begin, end = rows.minTS, rows.maxTS
				}
				count, countErr := countPoints(client, specs[group+"/"+resource], group, resource, u.Stage, begin, end)
				if countErr != nil {
					out = append(out, fmt.Sprintf("%s: count query: %v", label, countErr))
					continue
				}
				g.queried += count
				if count != rows.n {
					out = append(out, fmt.Sprintf("%s: exported %d rows, a count query answers %d", label, rows.n, count))
				}
			}
		}
	}
	return out
}

// countPoints counts the points of a measure in a stage between begin and end, both inclusive,
// with COUNT over its first entity tag.
func countPoints(client measurev1.MeasureServiceClient, spec *databasev1.Measure, group, name, stage string, begin, end int64) (uint64, error) {
	if spec == nil || len(spec.GetEntity().GetTagNames()) == 0 {
		return 0, fmt.Errorf("the exported schema has no entity for %s/%s", group, name)
	}
	tag := spec.GetEntity().GetTagNames()[0]
	var family string
	for _, f := range spec.GetTagFamilies() {
		if slices.ContainsFunc(f.GetTags(), func(t *databasev1.TagSpec) bool { return t.GetName() == tag }) {
			family = f.GetName()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), countQueryTimeout)
	defer cancel()
	resp, err := client.Query(ctx, &measurev1.QueryRequest{
		Groups: []string{group}, Name: name, Stages: []string{stage},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Unix(0, begin)),
			End:   timestamppb.New(time.Unix(0, end+int64(time.Millisecond))),
		},
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: family, Tags: []string{tag}}}},
		Agg:           &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT, TagName: tag, TagFamily: family},
	})
	if err != nil {
		return 0, err
	}
	// The count comes back as the one int value of the answer's only data point.
	for _, dp := range resp.GetDataPoints() {
		for _, f := range dp.GetFields() {
			if v := f.GetValue().GetInt(); v != nil {
				return uint64(v.GetValue()), nil
			}
		}
		for _, tf := range dp.GetTagFamilies() {
			for _, t := range tf.GetTags() {
				if v := t.GetValue().GetInt(); v != nil {
					return uint64(v.GetValue()), nil
				}
			}
		}
	}
	return 0, nil
}
