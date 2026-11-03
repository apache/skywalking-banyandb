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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T08FixtureName identifies the independent nullable native-aggregation fixture.
const T08FixtureName = "ql_t08"

// T08GroupsFixtureName identifies the complete tuple-group output fixture.
const T08GroupsFixtureName = "ql_t08_groups"

type t08Row struct {
	Key    any
	V      any
	F      any
	Host   string
	Region string
	Millis int64
	RID    int64
}

func t08Rows(name string) []t08Row {
	if name == T08FixtureName {
		return []t08Row{
			{Millis: 0, RID: 1, Host: "a", Region: "east", V: int64(9007199254740993), F: 0.25},
			{Millis: 1000, RID: 2, Host: "a", Region: "east", V: int64(9007199254740995), F: 0.75},
			{Millis: 2000, RID: 3, Host: "a", Region: "east"},
			{Millis: 3000, RID: 4, Host: "a", Region: "west", Key: "", V: int64(2), F: 0.125},
			{Millis: 4000, RID: 5, Host: "b", Region: "east", Key: "x", V: int64(4), F: 1.5},
			{Millis: 5000, RID: 6, Host: "b", Region: "east", Key: "x"},
			{Millis: 6000, RID: 7, Host: "c", Region: "west"},
			{Millis: 7000, RID: 8, Host: "c", Region: "west"},
			{Millis: 8000, RID: 9, Host: "overflow", Region: "east", Key: "x", V: int64(math.MaxInt64), F: 2.0},
			{Millis: 9000, RID: 10, Host: "overflow", Region: "east", Key: "x", V: int64(1)},
		}
	}
	var rows []t08Row
	for group := range 129 {
		for ordinal := range 1 + group%3 {
			row := t08Row{
				Millis: int64(len(rows) * 100), RID: int64(len(rows) + 1), Host: fmt.Sprintf("host-%03d", group/2),
				Region: []string{"east", "west"}[group%2], Key: fmt.Sprintf("key-%03d", group),
				V: int64(group*3 + ordinal), F: float64(group%5)/4 + float64(ordinal)/8,
			}
			if group%13 == 0 {
				row.V, row.F = nil, nil
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// T08FixtureSchema returns a measure whose full entity is the host/region tuple.
func T08FixtureSchema(name string) *databasev1.Measure {
	schema := T07FixtureSchema(name)
	schema.Metadata = &commonv1.Metadata{Name: name, Group: FixtureGroup}
	schema.Entity.TagNames = []string{"host", "region"}
	schema.TagFamilies[0].Tags = []*databasev1.TagSpec{
		{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
		{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "key", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	return schema
}

// T08FixtureIndexRule supplies the non-entity key predicate's required index.
func T08FixtureIndexRule(name string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: name + "_key_idx", Group: FixtureGroup},
		Tags:     []string{"key"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword",
	}
}

// T08FixtureIndexBinding activates the key index over the fixture's lifetime.
func T08FixtureIndexBinding(name string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: name + "_key_binding", Group: FixtureGroup},
		Rules:    []string{T08FixtureIndexRule(name).GetMetadata().GetName()},
		Subject:  &databasev1.Subject{Name: name, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

// T08FixtureWrites writes stale and final versions for every logical input row.
func T08FixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	rows := t08Rows(name)
	writes := make([]*measurev1.WriteRequest, 0, len(rows)*2)
	for _, stale := range []bool{true, false} {
		for _, row := range rows {
			v, f := row.V, row.F
			if stale {
				v, f = int64(-999), float64(-999)
			}
			writes = append(writes, &measurev1.WriteRequest{
				Metadata: T08FixtureSchema(name).GetMetadata(), MessageId: uint64(len(writes) + 1),
				DataPointSpec: &measurev1.DataPointSpec{
					TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid", "host", "region", "key"}}},
					FieldNames:    []string{"v", "f"},
				},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						tagValue(row.RID), tagValue(row.Host), tagValue(row.Region), tagValue(row.Key),
					}}}, Fields: []*modelv1.FieldValue{fieldValue(v), fieldValue(f)},
				},
			})
		}
	}
	return writes
}

func t08ReadinessCases() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T08FixtureName, T08GroupsFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := t08Rows(name)
		for start := 0; start < len(rows); start += 16 {
			end := min(start+16, len(rows))
			var points []any
			for _, row := range rows[start:end] {
				point, err := t08AggregatePoint([]t08Row{row}, []t07Column{
					{Target: "rid"}, {Target: "host"}, {Target: "region"}, {Target: "key"}, {Target: "v"}, {Target: "f"},
				})
				if err != nil {
					return nil, err
				}
				point["timestamp"] = timePlaceholder(row.Millis)
				points = append(points, point)
			}
			query := "SELECT rid, host, region, key, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + name +
				" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" +
				timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
			c, err := t07ExpectedCase(fmt.Sprintf("t08_ready_%s_%d", name, start/16), query, points, []string{"t08.fixture"})
			if err != nil {
				return nil, err
			}
			c.Ticket, c.Ordered = "T08", true
			fixture.Cases = append(fixture.Cases, c)
		}
		indexed, indexedErr := t08IndexedReadiness(name)
		if indexedErr != nil {
			return nil, indexedErr
		}
		fixture.Cases = append(fixture.Cases, indexed)
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}

func t08IndexedReadiness(name string) (Case, error) {
	key := ""
	if name == T08GroupsFixtureName {
		key = "key-000"
	}
	var points []any
	for _, row := range t08Rows(name) {
		if row.Key != key {
			continue
		}
		point, err := t08AggregatePoint([]t08Row{row}, []t07Column{
			{Target: "rid"}, {Target: "host"}, {Target: "region"}, {Target: "key"}, {Target: "v"}, {Target: "f"},
		})
		if err != nil {
			return Case{}, err
		}
		point["timestamp"] = timePlaceholder(row.Millis)
		points = append(points, point)
	}
	query := "SELECT rid, host, region, key, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + name +
		" WHERE time >= '" + timePlaceholder(-1) + "' AND time < '" + timePlaceholder(100000) +
		"' AND key = '" + key + "' ORDER BY TIME ASC"
	c, err := t07ExpectedCase("t08_ready_indexed_"+name, query, points, []string{"t08.fixture"})
	c.Ticket, c.Ordered = "T08", true
	return c, err
}
