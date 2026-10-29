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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T07FixtureName isolates the six-row SQL aggregation fixture F.
const T07FixtureName = "ql_t07"

// T07DecimalFixtureName identifies the bounded decimal aggregation fixture.
const T07DecimalFixtureName = "ql_t07_decimal"

// T07GroupsFixtureName identifies the complete-input and deduplication fixture.
const T07GroupsFixtureName = "ql_t07_groups"

func t07Rows(name string) []fixtureRow {
	if name == T07FixtureName {
		return fixtureRows()[:6]
	}
	if name == T07DecimalFixtureName {
		var rows []fixtureRow
		for i := range 10 {
			rows = append(rows, fixtureRow{Millis: int64(i * 1000), RID: int64(i + 1), Host: "a", Zone: "a", V: int64(999999999999999999), F: nil})
		}
		rows = append(rows,
			fixtureRow{Millis: 10000, RID: 11, Host: "b", Zone: "b", V: int64(-999999999999999999)},
			fixtureRow{Millis: 11000, RID: 12, Host: "b", Zone: "b", V: int64(-1)},
			fixtureRow{Millis: 12000, RID: 13, Host: "c", Zone: "c"},
		)
		return rows
	}
	var rows []fixtureRow
	for group := range 513 {
		for ordinal := range 1 + group%3 {
			row := fixtureRow{
				Millis: int64(len(rows) * 100), RID: int64(len(rows) + 1), Host: fmt.Sprintf("partition-%02d", group%2),
				Zone: fmt.Sprintf("group-%03d", group), V: int64(group%11 + ordinal), F: float64(group%7) + float64(ordinal)/4,
			}
			if len(rows)%5 == 0 {
				row.V = nil
			}
			if len(rows)%7 == 0 {
				row.F = nil
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// T07FixtureSchema returns F or the 513-group extension with identical types.
func T07FixtureSchema(name string) *databasev1.Measure {
	schema := FixtureSchema()
	schema.Metadata = &commonv1.Metadata{Name: name, Group: FixtureGroup}
	schema.Entity = &databasev1.Entity{TagNames: []string{"host"}}
	schema.TagFamilies[0].Tags = schema.TagFamilies[0].Tags[:3]
	return schema
}

// T07FixtureWrites contains an old version followed by each final fixture row.
// Identical series/timestamp pairs must be deduplicated before SQL aggregation.
func T07FixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	rows := t07Rows(name)
	writes := make([]*measurev1.WriteRequest, 0, len(rows)*2)
	for _, stale := range []bool{true, false} {
		for _, row := range rows {
			v, f := row.V, row.F
			if stale {
				v, f = int64(-999), float64(-999)
			}
			writes = append(writes, &measurev1.WriteRequest{
				Metadata: T07FixtureSchema(name).GetMetadata(), MessageId: uint64(len(writes) + 1),
				DataPointSpec: &measurev1.DataPointSpec{
					TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid", "host", "zone"}}},
					FieldNames:    []string{"v", "f"},
				},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
						tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone),
					}}}, Fields: []*modelv1.FieldValue{fieldValue(v), fieldValue(f)},
				},
			})
		}
	}
	return writes
}

func t07ReadinessCases() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T07FixtureName, T07GroupsFixtureName, T07DecimalFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := t07Rows(name)
		for start := 0; start < len(rows); start += 128 {
			end := min(start+128, len(rows))
			var points []any
			for _, row := range rows[start:end] {
				points = append(points, projectRow(row, []string{"rid", "host", "zone", "v", "f"}, true))
			}
			query := "SELECT rid, host, zone, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + name +
				" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" +
				timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
			c, err := t07ExpectedCase(fmt.Sprintf("t07_ready_%s_%d", name, start/128), query, points, []string{"t07.fixture"})
			if err != nil {
				return nil, err
			}
			c.Ordered = true
			fixture.Cases = append(fixture.Cases, c)
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}
