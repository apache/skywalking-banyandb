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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T16FixtureName isolates the published six-row F grouping fixture.
const T16FixtureName = "ql_t16"

// T16StarFixtureName has exactly two columns to pin ordinals after star expansion.
const T16StarFixtureName = "ql_t16_star"

// T16BulkFixtureName spans multiple raw batches with one deduplicated host entity.
const T16BulkFixtureName = "ql_t16_bulk"

func t16Rows(name string) []fixtureRow {
	if name != T16BulkFixtureName {
		return fixtureRows()[:6]
	}
	rows := make([]fixtureRow, 1153)
	for ordinal := range rows {
		rows[ordinal] = fixtureRow{
			RID: int64(ordinal + 1), Host: "bulk", Zone: "selected", Millis: int64(ordinal * 100),
			V: int64(ordinal % 11), F: float64(ordinal%13) / 4,
		}
	}
	return rows
}

// T16FixtureSchema returns F with its full host entity for eligible bucket merges.
func T16FixtureSchema(name string) *databasev1.Measure {
	schema := T07FixtureSchema(name)
	if name == T16StarFixtureName {
		schema.Interval = "2s"
		schema.TagFamilies[0].Tags = schema.TagFamilies[0].Tags[1:2]
		schema.Fields = schema.Fields[:1]
	}
	return schema
}

// T16FixtureWrites writes stale and final versions with identical source identities.
func T16FixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	var writes []*measurev1.WriteRequest
	for _, stale := range []bool{true, false} {
		for _, row := range t16Rows(name) {
			tags := []*modelv1.TagValue{tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone)}
			names := []string{"rid", "host", "zone"}
			v, f := row.V, row.F
			if stale {
				v, f = int64(-999), float64(-999)
			}
			fields := []*modelv1.FieldValue{fieldValue(v), fieldValue(f)}
			fieldNames := []string{"v", "f"}
			if name == T16StarFixtureName {
				tags, names = tags[1:2], names[1:2]
				fields, fieldNames = fields[:1], fieldNames[:1]
			}
			writes = append(writes, &measurev1.WriteRequest{
				Metadata: T16FixtureSchema(name).GetMetadata(), MessageId: uint64(len(writes) + 1),
				DataPointSpec: &measurev1.DataPointSpec{
					TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: names}}, FieldNames: fieldNames,
				},
				DataPoint: &measurev1.DataPointValue{
					Timestamp:   timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
					TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: tags}}, Fields: fields,
				},
			})
		}
	}
	return writes
}

func t16Readiness() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T16FixtureName, T16StarFixtureName, T16BulkFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := t16Rows(name)
		columns := []string{"rid", "host", "zone", "v", "f"}
		projection := "rid, host, zone, v::FIELD, f::FIELD"
		if name == T16StarFixtureName {
			columns, projection = []string{"host", "v"}, "host, v::FIELD"
		}
		for start := 0; start < len(rows); start += 128 {
			end := min(start+128, len(rows))
			var points []any
			for _, row := range rows[start:end] {
				points = append(points, projectRow(row, columns, true))
			}
			query := "SELECT " + projection + " FROM " + FixtureGroup + "." + name +
				" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" +
				timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
			c, err := t07ExpectedCase(fmt.Sprintf("t16_ready_%s_%d", name, start), query, points, []string{"t16.fixture"})
			if err != nil {
				return nil, err
			}
			c.Ticket, c.Ordered = "T16", true
			fixture.Cases = append(fixture.Cases, c)
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}
