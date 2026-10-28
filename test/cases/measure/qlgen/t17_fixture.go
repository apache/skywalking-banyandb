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
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T17WideFixtureName isolates eight exact-value rows and owned string payloads.
const T17WideFixtureName = "ql_t17_wide"

func t17Rows(name string) []fixtureRow {
	if name != T17WideFixtureName {
		return t16Rows(name)
	}
	rows := []fixtureRow{
		{RID: 1, V: int64(9007199254740993), F: float64(1), Zone: "a"},
		{RID: 2, V: int64(9007199254740992), F: math.NaN(), Zone: "A"},
		{RID: 3, V: int64(9007199254740995), Zone: ""},
		{RID: 4, F: math.NaN()},
		{RID: 5, V: int64(-9007199254740993), F: math.Inf(-1), Zone: "é"},
		{RID: 6, V: int64(-9007199254740992), F: math.Inf(1), Zone: "z"},
		{RID: 7, V: int64(math.MaxInt64), F: float64(0), Zone: "\x00"},
		{RID: 8, V: int64(math.MinInt64), F: math.Copysign(0, -1), Zone: "W"},
	}
	for ordinal := range rows {
		rows[ordinal].Millis = int64(ordinal) * 1000
		rows[ordinal].Host = fmt.Sprintf("row-%d:", ordinal+1) + strings.Repeat("owned-payload/", 1280)
	}
	return rows
}

// T17FixtureSchema reuses the existing F and bulk schemas plus a bounded wide fixture.
func T17FixtureSchema(name string) *databasev1.Measure {
	schema := T16FixtureSchema(name)
	if name == T17WideFixtureName {
		schema.Entity.TagNames = []string{"rid"}
	}
	return schema
}

// T17FixtureWrites reuses versioned F/bulk writes and encodes special floats as protobuf doubles.
func T17FixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	if name != T17WideFixtureName {
		return T16FixtureWrites(name, base)
	}
	var writes []*measurev1.WriteRequest
	for _, row := range t17Rows(name) {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T17FixtureSchema(name).GetMetadata(), MessageId: uint64(len(writes) + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid", "host", "zone"}}}, FieldNames: []string{"v", "f"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone)}}},
				Fields:      []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F)},
			},
		})
	}
	return writes
}

func t17Readiness() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T16FixtureName, T16BulkFixtureName, T17WideFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := t17Rows(name)
		for start := 0; start < len(rows); start += 128 {
			end := min(start+128, len(rows))
			var points []any
			for _, row := range rows[start:end] {
				points = append(points, projectRow(row, []string{"rid", "host", "zone", "v", "f"}, true))
			}
			query := "SELECT rid, host, zone, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + name +
				" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" + timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
			c, err := t07ExpectedCase(fmt.Sprintf("t17_ready_%s_%d", name, start), query, points, []string{"t17.fixture"})
			if err != nil {
				return nil, err
			}
			c.Ticket, c.Ordered = "T17", true
			fixture.Cases = append(fixture.Cases, c)
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}

func t17DigestRows() []any {
	var rows []any
	for _, row := range t17Rows(T17WideFixtureName) {
		rows = append(rows, projectRow(row, []string{"rid", "host", "zone", "v", "f"}, true))
	}
	return rows
}
