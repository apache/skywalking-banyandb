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

// T14FixtureName identifies independent grouped HAVING and membership fixture F.
const T14FixtureName = "ql_t14"

// T14LegacyFixtureName isolates indexed legacy membership from typed array scans.
const T14LegacyFixtureName = "ql_t14_legacy"

// T14GroupsFixtureName identifies the 129-group final-pagination fixture.
const T14GroupsFixtureName = "ql_t14_groups"

func t14Rows(name string) []fixtureRow {
	if name == T14ContainsFixtureName {
		return t14ContainsRows()
	}
	if name == T14IndexedFixtureName {
		return t14Rows(T14FixtureName)
	}
	if name == T14FixtureName || name == T14LegacyFixtureName {
		// Array tags remain constant within each entity. Empty arrays and absent
		// arrays share storage's null representation; membership retains legacy rules.
		rows := []fixtureRow{
			{Millis: 0, RID: 1, Host: "a", Zone: "x", Labels: []string{"red", "blue"}, Nums: []int64{1, 2}, V: int64(0), F: float64(0)},
			{Millis: 1000, RID: 2, Host: "a", Zone: "x", Labels: []string{"red", "blue"}, Nums: []int64{1, 2}, V: int64(2), F: float64(2)},
			{Millis: 2000, RID: 3, Host: "b", Zone: "y", Labels: []string{"blue"}, Nums: []int64{2}, V: int64(4), F: float64(4)},
			{Millis: 3000, RID: 4, Host: "b", Zone: "y", Labels: []string{"blue"}, Nums: []int64{2}},
			{Millis: 4000, RID: 5, Host: "c", Zone: "z", Labels: []string{}, Nums: []int64{}, V: int64(6), F: float64(6)},
			{Millis: 5000, RID: 6, Host: "c", Zone: "z", Labels: []string{}, Nums: []int64{}},
			{Millis: 6000, RID: 7, Host: "n", Zone: "null"},
			{Millis: 7000, RID: 8, Host: "n", Zone: "null"},
		}
		if name == T14LegacyFixtureName {
			values := map[string]int64{"a": 1, "b": 2, "c": 3, "n": 0}
			for ordinal := range rows {
				rows[ordinal].Zone = values[rows[ordinal].Host]
			}
		}
		return rows
	}
	var rows []fixtureRow
	for group := range 129 {
		for range 2 {
			rows = append(rows, fixtureRow{
				Millis: int64(len(rows) * 100), RID: int64(len(rows) + 1), Host: fmt.Sprintf("group-%03d", group),
				Zone: "many", V: int64(group), F: float64(group), Labels: []string{"blue"}, Nums: []int64{2},
			})
		}
	}
	return rows
}

// T14FixtureSchema gives each host one complete native aggregation entity.
func T14FixtureSchema(name string) *databasev1.Measure {
	if name == T14ContainsFixtureName || name == T14IndexedFixtureName {
		return t14ReviewSchema(name)
	}
	schema := FixtureSchema()
	schema.Metadata = &commonv1.Metadata{Name: name, Group: FixtureGroup}
	schema.Entity.TagNames = []string{"host"}
	if name == T14LegacyFixtureName {
		schema.Entity.TagNames = []string{"host", "zone"}
		schema.TagFamilies[0].Tags[2].Type = databasev1.TagType_TAG_TYPE_INT
	}
	return schema
}

// T14FixtureWrites writes only independent input values through the public writer.
func T14FixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	rows := t14Rows(name)
	tagNames := []string{"rid", "host", "zone", "labels", "nums"}
	if name == T14ContainsFixtureName {
		tagNames[1] = "contains"
	}
	writes := make([]*measurev1.WriteRequest, 0, len(rows))
	for ordinal, row := range rows {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T14FixtureSchema(name).GetMetadata(), MessageId: uint64(ordinal + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: tagNames}},
				FieldNames:    []string{"v", "f"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue(row.Labels), tagValue(row.Nums),
				}}},
				Fields: []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F)},
			},
		})
	}
	return writes
}

func t14ReadinessCases() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T14FixtureName, T14GroupsFixtureName, T14LegacyFixtureName} {
		fixture := AggregateFixture{Name: name}
		rows := t14Rows(name)
		for start := 0; start < len(rows); start += 64 {
			end := min(start+64, len(rows))
			var points []any
			for _, row := range rows[start:end] {
				// Persisted zero-length arrays are decoded as null, as in existing fixtures.
				if len(row.Labels) == 0 {
					row.Labels = nil
				}
				if len(row.Nums) == 0 {
					row.Nums = nil
				}
				columns := []string{"rid", "host", "zone", "labels", "nums", "v", "f"}
				if name == T14LegacyFixtureName {
					columns = []string{"rid", "host", "zone", "v", "f"}
				}
				points = append(points, projectRow(row, columns, true))
			}
			projection := "rid, host, zone, labels, nums, v::FIELD, f::FIELD"
			if name == T14LegacyFixtureName {
				projection = "rid, host, zone, v::FIELD, f::FIELD"
			}
			query := "SELECT " + projection + " FROM " + FixtureGroup + "." + name +
				" WHERE time >= '" + timePlaceholder(rows[start].Millis) + "' AND time < '" + timePlaceholder(rows[end-1].Millis+1) + "' ORDER BY TIME ASC"
			c, err := t07ExpectedCase(fmt.Sprintf("t14_ready_%s_%d", name, start/64), query, points, []string{"t14.fixture"})
			if err != nil {
				return nil, err
			}
			c.Ticket, c.Ordered = "T14", true
			fixture.Cases = append(fixture.Cases, c)
		}
		if name == T14LegacyFixtureName {
			for _, column := range []string{"host", "zone"} {
				var value any = "a"
				if column == "zone" {
					value = int64(1)
				}
				c, err := buildT14Case("t14_ready_indexed_"+column, t14Query{
					Legacy: true, Fixture: name, Columns: []t07Column{{Target: "rid"}},
					Membership: &t14Membership{Column: column, Operator: "HAVING", Form: "single", Values: []any{value}}, Begin: -1, End: 8000,
				}, "t14.fixture")
				if err != nil {
					return nil, err
				}
				fixture.Cases = append(fixture.Cases, c)
			}
		}
		fixtures = append(fixtures, fixture)
	}
	reviewFixtures, err := t14ReviewReadiness()
	if err != nil {
		return nil, err
	}
	fixtures = append(fixtures, reviewFixtures...)
	return fixtures, nil
}

// T14LegacyIndexRules supplies native scalar membership indexes.
func T14LegacyIndexRules() []*databasev1.IndexRule {
	return []*databasev1.IndexRule{
		{
			Metadata: &commonv1.Metadata{Name: T14LegacyFixtureName + "_host", Group: FixtureGroup}, Tags: []string{"host"},
			Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword", NoSort: true,
		},
		{
			Metadata: &commonv1.Metadata{Name: T14LegacyFixtureName + "_zone", Group: FixtureGroup}, Tags: []string{"zone"},
			Type: databasev1.IndexRule_TYPE_INVERTED, NoSort: true,
		},
	}
}

// T14LegacyIndexBinding activates both compatibility indexes over the fixed fixture window.
func T14LegacyIndexBinding() *databasev1.IndexRuleBinding {
	rules := T14LegacyIndexRules()
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: T14LegacyFixtureName + "_binding", Group: FixtureGroup},
		Rules:    []string{rules[0].Metadata.Name, rules[1].Metadata.Name},
		Subject:  &databasev1.Subject{Name: T14LegacyFixtureName, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}
