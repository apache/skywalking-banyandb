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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// T14ContainsFixtureName exercises contextual operator text in resource and group names.
const T14ContainsFixtureName = "contains-svc"

// T14IndexedFixtureName isolates rejected membership on incomplete native array projections.
const T14IndexedFixtureName = "ql_t14_indexed"

func t14ContainsRows() []fixtureRow {
	return []fixtureRow{
		{Millis: 0, RID: 1, Host: "a", V: int64(1)},
		{Millis: 1000, RID: 2, Host: "b", V: int64(2)},
		{Millis: 2000, RID: 3, Host: "a", V: int64(3)},
	}
}

func t14ReviewSchema(name string) *databasev1.Measure {
	schema := T14FixtureSchema(T14FixtureName)
	schema.Metadata.Name = name
	if name == T14ContainsFixtureName {
		schema.Metadata.Group = name
		schema.TagFamilies[0].Tags[1].Name = "contains"
		schema.Entity.TagNames = []string{"contains"}
	}
	return schema
}

// T14ReviewIndexRules indexes the scalar compatibility tag or both rejected array types.
func T14ReviewIndexRules(name string) []*databasev1.IndexRule {
	columns := []string{"labels", "nums"}
	if name == T14ContainsFixtureName {
		columns = []string{"contains"}
	}
	schema := T14FixtureSchema(name)
	rules := make([]*databasev1.IndexRule, 0, len(columns))
	for _, column := range columns {
		rule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: name + "_" + column, Group: schema.Metadata.Group},
			Tags:     []string{column}, Type: databasev1.IndexRule_TYPE_INVERTED, NoSort: true,
		}
		if column != "nums" {
			rule.Analyzer = "keyword"
		}
		rules = append(rules, rule)
	}
	return rules
}

// T14ReviewIndexBinding activates the compatibility indexes before independent writes.
func T14ReviewIndexBinding(name string) *databasev1.IndexRuleBinding {
	schema := T14FixtureSchema(name)
	rules := T14ReviewIndexRules(name)
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.Metadata.Name)
	}
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: name + "_binding", Group: schema.Metadata.Group}, Rules: names,
		Subject: &databasev1.Subject{Name: name, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

func t14ReviewReadiness() ([]AggregateFixture, error) {
	var fixtures []AggregateFixture
	for _, name := range []string{T14ContainsFixtureName, T14IndexedFixtureName} {
		projection := "rid, host, v::FIELD, f::FIELD"
		if name == T14ContainsFixtureName {
			projection = "rid, contains, v::FIELD, f::FIELD"
		}
		var points []any
		for _, row := range t14Rows(name) {
			point := projectRow(row, []string{"rid", "host", "v", "f"}, true)
			if name == T14ContainsFixtureName {
				tags := point["tagFamilies"].([]any)[0].(map[string]any)["tags"].([]any)
				tags[1].(map[string]any)["key"] = "contains"
			}
			points = append(points, point)
		}
		schema := T14FixtureSchema(name)
		query := "SELECT " + projection + " FROM MEASURE " + name + " IN " + schema.Metadata.Group +
			" TIME BETWEEN '" + timePlaceholder(-1) + "' AND '" + timePlaceholder(8000) + "' ORDER BY TIME ASC"
		c, err := t07ExpectedCase("t14_ready_"+name, query, points, []string{"t14.fixture"})
		if err != nil {
			return nil, err
		}
		c.Ticket, c.Ordered = "T14", true
		fixtures = append(fixtures, AggregateFixture{Name: name, Cases: []Case{c}})
	}
	return fixtures, nil
}
