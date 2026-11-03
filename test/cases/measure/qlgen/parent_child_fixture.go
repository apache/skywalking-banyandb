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

// ParentChildFixtureName holds repeated children in two time buckets.
const ParentChildFixtureName = "ql_parent_child"

// ParentChildPageFixtureName holds 137 tied groups for final pagination.
const ParentChildPageFixtureName = "ql_parent_child_pages"

type parentChildRow struct {
	Parent, Entity string
	Millis, Value  int64
}

func parentChildRows(name string) []parentChildRow {
	if name == ParentChildFixtureName {
		return []parentChildRow{
			{"a", "a/child1", 0, 10},
			{"a", "a/child2", 0, 20},
			{"a", "a/child1", 1000, 5},
			{"a", "a/child3", 1000, 7},
			{"b", "b/child4", 0, 8},
			{"b", "b/child5", 1000, 9},
			{"c", "c/child6", 1000, 4},
		}
	}
	rows := make([]parentChildRow, 137)
	for i := range rows {
		parent := fmt.Sprintf("parent-%03d", i)
		rows[i] = parentChildRow{parent, parent + "/child", int64(i%2) * 1000, int64(i + 1)}
	}
	return rows
}

// ParentChildFixtureSchema uses parent-qualified child identities as entities.
func ParentChildFixtureSchema(name string) *databasev1.Measure {
	schema := T12LegacyFixtureSchema(FixtureGroup)
	schema.Metadata.Name = name
	schema.Entity.TagNames = []string{"entity_id"}
	schema.TagFamilies[0].Tags = []*databasev1.TagSpec{
		{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
		{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
	}
	schema.Fields = schema.Fields[:1]
	schema.Fields[0].Name = "value"
	return schema
}

// ParentChildIndexRules makes both scalar string tags searchable.
func ParentChildIndexRules(name string) []*databasev1.IndexRule {
	var rules []*databasev1.IndexRule
	for _, column := range []string{"entity_id", "service_id"} {
		rules = append(rules, &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: name + "_" + column, Group: FixtureGroup},
			Tags:     []string{column}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword", NoSort: true,
		})
	}
	return rules
}

// ParentChildIndexBinding enables the fixture's searchable tags before writes.
func ParentChildIndexBinding(name string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: name + "_binding", Group: FixtureGroup},
		Rules:    []string{name + "_entity_id", name + "_service_id"},
		Subject:  &databasev1.Subject{Name: name, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

// ParentChildFixtureWrites materializes each bucket through the normal measure writer.
func ParentChildFixtureWrites(name string, base time.Time) []*measurev1.WriteRequest {
	var writes []*measurev1.WriteRequest
	for i, row := range parentChildRows(name) {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: ParentChildFixtureSchema(name).Metadata, MessageId: uint64(i + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"entity_id", "service_id"}}}, FieldNames: []string{"value"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{tagValue(row.Entity), tagValue(row.Parent)}}},
				Fields:      []*modelv1.FieldValue{fieldValue(row.Value)},
			},
		})
	}
	return writes
}
