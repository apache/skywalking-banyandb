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

package qlgen

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type fixtureRow struct {
	Zone   any
	V      any
	F      any
	Host   string
	Labels []string
	Nums   []int64
	Millis int64
	RID    int64
}

func fixtureRows() []fixtureRow {
	return []fixtureRow{
		{Millis: 0, RID: 1, Host: "a", Zone: "x", Labels: []string{"red", "blue"}, Nums: []int64{1, 2}, V: int64(0), F: 0.25},
		{Millis: 1000, RID: 2, Host: "a", Zone: "x", Labels: []string{"red"}, Nums: []int64{2, 3}, V: int64(2), F: 0.75},
		{Millis: 2000, RID: 3, Host: "b", Zone: "y", Labels: []string{"blue"}, Nums: []int64{3}, V: int64(4), F: 1.5},
		{Millis: 3000, RID: 4, Host: "b", Zone: "y", Labels: []string{"green"}, Nums: []int64{4, 5}},
		{Millis: 4000, RID: 5, Host: "c", V: int64(6), F: 2.5},
		{Millis: 5000, RID: 6, Host: "c", Zone: "", Labels: []string{"red", "green"}, Nums: []int64{1, 5}, F: 4.0},
		{Millis: 6000, RID: 7, Host: "d", Zone: "z", Labels: []string{"blue", "green"}, Nums: []int64{-1, 6}, V: int64(-2), F: -0.5},
		{Millis: 7000, RID: 8, Host: "d", Zone: "z", Labels: []string{"red", "blue", "green"}, Nums: []int64{1, 2, 3}, V: int64(8), F: 8.25},
	}
}

// FixtureSchema returns a fresh schema matching the oracle's typed rows.
func FixtureSchema() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: FixtureName, Group: FixtureGroup}, Interval: "1s",
		Entity: &databasev1.Entity{TagNames: []string{"rid"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			{Name: "nums", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
		}}},
		Fields: []*databasev1.FieldSpec{
			{
				Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
			{
				Name: "f", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
	}
}

// FixtureIndexRules returns the per-tag indexes required by generated predicates.
func FixtureIndexRules() []*databasev1.IndexRule {
	rules := make([]*databasev1.IndexRule, 0, 5)
	for _, tag := range FixtureSchema().GetTagFamilies()[0].GetTags() {
		rule := &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: FixtureName + "_" + tag.GetName() + "_idx", Group: FixtureGroup},
			Tags:     []string{tag.GetName()}, Type: databasev1.IndexRule_TYPE_INVERTED,
		}
		switch tag.GetType() {
		case databasev1.TagType_TAG_TYPE_STRING:
			rule.Analyzer = "keyword"
		case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
			rule.Analyzer = "keyword"
			rule.NoSort = true
		case databasev1.TagType_TAG_TYPE_INT_ARRAY:
			rule.NoSort = true
		default:
		}
		rules = append(rules, rule)
	}
	return rules
}

// FixtureIndexBinding activates the fixture indexes across its test lifetime.
func FixtureIndexBinding() *databasev1.IndexRuleBinding {
	rules := FixtureIndexRules()
	names := make([]string, len(rules))
	for index, rule := range rules {
		names[index] = rule.GetMetadata().GetName()
	}
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: FixtureName + "_binding", Group: FixtureGroup},
		Rules:    names, Subject: &databasev1.Subject{Name: FixtureName, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		BeginAt: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)), ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

// FixtureWrites converts those rows into independent write requests, never queries.
func FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	rows := fixtureRows()
	writes := make([]*measurev1.WriteRequest, 0, len(rows))
	for index, row := range rows {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: FixtureName, Group: FixtureGroup}, MessageId: uint64(index + 1),
			DataPointSpec: &measurev1.DataPointSpec{TagFamilySpec: []*measurev1.TagFamilySpec{{
				Name:     "default",
				TagNames: []string{"rid", "host", "zone", "labels", "nums"},
			}}, FieldNames: []string{"v", "f"}},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue(row.Labels), tagValue(row.Nums),
				}}}, Fields: []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F)},
			},
		})
	}
	return writes
}

// FixtureDigest identifies the exact schema and values used by generated expectations.
func FixtureDigest() string {
	data, err := json.Marshal(struct {
		T14Binding        *databasev1.IndexRuleBinding
		Schema            *databasev1.Measure
		Binding           *databasev1.IndexRuleBinding
		Rules             []*databasev1.IndexRule
		Rows              []fixtureRow
		T03Schema         *databasev1.Measure
		T05Schema         *databasev1.Measure
		T03Rows           []fixtureRow
		T07Schema         *databasev1.Measure
		T07Rows           []fixtureRow
		T07GroupsSchema   *databasev1.Measure
		T07GroupsRows     []fixtureRow
		T08Schema         *databasev1.Measure
		T08Rows           []t08Row
		T08GroupsSchema   *databasev1.Measure
		T08GroupsRows     []t08Row
		T08Indexes        []*databasev1.IndexRule
		T08Bindings       []*databasev1.IndexRuleBinding
		T10Schema         *databasev1.Measure
		T10Rows           []fixtureRow
		T12Schema         *databasev1.Measure
		T12Rows           []fixtureRow
		T12LegacySchemas  []*databasev1.Measure
		T12LegacyRows     []t12LegacyRow
		T14Schemas        []*databasev1.Measure
		T14Rows           [][]fixtureRow
		T14Indexes        []*databasev1.IndexRule
		T14ReviewBindings []*databasev1.IndexRuleBinding
	}{
		T14Schemas: []*databasev1.Measure{
			T14FixtureSchema(T14FixtureName), T14FixtureSchema(T14GroupsFixtureName), T14FixtureSchema(T14LegacyFixtureName),
			T14FixtureSchema(T14ContainsFixtureName), T14FixtureSchema(T14IndexedFixtureName),
		},
		T14Rows: [][]fixtureRow{
			t14Rows(T14FixtureName), t14Rows(T14GroupsFixtureName), t14Rows(T14LegacyFixtureName),
			t14Rows(T14ContainsFixtureName), t14Rows(T14IndexedFixtureName),
		},
		T14Indexes:        append(append(T14LegacyIndexRules(), T14ReviewIndexRules(T14ContainsFixtureName)...), T14ReviewIndexRules(T14IndexedFixtureName)...),
		T14Binding:        T14LegacyIndexBinding(),
		T14ReviewBindings: []*databasev1.IndexRuleBinding{T14ReviewIndexBinding(T14ContainsFixtureName), T14ReviewIndexBinding(T14IndexedFixtureName)},
		T10Schema:         T10FixtureSchema(), T10Rows: t10Rows(),
		T12Schema: T12FixtureSchema(), T12Rows: t12Rows(),
		T12LegacySchemas: []*databasev1.Measure{T12LegacyFixtureSchema(T12LegacyFixtureGroups()[0]), T12LegacyFixtureSchema(T12LegacyFixtureGroups()[1])},
		T12LegacyRows:    t12LegacyRows(),
		T08Indexes:       []*databasev1.IndexRule{T08FixtureIndexRule(T08FixtureName), T08FixtureIndexRule(T08GroupsFixtureName)},
		T08Bindings:      []*databasev1.IndexRuleBinding{T08FixtureIndexBinding(T08FixtureName), T08FixtureIndexBinding(T08GroupsFixtureName)},
		T08Schema:        T08FixtureSchema(T08FixtureName), T08Rows: t08Rows(T08FixtureName),
		T08GroupsSchema: T08FixtureSchema(T08GroupsFixtureName), T08GroupsRows: t08Rows(T08GroupsFixtureName),
		T07Schema: T07FixtureSchema(T07FixtureName), T07Rows: t07Rows(T07FixtureName),
		T07GroupsSchema: T07FixtureSchema(T07GroupsFixtureName), T07GroupsRows: t07Rows(T07GroupsFixtureName),
		T05Schema: T05FixtureSchema(), Rows: fixtureRows(), T03Schema: T03FixtureSchema(), T03Rows: t03Rows(),
		Schema: FixtureSchema(), Binding: FixtureIndexBinding(), Rules: FixtureIndexRules(),
	})
	if err != nil {
		panic(fmt.Sprintf("encode fixed QL fixture: %v", err))
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func tagValue(value any) *modelv1.TagValue {
	switch typed := value.(type) {
	case string:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: typed}}}
	case int64:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: typed}}}
	case []string:
		if typed == nil {
			return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
		}
		return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: typed}}}
	case []int64:
		if typed == nil {
			return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
		}
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: typed}}}
	default:
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
}

func fieldValue(value any) *modelv1.FieldValue {
	switch typed := value.(type) {
	case int64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: typed}}}
	case float64:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: typed}}}
	default:
		return &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
	}
}

func isTag(name string) bool {
	return name == "rid" || name == "host" || name == "zone" || name == "labels" || name == "nums"
}

func rowValue(row fixtureRow, column string) any {
	switch column {
	case "rid":
		return row.RID
	case "host":
		return row.Host
	case "zone":
		return row.Zone
	case "labels":
		return row.Labels
	case "nums":
		return row.Nums
	case "v":
		return row.V
	case "f":
		return row.F
	}
	return nil
}

func projectRow(row fixtureRow, columns []string, timestamp bool) map[string]any {
	point := map[string]any{}
	var tags, fields []any
	if timestamp {
		point["timestamp"] = timePlaceholder(row.Millis)
	}
	for _, column := range columns {
		if isTag(column) {
			tags = append(tags, map[string]any{"key": column, "value": jsonValue(rowValue(row, column))})
		} else {
			fields = append(fields, map[string]any{"name": column, "value": jsonValue(rowValue(row, column))})
		}
	}
	if len(tags) > 0 {
		point["tagFamilies"] = []any{map[string]any{"name": "default", "tags": tags}}
	}
	if len(fields) > 0 {
		point["fields"] = fields
	}
	return point
}
