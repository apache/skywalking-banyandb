// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to You under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package db

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestNativePropertyMatchParity(t *testing.T) {
	ctx := context.Background()
	type testCase struct {
		name     string
		analyzer string
		want     []string
		operator modelv1.Condition_MatchOption_Operator
	}
	cases := []testCase{
		{name: "default", want: []string{"one"}},
		{name: "keyword", analyzer: "keyword", want: []string{"one"}},
		{name: "simple", analyzer: "simple", want: []string{"two"}},
		{name: "standard", analyzer: "standard", want: []string{"two"}},
		{name: "and", analyzer: "standard", operator: modelv1.Condition_MatchOption_OPERATOR_AND, want: []string{}},
		{name: "or", analyzer: "standard", operator: modelv1.Condition_MatchOption_OPERATOR_OR, want: []string{"two"}},
	}
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()
	db, err := OpenDB(ctx, Config{
		Location: location, MetricsScopeName: "native_match_parity", FlushInterval: time.Second,
		Index: IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	defer db.Close()
	for revision, item := range []struct{ id, value string }{{"one", "red blue"}, {"two", "blue"}, {"three", "green"}} {
		p := &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: testPropertyGroup, Name: testPropertyName, ModRevision: int64(revision + 1)}, Id: item.id,
			Tags: []*modelv1.Tag{{Key: "tag1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: item.value}}}}},
		}
		require.NoError(t, db.Update(ctx, 0, GetPropertyID(p), p))
	}
	for _, tc := range cases {
		option := &modelv1.Condition_MatchOption{Analyzer: tc.analyzer, Operator: tc.operator}
		criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name: "tag1", Op: modelv1.Condition_BINARY_OP_MATCH, MatchOption: option,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "red blue"}}},
		}}}
		rows, queryErr := db.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}, Criteria: criteria})
		require.NoError(t, queryErr, tc.name)
		got := make([]string, 0, len(rows))
		for _, row := range rows {
			got = append(got, unmarshalProperty(t, row.Source()).Id)
		}
		sort.Strings(got)
		require.Equal(t, tc.want, got, tc.name)
	}
}

func TestNativePropertyEmptyStringConditions(t *testing.T) {
	ctx := context.Background()
	location, cleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer cleanup()
	db, err := OpenDB(ctx, Config{
		Location: location, MetricsScopeName: "native_empty_conditions", FlushInterval: time.Second,
		Index: IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	defer db.Close()
	p := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: testPropertyGroup, Name: testPropertyName, ModRevision: 1}, Id: "empty",
		Tags: []*modelv1.Tag{{Key: "tag1", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ""}}}}},
	}
	require.NoError(t, db.Update(ctx, 0, GetPropertyID(p), p))
	for _, op := range []modelv1.Condition_BinaryOp{modelv1.Condition_BINARY_OP_EQ, modelv1.Condition_BINARY_OP_IN, modelv1.Condition_BINARY_OP_NOT_IN} {
		value := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: ""}}}
		if op != modelv1.Condition_BINARY_OP_EQ {
			value = &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{""}}}}
		}
		criteria := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: "tag1", Op: op, Value: value}}}
		rows, queryErr := db.Query(ctx, &propertyv1.QueryRequest{Groups: []string{testPropertyGroup}, Criteria: criteria})
		require.NoError(t, queryErr, op.String())
		if op == modelv1.Condition_BINARY_OP_NOT_IN {
			require.Empty(t, rows, op.String())
		} else {
			require.Len(t, rows, 1, op.String())
		}
	}
}
