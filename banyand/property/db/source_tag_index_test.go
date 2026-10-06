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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

func TestBuildUpdateDocumentKeepsSourceTagOutOfTheIndex(t *testing.T) {
	s := &shard{repairState: &repair{}}
	property := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: "g", Name: "n", ModRevision: 1},
		Id:       "id",
		Tags: []*modelv1.Tag{
			{Key: unindexedSourceTag, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: `{"spec":true}`}}}},
			{Key: "kind", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "stream"}}}},
		},
	}
	document, err := s.buildUpdateDocument([]byte("id"), property, 0)
	require.NoError(t, err)
	byKey := make(map[uint32]index.Field, len(document.Fields))
	for _, field := range document.Fields {
		if field.Key.IndexRuleID != 0 {
			byKey[field.Key.IndexRuleID] = field
		}
	}
	source, found := byKey[uint32(convert.HashStr(unindexedSourceTag))]
	require.True(t, found, "the source tag must still be written")
	require.False(t, source.Index, "the source tag must not be indexed")
	require.False(t, source.NoSort, "the source tag must stay sortable")
	kind, found := byKey[uint32(convert.HashStr("kind"))]
	require.True(t, found)
	require.True(t, kind.Index, "other tags stay indexed")
}
