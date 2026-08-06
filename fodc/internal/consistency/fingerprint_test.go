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

package consistency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var fixedUpdatedAt = timestamppb.New(time.Unix(1700000000, 0).UTC())

func streamSpec(modRevision int64, tag string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo", ModRevision: modRevision, Id: 7},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: tag, Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Entity:    &databasev1.Entity{TagNames: []string{tag}},
		UpdatedAt: fixedUpdatedAt,
	}
}

func TestFingerprint_IgnoresModRevision(t *testing.T) {
	a, err := Fingerprint(streamSpec(1, "service_id"), nil)
	require.NoError(t, err)
	b, err := Fingerprint(streamSpec(999, "service_id"), nil)
	require.NoError(t, err)

	assert.Equal(t, a, b, "a revision bump with unchanged content must not change the fingerprint")
}

func TestFingerprint_DetectsContentChange(t *testing.T) {
	a, err := Fingerprint(streamSpec(1, "service_id"), nil)
	require.NoError(t, err)
	b, err := Fingerprint(streamSpec(1, "endpoint_id"), nil)
	require.NoError(t, err)

	assert.NotEqual(t, a, b)
}

func TestFingerprint_DetectsBoundRuleChange(t *testing.T) {
	s := streamSpec(1, "service_id")
	a, err := Fingerprint(s, []*databasev1.IndexRule{rule("g", "r1")})
	require.NoError(t, err)
	b, err := Fingerprint(s, []*databasev1.IndexRule{rule("g", "r2")})
	require.NoError(t, err)

	assert.NotEqual(t, a, b, "a different bound rule set must change the fingerprint")
}

func TestFingerprint_RuleOrderAndDuplicatesDoNotMatter(t *testing.T) {
	s := streamSpec(1, "service_id")
	r1, r2 := rule("g", "r1"), rule("g", "r2")
	a, err := Fingerprint(s, []*databasev1.IndexRule{r1, r2})
	require.NoError(t, err)
	b, err := Fingerprint(s, []*databasev1.IndexRule{r2, r1, r1})
	require.NoError(t, err)

	assert.Equal(t, a, b, "sort+dedup happens here so sync.Map iteration order cannot move the fingerprint")
}

func TestFingerprint_DoesNotMutateInput(t *testing.T) {
	// Fingerprinting clears identity fields, and doing so on the caller's object
	// would corrupt the live schema the node is serving.
	s := streamSpec(42, "service_id")

	_, err := Fingerprint(s, nil)
	require.NoError(t, err)

	assert.Equal(t, int64(42), s.GetMetadata().GetModRevision())
	assert.Equal(t, uint32(7), s.GetMetadata().GetId())
}

func TestFingerprint_RuleContentChangeIsDetected(t *testing.T) {
	// Same rule name, different rule body: the bound rule set must still differ.
	s := streamSpec(1, "service_id")
	plain := rule("g", "r1")
	modified := rule("g", "r1")
	modified.Tags = []string{"service_id"}

	a, err := Fingerprint(s, []*databasev1.IndexRule{plain})
	require.NoError(t, err)
	b, err := Fingerprint(s, []*databasev1.IndexRule{modified})
	require.NoError(t, err)

	assert.NotEqual(t, a, b)
}
