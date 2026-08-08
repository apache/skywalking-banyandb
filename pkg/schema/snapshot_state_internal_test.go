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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func indexRuleFixture(name string, tags ...string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g", Name: name},
		Tags:     tags,
	}
}

func TestRuleTable_InternDedupsIdenticalContent(t *testing.T) {
	tb := newRuleTable()

	// Two resources bind the same pair of rules -- distinct proto objects with
	// identical content. Each body must be stored once, so the second resource's
	// refs reuse the first's indexes.
	first, err := tb.intern([]*databasev1.IndexRule{indexRuleFixture("r1", "a"), indexRuleFixture("r2", "b")})
	require.NoError(t, err)
	second, err := tb.intern([]*databasev1.IndexRule{indexRuleFixture("r1", "a"), indexRuleFixture("r2", "b")})
	require.NoError(t, err)

	assert.Equal(t, []uint32{0, 1}, first)
	assert.Equal(t, []uint32{0, 1}, second, "identical bodies resolve to the already-stored indexes")
	assert.Len(t, tb.rules, 2, "each distinct body is stored once regardless of how many resources bind it")
}

func TestRuleTable_InternKeepsDivergentBodiesDistinct(t *testing.T) {
	tb := newRuleTable()

	// The cache holds the new rule body; a resource's runtime layer still holds
	// the stale one under the same name. They differ in content, so they must
	// occupy distinct entries -- otherwise the agent's cache-vs-runtime
	// fingerprint comparison could never surface RUNTIME_NOT_APPLIED.
	cacheRefs, err := tb.intern([]*databasev1.IndexRule{indexRuleFixture("r1", "new")})
	require.NoError(t, err)
	runtimeRefs, err := tb.intern([]*databasev1.IndexRule{indexRuleFixture("r1", "stale")})
	require.NoError(t, err)

	assert.NotEqual(t, cacheRefs, runtimeRefs, "a stale runtime body must not collapse onto its cache counterpart")
	assert.Len(t, tb.rules, 2)
}

func TestRuleTable_InternPreservesOrderAndHandlesEmpty(t *testing.T) {
	tb := newRuleTable()

	empty, err := tb.intern(nil)
	require.NoError(t, err)
	assert.Nil(t, empty, "an empty rule set yields no refs")

	refs, err := tb.intern([]*databasev1.IndexRule{
		indexRuleFixture("r2", "b"), indexRuleFixture("r1", "a"), indexRuleFixture("r2", "b"),
	})
	require.NoError(t, err)
	assert.Equal(t, []uint32{0, 1, 0}, refs, "refs follow input order and reuse the first index for repeats")
}
