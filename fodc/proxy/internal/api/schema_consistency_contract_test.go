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

package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// TestLifecycleGroupsJSONContract pins the exact JSON shape of the schema
// consistency verdict on /cluster/lifecycle.
//
// test/e2e-v2/cases/fodc/check-schema-consistency.sh reads this payload with
// jq. Its queries are string literals, so a rename here (proto field, JSON
// casing, or enum spelling) would not break compilation -- the script's filters
// would simply stop matching and every check in it would pass vacuously on a
// broken cluster. This test is what makes that drift fail loudly instead.
func TestLifecycleGroupsJSONContract(t *testing.T) {
	groups := []*fodcv1.GroupLifecycleInfo{{
		Name:    "g",
		Catalog: "CATALOG_STREAM",
		SchemaConsistency: &fodcv1.SchemaConsistency{
			Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT,
			Objects: []*fodcv1.ObjectConsistency{{
				Kind: "stream", Name: "foo", RegistryFingerprint: 1,
				NodeFingerprints: []*fodcv1.NodeFingerprint{{
					Node: "data-1", CacheFingerprint: 2, RuntimeFingerprint: 3,
				}},
			}},
			Issues: []*fodcv1.SchemaIssue{{
				Kind: "stream",
				Name: "foo",
				Node: "data-1",
				Type: fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE,
			}},
		},
	}}

	raw, err := marshalLifecycleGroups(groups)
	require.NoError(t, err)
	require.Len(t, raw, 1)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw[0], &decoded))

	// Proto names, not lowerCamelCase: the marshaler sets UseProtoNames.
	verdictRaw, ok := decoded["schema_consistency"]
	require.True(t, ok, "the script selects .schema_consistency; lowerCamelCase would silently match nothing")
	verdict, ok := verdictRaw.(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "CONSISTENCY_STATUS_CONSISTENT", verdict["status"],
		"the script compares status against full enum names")

	// objects is the group's single, object-centric schema table: registry truth
	// plus each node's cache/runtime fingerprints, all in one place.
	objects, ok := verdict["objects"].([]any)
	require.True(t, ok, "objects carries the per-object registry and per-node fingerprints")
	require.Len(t, objects, 1)
	object, ok := objects[0].(map[string]any)
	require.True(t, ok)
	for _, key := range []string{"kind", "name", "registry_fingerprint", "node_fingerprints"} {
		assert.Contains(t, object, key, "object field %q pins the schema table", key)
	}
	nodeFingerprints, ok := object["node_fingerprints"].([]any)
	require.True(t, ok)
	require.Len(t, nodeFingerprints, 1)
	nodeFingerprint, ok := nodeFingerprints[0].(map[string]any)
	require.True(t, ok)
	for _, key := range []string{"node", "cache_fingerprint", "runtime_fingerprint"} {
		assert.Contains(t, nodeFingerprint, key, "node fingerprint field %q pins the per-node layer", key)
	}

	issues, ok := verdict["issues"].([]any)
	require.True(t, ok, "the script prints .schema_consistency.issues on failure")
	require.Len(t, issues, 1)
	issue, ok := issues[0].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "SCHEMA_ISSUE_TYPE_CACHE_STALE", issue["type"])
	for _, key := range []string{"kind", "name", "node", "type"} {
		assert.Contains(t, issue, key, "issue field %q is what makes a finding actionable", key)
	}
}

// TestConsistencyStatusNamesAreExhaustive pins the status strings the e2e script
// buckets verdicts into. The script asserts every verdict falls into one of
// these four; a new enum value added here without updating the script would make
// that sum check fail rather than let an unrecognized status slip through.
func TestConsistencyStatusNamesAreExhaustive(t *testing.T) {
	expected := map[int32]string{
		0: "CONSISTENCY_STATUS_UNSPECIFIED",
		1: "CONSISTENCY_STATUS_CONSISTENT",
		2: "CONSISTENCY_STATUS_INCONSISTENT",
		3: "CONSISTENCY_STATUS_UNKNOWN",
	}

	assert.Equal(t, expected, fodcv1.ConsistencyStatus_name,
		"check-schema-consistency.sh buckets verdicts by these exact names; update it alongside the enum")
}
