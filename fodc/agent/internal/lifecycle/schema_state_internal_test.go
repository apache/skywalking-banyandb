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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
)

func mustAny(t *testing.T, m proto.Message) *anypb.Any {
	t.Helper()
	a, err := anypb.New(m)
	require.NoError(t, err)
	return a
}

func streamBody(tag string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo"},
		Entity:   &databasev1.Entity{TagNames: []string{tag}},
	}
}

func ruleBody(name, tag string) *databasev1.IndexRule {
	return &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g", Name: name},
		Tags:     []string{tag},
	}
}

// The agent must reconstruct exactly the fingerprint the node would have
// computed inline: unwrap the Any payload, resolve the refs against the table,
// and hash. If either step drifts, this equality breaks.
func TestFingerprintObjectSnapshot_ResolvesRefsMatchingDirectFingerprint(t *testing.T) {
	r0 := ruleBody("r0", "a")
	r1 := ruleBody("r1", "b")
	table := []*databasev1.IndexRule{r0, r1}
	body := streamBody("service_id")

	obj := &databasev1.ObjectSnapshot{
		Group: "g", Kind: "measure", Name: "foo",
		Cache: &databasev1.SchemaBody{Payload: mustAny(t, body), BoundIndexRuleRefs: []uint32{0, 1}},
	}

	cacheFP, runtimeFP, err := fingerprintObjectSnapshot(obj, table)
	require.NoError(t, err)

	want, err := consistency.Fingerprint(body, []*databasev1.IndexRule{r0, r1})
	require.NoError(t, err)
	assert.Equal(t, want, cacheFP, "resolving refs against the table reproduces the direct fingerprint")
	assert.Equal(t, cacheFP, runtimeFP, "an object with no runtime body takes its runtime fingerprint from cache")
}

// This is the invariant that forbids collapsing the table by rule name: the
// cache and runtime layers reference two table entries that share a name but
// differ in body, and the two fingerprints must stay distinct so the checker
// can report RUNTIME_NOT_APPLIED.
func TestFingerprintObjectSnapshot_RuntimeDivergenceSurvivesInterning(t *testing.T) {
	table := []*databasev1.IndexRule{ruleBody("r0", "new"), ruleBody("r0", "stale")}
	body := streamBody("service_id")

	obj := &databasev1.ObjectSnapshot{
		Group: "g", Kind: "measure", Name: "foo",
		Cache:   &databasev1.SchemaBody{Payload: mustAny(t, body), BoundIndexRuleRefs: []uint32{0}},
		Runtime: &databasev1.SchemaBody{Payload: mustAny(t, body), BoundIndexRuleRefs: []uint32{1}},
	}

	cacheFP, runtimeFP, err := fingerprintObjectSnapshot(obj, table)
	require.NoError(t, err)
	assert.NotEqual(t, cacheFP, runtimeFP, "a stale runtime rule body must still produce a divergent fingerprint")
}

// A ref past the end of the table means the table event was lost or the stream
// tore; the agent must error so the proxy checker degrades the node to UNKNOWN
// instead of hashing a truncated rule set as if it were real drift.
func TestFingerprintObjectSnapshot_RefOutOfRangeErrors(t *testing.T) {
	table := []*databasev1.IndexRule{ruleBody("r0", "a")}
	obj := &databasev1.ObjectSnapshot{
		Group: "g", Kind: "measure", Name: "foo",
		Cache: &databasev1.SchemaBody{Payload: mustAny(t, streamBody("service_id")), BoundIndexRuleRefs: []uint32{5}},
	}

	_, _, err := fingerprintObjectSnapshot(obj, table)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}
