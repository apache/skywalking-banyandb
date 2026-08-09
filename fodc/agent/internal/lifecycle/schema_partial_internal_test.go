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

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

func TestToSchemaConsistency_EmitsNodeFingerprints(t *testing.T) {
	st := newGroupSchemaState()
	st.node = "data-1"
	o := st.object("stream", "foo")
	o.cache, o.runtime, o.hasNode = 10, 11, true

	sc := st.toSchemaConsistency()

	require.Len(t, sc.GetObjects(), 1)
	got := sc.GetObjects()[0]
	assert.Zero(t, got.GetRegistryFingerprint(), "the agent never sets the registry fingerprint; the proxy fetches it")
	require.Len(t, got.GetNodeFingerprints(), 1)
	nf := got.GetNodeFingerprints()[0]
	assert.Equal(t, "data-1", nf.GetNode())
	assert.Equal(t, uint64(10), nf.GetCacheFingerprint())
	assert.Equal(t, uint64(11), nf.GetRuntimeFingerprint())
}

// When the trailer never arrived the node id is unknown; a node fingerprint
// stamped with "" would merge into a phantom node at the proxy, so it is dropped.
func TestToSchemaConsistency_DropsNodeFingerprintWhenNodeUnknown(t *testing.T) {
	st := newGroupSchemaState()
	o := st.object("stream", "foo")
	o.cache, o.runtime, o.hasNode = 10, 10, true

	sc := st.toSchemaConsistency()

	require.Len(t, sc.GetObjects(), 1)
	assert.Empty(t, sc.GetObjects()[0].GetNodeFingerprints(), "a node fingerprint without a node id is dropped")
}

func TestApplySchemaStates_AttachesToExistingAndAppendsUnknown(t *testing.T) {
	groups := []*fodcv1.GroupLifecycleInfo{{Name: "existing"}}

	existing := newGroupSchemaState()
	existing.node = "n1"
	existing.object("group", "existing").hasNode = true
	existing.errors = []string{"collect: boom"}

	unknown := newGroupSchemaState()
	unknown.node = "n1"
	unknown.object("group", "unknown").hasNode = true

	out := applySchemaStates(groups, map[string]*groupSchemaState{
		"existing": existing,
		"unknown":  unknown,
	})

	byName := make(map[string]*fodcv1.GroupLifecycleInfo, len(out))
	for _, g := range out {
		byName[g.GetName()] = g
	}
	require.Contains(t, byName, "existing")
	require.Contains(t, byName, "unknown")
	assert.NotNil(t, byName["existing"].GetSchemaConsistency(), "schema attaches to the InspectAll group entry")
	assert.Contains(t, byName["existing"].GetErrors(), "collect: boom", "collection errors ride on the group entry")
	assert.NotNil(t, byName["unknown"].GetSchemaConsistency(),
		"a group InspectAll did not return still carries its node's schema evidence")
}
