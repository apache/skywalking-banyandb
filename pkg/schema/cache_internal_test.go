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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestIndexRules_BehaviourContract locks the exact semantics indexRules must
// keep across the DeriveBoundRules refactor: duplicates preserved, missing rules
// skipped silently.
func TestIndexRules_BehaviourContract(t *testing.T) {
	sr := &schemaRepo{}

	bindings := &sync.Map{}
	bindings.Store("g/b1", &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b1"},
		Subject:  &databasev1.Subject{Name: "foo", Catalog: commonv1.Catalog_CATALOG_STREAM},
		Rules:    []string{"r1", "absent"},
	})
	bindings.Store("g/b2", &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "b2"},
		Subject:  &databasev1.Subject{Name: "foo", Catalog: commonv1.Catalog_CATALOG_STREAM},
		Rules:    []string{"r1"},
	})
	sr.bindingForwardMap.Store("g/foo", bindings)
	sr.indexRuleMap.Store("g/r1", &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g", Name: "r1"},
	})

	got := sr.IndexRules(&databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo"},
	})

	assert.Len(t, got, 2, "r1 must appear once per binding; the absent rule must be skipped")
	for _, r := range got {
		assert.Equal(t, "r1", r.GetMetadata().GetName())
	}
}

// TestIndexRules_IgnoresBindingValidityWindow pins the current (unfiltered)
// semantics. The registry-side helper in banyand/metadata/client.go DOES filter
// begin_at/expire_at; matching that here would change production behavior and
// turn every expired binding into a permanent false positive in the consistency
// check.
func TestIndexRules_IgnoresBindingValidityWindow(t *testing.T) {
	sr := &schemaRepo{}
	bindings := &sync.Map{}
	bindings.Store("g/expired", &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: "g", Name: "expired"},
		Subject:  &databasev1.Subject{Name: "foo", Catalog: commonv1.Catalog_CATALOG_STREAM},
		Rules:    []string{"r1"},
		// begin_at / expire_at left unset: an unset expire_at reads as "already
		// expired" under the registry-side filter, yet must still be applied here.
	})
	sr.bindingForwardMap.Store("g/foo", bindings)
	sr.indexRuleMap.Store("g/r1", &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g", Name: "r1"},
	})

	got := sr.IndexRules(&databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo"},
	})

	assert.Len(t, got, 1, "cache-side derivation must not filter by the binding validity window")
}

// TestIndexRules_NoBindingsYieldsNil pins the empty case.
func TestIndexRules_NoBindingsYieldsNil(t *testing.T) {
	sr := &schemaRepo{}

	got := sr.IndexRules(&databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: "g", Name: "foo"},
	})

	assert.Nil(t, got)
}

func TestLoadAllResources_FiltersByGroup(t *testing.T) {
	sr := &schemaRepo{}
	sr.resourceMap.Store("g1/a", &resourceSpec{
		schema: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: "g1", Name: "a"}},
	})
	sr.resourceMap.Store("g2/b", &resourceSpec{
		schema: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: "g2", Name: "b"}},
	})

	got := sr.LoadAllResources("g1")

	assert.Len(t, got, 1)
	assert.Equal(t, "a", got[0].Schema().GetMetadata().GetName())
}

// TestLoadAllResources_DoesNotMatchGroupPrefix guards against a naive prefix
// match leaking "g1x/..." into group "g1".
func TestLoadAllResources_DoesNotMatchGroupPrefix(t *testing.T) {
	sr := &schemaRepo{}
	sr.resourceMap.Store("g1/a", &resourceSpec{
		schema: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: "g1", Name: "a"}},
	})
	sr.resourceMap.Store("g1x/b", &resourceSpec{
		schema: &databasev1.Stream{Metadata: &commonv1.Metadata{Group: "g1x", Name: "b"}},
	})

	got := sr.LoadAllResources("g1")

	assert.Len(t, got, 1)
	assert.Equal(t, "g1", got[0].Schema().GetMetadata().GetGroup())
}

func TestLoadAllIndexRules_FiltersByGroup(t *testing.T) {
	sr := &schemaRepo{}
	sr.indexRuleMap.Store("g1/r1", &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g1", Name: "r1"},
	})
	sr.indexRuleMap.Store("g2/r2", &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Group: "g2", Name: "r2"},
	})

	got := sr.LoadAllIndexRules("g1")

	assert.Len(t, got, 1)
	assert.Equal(t, "r1", got[0].GetMetadata().GetName())
}
