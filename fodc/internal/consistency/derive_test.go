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

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

type stubBindings struct {
	m map[string][]*databasev1.IndexRuleBinding
}

func (s *stubBindings) BindingsOf(group, name string) []*databasev1.IndexRuleBinding {
	return s.m[group+"/"+name]
}

type stubRules struct {
	m map[string]*databasev1.IndexRule
}

func (s *stubRules) Rule(group, name string) *databasev1.IndexRule {
	return s.m[group+"/"+name]
}

const testGroup = "g"

func rule(_, name string) *databasev1.IndexRule {
	return &databasev1.IndexRule{Metadata: &commonv1.Metadata{Group: testGroup, Name: name}}
}

// testSubject is the single subject every derivation case binds to; the helper's
// behavior does not depend on the name.
const testSubject = "foo"

func binding(name string, rules ...string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Group: testGroup, Name: name},
		Subject:  &databasev1.Subject{Name: testSubject, Catalog: commonv1.Catalog_CATALOG_STREAM},
		Rules:    rules,
	}
}

func TestDeriveBoundRules_CollectsRulesFromAllBindings(t *testing.T) {
	b := &stubBindings{m: map[string][]*databasev1.IndexRuleBinding{
		"g/foo": {binding("b1", "r1"), binding("b2", "r2")},
	}}
	r := &stubRules{m: map[string]*databasev1.IndexRule{
		"g/r1": rule("g", "r1"),
		"g/r2": rule("g", "r2"),
	}}

	got := DeriveBoundRules("g", "foo", b, r)

	assert.Len(t, got, 2)
}

func TestDeriveBoundRules_SkipsMissingRule(t *testing.T) {
	b := &stubBindings{m: map[string][]*databasev1.IndexRuleBinding{
		"g/foo": {binding("b1", "r1", "absent")},
	}}
	r := &stubRules{m: map[string]*databasev1.IndexRule{"g/r1": rule("g", "r1")}}

	got := DeriveBoundRules("g", "foo", b, r)

	assert.Len(t, got, 1, "a rule name with no rule object must be skipped silently")
}

func TestDeriveBoundRules_KeepsDuplicatesAndOrder(t *testing.T) {
	// Two bindings referencing the same rule must yield it twice: the helper
	// deliberately mirrors pkg/schema.schemaRepo.indexRules, which does not
	// de-duplicate. Determinism is applied by Fingerprint instead.
	b := &stubBindings{m: map[string][]*databasev1.IndexRuleBinding{
		"g/foo": {binding("b1", "r1"), binding("b2", "r1")},
	}}
	r := &stubRules{m: map[string]*databasev1.IndexRule{"g/r1": rule("g", "r1")}}

	got := DeriveBoundRules("g", "foo", b, r)

	assert.Len(t, got, 2)
}

func TestDeriveBoundRules_NoBindingsYieldsNil(t *testing.T) {
	got := DeriveBoundRules("g", "foo", &stubBindings{}, &stubRules{})

	assert.Nil(t, got)
}
