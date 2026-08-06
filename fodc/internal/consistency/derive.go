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

// Package consistency derives and fingerprints schema state so the registry,
// node-cache and runtime views can be compared with identical semantics.
package consistency

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// BindingLookup returns every index rule binding whose subject is the given
// resource. Implementations are already scoped to one catalog.
type BindingLookup interface {
	BindingsOf(group, name string) []*databasev1.IndexRuleBinding
}

// RuleLookup resolves an index rule by name, returning nil when absent.
type RuleLookup interface {
	Rule(group, name string) *databasev1.IndexRule
}

// DeriveBoundRules returns the index rules bound to the given subject.
//
// Semantics deliberately mirror pkg/schema.schemaRepo.indexRules exactly: no
// sorting, no de-duplication, and no begin_at/expire_at filtering. Keeping the
// helper behavior-identical lets the production path delegate to it without
// changing what OnIndexUpdate receives. Determinism required by fingerprints is
// applied in Fingerprint, not here.
func DeriveBoundRules(group, name string, bindings BindingLookup, rules RuleLookup) []*databasev1.IndexRule {
	var result []*databasev1.IndexRule
	for _, b := range bindings.BindingsOf(group, name) {
		for _, ruleName := range b.GetRules() {
			if r := rules.Rule(group, ruleName); r != nil {
				result = append(result, r)
			}
		}
	}
	return result
}
