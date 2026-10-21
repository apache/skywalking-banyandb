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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import "strings"

// Empty entity selectors must retain their Boolean meaning when combined with
// another selector. The oracle evaluates these predicates over the fixture rows.
func generateEmptyEntityPredicates(b *suiteBuilder) {
	for _, op := range []string{"AND", "OR"} {
		for _, side := range []string{"left", "right", "both"} {
			empty := &Predicate{Op: "IN", Column: "rid"}
			value := &Predicate{Op: "=", Column: "rid", Values: []any{int64(1)}}
			children := []*Predicate{empty, value}
			if side == "right" {
				children = []*Predicate{value, empty}
			}
			if side == "both" {
				children = []*Predicate{empty, empty}
			}
			query := baseQuery()
			query.Where = &Predicate{Op: op, Children: children}
			b.add("generated_empty_entity_"+strings.ToLower(op)+"_"+side, query,
				"filter.in", "filter."+strings.ToLower(op), "values.int_list")
		}
	}
}
