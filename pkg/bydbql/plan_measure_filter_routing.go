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

package bydbql

import databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"

// legacyWhereNeedsSchemaFilter uses column names, not unified scalar types, to
// preserve native legacy queries across heterogeneous source schemas. Strict
// schema resolution follows only when the query needs typed execution.
func legacyWhereNeedsSchemaFilter(expression *GrammarOrExpr, schemas map[string]*databasev1.Measure) bool {
	fields, tags := make(map[string]bool), make(map[string]bool)
	for _, schema := range schemas {
		for _, field := range schema.Fields {
			fields[field.Name] = true
		}
		for _, family := range schema.TagFamilies {
			for _, tag := range family.Tags {
				tags[tag.Name] = true
				tags[family.Name+"."+tag.Name] = true
			}
		}
	}
	return legacyWhereNeedsFilter(expression, func(name string) bool { return fields[name] && !tags[name] })
}

func (c *measureColumns) fieldOnly(name string) bool {
	_, field := c.fields[name]
	return field && len(c.tags[name]) == 0
}

// legacyWhereNeedsFilter activates typed execution only for new capabilities.
// Ordinary tag criteria retain their original request and binding behavior.
func legacyWhereNeedsFilter(expression *GrammarOrExpr, fieldOnly func(string) bool) bool {
	if expression == nil {
		return false
	}
	type capability struct{ residual, temporal, other bool }
	combine := func(left, right capability, or bool) capability {
		combined := capability{residual: left.residual || right.residual, temporal: left.temporal || right.temporal, other: left.other || right.other}
		combined.residual = combined.residual || or && combined.temporal && combined.other
		return combined
	}
	var visitOr func(*GrammarOrExpr) capability
	var visitAnd func(*GrammarAndExpr) capability
	visitPredicate := func(predicate *GrammarPredicate) capability {
		if predicate.Paren != nil {
			return visitOr(predicate.Paren)
		}
		var identifier *GrammarIdentifierPath
		switch {
		case predicate.Binary != nil:
			identifier = predicate.Binary.Identifier
		case predicate.In != nil:
			identifier = predicate.In.Identifier
		case predicate.Having != nil:
			identifier = predicate.Having.Identifier
		}
		if isBuiltinTime(identifier) {
			return capability{temporal: true}
		}
		name, err := identifier.ToString(false)
		if err != nil {
			return capability{residual: true, other: true}
		}
		return capability{residual: fieldOnly(name), other: true}
	}
	visitAnd = func(and *GrammarAndExpr) capability {
		result := visitPredicate(and.Left)
		for _, right := range and.Right {
			result = combine(result, visitPredicate(right.Right), false)
		}
		return result
	}
	visitOr = func(or *GrammarOrExpr) capability {
		result := visitAnd(or.Left)
		for _, right := range or.Right {
			result = combine(result, visitAnd(right.Right), true)
		}
		return result
	}
	return visitOr(expression).residual
}
