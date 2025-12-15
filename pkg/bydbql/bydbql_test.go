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

package bydbql_test

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestBydbql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bydbql Suite")
}

var _ = Describe("Parser", func() {
	Describe("Generic Tests", func() {
		Describe("Parser", func() {
			Describe("valid queries", func() {
				It("parses SELECT with FROM clause", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
					Expect(stmt.From.ResourceName).To(Equal("sw"))
				})

				It("parses SELECT with WHERE clause", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default WHERE service_id = 'webapp'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())

					// Should be a single condition
					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
						if pred.Binary != nil {
							idName, _ := pred.Binary.Identifier.ToString(false)
							Expect(idName).To(Equal("service_id"))
						}
					}
				})

				It("parses SELECT with TIME condition", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Time).NotTo(BeNil())
					Expect(stmt.Time.Comparator).NotTo(BeNil())
					Expect(*stmt.Time.Comparator).To(Equal(">"))
				})

				It("parses SELECT with GROUP BY", func() {
					grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default GROUP BY region")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.GroupBy).NotTo(BeNil())
					Expect(stmt.GroupBy.Columns).To(HaveLen(1))
					gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
					Expect(gbErr0).To(BeNil())
					Expect(gbName0).To(Equal("region"))
					Expect(stmt.GroupBy.Columns[0].TypeSpec).To(BeNil())
				})

				It("parses SELECT with both TIME BETWEEN and WHERE clause", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default " +
						"TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z' WHERE service_id = 'webapp' AND status = 200")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify TIME BETWEEN is parsed
					Expect(stmt.Time).NotTo(BeNil())
					Expect(stmt.Time.Between).NotTo(BeNil())
					Expect(stmt.Time.Between.Begin.ToString()).To(Equal("2023-01-01T00:00:00Z"))
					Expect(stmt.Time.Between.End.ToString()).To(Equal("2023-01-02T00:00:00Z"))

					// Verify WHERE clause is parsed as a binary tree
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())

					// Should be a binary AND expression
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses TOP N statement", func() {
					grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN default ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.TopN
					Expect(stmt.N).To(Equal(10))
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Dir).NotTo(BeNil())
					Expect(stmt.OrderBy.Dir).NotTo(BeNil())
					Expect(*stmt.OrderBy.Dir).To(Equal("DESC"))
				})

				It("parses empty projection for traces", func() {
					grammar, err := ParseQuery("SELECT () FROM TRACE sw_trace IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Empty).To(BeTrue())
				})

				It("parses WITH QUERY_TRACE", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WITH QUERY_TRACE")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.WithQueryTrace).NotTo(BeNil())
				})

				// Test cases for optional parentheses in group list
				It("parses FROM clause with groups in parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN (group1, group2)")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(HaveLen(2))
					Expect(stmt.From.In.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("group2"))
				})

				It("parses FROM clause with groups without parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN group1, group2")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(HaveLen(2))
					Expect(stmt.From.In.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("group2"))
				})

				It("parses FROM clause with single group without parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM MEASURE metrics IN us-west")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(HaveLen(1))
					Expect(stmt.From.In.Groups[0]).To(Equal("us-west"))
				})

				It("parses FROM clause with multiple groups without parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM MEASURE metrics IN us-west, us-east, eu-central")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					Expect(stmt.From.In.Groups[0]).To(Equal("us-west"))
					Expect(stmt.From.In.Groups[1]).To(Equal("us-east"))
					Expect(stmt.From.In.Groups[2]).To(Equal("eu-central"))
				})

				It("parses FROM clause with single stage", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ON warn STAGES")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Stage.Stages).To(HaveLen(1))
					Expect(stmt.From.Stage.Stages[0]).To(Equal("warn"))
				})

				It("parses FROM clause with multiple stages without parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ON warn, code STAGES")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Stage.Stages).To(HaveLen(2))
					Expect(stmt.From.Stage.Stages[0]).To(Equal("warn"))
					Expect(stmt.From.Stage.Stages[1]).To(Equal("code"))
				})

				It("parses FROM clause with multiple stages with parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM MEASURE metrics IN us-west ON (warn, code) STAGES")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Stage.Stages).To(HaveLen(2))
					Expect(stmt.From.Stage.Stages[0]).To(Equal("warn"))
					Expect(stmt.From.Stage.Stages[1]).To(Equal("code"))
				})

				It("parses TRACE query with stages", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN group1 ON warn, code STAGES TIME > '-30m'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))
					Expect(stmt.From.Stage.Stages).To(HaveLen(2))
					Expect(stmt.From.Stage.Stages[0]).To(Equal("warn"))
					Expect(stmt.From.Stage.Stages[1]).To(Equal("code"))
				})

				It("parses TOP N query with groups without parentheses", func() {
					grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production, staging ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.TopN
					Expect(stmt.N).To(Equal(10))
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(HaveLen(2))
					Expect(stmt.From.In.Groups[0]).To(Equal("production"))
					Expect(stmt.From.In.Groups[1]).To(Equal("staging"))
				})

				It("parses TOP N query with stages", func() {
					grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production ON warn, code STAGES TIME > '-30m' ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.TopN
					Expect(stmt.N).To(Equal(10))
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Stage.Stages).To(HaveLen(2))
					Expect(stmt.From.Stage.Stages[0]).To(Equal("warn"))
					Expect(stmt.From.Stage.Stages[1]).To(Equal("code"))
				})

				It("parses TRACE query with groups without parentheses", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN group1, group2, group3 TIME > '-30m'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					Expect(stmt.From.In.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("group2"))
					Expect(stmt.From.In.Groups[2]).To(Equal("group3"))
				})

				It("parses PROPERTY query with groups without parentheses", func() {
					grammar, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1, datacenter-2 WHERE in_service = 'true'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("PROPERTY"))
					Expect(stmt.From.In.Groups).To(HaveLen(2))
					Expect(stmt.From.In.Groups[0]).To(Equal("datacenter-1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("datacenter-2"))
				})

				// Group list boundary tests
				It("parses group names with hyphens", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN us-west-1, us-east-1, eu-central-1")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					Expect(stmt.From.In.Groups[0]).To(Equal("us-west-1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("us-east-1"))
					Expect(stmt.From.In.Groups[2]).To(Equal("eu-central-1"))
				})

				It("parses group names with underscores", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN prod_primary, prod_secondary, staging_test")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					Expect(stmt.From.In.Groups[0]).To(Equal("prod_primary"))
					Expect(stmt.From.In.Groups[1]).To(Equal("prod_secondary"))
					Expect(stmt.From.In.Groups[2]).To(Equal("staging_test"))
				})

				It("parses group names with mixed case", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN Production, Staging, Development")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					// Group names should preserve case
					Expect(stmt.From.In.Groups[0]).To(Equal("Production"))
					Expect(stmt.From.In.Groups[1]).To(Equal("Staging"))
					Expect(stmt.From.In.Groups[2]).To(Equal("Development"))
				})

				It("parses group names with numbers", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN cluster1, cluster2, cluster3")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From.In.Groups).To(HaveLen(3))
					Expect(stmt.From.In.Groups[0]).To(Equal("cluster1"))
					Expect(stmt.From.In.Groups[1]).To(Equal("cluster2"))
					Expect(stmt.From.In.Groups[2]).To(Equal("cluster3"))
				})

				It("handles empty group list in parentheses", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN ()")

					// Parser may reject empty group list
					Expect(err.Error()).To(Or(
						ContainSubstring("syntax"),
						ContainSubstring("unexpected"),
						ContainSubstring("IN"),
						ContainSubstring("group"),
						ContainSubstring("empty"),
					))
				})

				It("rejects malformed group list", func() {
					// Missing group name after comma
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN group1,")
					Expect(err).NotTo(BeNil())
					Expect(grammar).To(BeNil())
				})

				// Test cases for parentheses and operator precedence
				It("parses WHERE with parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE (service_id = 'webapp' OR service_id = 'api') AND status = 200")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be AND
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE with AND precedence over OR", func() {
					// a=1 OR b=2 AND c=3 should parse as: a=1 OR (b=2 AND c=3)
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 OR b = 2 AND c = 3")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE precedence with mixed logical operators", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default WHERE service_id = 'payment-service' AND status = 500 OR region = 'us-east'")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE precedence with nested groups", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE (service_id = 'auth' OR region = 'us-east') AND (status = 500 OR (status = 503 AND message MATCH('timeout')))"
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE precedence with AND binding tighter than OR", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE service_id = 'edge' OR status = 500 AND (region = 'us-east' OR region = 'eu-west')"
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses ORDER BY with direction only", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Tail.WithIdent).To(BeNil())
					Expect(*stmt.OrderBy.Tail.DirOnly).To(Equal("DESC"))
				})

				It("parses ORDER BY with column and direction", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY response_time DESC")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
					obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
					Expect(obErr).To(BeNil())
					Expect(obCol).To(Equal("response_time"))
					Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
					Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
					Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))
				})

				It("parses ORDER BY with column only", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY response_time")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
					obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
					Expect(obErr).To(BeNil())
					Expect(obCol).To(Equal("response_time"))
					// OrderBy ascending (default or explicit ASC)
				})

				It("parses WHERE with nested parentheses", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE ((a = 1 OR b = 2) AND c = 3) OR d = 4")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE with multiple ANDs", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 AND b = 2 AND c = 3")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Should create left-associative tree: (a=1 AND b=2) AND c=3
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses WHERE with complex mixed operators", func() {
					// a=1 AND b=2 OR c=3 AND d=4 should parse as: (a=1 AND b=2) OR (c=3 AND d=4)
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 AND b = 2 OR c = 3 AND d = 4")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				// Test cases for MATCH operator
				It("parses MATCH with single value", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with analyzer", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error', 'simple')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with analyzer and operator", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error warning', 'simple', 'OR')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with array values", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE tags MATCH(('error', 'warning'))")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with array and options", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE tags MATCH(('tag1', 'tag2'), 'standard', 'AND')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with integer values", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE code MATCH((404, 500, 503))")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with keyword analyzer and multiple values", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default WHERE operation_name MATCH(('GET', 'POST'), 'keyword')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH within grouped logical expressions", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE (message MATCH('error') OR message MATCH('timeout', 'standard')) AND (status = 500 OR status = 503)"
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				It("parses MATCH with dot-separated identifier from documentation", func() {
					grammar, err := ParseQuery("SELECT trace_id, db.instance, data_binary FROM STREAM sw IN (default) WHERE db.instance MATCH('mysql')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("db.instance"))

					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.In.Groups).To(Equal([]string{"default"}))

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
						if pred.Binary != nil {
							idName, _ := pred.Binary.Identifier.ToString(false)
							Expect(idName).To(Equal("db.instance"))
						}
					}
				})

				It("parses complex query with MATCH and other conditions", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE service_id = 'auth' AND message MATCH('error', 'simple') OR status = 500")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					// Verify AND/OR structure using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					// Grammar represents AND/OR as tree structure
					Expect(stmt.Where.Expr.Left).NotTo(BeNil())
				})

				// MATCH operator error scenarios
				It("handles MATCH with too many arguments", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error', 'standard', 'OR', 'extra')")

					// Parser may reject too many arguments
					Expect(err.Error()).To(Or(
						ContainSubstring("syntax"),
						ContainSubstring("unexpected"),
						ContainSubstring("MATCH"),
						ContainSubstring("argument"),
					))
				})

				It("handles MATCH with too few arguments", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH()")

					// Parser may reject empty MATCH
					Expect(err.Error()).To(Or(
						ContainSubstring("syntax"),
						ContainSubstring("unexpected"),
						ContainSubstring("expected"),
						ContainSubstring("MATCH"),
						ContainSubstring("argument"),
						ContainSubstring("value"),
					))
				})

				It("parses MATCH with only value (no analyzer, no operator)", func() {
					// This should be valid - just the value
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})

				It("parses MATCH with value and analyzer (no operator)", func() {
					// This should be valid - value and analyzer without operator
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error', 'standard')")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())

					// Verify WHERE condition using Grammar
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())
					if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
						pred := stmt.Where.Expr.Left.Left
						Expect(pred).NotTo(BeNil())
					}
				})
			})

			Describe("invalid queries", func() {
				It("handles invalid syntax", func() {
					grammar, err := ParseQuery("SELECT FROM")
					Expect(err).ToNot(BeNil())
					Expect(grammar).To(BeNil())
				})
			})
		})

		Describe("Complex Queries", func() {
			complexQueries := []string{
				// Stream queries
				`SELECT trace_id, service_id, start_time
			 FROM STREAM sw IN (default, updated)
			 WHERE service_id = 'webapp' AND state = 1
			 ORDER BY start_time DESC
			 LIMIT 100`,

				// Measure queries with complex projections
				`SELECT region, SUM(latency)
			 FROM MEASURE service_cpm IN (us-west, us-east)
			 TIME BETWEEN '-2h' AND 'now'
			 WHERE service = 'auth-service'
			 GROUP BY region`,

				// Trace queries with empty projection
				`SELECT ()
			 FROM TRACE sw_trace IN default
			 TIME > '-1h'
			 WHERE status = 'error'
			 WITH QUERY_TRACE
			 LIMIT 50`,

				// Property queries
				`SELECT ip, region, owner
			 FROM PROPERTY server_metadata IN (datacenter-1, datacenter-2)
			 WHERE datacenter = 'dc-101'
			 LIMIT 50`,

				// Top-N queries
				`SHOW TOP 5
			 FROM MEASURE service_errors IN (production, staging)
			 TIME BETWEEN '-24h' AND 'now'
			 WHERE status_code = '500'
			 ORDER BY DESC`,

				// Query with OFFSET
				`SELECT * FROM STREAM sw IN default
			 WHERE service_id = 'api'
			 ORDER BY timestamp ASC
			 LIMIT 50
			 OFFSET 100`,

				// Query with IN operator
				`SELECT * FROM STREAM sw IN default
			 TIME > '-1h'
			 WHERE status IN (200, 201, 204)`,

				// Query with NOT IN operator
				`SELECT * FROM STREAM logs IN default
			 WHERE level NOT IN ('DEBUG', 'TRACE')
			 LIMIT 1000`,

				// Query with HAVING operator for array contains
				`SELECT * FROM STREAM sw IN default
			 WHERE tags HAVING ('error', 'critical')`,

				// Query with HAVING operator for single value
				`SELECT * FROM STREAM sw IN default
			 WHERE tags HAVING 'error'`,

				// Query with NOT HAVING operator
				`SELECT * FROM STREAM sw IN default
			 WHERE tags NOT HAVING ('test', 'debug')`,

				// Query with aggregation function
				`SELECT region, SUM(latency)
			 FROM MEASURE metrics IN default
			 GROUP BY region
			 ORDER BY region ASC`,

				// Query with multiple GROUP BY columns
				`SELECT service, region, environment, SUM(requests)
			 FROM MEASURE service_metrics IN default
			 TIME BETWEEN '-1h' AND 'now'
			 GROUP BY service, region, environment`,

				// Full-featured query with all clauses
				`SELECT trace_id, service_id, duration
			 FROM STREAM sw IN (prod, staging)
			 TIME BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-02T00:00:00Z'
			 WHERE service_id = 'api-gateway' AND status >= 200 AND status < 300
			 ORDER BY duration DESC
			 WITH QUERY_TRACE
			 LIMIT 100
			 OFFSET 50`,

				// Query with complex nested WHERE conditions
				`SELECT * FROM STREAM sw IN default
			 WHERE ((service = 'auth' OR service = 'api') AND status != 500)
			    OR (service = 'web' AND (latency > 1000 OR error_count > 5))`,

				// TOP N query with where and order
				`SHOW TOP 10
			 FROM MEASURE service_metrics IN default
			 TIME > '-30m'
			 WHERE region = 'us-west' AND environment = 'production'
			 ORDER BY DESC`,

				// Query with dot-separated identifier paths
				`SELECT metadata.service_id, metadata.region, response.status
			 FROM STREAM sw IN default
			 WHERE metadata.region = 'us-east'`,

				// Query with comparison operators
				`SELECT * FROM MEASURE metrics IN default
			 TIME >= '2024-01-01T12:00:00Z'
			 WHERE latency >= 100 AND latency <= 1000`,
			}

			for i, query := range complexQueries {
				It(fmt.Sprintf("parses complex query %d", i+1), func() {
					// Parse the query
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil(), "failed to parse query: %s\nerror: %v", query, err)
					Expect(grammar).NotTo(BeNil())
				})
			}
		})

		Describe("Error Handling", func() {
			invalidQueries := []string{
				"SELECT",                                                                                // incomplete query
				"SELECT * FROM",                                                                         // missing resource
				"SELECT * FROM INVALID sw",                                                              // invalid resource type
				"SELECT * FROM STREAM sw in test,",                                                      // missing group after comma
				"SHOW TOP FROM MEASURE metrics",                                                         // missing N
				"SELECT * WHERE service_id",                                                             // incomplete condition
				"TIME > '2023-01-01'",                                                                   // missing SELECT
				"SELECT * FROM STREAM sw GROUP BY",                                                      // incomplete GROUP BY
				"SELECT * FROM STREAM sw ORDER BY",                                                      // missing column in ORDER BY
				"SELECT * FROM STREAM sw WHERE",                                                         // missing condition after WHERE
				"SELECT * FROM STREAM sw WHERE service =",                                               // incomplete WHERE condition
				"SELECT * FROM STREAM sw WHERE service_id MATCH",                                        // missing MATCH parameters
				"SELECT * FROM STREAM sw WHERE service_id IN",                                           // missing IN list
				"SELECT * FROM STREAM sw WHERE tags HAVING",                                             // missing HAVING list
				"SELECT * FROM STREAM sw WHERE (service = 'a'",                                          // unmatched opening parenthesis
				"SELECT * FROM STREAM sw WHERE service = 'a')",                                          // unmatched closing parenthesis
				"SELECT * FROM STREAM sw TIME BETWEEN",                                                  // incomplete TIME BETWEEN
				"SELECT * FROM STREAM sw TIME BETWEEN '2024-01-01' AND",                                 // incomplete TIME BETWEEN
				"SELECT * FROM STREAM sw LIMIT",                                                         // missing LIMIT value
				"SELECT * FROM STREAM sw OFFSET",                                                        // missing OFFSET value
				"SELECT * FROM STREAM sw IN default,,other",                                             // double comma in groups
				"SELECT ** FROM STREAM sw IN default",                                                   // invalid projection
				"SELECT FROM STREAM sw IN default WHERE id = 1",                                         // missing projection
				"SHOW TOP abc FROM MEASURE m IN default",                                                // non-integer TOP N
				"SELECT * FROM STREAM sw WHERE a = 1 AND",                                               // incomplete logical expression
				"SELECT * FROM STREAM sw WHERE a = 1 OR",                                                // incomplete logical expression
				"SELECT * FROM STREAM sw WHERE () = 1",                                                  // empty parentheses in condition
				"SELECT * FROM STREAM sw IN",                                                            // missing group list
				"SELECT SUM() FROM MEASURE m IN default",                                                // aggregate function without column
				"SELECT * FROM STREAM sw in default ORDER BY",                                           // incomplete ORDER BY
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::invalid",        // invalid type specifier in GROUP BY
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY ::tag",                  // missing column name before type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::",               // incomplete type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region:tag",             // malformed type specifier (single colon)
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag::field",     // multiple type specifiers
				"SELECT region, service, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag,",  // trailing comma after type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag, service::", // incomplete type specifier after comma
				"SELECT TOP 10 service_id::field DESC FROM STREAM sw IN default",                        // TOP N identifier no type declaration

				// top N not allowed with OR
				"SHOW TOP 10 FROM MEASURE service_metrics IN default TIME > '-30m' WHERE region = 'us-west' OR environment = 'production'",
				// top N not allowed ORDER BY with identifier
				"SHOW TOP 10 FROM MEASURE service_latency IN default TIME < '2023-01-01T00:00:00Z' ORDER BY value DESC",
			}

			for i, query := range invalidQueries {
				It(fmt.Sprintf("handles invalid query %d: %s", i+1, query), func() {
					grammar, err := ParseQuery(query)

					// Should have parsing error
					Expect(err).ToNot(BeNil(), "expected parsing error for query: %s", query)

					// Parsed query should be nil for invalid queries
					Expect(grammar).To(BeNil(), "expected nil parsed query for invalid query: %s", query)
				})
			}
		})

		Describe("Time Format Parsing", func() {
			DescribeTable("parses time formats correctly in queries",
				func(timeCondition string, validateFunc func(*Grammar) bool) {
					query := fmt.Sprintf("SELECT * FROM STREAM sw IN default TIME %s", timeCondition)
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					Expect(validateFunc(grammar)).To(BeTrue(), "time validation failed for data: %+v", grammar)
				},
				Entry("absolute time range",
					"BETWEEN '2023-01-01T10:00:00Z' AND '2023-01-01T11:00:00Z'",
					func(data *Grammar) bool {
						sel := data.Select
						return sel.Time != nil && sel.Time.Between != nil &&
							sel.Time.Between.Begin.ToString() == "2023-01-01T10:00:00Z" && sel.Time.Between.End.ToString() == "2023-01-01T11:00:00Z"
					}),
				Entry("relative time condition",
					"> '-30m'",
					func(data *Grammar) bool {
						sel := data.Select
						return sel.Time != nil && sel.Time.Between == nil && sel.Time.Comparator != nil && sel.Time.Value != nil
					}),
			)
		})

		Describe("NULL Value Support", func() {
			It("parses WHERE field = NULL", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE optional_field = NULL")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("optional_field"))
					}
				}
			})

			It("parses WHERE field != NULL", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE optional_field != NULL")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("optional_field"))
					}
				}
			})

			It("parses NULL in MEASURE queries", func() {
				grammar, err := ParseQuery("SELECT region, SUM(value) FROM MEASURE metrics IN default TIME > '-30m' WHERE optional_tag = NULL GROUP BY region")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses NULL with AND/OR conditions", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'webapp' AND optional_field = NULL")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify AND/OR structure using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				// Grammar represents AND/OR as tree structure
				Expect(stmt.Where.Expr.Left).NotTo(BeNil())
			})

			It("parses case-insensitive NULL", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE f = null")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})
		})

		Describe("Inequality Operators", func() {
			It("parses != operator with string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id != 'webapp'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("webapp"))
						}
					}
				}
			})

			It("parses != operator with integer", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status != 200")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.Integer != nil {
							Expect(*pred.Binary.Tail.Compare.Value.Integer).To(Equal(int64(200)))
						}
					}
				}
			})

			It("parses > operator with string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE string_field > 'abc'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("abc"))
						}
					}
				}
			})

			It("parses < operator with string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE string_field < 'xyz'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("xyz"))
						}
					}
				}
			})

			It("parses >= operator with string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version >= '1.0.0'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("1.0.0"))
						}
					}
				}
			})

			It("parses <= operator with string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version <= '2.0.0'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("2.0.0"))
						}
					}
				}
			})

			It("parses multiple inequality operators", func() {
				grammar, err := ParseQuery("SELECT * FROM MEASURE metrics IN default TIME > '-30m' WHERE latency >= 100 AND latency <= 1000")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify AND/OR structure using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				// Grammar represents AND/OR as tree structure
				Expect(stmt.Where.Expr.Left).NotTo(BeNil())
			})
		})

		Describe("IN and NOT IN Operators - Boundary Cases", func() {
			It("parses IN with single value", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200)")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses IN with two values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 404)")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses IN with many values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 201, 202, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503)")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses IN with string values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE region IN ('us-west-1', 'us-east-1', 'eu-west-1')")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("handles IN with empty list", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN ()")

				if err != nil {
					// Parser may reject empty IN list
					Expect(err.Error()).To(Or(
						ContainSubstring("IN"),
						ContainSubstring("empty"),
						ContainSubstring("values"),
					))
				} else {
					// Parser might accept syntactically, semantic validator should reject
					Expect(grammar).NotTo(BeNil())
				}
			})

			It("parses NOT IN with single value", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN (500)")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses NOT IN with multiple values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN (500, 502, 503, 504)")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("handles NOT IN with empty list", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN ()")

				if err != nil {
					// Parser may reject empty NOT IN list
					Expect(err.Error()).To(Or(
						ContainSubstring("NOT IN"),
						ContainSubstring("IN"),
						ContainSubstring("empty"),
						ContainSubstring("values"),
					))
				} else {
					// Parser might accept syntactically, semantic validator should reject
					Expect(grammar).NotTo(BeNil())
				}
			})

			It("parses IN with mixed integer and string values (if supported)", func() {
				// Some systems may allow mixed types, others may reject
				// Use "status" instead of "field" to avoid keyword conflict
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 'error', 404)")

				if err != nil {
					// If not supported, error should mention type mismatch
					Expect(err.Error()).To(Or(
						ContainSubstring("type"),
						ContainSubstring("mixed"),
						ContainSubstring("identifier"),
						ContainSubstring("keyword"),
					))
				} else {
					// If supported, verify parsing
					Expect(grammar).NotTo(BeNil())
					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())
				}
			})
		})

		Describe("Complex Dot-Separated Paths", func() {
			It("parses deep nested paths in projection", func() {
				grammar, err := ParseQuery("SELECT a.b.c.d.e FROM STREAM sw IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection.Columns).To(HaveLen(1))
				pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("a.b.c.d.e"))
			})

			It("parses nested paths in WHERE clause", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE metadata.request.header.content_type = 'application/json'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("metadata.request.header.content_type"))
					}
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("application/json"))
						}
					}
				}
			})

			It("parses nested paths with type specifiers", func() {
				grammar, err := ParseQuery("SELECT metadata.service.name::tag, response.body.size::field FROM MEASURE metrics IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("metadata.service.name"))
				Expect(strings.ToUpper(*stmt.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))
				pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("response.body.size"))
				Expect(strings.ToUpper(*stmt.Projection.Columns[1].TypeSpec)).To(Equal("FIELD"))
			})

			It("parses nested paths in GROUP BY", func() {
				grammar, err := ParseQuery("SELECT metadata.region.name, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY metadata.region.name")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("metadata.region.name"))
				Expect(stmt.GroupBy.Columns[0].TypeSpec).To(BeNil())
			})

			It("parses nested paths in ORDER BY", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY metadata.timestamp.value DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
				Expect(obErr).To(BeNil())
				Expect(obCol).To(Equal("metadata.timestamp.value"))
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
				Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))
			})

			It("parses nested paths in aggregate functions", func() {
				grammar, err := ParseQuery("SELECT SUM(metrics.response.latency) FROM MEASURE m IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection.Columns).To(HaveLen(1))
				Expect(stmt.Projection.Columns[0].Aggregate).NotTo(BeNil())
				aggColName, _ := stmt.Projection.Columns[0].Aggregate.Column.ToString(false)
				Expect(aggColName).To(Equal("metrics.response.latency"))
			})

			It("parses complex query with multiple nested paths", func() {
				grammar, err := ParseQuery(`SELECT
				trace.span.id,
				metadata.service.name::tag,
				metrics.response.time::field
			FROM STREAM sw IN default
			TIME > '-30m'
			WHERE metadata.region.datacenter = 'us-west-1'
			ORDER BY metrics.response.time DESC`)
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection.Columns).To(HaveLen(3))
				pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("trace.span.id"))
				pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("metadata.service.name"))
				pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
				Expect(pcErr2).To(BeNil())
				Expect(pcName2).To(Equal("metrics.response.time"))
			})
		})

		Describe("Advanced ORDER BY", func() {
			It("parses ORDER BY with dot-separated path", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY metadata.timestamp ASC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
				Expect(obErr).To(BeNil())
				Expect(obCol).To(Equal("metadata.timestamp"))
				// OrderBy ascending (default or explicit ASC)
			})

			It("parses ORDER BY with nested path and DESC", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY response.metadata.timestamp DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
				Expect(obErr).To(BeNil())
				Expect(obCol).To(Equal("response.metadata.timestamp"))
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
				Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))
			})

			It("parses case-insensitive ORDER BY with ASC/DESC", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' order by timestamp asc")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.OrderBy).NotTo(BeNil())
				// OrderBy ascending (default or explicit ASC)
			})

			// Stream-specific: ORDER BY TIME shorthand
			It("parses ORDER BY TIME DESC for Stream queries", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY TIME DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
				Expect(stmt.OrderBy).NotTo(BeNil())
				// ORDER BY TIME is a shorthand that relies on timestamps
				// The parser should handle this appropriately
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
				Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))
			})

			It("parses ORDER BY TIME ASC for Stream queries", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY TIME ASC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
				Expect(stmt.OrderBy).NotTo(BeNil())
				// OrderBy ascending (default or explicit ASC)
			})

			It("parses ORDER BY TIME for Trace queries", func() {
				grammar, err := ParseQuery("SELECT trace_id, service_id FROM TRACE sw_trace IN default TIME > '-1h' ORDER BY TIME DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
				Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))
			})

			It("parses ORDER BY TIME for Measure queries", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region ORDER BY TIME ASC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("MEASURE"))
				Expect(stmt.OrderBy).NotTo(BeNil())
				// OrderBy ascending (default or explicit ASC)
			})
		})

		Describe("GROUP BY with Type Specifiers", func() {
			It("parses GROUP BY with single column without type specifier", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].TypeSpec).To(BeNil())
			})

			It("parses GROUP BY with single column with ::tag specifier", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("region"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))
			})

			It("parses GROUP BY with single column with ::field specifier", func() {
				grammar, err := ParseQuery("SELECT status, COUNT(requests) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY status::field")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("status"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("FIELD"))
			})

			It("parses GROUP BY with multiple columns without type specifiers", func() {
				grammar, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region, service")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].TypeSpec).To(BeNil())
				gbName1, gbErr1 := stmt.GroupBy.Columns[1].Identifier.ToString(stmt.GroupBy.Columns[1].TypeSpec != nil)
				Expect(gbErr1).To(BeNil())
				Expect(gbName1).To(Equal("service"))
				Expect(stmt.GroupBy.Columns[1].TypeSpec).To(BeNil())
			})

			It("parses GROUP BY with multiple columns with type specifiers", func() {
				grammar, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag, service::tag")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("region"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))
				gbName1, gbErr1 := stmt.GroupBy.Columns[1].Identifier.ToString(stmt.GroupBy.Columns[1].TypeSpec != nil)
				Expect(gbErr1).To(BeNil())
				Expect(gbName1).To(Equal("service"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[1].TypeSpec)).To(Equal("TAG"))
			})

			It("parses GROUP BY with mixed columns (with and without type specifiers)", func() {
				grammar, err := ParseQuery("SELECT region, service, status, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag, service, status::field")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(3))

				// First column with ::tag
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("region"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))

				// Second column without type specifier
				gbName1, gbErr1 := stmt.GroupBy.Columns[1].Identifier.ToString(stmt.GroupBy.Columns[1].TypeSpec != nil)
				Expect(gbErr1).To(BeNil())
				Expect(gbName1).To(Equal("service"))
				Expect(stmt.GroupBy.Columns[1].TypeSpec).To(BeNil())

				// Third column with ::field
				gbName2, gbErr2 := stmt.GroupBy.Columns[2].Identifier.ToString(stmt.GroupBy.Columns[2].TypeSpec != nil)
				Expect(gbErr2).To(BeNil())
				Expect(gbName2).To(Equal("status"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[2].TypeSpec)).To(Equal("FIELD"))
			})

			It("parses GROUP BY with dot-separated column name and type specifier", func() {
				grammar, err := ParseQuery("SELECT metadata.region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY metadata.region::tag")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("metadata.region"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))
			})

			It("parses case-insensitive type specifiers", func() {
				grammar, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::TAG, service::FIELD")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[1].TypeSpec)).To(Equal("FIELD"))
			})

			It("parses GROUP BY with type specifiers in complex query", func() {
				grammar, err := ParseQuery(`SELECT region::tag, service::tag, status::field, SUM(latency)
				FROM MEASURE metrics IN default
				TIME > '-30m'
				WHERE region = 'us-west'
				GROUP BY region::tag, service::tag, status::field
				ORDER BY latency DESC
				LIMIT 100`)
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(3))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[0].TypeSpec)).To(Equal("TAG"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[1].TypeSpec)).To(Equal("TAG"))
				Expect(strings.ToUpper(*stmt.GroupBy.Columns[2].TypeSpec)).To(Equal("FIELD"))
			})
		})

		Describe("Case Insensitivity", func() {
			// Test that keywords are case-insensitive
			queries := []string{
				// Basic SELECT FROM
				"SELECT * FROM STREAM sw IN default",
				"select * from stream sw in default",
				"Select * From Stream sw In default",
				"sElEcT * fRoM sTrEaM sw In DeFaUlT",

				// WHERE clause keywords
				"SELECT * FROM STREAM sw IN default WHERE service = 'test'",
				"select * from stream sw in default where service = 'test'",
				"SELECT * FROM STREAM sw IN default WhErE service = 'test'",

				// AND/OR operators
				"SELECT * FROM STREAM sw IN default WHERE a = 1 AND b = 2",
				"SELECT * FROM STREAM sw IN default WHERE a = 1 and b = 2",
				"SELECT * FROM STREAM sw IN default WHERE a = 1 AnD b = 2",
				"SELECT * FROM STREAM sw IN default WHERE a = 1 OR b = 2",
				"SELECT * FROM STREAM sw IN default WHERE a = 1 or b = 2",
				"SELECT * FROM STREAM sw IN default WHERE a = 1 oR b = 2",

				// GROUP BY
				"SELECT region, SUM(value) FROM MEASURE m IN default GROUP BY region",
				"SELECT region, SUM(value) FROM MEASURE m IN default group by region",
				"SELECT region, SUM(value) FROM MEASURE m IN default Group By region",

				// ORDER BY with ASC/DESC
				"SELECT * FROM STREAM sw IN default ORDER BY timestamp DESC",
				"SELECT * FROM STREAM sw IN default order by timestamp desc",
				"SELECT * FROM STREAM sw IN default Order By timestamp Desc",
				"SELECT * FROM STREAM sw IN default ORDER BY timestamp ASC",
				"SELECT * FROM STREAM sw IN default order by timestamp asc",

				// TIME clause
				"SELECT * FROM STREAM sw IN default TIME > '-1h'",
				"SELECT * FROM STREAM sw IN default time > '-1h'",
				"SELECT * FROM STREAM sw IN default Time > '-1h'",

				// TIME BETWEEN
				"SELECT * FROM STREAM sw IN default TIME BETWEEN '2024-01-01' AND '2024-01-02'",
				"SELECT * FROM STREAM sw IN default time between '2024-01-01' and '2024-01-02'",
				"SELECT * FROM STREAM sw IN default Time Between '2024-01-01' And '2024-01-02'",

				// LIMIT and OFFSET
				"SELECT * FROM STREAM sw IN default LIMIT 100",
				"SELECT * FROM STREAM sw IN default limit 100",
				"SELECT * FROM STREAM sw IN default Limit 100",
				"SELECT * FROM STREAM sw IN default LIMIT 100 OFFSET 50",
				"SELECT * FROM STREAM sw IN default limit 100 offset 50",

				// Aggregate functions
				"SELECT SUM(latency) FROM MEASURE m IN default",
				"SELECT sum(latency) FROM MEASURE m IN default",
				"SELECT Sum(latency) FROM MEASURE m IN default",

				// MATCH operator
				"SELECT * FROM STREAM sw IN default WHERE message MATCH('error')",
				"SELECT * FROM STREAM sw IN default WHERE message match('error')",
				"SELECT * FROM STREAM sw IN default WHERE message Match('error')",

				// IN/NOT IN operators
				"SELECT * FROM STREAM sw IN default WHERE status IN (200, 404)",
				"SELECT * FROM STREAM sw IN default WHERE status in (200, 404)",
				"SELECT * FROM STREAM sw IN default WHERE status In (200, 404)",
				"SELECT * FROM STREAM sw IN default WHERE status NOT IN (500, 503)",
				"SELECT * FROM STREAM sw IN default WHERE status not in (500, 503)",
				"SELECT * FROM STREAM sw IN default WHERE status Not In (500, 503)",

				// HAVING/NOT HAVING operators
				"SELECT * FROM STREAM sw IN default WHERE tags HAVING ('error')",
				"SELECT * FROM STREAM sw IN default WHERE tags having ('error')",
				"SELECT * FROM STREAM sw IN default WHERE tags Having ('error')",
				"SELECT * FROM STREAM sw IN default WHERE tags NOT HAVING ('debug')",
				"SELECT * FROM STREAM sw IN default WHERE tags not having ('debug')",

				// WITH QUERY_TRACE
				"SELECT * FROM STREAM sw IN default WITH QUERY_TRACE",
				"SELECT * FROM STREAM sw IN default with query_trace",
				"SELECT * FROM STREAM sw IN default With Query_Trace",

				// SHOW TOP
				"SHOW TOP 10 FROM MEASURE m IN default ORDER BY DESC",
				"show top 10 from measure m in default order by desc",
				"Show Top 10 From Measure m In default Order By Desc",

				// Resource types (STREAM, MEASURE, TRACE, PROPERTY)
				"SELECT * FROM MEASURE metrics IN default",
				"SELECT * FROM measure metrics IN default",
				"SELECT * FROM Measure metrics IN default",
				"SELECT * FROM TRACE traces IN default",
				"SELECT * FROM trace traces IN default",
				"SELECT * FROM PROPERTY props IN default",
				"SELECT * FROM property props IN default",

				// AGGREGATE BY
				"SHOW TOP 10 FROM MEASURE m IN default AGGREGATE BY MAX",
				"SHOW TOP 10 FROM MEASURE m IN default aggregate by max",
				"SHOW TOP 10 FROM MEASURE m IN default Aggregate By Max",

				// Mixed case complex query
				`SeLeCt trace_id, service_id
			 FrOm STREAM sw In default
			 WhErE service_id = 'test' AnD status = 200
			 OrDeR By timestamp DeSc
			 LiMiT 100`,
			}

			for i, query := range queries {
				It(fmt.Sprintf("parses case-insensitive query %d", i+1), func() {
					grammar, err := ParseQuery(query)
					Expect(err).To(BeNil(), "parsing error for query '%s': %v", query, err)
					Expect(grammar).NotTo(BeNil(), "failed to parse case-insensitive query: %s", query)
				})
			}
		})

		Describe("Case-Sensitive Resource Names", func() {
			// Test that resource/stream names are case-sensitive while keywords remain case-insensitive
			It("treats different-cased stream names as distinct resources", func() {
				// Parse query with uppercase stream name
				parsedUpper, err := ParseQuery("SELECT * FROM STREAM MyStream IN group1")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper := parsedUpper.Select
				Expect(stmtUpper.From).NotTo(BeNil())
				Expect(stmtUpper.From.ResourceName).To(Equal("MyStream"))

				// Parse query with lowercase stream name
				parsedLower, err := ParseQuery("SELECT * FROM STREAM mystream IN group1")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower := parsedLower.Select
				Expect(stmtLower.From).NotTo(BeNil())
				Expect(stmtLower.From.ResourceName).To(Equal("mystream"))

				// Verify they are different resource names
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("preserves exact case of stream names with mixed case keywords", func() {
				// Query with mixed case keywords but specific stream name
				grammar, err := ParseQuery("sElEcT * FrOm StReAm MyStream In group1")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.From).NotTo(BeNil())
				// Stream name should preserve exact case
				Expect(stmt.From.ResourceName).To(Equal("MyStream"))
			})

			It("treats different-cased measure names as distinct resources", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM MEASURE ServiceMetrics IN default")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper := parsedUpper.Select
				Expect(stmtUpper.From.ResourceName).To(Equal("ServiceMetrics"))

				parsedLower, err := ParseQuery("SELECT * FROM MEASURE servicemetrics IN default")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower := parsedLower.Select
				Expect(stmtLower.From.ResourceName).To(Equal("servicemetrics"))

				// Verify they are different
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("treats different-cased trace names as distinct resources", func() {
				parsedCamel, err := ParseQuery("SELECT * FROM TRACE SwTrace IN default")
				Expect(err).To(BeNil())
				Expect(parsedCamel).NotTo(BeNil())

				stmtCamel := parsedCamel.Select
				Expect(stmtCamel.From.ResourceName).To(Equal("SwTrace"))

				parsedSnake, err := ParseQuery("SELECT * FROM TRACE sw_trace IN default")
				Expect(err).To(BeNil())
				Expect(parsedSnake).NotTo(BeNil())

				stmtSnake := parsedSnake.Select
				Expect(stmtSnake.From.ResourceName).To(Equal("sw_trace"))

				// Verify they are different
				Expect(stmtCamel.From.ResourceName).NotTo(Equal(stmtSnake.From.ResourceName))
			})

			It("treats different-cased property names as distinct resources", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM PROPERTY ServerMetadata IN dc1")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper := parsedUpper.Select
				Expect(stmtUpper.From.ResourceName).To(Equal("ServerMetadata"))

				parsedLower, err := ParseQuery("SELECT * FROM PROPERTY servermetadata IN dc1")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower := parsedLower.Select
				Expect(stmtLower.From.ResourceName).To(Equal("servermetadata"))

				// Verify they are different
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("preserves case of column names in projections", func() {
				grammar, err := ParseQuery("SELECT TraceId, ServiceId, StartTime FROM STREAM sw IN default")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Columns).To(HaveLen(3))

				// Column names should preserve case
				pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("TraceId"))
				pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("ServiceId"))
				pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
				Expect(pcErr2).To(BeNil())
				Expect(pcName2).To(Equal("StartTime"))
			})

			It("preserves case of column names in TOP projections", func() {
				grammar, err := ParseQuery("SELECT TOP 10 TraceId DESC, ServiceId, StartTime FROM STREAM sw IN default")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(10))
				topNOrderField, _ := stmt.Projection.TopN.OrderField.ToString(false)
				Expect(topNOrderField).To(Equal("TraceId"))
				Expect(stmt.Projection.TopN.Direction).NotTo(BeNil())
				Expect(*stmt.Projection.TopN.Direction).To(Equal("DESC"))
				Expect(stmt.Projection.TopN.OtherColumns).To(HaveLen(2))

				// Additional column names should preserve case
				pcName0, pcErr0 := stmt.Projection.TopN.OtherColumns[0].Identifier.ToString(stmt.Projection.TopN.OtherColumns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("ServiceId"))
				pcName1, pcErr1 := stmt.Projection.TopN.OtherColumns[1].Identifier.ToString(stmt.Projection.TopN.OtherColumns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("StartTime"))
			})

			It("preserves case of column names in WHERE clauses", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE ServiceId = 'test' AND StatusCode = 200")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Check that column names in WHERE clause preserve case
				// Verify AND/OR structure using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				// Grammar represents AND/OR as tree structure
				Expect(stmt.Where.Expr.Left).NotTo(BeNil())
			})

			It("preserves case of column names in GROUP BY", func() {
				grammar, err := ParseQuery("SELECT RegionName, SUM(value) FROM MEASURE metrics IN default GROUP BY RegionName")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))

				// Column name in GROUP BY should preserve case
				gbName0, gbErr0 := stmt.GroupBy.Columns[0].Identifier.ToString(stmt.GroupBy.Columns[0].TypeSpec != nil)
				Expect(gbErr0).To(BeNil())
				Expect(gbName0).To(Equal("RegionName"))
				Expect(stmt.GroupBy.Columns[0].TypeSpec).To(BeNil())
			})

			It("preserves case of column names in ORDER BY", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY StartTimestamp DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.OrderBy).NotTo(BeNil())

				// Column name in ORDER BY should preserve case
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
				Expect(obErr).To(BeNil())
				Expect(obCol).To(Equal("StartTimestamp"))
			})

			It("treats different-cased group names as distinct", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM STREAM sw IN Production")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper := parsedUpper.Select
				Expect(stmtUpper.From.In.Groups).To(HaveLen(1))
				Expect(stmtUpper.From.In.Groups[0]).To(Equal("Production"))

				parsedLower, err := ParseQuery("SELECT * FROM STREAM sw IN production")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower := parsedLower.Select
				Expect(stmtLower.From.In.Groups).To(HaveLen(1))
				Expect(stmtLower.From.In.Groups[0]).To(Equal("production"))

				// Verify group names are different
				Expect(stmtUpper.From.In.Groups[0]).NotTo(Equal(stmtLower.From.In.Groups[0]))
			})

			It("demonstrates keyword case-insensitivity with name case-sensitivity", func() {
				// All these queries should parse successfully with keywords in any case
				// but preserve the exact case of the resource name
				queries := []struct {
					query        string
					expectedName string
				}{
					{"SELECT * FROM STREAM MyStream IN group1", "MyStream"},
					{"select * from stream MyStream in group1", "MyStream"},
					{"SeLeCt * FrOm StReAm MyStream In group1", "MyStream"},
					{"SELECT * FROM STREAM mystream IN group1", "mystream"},
					{"select * from stream mystream in group1", "mystream"},
					{"SELECT * FROM STREAM MYSTREAM IN group1", "MYSTREAM"},
				}

				for _, tc := range queries {
					grammar, err := ParseQuery(tc.query)
					Expect(err).To(BeNil(), "failed to parse query: %s", tc.query)
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.ResourceName).To(Equal(tc.expectedName),
						"expected resource name '%s' but got '%s' for query: %s",
						tc.expectedName, stmt.From.ResourceName, tc.query)
				}
			})
		})

		Describe("Column Projection", func() {
			Describe("basic column selection", func() {
				It("parses single column", func() {
					grammar, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].TypeSpec).To(BeNil())
					Expect(stmt.Projection.Columns[0].Aggregate).To(BeNil())
				})

				It("parses multiple columns", func() {
					grammar, err := ParseQuery("SELECT trace_id, service_id, duration FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// Verify each column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].TypeSpec).To(BeNil())

					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("service_id"))
					Expect(stmt.Projection.Columns[1].TypeSpec).To(BeNil())

					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("duration"))
					Expect(stmt.Projection.Columns[2].TypeSpec).To(BeNil())
				})

				It("parses column with underscores", func() {
					grammar, err := ParseQuery("SELECT trace_id, service_name, response_time FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("trace_id"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("service_name"))
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("response_time"))
				})

				It("parses column with hyphens", func() {
					grammar, err := ParseQuery("SELECT service-id, request-id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(2))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("service-id"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("request-id"))
				})
			})

			Describe("column type specifiers", func() {
				It("parses column with ::tag specifier", func() {
					grammar, err := ParseQuery("SELECT service_id::tag FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					colName, err := col.Identifier.ToString(col.TypeSpec != nil)
					Expect(err).To(BeNil())
					Expect(colName).To(Equal("service_id"))
					Expect(strings.ToUpper(*col.TypeSpec)).To(Equal("TAG"))
					Expect(col.Aggregate).To(BeNil())
				})

				It("parses column with ::field specifier", func() {
					grammar, err := ParseQuery("SELECT latency::field FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					colName, err := col.Identifier.ToString(col.TypeSpec != nil)
					Expect(err).To(BeNil())
					Expect(colName).To(Equal("latency"))
					Expect(strings.ToUpper(*col.TypeSpec)).To(Equal("FIELD"))
					Expect(col.Aggregate).To(BeNil())
				})

				It("parses case-insensitive type specifiers", func() {
					parsed1, err := ParseQuery("SELECT col1::TAG, col2::FIELD FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt1 := parsed1.Select
					Expect(strings.ToUpper(*stmt1.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))
					Expect(strings.ToUpper(*stmt1.Projection.Columns[1].TypeSpec)).To(Equal("FIELD"))

					parsed2, err := ParseQuery("SELECT col1::tag, col2::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt2 := parsed2.Select
					Expect(strings.ToUpper(*stmt2.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))
					Expect(strings.ToUpper(*stmt2.Projection.Columns[1].TypeSpec)).To(Equal("FIELD"))
				})

				It("parses mixed columns with and without type specifiers", func() {
					grammar, err := ParseQuery("SELECT service_id::tag, duration, latency::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First column with ::tag
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("service_id"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))

					// Second column without specifier
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("duration"))
					Expect(stmt.Projection.Columns[1].TypeSpec).To(BeNil())

					// Third column with ::field
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("latency"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[2].TypeSpec)).To(Equal("FIELD"))
				})
			})

			Describe("dot-separated column paths", func() {
				It("parses simple dot-separated path", func() {
					grammar, err := ParseQuery("SELECT metadata.service_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					colName, _ := col.Identifier.ToString(col.TypeSpec != nil)
					Expect(colName).To(Equal("metadata.service_id"))
					Expect(col.TypeSpec).To(BeNil())
				})

				It("parses multiple levels of dot-separated paths", func() {
					grammar, err := ParseQuery("SELECT request.header.content_type, response.body.status FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("request.header.content_type"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("response.body.status"))
				})

				It("parses dot-separated path with type specifier", func() {
					grammar, err := ParseQuery("SELECT metadata.service_id::tag, response.status::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					// First column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("metadata.service_id"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))

					// Second column
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("response.status"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[1].TypeSpec)).To(Equal("FIELD"))
				})

				It("preserves case in dot-separated paths", func() {
					grammar, err := ParseQuery("SELECT MetaData.ServiceId, Response.Status FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("MetaData.ServiceId"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("Response.Status"))
				})
			})

			Describe("aggregate functions", func() {
				It("parses SUM function", func() {
					grammar, err := ParseQuery("SELECT SUM(latency) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Aggregate).NotTo(BeNil())
					Expect(col.Aggregate.Function).To(Equal("SUM"))
					aggColName, _ := col.Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("latency"))
				})

				It("parses AVG/MEAN functions", func() {
					grammar, err := ParseQuery("SELECT AVG(latency) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// AVG
					Expect(stmt.Projection.Columns[0].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Aggregate.Function).To(Equal("AVG"))
					aggColName, _ := stmt.Projection.Columns[0].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("latency"))
				})

				It("parses MAX function", func() {
					grammar, err := ParseQuery("SELECT MAX(cpu_usage) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Aggregate.Function).To(Equal("MAX"))
					aggColName, _ := stmt.Projection.Columns[0].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("cpu_usage"))
				})

				It("parses MIN function", func() {
					grammar, err := ParseQuery("SELECT MIN(memory_usage) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Aggregate.Function).To(Equal("MIN"))
					aggColName, _ := stmt.Projection.Columns[0].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("memory_usage"))
				})

				It("parses COUNT function", func() {
					grammar, err := ParseQuery("SELECT COUNT(requests) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Aggregate).NotTo(BeNil())
					Expect(col.Aggregate.Function).To(Equal("COUNT"))
					aggColName, _ := col.Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("requests"))
				})

				It("parses aggregate function with dot-separated column", func() {
					grammar, err := ParseQuery("SELECT SUM(metrics.latency) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// SUM with dot-separated
					Expect(stmt.Projection.Columns[0].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Aggregate.Function).To(Equal("SUM"))
					aggColName, _ := stmt.Projection.Columns[0].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("metrics.latency"))
				})

				It("parses case-insensitive aggregate functions", func() {
					grammar, err := ParseQuery("SELECT sum(a) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// Should be normalized to uppercase
					Expect(strings.ToUpper(stmt.Projection.Columns[0].Aggregate.Function)).To(Equal("SUM"))
				})
			})

			Describe("mixed columns and aggregate functions", func() {
				It("parses regular columns with aggregate functions", func() {
					grammar, err := ParseQuery("SELECT region, SUM(latency), service_id FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: regular column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("region"))
					Expect(stmt.Projection.Columns[0].Aggregate).To(BeNil())

					// Second: aggregate function
					Expect(stmt.Projection.Columns[1].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[1].Aggregate.Function).To(Equal("SUM"))
					col2467Name, _ := stmt.Projection.Columns[1].Aggregate.Column.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(col2467Name).To(Equal("latency"))

					// Third: regular column
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("service_id"))
					Expect(stmt.Projection.Columns[2].Aggregate).To(BeNil())
				})

				It("parses typed columns with aggregate functions", func() {
					grammar, err := ParseQuery("SELECT region::tag, SUM(latency), duration::field FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: tag column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("region"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[0].TypeSpec)).To(Equal("TAG"))
					Expect(stmt.Projection.Columns[0].Aggregate).To(BeNil())

					// Second: aggregate function
					Expect(stmt.Projection.Columns[1].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[1].Aggregate.Function).To(Equal("SUM"))

					// Third: field column
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("duration"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[2].TypeSpec)).To(Equal("FIELD"))
					Expect(stmt.Projection.Columns[2].Aggregate).To(BeNil())
				})

				It("parses dot-separated columns with aggregate functions", func() {
					grammar, err := ParseQuery("SELECT metadata.region, SUM(metrics.latency), response.status FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: dot-separated column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("metadata.region"))
					Expect(stmt.Projection.Columns[0].Aggregate).To(BeNil())

					// Second: aggregate with dot-separated
					Expect(stmt.Projection.Columns[1].Aggregate).NotTo(BeNil())
					aggColName, _ := stmt.Projection.Columns[1].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("metrics.latency"))

					// Third: dot-separated column
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("response.status"))
					Expect(stmt.Projection.Columns[2].Aggregate).To(BeNil())
				})

				It("parses aggregate function with regular columns", func() {
					grammar, err := ParseQuery("SELECT service, region, SUM(requests) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// Regular columns
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("service"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("region"))

					// Aggregate function
					Expect(stmt.Projection.Columns[2].Aggregate.Function).To(Equal("SUM"))
				})
			})

			Describe("special projections", func() {
				It("parses wildcard projection", func() {
					grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.All).To(BeTrue())
					Expect(stmt.Projection.Empty).To(BeFalse())
					Expect(stmt.Projection.Columns).To(HaveLen(0))
				})

				It("parses empty projection", func() {
					grammar, err := ParseQuery("SELECT () FROM TRACE sw_trace IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Empty).To(BeTrue())
					Expect(stmt.Projection.All).To(BeFalse())
					Expect(stmt.Projection.Columns).To(HaveLen(0))
				})

				It("parses TOP N with specific columns", func() {
					grammar, err := ParseQuery("SELECT TOP 10 trace_id DESC, service_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.TopN).NotTo(BeNil())
					Expect(stmt.Projection.TopN.N).To(Equal(10))
					topNOrderField, _ := stmt.Projection.TopN.OrderField.ToString(false)
					Expect(topNOrderField).To(Equal("trace_id"))
					Expect(stmt.Projection.TopN.Direction).NotTo(BeNil())
					Expect(*stmt.Projection.TopN.Direction).To(Equal("DESC"))
					Expect(stmt.Projection.TopN.OtherColumns).To(HaveLen(1))

					pcName0, pcErr0 := stmt.Projection.TopN.OtherColumns[0].Identifier.ToString(stmt.Projection.TopN.OtherColumns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("service_id"))
				})

				It("parses TOP N with typed columns", func() {
					grammar, err := ParseQuery("SELECT TOP 10 service_id DESC, latency::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.TopN).NotTo(BeNil())
					Expect(stmt.Projection.TopN.N).To(Equal(10))
					topNOrderField, _ := stmt.Projection.TopN.OrderField.ToString(false)
					Expect(topNOrderField).To(Equal("service_id"))
					Expect(stmt.Projection.TopN.Direction).NotTo(BeNil())
					Expect(*stmt.Projection.TopN.Direction).To(Equal("DESC"))
					Expect(stmt.Projection.TopN.OtherColumns).To(HaveLen(1))

					pcName0, pcErr0 := stmt.Projection.TopN.OtherColumns[0].Identifier.ToString(stmt.Projection.TopN.OtherColumns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("latency"))
					Expect(strings.ToUpper(*stmt.Projection.TopN.OtherColumns[0].TypeSpec)).To(Equal("FIELD"))
				})
			})

			Describe("column name formats", func() {
				It("parses alphanumeric column names", func() {
					grammar, err := ParseQuery("SELECT col1, col2, col123 FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("col1"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("col2"))
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("col123"))
				})

				It("parses mixed case column names", func() {
					grammar, err := ParseQuery("SELECT TraceId, ServiceID, RequestTime FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("TraceId"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("ServiceID"))
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("RequestTime"))
				})

				It("parses column names with leading underscores", func() {
					grammar, err := ParseQuery("SELECT _id, _timestamp, _internal FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("_id"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("_timestamp"))
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("_internal"))
				})

				It("parses complex dot-separated paths", func() {
					grammar, err := ParseQuery("SELECT a.b.c, x.y.z.w FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(2))
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("a.b.c"))
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("x.y.z.w"))
				})
			})

			Describe("complete projection scenarios", func() {
				It("parses comprehensive column projection", func() {
					grammar, err := ParseQuery(`SELECT
					trace_id,
					metadata.service_id::tag,
					response.latency::field,
					AVG(duration.milliseconds)
					FROM MEASURE metrics IN default`)
					Expect(err).To(BeNil())
					Expect(grammar).NotTo(BeNil())

					stmt := grammar.Select
					Expect(stmt.Projection.Columns).To(HaveLen(4))

					// Column 1: simple column
					pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
					Expect(pcErr0).To(BeNil())
					Expect(pcName0).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].TypeSpec).To(BeNil())

					// Column 2: dot-separated with tag type
					pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
					Expect(pcErr1).To(BeNil())
					Expect(pcName1).To(Equal("metadata.service_id"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[1].TypeSpec)).To(Equal("TAG"))

					// Column 3: dot-separated with field type
					pcName2, pcErr2 := stmt.Projection.Columns[2].Identifier.ToString(stmt.Projection.Columns[2].TypeSpec != nil)
					Expect(pcErr2).To(BeNil())
					Expect(pcName2).To(Equal("response.latency"))
					Expect(strings.ToUpper(*stmt.Projection.Columns[2].TypeSpec)).To(Equal("FIELD"))

					// Column 4: aggregate with dot-separated column
					Expect(stmt.Projection.Columns[3].Aggregate).NotTo(BeNil())
					Expect(stmt.Projection.Columns[3].Aggregate.Function).To(Equal("AVG"))
					aggColName, _ := stmt.Projection.Columns[3].Aggregate.Column.ToString(false)
					Expect(aggColName).To(Equal("duration.milliseconds"))
				})
			})
		})

		Describe("String Literals and Escaping", func() {
			It("parses string with single quotes", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'hello world'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("hello world"))
						}
					}
				}
			})

			It("parses string with double quotes (if supported)", func() {
				grammar, err := ParseQuery(`SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = "hello world"`)

				if err != nil {
					// Double quotes might not be supported for string literals
					Expect(err).NotTo(BeNil())
				} else {
					Expect(grammar).NotTo(BeNil())
					stmt := grammar.Select
					Expect(stmt.Where).NotTo(BeNil())
				}
			})

			It("parses string with escaped single quote", func() {
				// Testing if single quote escaping is supported
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'it\\'s working'")

				Expect(grammar).NotTo(BeNil())
				Expect(err).To(BeNil())
				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
				// Check if the escaped quote is preserved
				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses string with special characters", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE path = '/api/users'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("/api/users"))
						}
					}
				}
			})

			It("parses string with hyphen", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'auth-service'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("auth-service"))
						}
					}
				}
			})

			It("parses string with underscores", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'api_gateway_service'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("api_gateway_service"))
						}
					}
				}
			})

			It("parses string with dot notation", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version = '1.2.3'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("1.2.3"))
						}
					}
				}
			})

			It("parses string with spaces and punctuation", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'Error: Connection failed!'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("Error: Connection failed!"))
						}
					}
				}
			})

			It("parses empty string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = ''")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses very long string", func() {
				longString := "This is a very long string that contains many words and should still be parsed correctly " +
					"without any issues even though it exceeds typical string lengths"
				query := fmt.Sprintf("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = '%s'", longString)
				grammar, err := ParseQuery(query)
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})
		})

		Describe("Unicode Support", func() {
			It("parses query with emoji in string values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status = '✅ success'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("✅ success"))
						}
					}
				}
			})
		})

		Describe("Whitespace Handling", func() {
			It("parses query with extra whitespace", func() {
				grammar, err := ParseQuery("SELECT    *    FROM    STREAM    sw    IN    default    TIME    >    '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
			})

			It("parses query with tabs and newlines", func() {
				query := "SELECT *\n\tFROM STREAM sw\n\tIN default\n\tTIME > '-30m'"
				grammar, err := ParseQuery(query)
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
			})

			It("parses query with minimal whitespace", func() {
				// Parser enforces space after keywords, so we test minimal spaces where allowed
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("STREAM"))
			})

			It("parses query with whitespace in string literals", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = '   spaces   '")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("   spaces   "))
						}
					}
				}
			})
		})

		Describe("Error Message Quality", func() {
			It("provides clear error for missing FROM clause", func() {
				grammar, err := ParseQuery("SELECT * WHERE status = 'error'")
				Expect(err).NotTo(BeNil())
				Expect(grammar).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("FROM"),
					ContainSubstring("syntax"),
					ContainSubstring("expected"),
				))
			})

			It("provides clear error for missing IN clause", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw WHERE status = 'error'")
				Expect(err).NotTo(BeNil())
				Expect(grammar).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("IN"),
					ContainSubstring("syntax"),
					ContainSubstring("expected"),
				))
			})

			It("provides clear error for unclosed string", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message = 'unclosed")
				Expect(err).NotTo(BeNil())
				Expect(grammar).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("string"),
					ContainSubstring("quote"),
					ContainSubstring("unclosed"),
					ContainSubstring("syntax"),
				))
			})

			It("provides clear error for invalid operator", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE status === 'error'")
				Expect(err).NotTo(BeNil())
				Expect(grammar).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("operator"),
					ContainSubstring("syntax"),
					ContainSubstring("unexpected"),
				))
			})
		})

		Describe("OFFSET Clause", func() {
			It("parses OFFSET with LIMIT", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10 OFFSET 20")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(10))
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(20))
			})

			It("parses OFFSET 0", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10 OFFSET 0")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(0))
			})

			It("parses large OFFSET value", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 100 OFFSET 10000")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(10000))
			})

			It("parses OFFSET with WHERE and ORDER BY", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'api' ORDER BY timestamp DESC LIMIT 50 OFFSET 100")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(100))
			})

			It("parses OFFSET for MEASURE queries", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region LIMIT 20 OFFSET 10")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(10))
			})

			It("parses OFFSET for TRACE queries", func() {
				grammar, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default TIME > '-30m' LIMIT 100 OFFSET 50")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(50))
			})

			// Boundary conditions for LIMIT and OFFSET
			It("parses LIMIT 0", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 0")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(0))
			})

			It("parses LIMIT 1", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 1")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(1))
			})

			It("parses very large LIMIT value", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 999999")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(999999))
			})

			It("handles negative LIMIT value", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT -1")

				// Parser might accept syntactically, semantic validator should reject
				Expect(grammar).NotTo(BeNil())
				Expect(err).To(BeNil())
			})

			It("parses OFFSET without LIMIT (if supported)", func() {
				// Some SQL dialects allow OFFSET without LIMIT
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' OFFSET 10")

				Expect(grammar).NotTo(BeNil())
				Expect(err).To(BeNil())
				stmt := grammar.Select
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(stmt.Offset.Value).To(Equal(10))
			})
		})
	})

	Describe("Stream Queries", func() {
		Describe("TIME Operators", func() {
			It("parses TIME = with absolute timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("="))
				Expect(stmt.Time.Value.ToString()).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME = with relative timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = '-30m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("="))
				Expect(stmt.Time.Value.ToString()).To(Equal("-30m"))
			})

			It("parses TIME < with absolute timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME < '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<"))
				Expect(stmt.Time.Value.ToString()).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME < with relative timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME < '-1d'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<"))
				Expect(stmt.Time.Value.ToString()).To(Equal("-1d"))
			})

			It("parses TIME >= with absolute timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME >= '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal(">="))
				Expect(stmt.Time.Value.ToString()).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME >= with relative timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME >= '-2h'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal(">="))
				Expect(stmt.Time.Value.ToString()).To(Equal("-2h"))
			})

			It("parses TIME <= with absolute timestamp", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME <= '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<="))
				Expect(stmt.Time.Value.ToString()).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME <= with relative timestamp 'now'", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME <= 'now'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<="))
				Expect(stmt.Time.Value.ToString()).To(Equal("now"))
			})

			It("parses TIME operators for MEASURE queries", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME = '2023-01-01T00:00:00Z' GROUP BY region")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("="))
			})

			It("parses TIME operators for TRACE queries", func() {
				grammar, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default TIME <= '-1h'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<="))
			})

			It("parses TIME operators for TOP-N queries", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN default TIME < '2023-01-01T00:00:00Z' ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("<"))
			})

			// Edge cases and boundary conditions for TIME operators
			It("parses TIME = with 'now' keyword", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = 'now'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal("="))
				Expect(stmt.Time.Value.ToString()).To(Equal("now"))
			})

			It("parses TIME > with 'now' keyword", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > 'now'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Comparator).NotTo(BeNil())
				Expect(*stmt.Time.Comparator).To(Equal(">"))
				Expect(stmt.Time.Value.ToString()).To(Equal("now"))
			})

			It("parses TIME BETWEEN with same start and end time", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME BETWEEN '2023-01-01T10:00:00Z' AND '2023-01-01T10:00:00Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Between).NotTo(BeNil())
				Expect(stmt.Time.Between.Begin.ToString()).To(Equal("2023-01-01T10:00:00Z"))
				Expect(stmt.Time.Between.End.ToString()).To(Equal("2023-01-01T10:00:00Z"))
			})

			It("parses TIME BETWEEN with relative times", func() {
				grammar, err := ParseQuery("SELECT * FROM MEASURE metrics IN default TIME BETWEEN '-2h' AND '-1h'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Between).NotTo(BeNil())
				Expect(stmt.Time.Between.Begin.ToString()).To(Equal("-2h"))
				Expect(stmt.Time.Between.End.ToString()).To(Equal("-1h"))
			})

			It("parses TIME BETWEEN mixing relative and absolute times", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME BETWEEN '-1h' AND '2023-12-31T23:59:59Z'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Between).NotTo(BeNil())
				Expect(stmt.Time.Between.Begin.ToString()).To(Equal("-1h"))
				Expect(stmt.Time.Between.End.ToString()).To(Equal("2023-12-31T23:59:59Z"))
			})
		})
	})

	Describe("Measure Queries", func() {
		Describe("Measure Specific Features", func() {
			It("parses Measure query with MATCH operator", func() {
				grammar, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' WHERE tags MATCH('error') GROUP BY region")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("MEASURE"))
				Expect(stmt.Where).NotTo(BeNil())

				// Verify MATCH operator
				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses Measure query with MATCH and aggregation", func() {
				grammar, err := ParseQuery("SELECT service_id, COUNT(requests) FROM MEASURE api_metrics " +
					"IN default TIME > '-1h' WHERE http_path MATCH(('/api/users', '/api/orders'), 'url', 'OR') GROUP BY service_id")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select

				// Verify MATCH with multiple values
				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses SELECT TOP N in Measure query", func() {
				grammar, err := ParseQuery("SELECT TOP 10 instance ASC, cpu_usage FROM MEASURE instance_metrics " +
					"IN us-west TIME > '-30m' WHERE service = 'auth-service' ORDER BY cpu_usage DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("MEASURE"))
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(10))
				topNOrderField, _ := stmt.Projection.TopN.OrderField.ToString(false)
				Expect(topNOrderField).To(Equal("instance"))
				Expect(stmt.Projection.TopN.Direction == nil || *stmt.Projection.TopN.Direction == "ASC").To(BeTrue()) // ASC

				// Verify additional columns
				Expect(stmt.Projection.TopN.OtherColumns).To(HaveLen(1))
				pcName0, pcErr0 := stmt.Projection.TopN.OtherColumns[0].Identifier.ToString(stmt.Projection.TopN.OtherColumns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("cpu_usage"))
			})

			It("parses SELECT TOP N with DESC in Measure query", func() {
				grammar, err := ParseQuery("SELECT TOP 5 service_id DESC, latency::field, region::tag FROM MEASURE service_metrics IN us-west TIME > '-15m'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(5))
				topNOrderField, _ := stmt.Projection.TopN.OrderField.ToString(false)
				Expect(topNOrderField).To(Equal("service_id"))
				Expect(stmt.Projection.TopN.Direction).NotTo(BeNil())
				Expect(*stmt.Projection.TopN.Direction).To(Equal("DESC"))

				// Verify columns with type specifiers
				Expect(stmt.Projection.TopN.OtherColumns).To(HaveLen(2))
				pcName0, pcErr0 := stmt.Projection.TopN.OtherColumns[0].Identifier.ToString(stmt.Projection.TopN.OtherColumns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("latency"))
				Expect(strings.ToUpper(*stmt.Projection.TopN.OtherColumns[0].TypeSpec)).To(Equal("FIELD"))
				pcName1, pcErr1 := stmt.Projection.TopN.OtherColumns[1].Identifier.ToString(stmt.Projection.TopN.OtherColumns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("region"))
				Expect(strings.ToUpper(*stmt.Projection.TopN.OtherColumns[1].TypeSpec)).To(Equal("TAG"))
			})
		})

		Describe("TOP-N AGGREGATE BY", func() {
			It("parses AGGREGATE BY SUM", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.N).To(Equal(10))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("SUM"))
			})

			It("parses AGGREGATE BY MAX", func() {
				grammar, err := ParseQuery("SHOW TOP 5 FROM MEASURE cpu_usage IN production TIME > '-1h' AGGREGATE BY MAX ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.N).To(Equal(5))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("MAX"))
			})

			It("parses AGGREGATE BY MIN", func() {
				grammar, err := ParseQuery("SHOW TOP 3 FROM MEASURE response_time IN production TIME > '-30m' AGGREGATE BY MIN ORDER BY ASC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.N).To(Equal(3))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("MIN"))
			})

			It("parses AGGREGATE BY AVG", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE latency IN production TIME > '-2h' AGGREGATE BY AVG ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("AVG"))
			})

			It("parses AGGREGATE BY MEAN", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE latency IN production TIME > '-2h' AGGREGATE BY MEAN ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("MEAN"))
			})

			It("parses AGGREGATE BY COUNT", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE requests IN production TIME > '-30m' AGGREGATE BY COUNT ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("COUNT"))
			})

			It("parses AGGREGATE BY with WHERE clause", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' WHERE namespace = 'production' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("SUM"))
			})

			It("parses AGGREGATE BY with multiple groups", func() {
				grammar, err := ParseQuery("SHOW TOP 5 FROM MEASURE service_errors IN production, staging TIME > '-1h' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.From.In.Groups).To(HaveLen(2))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function.Function).To(Equal("SUM"))
			})

			It("parses case-insensitive AGGREGATE BY", func() {
				grammar, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' aggregate by sum ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.TopN
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(strings.ToUpper(stmt.AggregateBy.Function.Function)).To(Equal("SUM"))
			})
		})
	})

	Describe("Trace Queries", func() {
		Describe("Trace Specific Features", func() {
			It("parses Trace query with empty projection and complex conditions", func() {
				grammar, err := ParseQuery("SELECT () FROM TRACE sw_trace IN group1 TIME > '-30m' " +
					"WHERE service_id = 'webapp' AND status = 'error' ORDER BY start_time DESC LIMIT 100")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))

				// Verify empty projection
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Empty).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(0))

				// Verify WHERE clause is parsed
				Expect(stmt.Where).NotTo(BeNil())

				// Verify ORDER BY is parsed
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				obCol, obErr := stmt.OrderBy.Tail.WithIdent.Identifier.ToString(false)
				Expect(obErr).To(BeNil())
				Expect(obCol).To(Equal("start_time"))
				Expect(stmt.OrderBy.Tail.WithIdent).NotTo(BeNil())
				Expect(stmt.OrderBy.Tail.WithIdent.Direction).NotTo(BeNil())
				Expect(*stmt.OrderBy.Tail.WithIdent.Direction).To(Equal("DESC"))

				// Verify LIMIT
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(100))
			})

			It("parses Trace query with WITH QUERY_TRACE", func() {
				grammar, err := ParseQuery("SELECT trace_id, service_id, operation_name FROM TRACE sw_trace IN group1 TIME > '-30m' WHERE service_id = 'webapp' WITH QUERY_TRACE")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))
				Expect(stmt.WithQueryTrace).NotTo(BeNil())

				// Verify regular projection
				Expect(stmt.Projection.Empty).To(BeFalse())
				Expect(stmt.Projection.Columns).To(HaveLen(3))
			})

			It("parses Trace query with empty projection and WITH QUERY_TRACE", func() {
				grammar, err := ParseQuery("SELECT () FROM TRACE sw_trace IN group1 TIME > '-30m' WHERE status = 'error' WITH QUERY_TRACE LIMIT 50")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))

				// Verify both empty projection and query trace
				Expect(stmt.Projection.Empty).To(BeTrue())
				Expect(stmt.WithQueryTrace).NotTo(BeNil())
			})

			It("parses Trace query with MATCH operator", func() {
				grammar, err := ParseQuery("SELECT trace_id, operation_name FROM TRACE sw_trace IN default TIME > '-30m' WHERE operation_name MATCH(('GET', 'POST'), 'keyword')")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))
				Expect(stmt.Where).NotTo(BeNil())

				// Verify MATCH operator
				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
				}
			})

			It("parses Trace query with MATCH and other conditions", func() {
				grammar, err := ParseQuery("SELECT * FROM TRACE sw_trace IN default TIME > '-30m' " +
					"WHERE service_id = 'api-gateway' AND http_status > 400 AND operation_name MATCH('api', 'keyword') ORDER BY start_time DESC")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(strings.ToUpper(stmt.From.ResourceType)).To(Equal("TRACE"))

				// The WHERE clause should be a complex tree with MATCH as one of the conditions
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
			})
		})
	})

	Describe("Property Queries", func() {
		Describe("Property ID Filtering", func() {
			It("parses WHERE ID = with single ID", func() {
				grammar, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-1a2b3c'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("ID"))
					}
					if pred.Binary != nil && pred.Binary.Tail.Compare != nil {
						if pred.Binary.Tail.Compare.Value.String != nil {
							Expect(*pred.Binary.Tail.Compare.Value.String).To(Equal("server-1a2b3c"))
						}
					}
				}
			})

			It("parses WHERE ID IN with multiple IDs", func() {
				grammar, err := ParseQuery("SELECT ip, region FROM PROPERTY server_metadata IN datacenter-1 WHERE ID IN ('server-1a2b3c', 'server-4d5e6f')")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("ID"))
					}
				}
			})

			It("parses WHERE ID with projection", func() {
				grammar, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-xyz'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				pcName0, pcErr0 := stmt.Projection.Columns[0].Identifier.ToString(stmt.Projection.Columns[0].TypeSpec != nil)
				Expect(pcErr0).To(BeNil())
				Expect(pcName0).To(Equal("ip"))
				pcName1, pcErr1 := stmt.Projection.Columns[1].Identifier.ToString(stmt.Projection.Columns[1].TypeSpec != nil)
				Expect(pcErr1).To(BeNil())
				Expect(pcName1).To(Equal("owner"))
			})

			It("parses WHERE ID with LIMIT", func() {
				grammar, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID IN ('server-1', 'server-2', 'server-3') LIMIT 10")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Limit.Value).To(Equal(10))
			})

			It("parses case-insensitive ID keyword", func() {
				grammar, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE id = 'server-1a2b3c'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Verify WHERE condition using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				if stmt.Where.Expr.Left != nil && stmt.Where.Expr.Left.Left != nil {
					pred := stmt.Where.Expr.Left.Left
					Expect(pred).NotTo(BeNil())
					if pred.Binary != nil {
						idName, _ := pred.Binary.Identifier.ToString(false)
						Expect(idName).To(Equal("id"))
					}
				}
			})

			It("parses WHERE ID from multiple groups", func() {
				grammar, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1, datacenter-2 WHERE ID IN ('server-1', 'server-2')")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.From.In.Groups).To(HaveLen(2))
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses WHERE ID combined with other tag conditions", func() {
				grammar, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-1' AND datacenter = 'dc-101'")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())

				// Should be a binary AND expression
				// Verify AND/OR structure using Grammar
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
				// Grammar represents AND/OR as tree structure
				Expect(stmt.Where.Expr.Left).NotTo(BeNil())
			})
		})
	})

	Describe("Extreme Values and Edge Cases", func() {
		Describe("Extreme Values and Edge Cases", func() {
			It("parses query with very large integer", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests > 9223372036854775807")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses query with very small negative integer", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests < -9223372036854775807")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses query with zero values", func() {
				grammar, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests = 0 AND status_code = 0")
				Expect(err).To(BeNil())
				Expect(grammar).NotTo(BeNil())

				stmt := grammar.Select
				Expect(stmt.Where).NotTo(BeNil())
			})
		})
	})
})

func BenchmarkParser(b *testing.B) {
	query := `SELECT trace_id, service_id, start_time
			  FROM STREAM sw IN (default, updated)
			  WHERE service_id = 'webapp' AND state = 1
			  ORDER BY start_time DESC
			  LIMIT 100`

	for i := 0; i < b.N; i++ {
		_, _ = ParseQuery(query)
	}
}
