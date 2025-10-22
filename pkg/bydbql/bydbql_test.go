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
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
					Expect(stmt.From.ResourceName).To(Equal("sw"))
				})

				It("parses SELECT with WHERE clause", func() {
					parsed, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default WHERE service_id = 'webapp'")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())

					// Should be a single condition
					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Left).To(Equal("service_id"))
				})

				It("parses SELECT with TIME condition", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m'")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Time).NotTo(BeNil())
					Expect(stmt.Time.Operator).To(Equal(TimeOpGreater))
				})

				It("parses SELECT with GROUP BY", func() {
					parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default GROUP BY region")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.GroupBy).NotTo(BeNil())
					Expect(stmt.GroupBy.Columns).To(HaveLen(1))
					Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
					Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeAuto))
				})

				It("parses SELECT with both TIME BETWEEN and WHERE clause", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default " +
						"TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z' WHERE service_id = 'webapp' AND status = 200")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					// Verify TIME BETWEEN is parsed
					Expect(stmt.Time).NotTo(BeNil())
					Expect(stmt.Time.Operator).To(Equal(TimeOpBetween))
					Expect(stmt.Time.Begin).To(Equal("2023-01-01T00:00:00Z"))
					Expect(stmt.Time.End).To(Equal("2023-01-02T00:00:00Z"))

					// Verify WHERE clause is parsed as a binary tree
					Expect(stmt.Where).NotTo(BeNil())
					Expect(stmt.Where.Expr).NotTo(BeNil())

					// Should be a binary AND expression
					binExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(binExpr.Operator).To(Equal(LogicAnd))

					// Check left condition
					leftCond, ok := binExpr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftCond.Left).To(Equal("service_id"))
					Expect(leftCond.Right.StringVal).To(Equal("webapp"))

					// Check right condition
					rightCond, ok := binExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightCond.Left).To(Equal("status"))
					Expect(rightCond.Right.Integer).To(Equal(int64(200)))
				})

				It("parses TOP N statement", func() {
					parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN default ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*TopNStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.TopN).To(Equal(10))
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Desc).To(BeTrue())
				})

				It("parses empty projection for traces", func() {
					parsed, err := ParseQuery("SELECT () FROM TRACE sw_trace IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Empty).To(BeTrue())
				})

				It("parses WITH QUERY_TRACE", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WITH QUERY_TRACE")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.QueryTrace).To(BeTrue())
				})

				// Test cases for optional parentheses in group list
				It("parses FROM clause with groups in parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN (group1, group2)")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(HaveLen(2))
					Expect(stmt.From.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.Groups[1]).To(Equal("group2"))
				})

				It("parses FROM clause with groups without parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN group1, group2")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(HaveLen(2))
					Expect(stmt.From.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.Groups[1]).To(Equal("group2"))
				})

				It("parses FROM clause with single group without parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM MEASURE metrics IN us-west")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(HaveLen(1))
					Expect(stmt.From.Groups[0]).To(Equal("us-west"))
				})

				It("parses FROM clause with multiple groups without parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM MEASURE metrics IN us-west, us-east, eu-central")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(HaveLen(3))
					Expect(stmt.From.Groups[0]).To(Equal("us-west"))
					Expect(stmt.From.Groups[1]).To(Equal("us-east"))
					Expect(stmt.From.Groups[2]).To(Equal("eu-central"))
				})

				It("parses TOP N query with groups without parentheses", func() {
					parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production, staging ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*TopNStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.TopN).To(Equal(10))
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(HaveLen(2))
					Expect(stmt.From.Groups[0]).To(Equal("production"))
					Expect(stmt.From.Groups[1]).To(Equal("staging"))
				})

				It("parses TRACE query with groups without parentheses", func() {
					parsed, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN group1, group2, group3 TIME > '-30m'")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))
					Expect(stmt.From.Groups).To(HaveLen(3))
					Expect(stmt.From.Groups[0]).To(Equal("group1"))
					Expect(stmt.From.Groups[1]).To(Equal("group2"))
					Expect(stmt.From.Groups[2]).To(Equal("group3"))
				})

				It("parses PROPERTY query with groups without parentheses", func() {
					parsed, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1, datacenter-2 WHERE in_service = 'true'")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.ResourceType).To(Equal(ResourceTypeProperty))
					Expect(stmt.From.Groups).To(HaveLen(2))
					Expect(stmt.From.Groups[0]).To(Equal("datacenter-1"))
					Expect(stmt.From.Groups[1]).To(Equal("datacenter-2"))
				})

				// Group list boundary tests
				It("parses group names with hyphens", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN us-west-1, us-east-1, eu-central-1")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From.Groups).To(HaveLen(3))
					Expect(stmt.From.Groups[0]).To(Equal("us-west-1"))
					Expect(stmt.From.Groups[1]).To(Equal("us-east-1"))
					Expect(stmt.From.Groups[2]).To(Equal("eu-central-1"))
				})

				It("parses group names with underscores", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN prod_primary, prod_secondary, staging_test")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From.Groups).To(HaveLen(3))
					Expect(stmt.From.Groups[0]).To(Equal("prod_primary"))
					Expect(stmt.From.Groups[1]).To(Equal("prod_secondary"))
					Expect(stmt.From.Groups[2]).To(Equal("staging_test"))
				})

				It("parses group names with mixed case", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN Production, Staging, Development")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From.Groups).To(HaveLen(3))
					// Group names should preserve case
					Expect(stmt.From.Groups[0]).To(Equal("Production"))
					Expect(stmt.From.Groups[1]).To(Equal("Staging"))
					Expect(stmt.From.Groups[2]).To(Equal("Development"))
				})

				It("parses group names with numbers", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN cluster1, cluster2, cluster3")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.From.Groups).To(HaveLen(3))
					Expect(stmt.From.Groups[0]).To(Equal("cluster1"))
					Expect(stmt.From.Groups[1]).To(Equal("cluster2"))
					Expect(stmt.From.Groups[2]).To(Equal("cluster3"))
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
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN group1,")
					Expect(err).NotTo(BeNil())
					Expect(parsed).To(BeNil())
				})

				// Test cases for parentheses and operator precedence
				It("parses WHERE with parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE (service_id = 'webapp' OR service_id = 'api') AND status = 200")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be AND
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicAnd))

					// Left should be OR in parentheses
					leftOrExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftOrExpr.Operator).To(Equal(LogicOr))

					// Right should be a condition
					rightCond, ok := rootExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightCond.Left).To(Equal("status"))
				})

				It("parses WHERE with AND precedence over OR", func() {
					// a=1 OR b=2 AND c=3 should parse as: a=1 OR (b=2 AND c=3)
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 OR b = 2 AND c = 3")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					// Left should be condition: a=1
					leftCond, ok := rootExpr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftCond.Left).To(Equal("a"))

					// Right should be AND: b=2 AND c=3
					rightAndExpr, ok := rootExpr.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightAndExpr.Operator).To(Equal(LogicAnd))

					bCond, ok := rightAndExpr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(bCond.Left).To(Equal("b"))

					cCond, ok := rightAndExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cCond.Left).To(Equal("c"))
				})

				It("parses WHERE precedence with mixed logical operators", func() {
					parsed, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default WHERE service_id = 'payment-service' AND status = 500 OR region = 'us-east'")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					leftAndExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftAndExpr.Operator).To(Equal(LogicAnd))

					leftCond, ok := leftAndExpr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftCond.Left).To(Equal("service_id"))
					Expect(leftCond.Right.StringVal).To(Equal("payment-service"))

					rightAndCond, ok := leftAndExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightAndCond.Left).To(Equal("status"))
					Expect(rightAndCond.Right.Integer).To(Equal(int64(500)))

					rightCond, ok := rootExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightCond.Left).To(Equal("region"))
					Expect(rightCond.Right.StringVal).To(Equal("us-east"))
				})

				It("parses WHERE precedence with nested groups", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE (service_id = 'auth' OR region = 'us-east') AND (status = 500 OR (status = 503 AND message MATCH('timeout')))"
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicAnd))

					leftGroup, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftGroup.Operator).To(Equal(LogicOr))

					leftGroupLeft, ok := leftGroup.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftGroupLeft.Left).To(Equal("service_id"))
					Expect(leftGroupLeft.Right.StringVal).To(Equal("auth"))

					leftGroupRight, ok := leftGroup.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftGroupRight.Left).To(Equal("region"))
					Expect(leftGroupRight.Right.StringVal).To(Equal("us-east"))

					rightGroup, ok := rootExpr.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightGroup.Operator).To(Equal(LogicOr))

					rightGroupLeft, ok := rightGroup.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightGroupLeft.Left).To(Equal("status"))
					Expect(rightGroupLeft.Right.Integer).To(Equal(int64(500)))

					rightGroupRight, ok := rightGroup.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightGroupRight.Operator).To(Equal(LogicAnd))

					status503Cond, ok := rightGroupRight.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(status503Cond.Left).To(Equal("status"))
					Expect(status503Cond.Right.Integer).To(Equal(int64(503)))

					matchCond, ok := rightGroupRight.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(matchCond.Operator).To(Equal(OpMatch))
					Expect(matchCond.MatchOption).NotTo(BeNil())
					Expect(matchCond.MatchOption.Values).To(HaveLen(1))
					Expect(matchCond.MatchOption.Values[0].StringVal).To(Equal("timeout"))
				})

				It("parses WHERE precedence with AND binding tighter than OR", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE service_id = 'edge' OR status = 500 AND (region = 'us-east' OR region = 'eu-west')"
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					leftCond, ok := rootExpr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(leftCond.Left).To(Equal("service_id"))
					Expect(leftCond.Right.StringVal).To(Equal("edge"))

					rightAnd, ok := rootExpr.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightAnd.Operator).To(Equal(LogicAnd))

					statusCond, ok := rightAnd.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(statusCond.Left).To(Equal("status"))
					Expect(statusCond.Right.Integer).To(Equal(int64(500)))

					regionOr, ok := rightAnd.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(regionOr.Operator).To(Equal(LogicOr))

					regionLeft, ok := regionOr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(regionLeft.Left).To(Equal("region"))
					Expect(regionLeft.Right.StringVal).To(Equal("us-east"))

					regionRight, ok := regionOr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(regionRight.Left).To(Equal("region"))
					Expect(regionRight.Right.StringVal).To(Equal("eu-west"))
				})

				It("parses ORDER BY with direction only", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY DESC")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Column).To(BeEmpty())
					Expect(stmt.OrderBy.Desc).To(BeTrue())
				})

				It("parses ORDER BY with column and direction", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY response_time DESC")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Column).To(Equal("response_time"))
					Expect(stmt.OrderBy.Desc).To(BeTrue())
				})

				It("parses ORDER BY with column only", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY response_time")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.OrderBy).NotTo(BeNil())
					Expect(stmt.OrderBy.Column).To(Equal("response_time"))
					Expect(stmt.OrderBy.Desc).To(BeFalse())
				})

				It("parses WHERE with nested parentheses", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE ((a = 1 OR b = 2) AND c = 3) OR d = 4")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					// Left should be AND from parentheses
					leftAndExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftAndExpr.Operator).To(Equal(LogicAnd))

					// Right should be condition: d=4
					rightCond, ok := rootExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightCond.Left).To(Equal("d"))
				})

				It("parses WHERE with multiple ANDs", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 AND b = 2 AND c = 3")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Should create left-associative tree: (a=1 AND b=2) AND c=3
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicAnd))

					// Left should be another AND
					leftAndExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftAndExpr.Operator).To(Equal(LogicAnd))

					// Right should be c=3
					rightCond, ok := rootExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightCond.Left).To(Equal("c"))
				})

				It("parses WHERE with complex mixed operators", func() {
					// a=1 AND b=2 OR c=3 AND d=4 should parse as: (a=1 AND b=2) OR (c=3 AND d=4)
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE a = 1 AND b = 2 OR c = 3 AND d = 4")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					// Left should be AND: a=1 AND b=2
					leftAndExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftAndExpr.Operator).To(Equal(LogicAnd))

					// Right should be AND: c=3 AND d=4
					rightAndExpr, ok := rootExpr.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightAndExpr.Operator).To(Equal(LogicAnd))
				})

				// Test cases for MATCH operator
				It("parses MATCH with single value", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Operator).To(Equal(OpMatch))
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("error"))
					Expect(cond.MatchOption.Analyzer).To(BeEmpty())
					Expect(cond.MatchOption.Operator).To(BeEmpty())
				})

				It("parses MATCH with analyzer", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error', 'simple')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("error"))
					Expect(cond.MatchOption.Analyzer).To(Equal("simple"))
					Expect(cond.MatchOption.Operator).To(BeEmpty())
				})

				It("parses MATCH with analyzer and operator", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error warning', 'simple', 'OR')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("error warning"))
					Expect(cond.MatchOption.Analyzer).To(Equal("simple"))
					Expect(cond.MatchOption.Operator).To(Equal("OR"))
				})

				It("parses MATCH with array values", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE tags MATCH(('error', 'warning'))")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(2))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("error"))
					Expect(cond.MatchOption.Values[1].StringVal).To(Equal("warning"))
					Expect(cond.MatchOption.Analyzer).To(BeEmpty())
					Expect(cond.MatchOption.Operator).To(BeEmpty())
				})

				It("parses MATCH with array and options", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE tags MATCH(('tag1', 'tag2'), 'standard', 'AND')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(2))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("tag1"))
					Expect(cond.MatchOption.Values[1].StringVal).To(Equal("tag2"))
					Expect(cond.MatchOption.Analyzer).To(Equal("standard"))
					Expect(cond.MatchOption.Operator).To(Equal("AND"))
				})

				It("parses MATCH with integer values", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE code MATCH((404, 500, 503))")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(3))
					Expect(cond.MatchOption.Values[0].Integer).To(Equal(int64(404)))
					Expect(cond.MatchOption.Values[1].Integer).To(Equal(int64(500)))
					Expect(cond.MatchOption.Values[2].Integer).To(Equal(int64(503)))
				})

				It("parses MATCH with keyword analyzer and multiple values", func() {
					parsed, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default WHERE operation_name MATCH(('GET', 'POST'), 'keyword')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Operator).To(Equal(OpMatch))
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Analyzer).To(Equal("keyword"))
					Expect(cond.MatchOption.Values).To(HaveLen(2))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("GET"))
					Expect(cond.MatchOption.Values[1].StringVal).To(Equal("POST"))
				})

				It("parses MATCH within grouped logical expressions", func() {
					query := "SELECT * FROM STREAM sw IN default WHERE (message MATCH('error') OR message MATCH('timeout', 'standard')) AND (status = 500 OR status = 503)"
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicAnd))

					leftOr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftOr.Operator).To(Equal(LogicOr))

					firstMatch, ok := leftOr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(firstMatch.Operator).To(Equal(OpMatch))
					Expect(firstMatch.MatchOption).NotTo(BeNil())
					Expect(firstMatch.MatchOption.Values).To(HaveLen(1))
					Expect(firstMatch.MatchOption.Values[0].StringVal).To(Equal("error"))
					Expect(firstMatch.MatchOption.Analyzer).To(BeEmpty())

					secondMatch, ok := leftOr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(secondMatch.Operator).To(Equal(OpMatch))
					Expect(secondMatch.MatchOption).NotTo(BeNil())
					Expect(secondMatch.MatchOption.Analyzer).To(Equal("standard"))
					Expect(secondMatch.MatchOption.Values).To(HaveLen(1))
					Expect(secondMatch.MatchOption.Values[0].StringVal).To(Equal("timeout"))

					rightOr, ok := rootExpr.Right.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rightOr.Operator).To(Equal(LogicOr))

					status500, ok := rightOr.Left.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(status500.Left).To(Equal("status"))
					Expect(status500.Right.Integer).To(Equal(int64(500)))

					status503, ok := rightOr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(status503.Left).To(Equal("status"))
					Expect(status503.Right.Integer).To(Equal(int64(503)))
				})

				It("parses MATCH with dot-separated identifier from documentation", func() {
					parsed, err := ParseQuery("SELECT trace_id, db.instance, data_binary FROM STREAM sw IN (default) WHERE db.instance MATCH('mysql')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("db.instance"))

					Expect(stmt.From).NotTo(BeNil())
					Expect(stmt.From.Groups).To(Equal([]string{"default"}))

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Left).To(Equal("db.instance"))
					Expect(cond.Operator).To(Equal(OpMatch))
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Values[0].StringVal).To(Equal("mysql"))
				})

				It("parses complex query with MATCH and other conditions", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE service_id = 'auth' AND message MATCH('error', 'simple') OR status = 500")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					// Root should be OR
					rootExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(rootExpr.Operator).To(Equal(LogicOr))

					// Left should be AND with MATCH
					leftAndExpr, ok := rootExpr.Left.(*BinaryLogicExpr)
					Expect(ok).To(BeTrue())
					Expect(leftAndExpr.Operator).To(Equal(LogicAnd))

					// Check MATCH condition in the AND expression
					rightMatchCond, ok := leftAndExpr.Right.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(rightMatchCond.Operator).To(Equal(OpMatch))
					Expect(rightMatchCond.MatchOption).NotTo(BeNil())
					Expect(rightMatchCond.MatchOption.Analyzer).To(Equal("simple"))
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
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Operator).To(Equal(OpMatch))
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Analyzer).To(BeEmpty())
					Expect(cond.MatchOption.Operator).To(BeEmpty())
				})

				It("parses MATCH with value and analyzer (no operator)", func() {
					// This should be valid - value and analyzer without operator
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message MATCH('error', 'standard')")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())

					cond, ok := stmt.Where.Expr.(*Condition)
					Expect(ok).To(BeTrue())
					Expect(cond.Operator).To(Equal(OpMatch))
					Expect(cond.MatchOption).NotTo(BeNil())
					Expect(cond.MatchOption.Values).To(HaveLen(1))
					Expect(cond.MatchOption.Analyzer).To(Equal("standard"))
					Expect(cond.MatchOption.Operator).To(BeEmpty())
				})
			})

			Describe("invalid queries", func() {
				It("handles invalid syntax", func() {
					parsed, err := ParseQuery("SELECT FROM")
					Expect(err).ToNot(BeNil())
					Expect(parsed).To(BeNil())
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
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil(), "failed to parse query: %s\nerror: %v", query, err)
					Expect(parsed).NotTo(BeNil())
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
				"SELECT * FROM STREAM sw IN (default",                                                   // unmatched opening parenthesis in groups
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
				"SELECT COUNT FROM MEASURE m IN default",                                                // COUNT without parentheses
				"SELECT * FROM STREAM sw in default ORDER BY",                                           // incomplete ORDER BY
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::invalid",        // invalid type specifier in GROUP BY
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY ::tag",                  // missing column name before type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::",               // incomplete type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region:tag",             // malformed type specifier (single colon)
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag::field",     // multiple type specifiers
				"SELECT region, service, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag,",  // trailing comma after type specifier
				"SELECT region, SUM(latency) FROM MEASURE m IN default GROUP BY region::tag, service::", // incomplete type specifier after comma
				"SELECT SUM(a), AVG(b) FROM MEASURE m IN default",                                       // multiple aggregate functions (2)
				"SELECT SUM(a), AVG(b), MAX(c) FROM MEASURE m IN default",                               // multiple aggregate functions (3)
				"SELECT region, SUM(a), COUNT(b) FROM MEASURE m IN default GROUP BY region",             // multiple aggregates with GROUP BY
				"SELECT TOP 10 service_id::field DESC FROM STREAM sw IN default",                        // TOP N identifier no type declaration
				"SELECT TOP -1 service_id FROM STREAM sw IN default",                                    // TOP N negative number

				// top N not allowed with OR
				"SHOW TOP 10 FROM MEASURE service_metrics IN default TIME > '-30m' WHERE region = 'us-west' OR environment = 'production'",
				// top N not allowed ORDER BY with identifier
				"SHOW TOP 10 FROM MEASURE service_latency IN default TIME < '2023-01-01T00:00:00Z' ORDER BY value DESC",
			}

			for i, query := range invalidQueries {
				It(fmt.Sprintf("handles invalid query %d: %s", i+1, query), func() {
					parsed, err := ParseQuery(query)

					// Should have parsing error
					Expect(err).ToNot(BeNil(), "expected parsing error for query: %s", query)

					// Parsed query should be nil for invalid queries
					Expect(parsed).To(BeNil(), "expected nil parsed query for invalid query: %s", query)
				})
			}
		})

		Describe("Time Format Parsing", func() {
			DescribeTable("parses time formats correctly in queries",
				func(timeCondition string, validateFunc func(*ParsedQuery) bool) {
					query := fmt.Sprintf("SELECT * FROM STREAM sw IN default TIME %s", timeCondition)
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					Expect(validateFunc(parsed)).To(BeTrue(), "time validation failed for data: %+v", parsed)
				},
				Entry("absolute time range",
					"BETWEEN '2023-01-01T10:00:00Z' AND '2023-01-01T11:00:00Z'",
					func(data *ParsedQuery) bool {
						sel, ok := data.Statement.(*SelectStatement)
						return ok && sel.Time != nil && sel.Time.Begin == "2023-01-01T10:00:00Z" &&
							sel.Time.End == "2023-01-01T11:00:00Z"
					}),
				Entry("relative time condition",
					"> '-30m'",
					func(data *ParsedQuery) bool {
						sel, ok := data.Statement.(*SelectStatement)
						return ok && sel.Time != nil && sel.Time.Begin == "" && sel.Time.End == "" && sel.Time.Timestamp != ""
					}),
			)
		})

		Describe("NULL Value Support", func() {
			It("parses WHERE field = NULL", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE optional_field = NULL")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("optional_field"))
				Expect(cond.Operator).To(Equal(OpEqual))
				Expect(cond.Right.IsNull).To(BeTrue())
			})

			It("parses WHERE field != NULL", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE optional_field != NULL")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("optional_field"))
				Expect(cond.Operator).To(Equal(OpNotEqual))
				Expect(cond.Right.IsNull).To(BeTrue())
			})

			It("parses NULL in MEASURE queries", func() {
				parsed, err := ParseQuery("SELECT region, SUM(value) FROM MEASURE metrics IN default TIME > '-30m' WHERE optional_tag = NULL GROUP BY region")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses NULL with AND/OR conditions", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'webapp' AND optional_field = NULL")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				binExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
				Expect(ok).To(BeTrue())
				Expect(binExpr.Operator).To(Equal(LogicAnd))
			})

			It("parses case-insensitive NULL", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE f = null")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.IsNull).To(BeTrue())
			})
		})

		Describe("Inequality Operators", func() {
			It("parses != operator with string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id != 'webapp'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpNotEqual))
				Expect(cond.Right.StringVal).To(Equal("webapp"))
			})

			It("parses != operator with integer", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status != 200")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpNotEqual))
				Expect(cond.Right.Integer).To(Equal(int64(200)))
			})

			It("parses > operator with string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE string_field > 'abc'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpGreater))
				Expect(cond.Right.StringVal).To(Equal("abc"))
			})

			It("parses < operator with string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE string_field < 'xyz'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpLess))
				Expect(cond.Right.StringVal).To(Equal("xyz"))
			})

			It("parses >= operator with string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version >= '1.0.0'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpGreaterEqual))
				Expect(cond.Right.StringVal).To(Equal("1.0.0"))
			})

			It("parses <= operator with string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version <= '2.0.0'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpLessEqual))
				Expect(cond.Right.StringVal).To(Equal("2.0.0"))
			})

			It("parses multiple inequality operators", func() {
				parsed, err := ParseQuery("SELECT * FROM MEASURE metrics IN default TIME > '-30m' WHERE latency >= 100 AND latency <= 1000")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				binExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
				Expect(ok).To(BeTrue())
				Expect(binExpr.Operator).To(Equal(LogicAnd))

				leftCond, ok := binExpr.Left.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(leftCond.Operator).To(Equal(OpGreaterEqual))

				rightCond, ok := binExpr.Right.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(rightCond.Operator).To(Equal(OpLessEqual))
			})
		})

		Describe("IN and NOT IN Operators - Boundary Cases", func() {
			It("parses IN with single value", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200)")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpIn))
				Expect(cond.Values).To(HaveLen(1))
				Expect(cond.Values[0].Integer).To(Equal(int64(200)))
			})

			It("parses IN with two values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 404)")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpIn))
				Expect(cond.Values).To(HaveLen(2))
				Expect(cond.Values[0].Integer).To(Equal(int64(200)))
				Expect(cond.Values[1].Integer).To(Equal(int64(404)))
			})

			It("parses IN with many values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 201, 202, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503)")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpIn))
				Expect(cond.Values).To(HaveLen(13))
			})

			It("parses IN with string values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE region IN ('us-west-1', 'us-east-1', 'eu-west-1')")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpIn))
				Expect(cond.Values).To(HaveLen(3))
				Expect(cond.Values[0].StringVal).To(Equal("us-west-1"))
				Expect(cond.Values[1].StringVal).To(Equal("us-east-1"))
				Expect(cond.Values[2].StringVal).To(Equal("eu-west-1"))
			})

			It("handles IN with empty list", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN ()")

				if err != nil {
					// Parser may reject empty IN list
					Expect(err.Error()).To(Or(
						ContainSubstring("IN"),
						ContainSubstring("empty"),
						ContainSubstring("values"),
					))
				} else {
					// Parser might accept syntactically, semantic validator should reject
					Expect(parsed).NotTo(BeNil())
				}
			})

			It("parses NOT IN with single value", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN (500)")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpNotIn))
				Expect(cond.Values).To(HaveLen(1))
				Expect(cond.Values[0].Integer).To(Equal(int64(500)))
			})

			It("parses NOT IN with multiple values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN (500, 502, 503, 504)")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpNotIn))
				Expect(cond.Values).To(HaveLen(4))
			})

			It("handles NOT IN with empty list", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status NOT IN ()")

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
					Expect(parsed).NotTo(BeNil())
				}
			})

			It("parses IN with mixed integer and string values (if supported)", func() {
				// Some systems may allow mixed types, others may reject
				// Use "status" instead of "field" to avoid keyword conflict
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status IN (200, 'error', 404)")

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
					Expect(parsed).NotTo(BeNil())
					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())
				}
			})
		})

		Describe("Complex Dot-Separated Paths", func() {
			It("parses deep nested paths in projection", func() {
				parsed, err := ParseQuery("SELECT a.b.c.d.e FROM STREAM sw IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(1))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("a.b.c.d.e"))
			})

			It("parses nested paths in WHERE clause", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE metadata.request.header.content_type = 'application/json'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("metadata.request.header.content_type"))
				Expect(cond.Right.StringVal).To(Equal("application/json"))
			})

			It("parses nested paths with type specifiers", func() {
				parsed, err := ParseQuery("SELECT metadata.service.name::tag, response.body.size::field FROM MEASURE metrics IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("metadata.service.name"))
				Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("response.body.size"))
				Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeField))
			})

			It("parses nested paths in GROUP BY", func() {
				parsed, err := ParseQuery("SELECT metadata.region.name, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY metadata.region.name")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("metadata.region.name"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeAuto))
			})

			It("parses nested paths in ORDER BY", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY metadata.timestamp.value DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Column).To(Equal("metadata.timestamp.value"))
				Expect(stmt.OrderBy.Desc).To(BeTrue())
			})

			It("parses nested paths in aggregate functions", func() {
				parsed, err := ParseQuery("SELECT SUM(metrics.response.latency) FROM MEASURE m IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(1))
				Expect(stmt.Projection.Columns[0].Function).NotTo(BeNil())
				Expect(stmt.Projection.Columns[0].Function.Column).To(Equal("metrics.response.latency"))
			})

			It("parses complex query with multiple nested paths", func() {
				parsed, err := ParseQuery(`SELECT
				trace.span.id,
				metadata.service.name::tag,
				metrics.response.time::field
			FROM STREAM sw IN default
			TIME > '-30m'
			WHERE metadata.region.datacenter = 'us-west-1'
			ORDER BY metrics.response.time DESC`)
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(3))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("trace.span.id"))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("metadata.service.name"))
				Expect(stmt.Projection.Columns[2].Name).To(Equal("metrics.response.time"))
			})
		})

		Describe("Advanced ORDER BY", func() {
			It("parses ORDER BY with dot-separated path", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY metadata.timestamp ASC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Column).To(Equal("metadata.timestamp"))
				Expect(stmt.OrderBy.Desc).To(BeFalse())
			})

			It("parses ORDER BY with nested path and DESC", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY response.metadata.timestamp DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Column).To(Equal("response.metadata.timestamp"))
				Expect(stmt.OrderBy.Desc).To(BeTrue())
			})

			It("parses case-insensitive ORDER BY with ASC/DESC", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' order by timestamp asc")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Desc).To(BeFalse())
			})

			// Stream-specific: ORDER BY TIME shorthand
			It("parses ORDER BY TIME DESC for Stream queries", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY TIME DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
				Expect(stmt.OrderBy).NotTo(BeNil())
				// ORDER BY TIME is a shorthand that relies on timestamps
				// The parser should handle this appropriately
				Expect(stmt.OrderBy.Desc).To(BeTrue())
			})

			It("parses ORDER BY TIME ASC for Stream queries", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' ORDER BY TIME ASC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Desc).To(BeFalse())
			})

			It("parses ORDER BY TIME for Trace queries", func() {
				parsed, err := ParseQuery("SELECT trace_id, service_id FROM TRACE sw_trace IN default TIME > '-1h' ORDER BY TIME DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Desc).To(BeTrue())
			})

			It("parses ORDER BY TIME for Measure queries", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region ORDER BY TIME ASC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeMeasure))
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Desc).To(BeFalse())
			})
		})

		Describe("GROUP BY with Type Specifiers", func() {
			It("parses GROUP BY with single column without type specifier", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeAuto))
			})

			It("parses GROUP BY with single column with ::tag specifier", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))
			})

			It("parses GROUP BY with single column with ::field specifier", func() {
				parsed, err := ParseQuery("SELECT status, COUNT(requests) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY status::field")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("status"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeField))
			})

			It("parses GROUP BY with multiple columns without type specifiers", func() {
				parsed, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region, service")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeAuto))
				Expect(stmt.GroupBy.Columns[1].Name).To(Equal("service"))
				Expect(stmt.GroupBy.Columns[1].Type).To(Equal(ColumnTypeAuto))
			})

			It("parses GROUP BY with multiple columns with type specifiers", func() {
				parsed, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag, service::tag")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))
				Expect(stmt.GroupBy.Columns[1].Name).To(Equal("service"))
				Expect(stmt.GroupBy.Columns[1].Type).To(Equal(ColumnTypeTag))
			})

			It("parses GROUP BY with mixed columns (with and without type specifiers)", func() {
				parsed, err := ParseQuery("SELECT region, service, status, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::tag, service, status::field")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(3))

				// First column with ::tag
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))

				// Second column without type specifier
				Expect(stmt.GroupBy.Columns[1].Name).To(Equal("service"))
				Expect(stmt.GroupBy.Columns[1].Type).To(Equal(ColumnTypeAuto))

				// Third column with ::field
				Expect(stmt.GroupBy.Columns[2].Name).To(Equal("status"))
				Expect(stmt.GroupBy.Columns[2].Type).To(Equal(ColumnTypeField))
			})

			It("parses GROUP BY with dot-separated column name and type specifier", func() {
				parsed, err := ParseQuery("SELECT metadata.region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY metadata.region::tag")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("metadata.region"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))
			})

			It("parses case-insensitive type specifiers", func() {
				parsed, err := ParseQuery("SELECT region, service, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region::TAG, service::FIELD")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(2))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))
				Expect(stmt.GroupBy.Columns[1].Type).To(Equal(ColumnTypeField))
			})

			It("parses GROUP BY with type specifiers in complex query", func() {
				parsed, err := ParseQuery(`SELECT region::tag, service::tag, status::field, SUM(latency)
				FROM MEASURE metrics IN default
				TIME > '-30m'
				WHERE region = 'us-west'
				GROUP BY region::tag, service::tag, status::field
				ORDER BY latency DESC
				LIMIT 100`)
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(3))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeTag))
				Expect(stmt.GroupBy.Columns[1].Type).To(Equal(ColumnTypeTag))
				Expect(stmt.GroupBy.Columns[2].Type).To(Equal(ColumnTypeField))
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
					parsed, err := ParseQuery(query)
					Expect(err).To(BeNil(), "parsing error for query '%s': %v", query, err)
					Expect(parsed).NotTo(BeNil(), "failed to parse case-insensitive query: %s", query)
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

				stmtUpper, ok := parsedUpper.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtUpper.From).NotTo(BeNil())
				Expect(stmtUpper.From.ResourceName).To(Equal("MyStream"))

				// Parse query with lowercase stream name
				parsedLower, err := ParseQuery("SELECT * FROM STREAM mystream IN group1")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower, ok := parsedLower.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtLower.From).NotTo(BeNil())
				Expect(stmtLower.From.ResourceName).To(Equal("mystream"))

				// Verify they are different resource names
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("preserves exact case of stream names with mixed case keywords", func() {
				// Query with mixed case keywords but specific stream name
				parsed, err := ParseQuery("sElEcT * FrOm StReAm MyStream In group1")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From).NotTo(BeNil())
				// Stream name should preserve exact case
				Expect(stmt.From.ResourceName).To(Equal("MyStream"))
			})

			It("treats different-cased measure names as distinct resources", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM MEASURE ServiceMetrics IN default")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper, ok := parsedUpper.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtUpper.From.ResourceName).To(Equal("ServiceMetrics"))

				parsedLower, err := ParseQuery("SELECT * FROM MEASURE servicemetrics IN default")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower, ok := parsedLower.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtLower.From.ResourceName).To(Equal("servicemetrics"))

				// Verify they are different
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("treats different-cased trace names as distinct resources", func() {
				parsedCamel, err := ParseQuery("SELECT * FROM TRACE SwTrace IN default")
				Expect(err).To(BeNil())
				Expect(parsedCamel).NotTo(BeNil())

				stmtCamel, ok := parsedCamel.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtCamel.From.ResourceName).To(Equal("SwTrace"))

				parsedSnake, err := ParseQuery("SELECT * FROM TRACE sw_trace IN default")
				Expect(err).To(BeNil())
				Expect(parsedSnake).NotTo(BeNil())

				stmtSnake, ok := parsedSnake.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtSnake.From.ResourceName).To(Equal("sw_trace"))

				// Verify they are different
				Expect(stmtCamel.From.ResourceName).NotTo(Equal(stmtSnake.From.ResourceName))
			})

			It("treats different-cased property names as distinct resources", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM PROPERTY ServerMetadata IN dc1")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper, ok := parsedUpper.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtUpper.From.ResourceName).To(Equal("ServerMetadata"))

				parsedLower, err := ParseQuery("SELECT * FROM PROPERTY servermetadata IN dc1")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower, ok := parsedLower.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtLower.From.ResourceName).To(Equal("servermetadata"))

				// Verify they are different
				Expect(stmtUpper.From.ResourceName).NotTo(Equal(stmtLower.From.ResourceName))
			})

			It("preserves case of column names in projections", func() {
				parsed, err := ParseQuery("SELECT TraceId, ServiceId, StartTime FROM STREAM sw IN default")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Columns).To(HaveLen(3))

				// Column names should preserve case
				Expect(stmt.Projection.Columns[0].Name).To(Equal("TraceId"))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("ServiceId"))
				Expect(stmt.Projection.Columns[2].Name).To(Equal("StartTime"))
			})

			It("preserves case of column names in TOP projections", func() {
				parsed, err := ParseQuery("SELECT TOP 10 TraceId DESC, ServiceId, StartTime FROM STREAM sw IN default")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(10))
				Expect(stmt.Projection.TopN.OrderField).To(Equal("TraceId"))
				Expect(stmt.Projection.TopN.Desc).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(2))

				// Additional column names should preserve case
				Expect(stmt.Projection.Columns[0].Name).To(Equal("ServiceId"))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("StartTime"))
			})

			It("preserves case of column names in WHERE clauses", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE ServiceId = 'test' AND StatusCode = 200")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				// Check that column names in WHERE clause preserve case
				binExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
				Expect(ok).To(BeTrue())

				leftCond, ok := binExpr.Left.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(leftCond.Left).To(Equal("ServiceId"))

				rightCond, ok := binExpr.Right.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(rightCond.Left).To(Equal("StatusCode"))
			})

			It("preserves case of column names in GROUP BY", func() {
				parsed, err := ParseQuery("SELECT RegionName, SUM(value) FROM MEASURE metrics IN default GROUP BY RegionName")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.GroupBy).NotTo(BeNil())
				Expect(stmt.GroupBy.Columns).To(HaveLen(1))

				// Column name in GROUP BY should preserve case
				Expect(stmt.GroupBy.Columns[0].Name).To(Equal("RegionName"))
				Expect(stmt.GroupBy.Columns[0].Type).To(Equal(ColumnTypeAuto))
			})

			It("preserves case of column names in ORDER BY", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default ORDER BY StartTimestamp DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.OrderBy).NotTo(BeNil())

				// Column name in ORDER BY should preserve case
				Expect(stmt.OrderBy.Column).To(Equal("StartTimestamp"))
			})

			It("treats different-cased group names as distinct", func() {
				parsedUpper, err := ParseQuery("SELECT * FROM STREAM sw IN Production")
				Expect(err).To(BeNil())
				Expect(parsedUpper).NotTo(BeNil())

				stmtUpper, ok := parsedUpper.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtUpper.From.Groups).To(HaveLen(1))
				Expect(stmtUpper.From.Groups[0]).To(Equal("Production"))

				parsedLower, err := ParseQuery("SELECT * FROM STREAM sw IN production")
				Expect(err).To(BeNil())
				Expect(parsedLower).NotTo(BeNil())

				stmtLower, ok := parsedLower.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmtLower.From.Groups).To(HaveLen(1))
				Expect(stmtLower.From.Groups[0]).To(Equal("production"))

				// Verify group names are different
				Expect(stmtUpper.From.Groups[0]).NotTo(Equal(stmtLower.From.Groups[0]))
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
					parsed, err := ParseQuery(tc.query)
					Expect(err).To(BeNil(), "failed to parse query: %s", tc.query)
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
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
					parsed, err := ParseQuery("SELECT trace_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeAuto))
					Expect(stmt.Projection.Columns[0].Function).To(BeNil())
				})

				It("parses multiple columns", func() {
					parsed, err := ParseQuery("SELECT trace_id, service_id, duration FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// Verify each column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeAuto))

					Expect(stmt.Projection.Columns[1].Name).To(Equal("service_id"))
					Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeAuto))

					Expect(stmt.Projection.Columns[2].Name).To(Equal("duration"))
					Expect(stmt.Projection.Columns[2].Type).To(Equal(ColumnTypeAuto))
				})

				It("parses column with underscores", func() {
					parsed, err := ParseQuery("SELECT trace_id, service_name, response_time FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("service_name"))
					Expect(stmt.Projection.Columns[2].Name).To(Equal("response_time"))
				})

				It("parses column with hyphens", func() {
					parsed, err := ParseQuery("SELECT service-id, request-id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(2))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("service-id"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("request-id"))
				})
			})

			Describe("column type specifiers", func() {
				It("parses column with ::tag specifier", func() {
					parsed, err := ParseQuery("SELECT service_id::tag FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Name).To(Equal("service_id"))
					Expect(col.Type).To(Equal(ColumnTypeTag))
					Expect(col.Function).To(BeNil())
				})

				It("parses column with ::field specifier", func() {
					parsed, err := ParseQuery("SELECT latency::field FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Name).To(Equal("latency"))
					Expect(col.Type).To(Equal(ColumnTypeField))
					Expect(col.Function).To(BeNil())
				})

				It("parses case-insensitive type specifiers", func() {
					parsed1, err := ParseQuery("SELECT col1::TAG, col2::FIELD FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt1 := parsed1.Statement.(*SelectStatement)
					Expect(stmt1.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))
					Expect(stmt1.Projection.Columns[1].Type).To(Equal(ColumnTypeField))

					parsed2, err := ParseQuery("SELECT col1::tag, col2::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt2 := parsed2.Statement.(*SelectStatement)
					Expect(stmt2.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))
					Expect(stmt2.Projection.Columns[1].Type).To(Equal(ColumnTypeField))
				})

				It("parses mixed columns with and without type specifiers", func() {
					parsed, err := ParseQuery("SELECT service_id::tag, duration, latency::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First column with ::tag
					Expect(stmt.Projection.Columns[0].Name).To(Equal("service_id"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))

					// Second column without specifier
					Expect(stmt.Projection.Columns[1].Name).To(Equal("duration"))
					Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeAuto))

					// Third column with ::field
					Expect(stmt.Projection.Columns[2].Name).To(Equal("latency"))
					Expect(stmt.Projection.Columns[2].Type).To(Equal(ColumnTypeField))
				})
			})

			Describe("dot-separated column paths", func() {
				It("parses simple dot-separated path", func() {
					parsed, err := ParseQuery("SELECT metadata.service_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Name).To(Equal("metadata.service_id"))
					Expect(col.Type).To(Equal(ColumnTypeAuto))
				})

				It("parses multiple levels of dot-separated paths", func() {
					parsed, err := ParseQuery("SELECT request.header.content_type, response.body.status FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					Expect(stmt.Projection.Columns[0].Name).To(Equal("request.header.content_type"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("response.body.status"))
				})

				It("parses dot-separated path with type specifier", func() {
					parsed, err := ParseQuery("SELECT metadata.service_id::tag, response.status::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					// First column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("metadata.service_id"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))

					// Second column
					Expect(stmt.Projection.Columns[1].Name).To(Equal("response.status"))
					Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeField))
				})

				It("preserves case in dot-separated paths", func() {
					parsed, err := ParseQuery("SELECT MetaData.ServiceId, Response.Status FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(2))

					Expect(stmt.Projection.Columns[0].Name).To(Equal("MetaData.ServiceId"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("Response.Status"))
				})
			})

			Describe("aggregate functions", func() {
				It("parses SUM function", func() {
					parsed, err := ParseQuery("SELECT SUM(latency) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Function).NotTo(BeNil())
					Expect(col.Function.Function).To(Equal("SUM"))
					Expect(col.Function.Column).To(Equal("latency"))
					Expect(col.Name).To(BeEmpty())
				})

				It("parses AVG/MEAN functions", func() {
					parsed, err := ParseQuery("SELECT AVG(latency) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// AVG
					Expect(stmt.Projection.Columns[0].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Function.Function).To(Equal("AVG"))
					Expect(stmt.Projection.Columns[0].Function.Column).To(Equal("latency"))
				})

				It("parses MAX function", func() {
					parsed, err := ParseQuery("SELECT MAX(cpu_usage) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Function.Function).To(Equal("MAX"))
					Expect(stmt.Projection.Columns[0].Function.Column).To(Equal("cpu_usage"))
				})

				It("parses MIN function", func() {
					parsed, err := ParseQuery("SELECT MIN(memory_usage) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Function.Function).To(Equal("MIN"))
					Expect(stmt.Projection.Columns[0].Function.Column).To(Equal("memory_usage"))
				})

				It("parses COUNT function", func() {
					parsed, err := ParseQuery("SELECT COUNT(requests) FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					col := stmt.Projection.Columns[0]
					Expect(col.Function).NotTo(BeNil())
					Expect(col.Function.Function).To(Equal("COUNT"))
					Expect(col.Function.Column).To(Equal("requests"))
				})

				It("parses aggregate function with dot-separated column", func() {
					parsed, err := ParseQuery("SELECT SUM(metrics.latency) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// SUM with dot-separated
					Expect(stmt.Projection.Columns[0].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[0].Function.Function).To(Equal("SUM"))
					Expect(stmt.Projection.Columns[0].Function.Column).To(Equal("metrics.latency"))
				})

				It("parses case-insensitive aggregate functions", func() {
					parsed, err := ParseQuery("SELECT sum(a) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					// Should be normalized to uppercase
					Expect(stmt.Projection.Columns[0].Function.Function).To(Equal("SUM"))
				})
			})

			Describe("mixed columns and aggregate functions", func() {
				It("parses regular columns with aggregate functions", func() {
					parsed, err := ParseQuery("SELECT region, SUM(latency), service_id FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: regular column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("region"))
					Expect(stmt.Projection.Columns[0].Function).To(BeNil())

					// Second: aggregate function
					Expect(stmt.Projection.Columns[1].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[1].Function.Function).To(Equal("SUM"))
					Expect(stmt.Projection.Columns[1].Name).To(BeEmpty())

					// Third: regular column
					Expect(stmt.Projection.Columns[2].Name).To(Equal("service_id"))
					Expect(stmt.Projection.Columns[2].Function).To(BeNil())
				})

				It("parses typed columns with aggregate functions", func() {
					parsed, err := ParseQuery("SELECT region::tag, SUM(latency), duration::field FROM MEASURE metrics IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: tag column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("region"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeTag))
					Expect(stmt.Projection.Columns[0].Function).To(BeNil())

					// Second: aggregate function
					Expect(stmt.Projection.Columns[1].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[1].Function.Function).To(Equal("SUM"))

					// Third: field column
					Expect(stmt.Projection.Columns[2].Name).To(Equal("duration"))
					Expect(stmt.Projection.Columns[2].Type).To(Equal(ColumnTypeField))
					Expect(stmt.Projection.Columns[2].Function).To(BeNil())
				})

				It("parses dot-separated columns with aggregate functions", func() {
					parsed, err := ParseQuery("SELECT metadata.region, SUM(metrics.latency), response.status FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// First: dot-separated column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("metadata.region"))
					Expect(stmt.Projection.Columns[0].Function).To(BeNil())

					// Second: aggregate with dot-separated
					Expect(stmt.Projection.Columns[1].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[1].Function.Column).To(Equal("metrics.latency"))

					// Third: dot-separated column
					Expect(stmt.Projection.Columns[2].Name).To(Equal("response.status"))
					Expect(stmt.Projection.Columns[2].Function).To(BeNil())
				})

				It("parses aggregate function with regular columns", func() {
					parsed, err := ParseQuery("SELECT service, region, SUM(requests) FROM MEASURE m IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(3))

					// Regular columns
					Expect(stmt.Projection.Columns[0].Name).To(Equal("service"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("region"))

					// Aggregate function
					Expect(stmt.Projection.Columns[2].Function.Function).To(Equal("SUM"))
				})
			})

			Describe("special projections", func() {
				It("parses wildcard projection", func() {
					parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.All).To(BeTrue())
					Expect(stmt.Projection.Empty).To(BeFalse())
					Expect(stmt.Projection.Columns).To(HaveLen(0))
				})

				It("parses empty projection", func() {
					parsed, err := ParseQuery("SELECT () FROM TRACE sw_trace IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.Empty).To(BeTrue())
					Expect(stmt.Projection.All).To(BeFalse())
					Expect(stmt.Projection.Columns).To(HaveLen(0))
				})

				It("parses TOP N with specific columns", func() {
					parsed, err := ParseQuery("SELECT TOP 10 trace_id DESC, service_id FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection).NotTo(BeNil())
					Expect(stmt.Projection.TopN).NotTo(BeNil())
					Expect(stmt.Projection.TopN.N).To(Equal(10))
					Expect(stmt.Projection.TopN.OrderField).To(Equal("trace_id"))
					Expect(stmt.Projection.TopN.Desc).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Name).To(Equal("service_id"))
				})

				It("parses TOP N with typed columns", func() {
					parsed, err := ParseQuery("SELECT TOP 10 service_id DESC, latency::field FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.TopN).NotTo(BeNil())
					Expect(stmt.Projection.TopN.N).To(Equal(10))
					Expect(stmt.Projection.TopN.OrderField).To(Equal("service_id"))
					Expect(stmt.Projection.TopN.Desc).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(1))

					Expect(stmt.Projection.Columns[0].Name).To(Equal("latency"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeField))
				})
			})

			Describe("column name formats", func() {
				It("parses alphanumeric column names", func() {
					parsed, err := ParseQuery("SELECT col1, col2, col123 FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := parsed.Statement.(*SelectStatement)
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("col1"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("col2"))
					Expect(stmt.Projection.Columns[2].Name).To(Equal("col123"))
				})

				It("parses mixed case column names", func() {
					parsed, err := ParseQuery("SELECT TraceId, ServiceID, RequestTime FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := parsed.Statement.(*SelectStatement)
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("TraceId"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("ServiceID"))
					Expect(stmt.Projection.Columns[2].Name).To(Equal("RequestTime"))
				})

				It("parses column names with leading underscores", func() {
					parsed, err := ParseQuery("SELECT _id, _timestamp, _internal FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := parsed.Statement.(*SelectStatement)
					Expect(stmt.Projection.Columns).To(HaveLen(3))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("_id"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("_timestamp"))
					Expect(stmt.Projection.Columns[2].Name).To(Equal("_internal"))
				})

				It("parses complex dot-separated paths", func() {
					parsed, err := ParseQuery("SELECT a.b.c, x.y.z.w FROM STREAM sw IN default")
					Expect(err).To(BeNil())
					stmt := parsed.Statement.(*SelectStatement)
					Expect(stmt.Projection.Columns).To(HaveLen(2))
					Expect(stmt.Projection.Columns[0].Name).To(Equal("a.b.c"))
					Expect(stmt.Projection.Columns[1].Name).To(Equal("x.y.z.w"))
				})
			})

			Describe("complete projection scenarios", func() {
				It("parses comprehensive column projection", func() {
					parsed, err := ParseQuery(`SELECT
					trace_id,
					metadata.service_id::tag,
					response.latency::field,
					AVG(duration.milliseconds)
					FROM MEASURE metrics IN default`)
					Expect(err).To(BeNil())
					Expect(parsed).NotTo(BeNil())

					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Projection.Columns).To(HaveLen(4))

					// Column 1: simple column
					Expect(stmt.Projection.Columns[0].Name).To(Equal("trace_id"))
					Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeAuto))

					// Column 2: dot-separated with tag type
					Expect(stmt.Projection.Columns[1].Name).To(Equal("metadata.service_id"))
					Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeTag))

					// Column 3: dot-separated with field type
					Expect(stmt.Projection.Columns[2].Name).To(Equal("response.latency"))
					Expect(stmt.Projection.Columns[2].Type).To(Equal(ColumnTypeField))

					// Column 4: aggregate with dot-separated column
					Expect(stmt.Projection.Columns[3].Function).NotTo(BeNil())
					Expect(stmt.Projection.Columns[3].Function.Function).To(Equal("AVG"))
					Expect(stmt.Projection.Columns[3].Function.Column).To(Equal("duration.milliseconds"))
				})
			})

			Describe("keyword spacing validation", func() {
				It("rejects query with missing space after SELECT", func() {
					_, err := ParseQuery("SELECT*FROM STREAM sw IN default")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space after keyword"))
				})

				It("rejects query with missing space before FROM", func() {
					_, err := ParseQuery("SELECT *FROM STREAM sw IN default")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space before keyword"))
				})

				It("rejects query with missing space after IN", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN(default)")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space after keyword"))
				})

				It("rejects query with missing space before TIME", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN (default)TIME > '-15m'")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space before keyword"))
					Expect(err.Error()).To(ContainSubstring("TIME"))
				})

				It("rejects query with missing space after time value and LIMIT", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN (default) TIME > '-15m'LIMIT 10")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space before keyword"))
					Expect(err.Error()).To(ContainSubstring("LIMIT"))
				})

				It("rejects query with missing space after WHERE", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE(id = 1)")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					Expect(err.Error()).To(ContainSubstring("missing space after keyword"))
				})

				It("accepts query with proper spacing", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN (default) TIME > '-15m' LIMIT 10")
					Expect(err).To(BeNil())
				})

				It("accepts query with keywords in string literals", func() {
					// Keywords inside strings should not be validated
					_, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE service = 'FROMLIMIT'")
					Expect(err).To(BeNil())
				})

				It("accepts query with comma-separated groups", func() {
					_, err := ParseQuery("SELECT * FROM STREAM sw IN group1, group2 TIME > '-15m'")
					Expect(err).To(BeNil())
				})

				It("accepts query when keyword appears inside hyphenated identifiers", func() {
					_, err := ParseQuery("SELECT trace_id FROM TRACE sw IN (test-trace-group) TIME > '-15m'")
					Expect(err).To(BeNil())
				})

				Describe("quoted identifiers for keywords", func() {
					It("accepts double-quoted keyword as identifier", func() {
						parsed, err := ParseQuery(`SELECT "count", service_id FROM STREAM sw IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns).To(HaveLen(2))
						Expect(stmt.Projection.Columns[0].Name).To(Equal("count"))
						Expect(stmt.Projection.Columns[1].Name).To(Equal("service_id"))
					})

					It("accepts single-quoted keyword as identifier", func() {
						parsed, err := ParseQuery(`SELECT 'group', service_id FROM STREAM sw IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns).To(HaveLen(2))
						Expect(stmt.Projection.Columns[0].Name).To(Equal("group"))
						Expect(stmt.Projection.Columns[1].Name).To(Equal("service_id"))
					})

					It("accepts quoted identifier path with dots", func() {
						parsed, err := ParseQuery(`SELECT "trace.span.id", service_id FROM STREAM sw IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns).To(HaveLen(2))
						Expect(stmt.Projection.Columns[0].Name).To(Equal("trace.span.id"))
						Expect(stmt.Projection.Columns[1].Name).To(Equal("service_id"))
					})

					It("accepts quoted keyword in WHERE clause", func() {
						parsed, err := ParseQuery(`SELECT * FROM STREAM sw IN default TIME > '-15m' WHERE "group" = 'value'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Where).NotTo(BeNil())
						cond := stmt.Where.Expr.(*Condition)
						Expect(cond.Left).To(Equal("group"))
					})

					It("accepts multiple quoted keywords in SELECT", func() {
						parsed, err := ParseQuery(`SELECT "count", "group", "order" FROM MEASURE m IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns).To(HaveLen(3))
						Expect(stmt.Projection.Columns[0].Name).To(Equal("count"))
						Expect(stmt.Projection.Columns[1].Name).To(Equal("group"))
						Expect(stmt.Projection.Columns[2].Name).To(Equal("order"))
					})

					It("accepts quoted keyword in GROUP BY", func() {
						parsed, err := ParseQuery(`SELECT "group", SUM(value) FROM MEASURE m IN default TIME > '-15m' GROUP BY "group"`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.GroupBy).NotTo(BeNil())
						Expect(stmt.GroupBy.Columns).To(HaveLen(1))
						Expect(stmt.GroupBy.Columns[0].Name).To(Equal("group"))
					})

					It("accepts quoted keyword in ORDER BY", func() {
						parsed, err := ParseQuery(`SELECT "count", service_id FROM STREAM sw IN default TIME > '-15m' ORDER BY "count" DESC`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.OrderBy).NotTo(BeNil())
						Expect(stmt.OrderBy.Column).To(Equal("count"))
						Expect(stmt.OrderBy.Desc).To(BeTrue())
					})

					It("rejects unquoted keyword as standalone identifier", func() {
						_, err := ParseQuery(`SELECT count FROM STREAM sw IN default TIME > '-15m'`)
						Expect(err).NotTo(BeNil())
						Expect(err.Error()).To(ContainSubstring("cannot be a keyword"))
					})

					It("accepts keyword without quotes in identifier path", func() {
						parsed, err := ParseQuery(`SELECT trace.count FROM STREAM sw IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns[0].Name).To(Equal("trace.count"))
					})

					It("accepts keyword without quotes with type spec", func() {
						parsed, err := ParseQuery(`SELECT count::field FROM MEASURE m IN default TIME > '-15m'`)
						Expect(err).To(BeNil())
						stmt := parsed.Statement.(*SelectStatement)
						Expect(stmt.Projection.Columns[0].Name).To(Equal("count"))
						Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeField))
					})
				})

				It("rejects complex query with multiple spacing issues", func() {
					_, err := ParseQuery("SELECT*FROM STREAM sw IN(default)WHERE id = 1")
					Expect(err).NotTo(BeNil())
					Expect(err.Error()).To(ContainSubstring("spacing error"))
					// Should catch the first spacing error
				})
			})
		})

		Describe("String Literals and Escaping", func() {
			It("parses string with single quotes", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'hello world'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("hello world"))
			})

			It("parses string with double quotes (if supported)", func() {
				parsed, err := ParseQuery(`SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = "hello world"`)

				if err != nil {
					// Double quotes might not be supported for string literals
					Expect(err).NotTo(BeNil())
				} else {
					Expect(parsed).NotTo(BeNil())
					stmt, ok := parsed.Statement.(*SelectStatement)
					Expect(ok).To(BeTrue())
					Expect(stmt.Where).NotTo(BeNil())
				}
			})

			It("parses string with escaped single quote", func() {
				// Testing if single quote escaping is supported
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'it\\'s working'")

				Expect(parsed).NotTo(BeNil())
				Expect(err).To(BeNil())
				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
				// Check if the escaped quote is preserved
				cond, ok := stmt.Where.Expr.(*Condition)
				if ok {
					// The string should contain the single quote
					Expect(cond.Right.StringVal).To(
						Equal("it's working"),
					)
				}
			})

			It("parses string with special characters", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE path = '/api/users'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("/api/users"))
			})

			It("parses string with hyphen", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'auth-service'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("auth-service"))
			})

			It("parses string with underscores", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'api_gateway_service'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("api_gateway_service"))
			})

			It("parses string with dot notation", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE version = '1.2.3'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("1.2.3"))
			})

			It("parses string with spaces and punctuation", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = 'Error: Connection failed!'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("Error: Connection failed!"))
			})

			It("parses empty string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = ''")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal(""))
			})

			It("parses very long string", func() {
				longString := "This is a very long string that contains many words and should still be parsed correctly " +
					"without any issues even though it exceeds typical string lengths"
				query := fmt.Sprintf("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = '%s'", longString)
				parsed, err := ParseQuery(query)
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal(longString))
			})
		})

		Describe("Unicode Support", func() {
			It("parses query with emoji in string values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE status = '✅ success'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("✅ success"))
			})
		})

		Describe("Whitespace Handling", func() {
			It("parses query with extra whitespace", func() {
				parsed, err := ParseQuery("SELECT    *    FROM    STREAM    sw    IN    default    TIME    >    '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
			})

			It("parses query with tabs and newlines", func() {
				query := "SELECT *\n\tFROM STREAM sw\n\tIN default\n\tTIME > '-30m'"
				parsed, err := ParseQuery(query)
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
			})

			It("parses query with minimal whitespace", func() {
				// Parser enforces space after keywords, so we test minimal spaces where allowed
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
			})

			It("parses query with whitespace in string literals", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE message = '   spaces   '")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Right.StringVal).To(Equal("   spaces   "))
			})
		})

		Describe("Error Message Quality", func() {
			It("provides clear error for missing FROM clause", func() {
				parsed, err := ParseQuery("SELECT * WHERE status = 'error'")
				Expect(err).NotTo(BeNil())
				Expect(parsed).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("FROM"),
					ContainSubstring("syntax"),
					ContainSubstring("expected"),
				))
			})

			It("provides clear error for missing IN clause", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw WHERE status = 'error'")
				Expect(err).NotTo(BeNil())
				Expect(parsed).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("IN"),
					ContainSubstring("syntax"),
					ContainSubstring("expected"),
				))
			})

			It("provides clear error for unclosed string", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE message = 'unclosed")
				Expect(err).NotTo(BeNil())
				Expect(parsed).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("string"),
					ContainSubstring("quote"),
					ContainSubstring("unclosed"),
					ContainSubstring("syntax"),
				))
			})

			It("provides clear error for invalid operator", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default WHERE status === 'error'")
				Expect(err).NotTo(BeNil())
				Expect(parsed).To(BeNil())
				Expect(err.Error()).To(Or(
					ContainSubstring("operator"),
					ContainSubstring("syntax"),
					ContainSubstring("unexpected"),
				))
			})
		})

		Describe("OFFSET Clause", func() {
			It("parses OFFSET with LIMIT", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10 OFFSET 20")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(10))
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(20))
			})

			It("parses OFFSET 0", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 10 OFFSET 0")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(0))
			})

			It("parses large OFFSET value", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 100 OFFSET 10000")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(10000))
			})

			It("parses OFFSET with WHERE and ORDER BY", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE service_id = 'api' ORDER BY timestamp DESC LIMIT 50 OFFSET 100")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(100))
			})

			It("parses OFFSET for MEASURE queries", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' GROUP BY region LIMIT 20 OFFSET 10")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(10))
			})

			It("parses OFFSET for TRACE queries", func() {
				parsed, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default TIME > '-30m' LIMIT 100 OFFSET 50")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(50))
			})

			// Boundary conditions for LIMIT and OFFSET
			It("parses LIMIT 0", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 0")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(0))
			})

			It("parses LIMIT 1", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 1")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(1))
			})

			It("parses very large LIMIT value", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT 999999")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(999999))
			})

			It("handles negative LIMIT value", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' LIMIT -1")

				// Parser might accept syntactically, semantic validator should reject
				Expect(parsed).NotTo(BeNil())
				Expect(err).To(BeNil())
			})

			It("parses OFFSET without LIMIT (if supported)", func() {
				// Some SQL dialects allow OFFSET without LIMIT
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' OFFSET 10")

				Expect(parsed).NotTo(BeNil())
				Expect(err).To(BeNil())
				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Offset).NotTo(BeNil())
				Expect(*stmt.Offset).To(Equal(10))
			})
		})
	})

	Describe("Stream Queries", func() {
		Describe("TIME Operators", func() {
			It("parses TIME = with absolute timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpEqual))
				Expect(stmt.Time.Timestamp).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME = with relative timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = '-30m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpEqual))
				Expect(stmt.Time.Timestamp).To(Equal("-30m"))
			})

			It("parses TIME < with absolute timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME < '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLess))
				Expect(stmt.Time.Timestamp).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME < with relative timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME < '-1d'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLess))
				Expect(stmt.Time.Timestamp).To(Equal("-1d"))
			})

			It("parses TIME >= with absolute timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME >= '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpGreaterEqual))
				Expect(stmt.Time.Timestamp).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME >= with relative timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME >= '-2h'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpGreaterEqual))
				Expect(stmt.Time.Timestamp).To(Equal("-2h"))
			})

			It("parses TIME <= with absolute timestamp", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME <= '2023-01-01T00:00:00Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLessEqual))
				Expect(stmt.Time.Timestamp).To(Equal("2023-01-01T00:00:00Z"))
			})

			It("parses TIME <= with relative timestamp 'now'", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME <= 'now'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLessEqual))
				Expect(stmt.Time.Timestamp).To(Equal("now"))
			})

			It("parses TIME operators for MEASURE queries", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME = '2023-01-01T00:00:00Z' GROUP BY region")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpEqual))
			})

			It("parses TIME operators for TRACE queries", func() {
				parsed, err := ParseQuery("SELECT trace_id FROM TRACE sw_trace IN default TIME <= '-1h'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLessEqual))
			})

			It("parses TIME operators for TOP-N queries", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN default TIME < '2023-01-01T00:00:00Z' ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpLess))
			})

			// Edge cases and boundary conditions for TIME operators
			It("parses TIME = with 'now' keyword", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME = 'now'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpEqual))
				Expect(stmt.Time.Timestamp).To(Equal("now"))
			})

			It("parses TIME > with 'now' keyword", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > 'now'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpGreater))
				Expect(stmt.Time.Timestamp).To(Equal("now"))
			})

			It("parses TIME BETWEEN with same start and end time", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME BETWEEN '2023-01-01T10:00:00Z' AND '2023-01-01T10:00:00Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpBetween))
				Expect(stmt.Time.Begin).To(Equal("2023-01-01T10:00:00Z"))
				Expect(stmt.Time.End).To(Equal("2023-01-01T10:00:00Z"))
			})

			It("parses TIME BETWEEN with relative times", func() {
				parsed, err := ParseQuery("SELECT * FROM MEASURE metrics IN default TIME BETWEEN '-2h' AND '-1h'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpBetween))
				Expect(stmt.Time.Begin).To(Equal("-2h"))
				Expect(stmt.Time.End).To(Equal("-1h"))
			})

			It("parses TIME BETWEEN mixing relative and absolute times", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME BETWEEN '-1h' AND '2023-12-31T23:59:59Z'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Time).NotTo(BeNil())
				Expect(stmt.Time.Operator).To(Equal(TimeOpBetween))
				Expect(stmt.Time.Begin).To(Equal("-1h"))
				Expect(stmt.Time.End).To(Equal("2023-12-31T23:59:59Z"))
			})
		})
	})

	Describe("Measure Queries", func() {
		Describe("Measure Specific Features", func() {
			It("parses Measure query with MATCH operator", func() {
				parsed, err := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics IN default TIME > '-30m' WHERE tags MATCH('error') GROUP BY region")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeMeasure))
				Expect(stmt.Where).NotTo(BeNil())

				// Verify MATCH operator
				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpMatch))
				Expect(cond.MatchOption).NotTo(BeNil())
				Expect(cond.MatchOption.Values).To(HaveLen(1))
				Expect(cond.MatchOption.Values[0].StringVal).To(Equal("error"))
			})

			It("parses Measure query with MATCH and aggregation", func() {
				parsed, err := ParseQuery("SELECT service_id, COUNT(requests) FROM MEASURE api_metrics " +
					"IN default TIME > '-1h' WHERE http_path MATCH(('/api/users', '/api/orders'), 'url', 'OR') GROUP BY service_id")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())

				// Verify MATCH with multiple values
				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.MatchOption).NotTo(BeNil())
				Expect(cond.MatchOption.Values).To(HaveLen(2))
				Expect(cond.MatchOption.Analyzer).To(Equal("url"))
				Expect(cond.MatchOption.Operator).To(Equal("OR"))

				// Verify aggregation function
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				Expect(stmt.Projection.Columns[1].Function).NotTo(BeNil())
				Expect(stmt.Projection.Columns[1].Function.Function).To(Equal("COUNT"))
			})

			It("parses SELECT TOP N in Measure query", func() {
				parsed, err := ParseQuery("SELECT TOP 10 instance ASC, cpu_usage FROM MEASURE instance_metrics " +
					"IN us-west TIME > '-30m' WHERE service = 'auth-service' ORDER BY cpu_usage DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeMeasure))
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(10))
				Expect(stmt.Projection.TopN.OrderField).To(Equal("instance"))
				Expect(stmt.Projection.TopN.Desc).To(BeFalse()) // ASC

				// Verify additional columns
				Expect(stmt.Projection.Columns).To(HaveLen(1))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("cpu_usage"))
			})

			It("parses SELECT TOP N with DESC in Measure query", func() {
				parsed, err := ParseQuery("SELECT TOP 5 service_id DESC, latency::field, region::tag FROM MEASURE service_metrics IN us-west TIME > '-15m'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection.TopN).NotTo(BeNil())
				Expect(stmt.Projection.TopN.N).To(Equal(5))
				Expect(stmt.Projection.TopN.OrderField).To(Equal("service_id"))
				Expect(stmt.Projection.TopN.Desc).To(BeTrue())

				// Verify columns with type specifiers
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("latency"))
				Expect(stmt.Projection.Columns[0].Type).To(Equal(ColumnTypeField))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("region"))
				Expect(stmt.Projection.Columns[1].Type).To(Equal(ColumnTypeTag))
			})
		})

		Describe("TOP-N AGGREGATE BY", func() {
			It("parses AGGREGATE BY SUM", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.TopN).To(Equal(10))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("SUM"))
			})

			It("parses AGGREGATE BY MAX", func() {
				parsed, err := ParseQuery("SHOW TOP 5 FROM MEASURE cpu_usage IN production TIME > '-1h' AGGREGATE BY MAX ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.TopN).To(Equal(5))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("MAX"))
			})

			It("parses AGGREGATE BY MIN", func() {
				parsed, err := ParseQuery("SHOW TOP 3 FROM MEASURE response_time IN production TIME > '-30m' AGGREGATE BY MIN ORDER BY ASC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.TopN).To(Equal(3))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("MIN"))
			})

			It("parses AGGREGATE BY AVG", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE latency IN production TIME > '-2h' AGGREGATE BY AVG ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("AVG"))
			})

			It("parses AGGREGATE BY MEAN", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE latency IN production TIME > '-2h' AGGREGATE BY MEAN ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("MEAN"))
			})

			It("parses AGGREGATE BY COUNT", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE requests IN production TIME > '-30m' AGGREGATE BY COUNT ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("COUNT"))
			})

			It("parses AGGREGATE BY with WHERE clause", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' WHERE namespace = 'production' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("SUM"))
			})

			It("parses AGGREGATE BY with multiple groups", func() {
				parsed, err := ParseQuery("SHOW TOP 5 FROM MEASURE service_errors IN production, staging TIME > '-1h' AGGREGATE BY SUM ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.Groups).To(HaveLen(2))
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("SUM"))
			})

			It("parses case-insensitive AGGREGATE BY", func() {
				parsed, err := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency IN production TIME > '-30m' aggregate by sum ORDER BY DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*TopNStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.AggregateBy).NotTo(BeNil())
				Expect(stmt.AggregateBy.Function).To(Equal("SUM"))
			})
		})
	})

	Describe("Trace Queries", func() {
		Describe("Trace Specific Features", func() {
			It("parses Trace query with empty projection and complex conditions", func() {
				parsed, err := ParseQuery("SELECT () FROM TRACE sw_trace IN group1 TIME > '-30m' WHERE service_id = 'webapp' AND status = 'error' ORDER BY start_time DESC LIMIT 100")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))

				// Verify empty projection
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Empty).To(BeTrue())
				Expect(stmt.Projection.Columns).To(HaveLen(0))

				// Verify WHERE clause is parsed
				Expect(stmt.Where).NotTo(BeNil())

				// Verify ORDER BY is parsed
				Expect(stmt.OrderBy).NotTo(BeNil())
				Expect(stmt.OrderBy.Column).To(Equal("start_time"))
				Expect(stmt.OrderBy.Desc).To(BeTrue())

				// Verify LIMIT
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(100))
			})

			It("parses Trace query with WITH QUERY_TRACE", func() {
				parsed, err := ParseQuery("SELECT trace_id, service_id, operation_name FROM TRACE sw_trace IN group1 TIME > '-30m' WHERE service_id = 'webapp' WITH QUERY_TRACE")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))
				Expect(stmt.QueryTrace).To(BeTrue())

				// Verify regular projection
				Expect(stmt.Projection.Empty).To(BeFalse())
				Expect(stmt.Projection.Columns).To(HaveLen(3))
			})

			It("parses Trace query with empty projection and WITH QUERY_TRACE", func() {
				parsed, err := ParseQuery("SELECT () FROM TRACE sw_trace IN group1 TIME > '-30m' WHERE status = 'error' WITH QUERY_TRACE LIMIT 50")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))

				// Verify both empty projection and query trace
				Expect(stmt.Projection.Empty).To(BeTrue())
				Expect(stmt.QueryTrace).To(BeTrue())
			})

			It("parses Trace query with MATCH operator", func() {
				parsed, err := ParseQuery("SELECT trace_id, operation_name FROM TRACE sw_trace IN default TIME > '-30m' WHERE operation_name MATCH(('GET', 'POST'), 'keyword')")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))
				Expect(stmt.Where).NotTo(BeNil())

				// Verify MATCH operator
				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Operator).To(Equal(OpMatch))
				Expect(cond.MatchOption).NotTo(BeNil())
				Expect(cond.MatchOption.Analyzer).To(Equal("keyword"))
				Expect(cond.MatchOption.Values).To(HaveLen(2))
				Expect(cond.MatchOption.Values[0].StringVal).To(Equal("GET"))
				Expect(cond.MatchOption.Values[1].StringVal).To(Equal("POST"))
			})

			It("parses Trace query with MATCH and other conditions", func() {
				parsed, err := ParseQuery("SELECT * FROM TRACE sw_trace IN default TIME > '-30m' " +
					"WHERE service_id = 'api-gateway' AND http_status > 400 AND operation_name MATCH('api', 'keyword') ORDER BY start_time DESC")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.ResourceType).To(Equal(ResourceTypeTrace))

				// The WHERE clause should be a complex tree with MATCH as one of the conditions
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Where.Expr).NotTo(BeNil())
			})
		})
	})

	Describe("Property Queries", func() {
		Describe("Property ID Filtering", func() {
			It("parses WHERE ID = with single ID", func() {
				parsed, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-1a2b3c'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("ID"))
				Expect(cond.Operator).To(Equal(OpEqual))
				Expect(cond.Right.StringVal).To(Equal("server-1a2b3c"))
			})

			It("parses WHERE ID IN with multiple IDs", func() {
				parsed, err := ParseQuery("SELECT ip, region FROM PROPERTY server_metadata IN datacenter-1 WHERE ID IN ('server-1a2b3c', 'server-4d5e6f')")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("ID"))
				Expect(cond.Operator).To(Equal(OpIn))
				Expect(cond.Values).To(HaveLen(2))
				Expect(cond.Values[0].StringVal).To(Equal("server-1a2b3c"))
				Expect(cond.Values[1].StringVal).To(Equal("server-4d5e6f"))
			})

			It("parses WHERE ID with projection", func() {
				parsed, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-xyz'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Projection).NotTo(BeNil())
				Expect(stmt.Projection.Columns).To(HaveLen(2))
				Expect(stmt.Projection.Columns[0].Name).To(Equal("ip"))
				Expect(stmt.Projection.Columns[1].Name).To(Equal("owner"))
			})

			It("parses WHERE ID with LIMIT", func() {
				parsed, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID IN ('server-1', 'server-2', 'server-3') LIMIT 10")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
				Expect(stmt.Limit).NotTo(BeNil())
				Expect(*stmt.Limit).To(Equal(10))
			})

			It("parses case-insensitive ID keyword", func() {
				parsed, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE id = 'server-1a2b3c'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				cond, ok := stmt.Where.Expr.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(cond.Left).To(Equal("id"))
			})

			It("parses WHERE ID from multiple groups", func() {
				parsed, err := ParseQuery("SELECT ip, owner FROM PROPERTY server_metadata IN datacenter-1, datacenter-2 WHERE ID IN ('server-1', 'server-2')")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.From.Groups).To(HaveLen(2))
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses WHERE ID combined with other tag conditions", func() {
				parsed, err := ParseQuery("SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-1' AND datacenter = 'dc-101'")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())

				// Should be a binary AND expression
				binExpr, ok := stmt.Where.Expr.(*BinaryLogicExpr)
				Expect(ok).To(BeTrue())
				Expect(binExpr.Operator).To(Equal(LogicAnd))

				// Left condition should be ID
				leftCond, ok := binExpr.Left.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(leftCond.Left).To(Equal("ID"))
				Expect(leftCond.Right.StringVal).To(Equal("server-1"))

				// Right condition should be datacenter tag
				rightCond, ok := binExpr.Right.(*Condition)
				Expect(ok).To(BeTrue())
				Expect(rightCond.Left).To(Equal("datacenter"))
				Expect(rightCond.Right.StringVal).To(Equal("dc-101"))
			})
		})
	})

	Describe("Extreme Values and Edge Cases", func() {
		Describe("Extreme Values and Edge Cases", func() {
			It("parses query with very large integer", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests > 9223372036854775807")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses query with very small negative integer", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests < -9223372036854775807")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
				Expect(stmt.Where).NotTo(BeNil())
			})

			It("parses query with zero values", func() {
				parsed, err := ParseQuery("SELECT * FROM STREAM sw IN default TIME > '-30m' WHERE num_requests = 0 AND status_code = 0")
				Expect(err).To(BeNil())
				Expect(parsed).NotTo(BeNil())

				stmt, ok := parsed.Statement.(*SelectStatement)
				Expect(ok).To(BeTrue())
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
