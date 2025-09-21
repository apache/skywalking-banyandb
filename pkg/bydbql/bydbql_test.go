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
// Unless required by Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bydbql_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

var _ = Describe("Lexer", func() {
	DescribeTable("tokenizes queries correctly",
		func(input string, expected []TokenType) {
			lexer := NewLexer(input)
			tokens := lexer.GetAllTokens()

			Expect(tokens).To(HaveLen(len(expected)))

			for i, expectedToken := range expected {
				Expect(tokens[i].Type).To(Equal(expectedToken),
					"token %d: expected %s, got %s", i, expectedToken.String(), tokens[i].Type.String())
			}
		},
		Entry("basic SELECT query",
			"SELECT * FROM STREAM sw",
			[]TokenType{TokenSelect, TokenStar, TokenFrom, TokenStream, TokenIdentifier, TokenEOF}),
		Entry("SELECT with WHERE clause",
			"SELECT trace_id FROM STREAM sw WHERE service_id = 'webapp'",
			[]TokenType{
				TokenSelect, TokenIdentifier, TokenFrom, TokenStream, TokenIdentifier,
				TokenWhere, TokenIdentifier, TokenEqual, TokenString, TokenEOF,
			}),
		Entry("TOP N query",
			"SHOW TOP 10 FROM MEASURE service_latency",
			[]TokenType{TokenShow, TokenTop, TokenInteger, TokenFrom, TokenMeasure, TokenIdentifier, TokenEOF}),
		Entry("aggregate function",
			"SELECT SUM(latency) FROM MEASURE metrics",
			[]TokenType{
				TokenSelect, TokenSum, TokenLeftParen, TokenIdentifier, TokenRightParen,
				TokenFrom, TokenMeasure, TokenIdentifier, TokenEOF,
			}),
		Entry("time range",
			"TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			[]TokenType{TokenTime, TokenBetween, TokenString, TokenAnd, TokenString, TokenEOF}),
		Entry("column type disambiguator",
			"SELECT status::tag, status::field FROM MEASURE metrics",
			[]TokenType{
				TokenSelect, TokenIdentifier, TokenDoubleColon, TokenIdentifier,
				TokenComma, TokenIdentifier, TokenDoubleColon, TokenIdentifier,
				TokenFrom, TokenMeasure, TokenIdentifier, TokenEOF,
			}),
	)
})

var _ = Describe("Parser", func() {
	Describe("valid queries", func() {
		It("parses simple SELECT *", func() {
			parsed, errors := ParseQuery("SELECT *")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.Projection).NotTo(BeNil())
			Expect(stmt.Projection.All).To(BeTrue())
		})

		It("parses SELECT with FROM clause", func() {
			parsed, errors := ParseQuery("SELECT * FROM STREAM sw")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.From).NotTo(BeNil())
			Expect(stmt.From.ResourceType).To(Equal(ResourceTypeStream))
			Expect(stmt.From.ResourceName).To(Equal("sw"))
		})

		It("parses SELECT with WHERE clause", func() {
			parsed, errors := ParseQuery("SELECT trace_id FROM STREAM sw WHERE service_id = 'webapp'")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.Where).NotTo(BeNil())
			Expect(stmt.Where.Conditions).To(HaveLen(1))
			Expect(stmt.Where.Conditions[0].Left).To(Equal("service_id"))
		})

		It("parses SELECT with TIME condition", func() {
			parsed, errors := ParseQuery("SELECT * FROM STREAM sw TIME > '-30m'")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.Time).NotTo(BeNil())
			Expect(stmt.Time.Operator).To(Equal(TimeOpGreater))
		})

		It("parses SELECT with GROUP BY", func() {
			parsed, errors := ParseQuery("SELECT region, SUM(latency) FROM MEASURE metrics GROUP BY region")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.GroupBy).NotTo(BeNil())
			Expect(stmt.GroupBy.Columns).To(HaveLen(1))
			Expect(stmt.GroupBy.Columns[0]).To(Equal("region"))
		})

		It("parses SELECT with both TIME BETWEEN and WHERE clause", func() {
			parsed, errors := ParseQuery("SELECT * FROM STREAM sw TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z' WHERE service_id = 'webapp' AND status = 200")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())

			// Verify TIME BETWEEN is parsed
			Expect(stmt.Time).NotTo(BeNil())
			Expect(stmt.Time.Operator).To(Equal(TimeOpBetween))
			Expect(stmt.Time.Begin).To(Equal("2023-01-01T00:00:00Z"))
			Expect(stmt.Time.End).To(Equal("2023-01-02T00:00:00Z"))

			// Verify WHERE clause is parsed
			Expect(stmt.Where).NotTo(BeNil())
			Expect(stmt.Where.Conditions).To(HaveLen(2))
			Expect(stmt.Where.Conditions[0].Left).To(Equal("service_id"))
			Expect(stmt.Where.Conditions[0].Right.StringVal).To(Equal("webapp"))
			Expect(stmt.Where.Conditions[1].Left).To(Equal("status"))
			Expect(stmt.Where.Conditions[1].Right.Integer).To(Equal(int64(200)))
		})

		It("parses TOP N statement", func() {
			parsed, errors := ParseQuery("SHOW TOP 10 FROM MEASURE service_latency ORDER BY value DESC")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*TopNStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.TopN).To(Equal(10))
			Expect(stmt.OrderBy).NotTo(BeNil())
			Expect(stmt.OrderBy.Desc).To(BeTrue())
		})

		It("parses empty projection for traces", func() {
			parsed, errors := ParseQuery("SELECT () FROM TRACE sw_trace")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.Projection).NotTo(BeNil())
			Expect(stmt.Projection.Empty).To(BeTrue())
		})

		It("parses WITH QUERY_TRACE", func() {
			parsed, errors := ParseQuery("SELECT * FROM STREAM sw WITH QUERY_TRACE")
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			stmt, ok := parsed.Statement.(*SelectStatement)
			Expect(ok).To(BeTrue())
			Expect(stmt.QueryTrace).To(BeTrue())
		})
	})

	Describe("invalid queries", func() {
		It("handles invalid syntax", func() {
			parsed, errors := ParseQuery("SELECT FROM")
			Expect(errors).NotTo(BeEmpty())
			Expect(parsed).To(BeNil())
		})
	})
})

var _ = Describe("Translator", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			DefaultGroup:        "default",
			DefaultResourceName: "test_resource",
			DefaultResourceType: ResourceTypeStream,
			CurrentTime:         time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

	DescribeTable("translates queries correctly",
		func(input string, validateFunc func(map[string]any) bool) {
			parsed, errors := ParseQuery(input)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			translator := NewTranslator(context)
			data, err := translator.TranslateToMap(parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(validateFunc(data)).To(BeTrue(), "validation failed for data: %+v", data)
		},
		Entry("simple stream query",
			"SELECT * FROM STREAM sw",
			func(data map[string]any) bool {
				return data["name"] == "sw"
			}),
		Entry("measure query with aggregation",
			"SELECT region, SUM(latency) FROM MEASURE metrics GROUP BY region",
			func(data map[string]any) bool {
				agg, ok := data["agg"].(map[string]any)
				return ok && agg["function"] == "SUM" && agg["field_name"] == "latency"
			}),
		Entry("time range query",
			"SELECT * FROM STREAM sw TIME BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			func(data map[string]any) bool {
				timeRange, ok := data["timeRange"].(map[string]any)
				return ok && timeRange["begin"] == "2023-01-01T00:00:00Z" &&
					timeRange["end"] == "2023-01-02T00:00:00Z"
			}),
		Entry("relative time query",
			"SELECT * FROM STREAM sw TIME > '-30m'",
			func(data map[string]any) bool {
				timeRange, ok := data["timeRange"].(map[string]any)
				return ok && timeRange["begin"] != nil && timeRange["end"] != nil
			}),
		Entry("WHERE clause with criteria",
			"SELECT * FROM STREAM sw WHERE service_id = 'webapp'",
			func(data map[string]any) bool {
				criteria, ok := data["criteria"].(map[string]any)
				if !ok {
					return false
				}
				condition, ok := criteria["condition"].(map[string]any)
				return ok && condition["name"] == "service_id" && condition["op"] == "BINARY_OP_EQ"
			}),
		Entry("TOP N query",
			"SHOW TOP 10 FROM MEASURE service_latency ORDER BY value DESC",
			func(data map[string]any) bool {
				topN := data["top_n"]
				fieldValueSort := data["field_value_sort"]
				return topN == 10 && fieldValueSort == "DESC"
			}),
		Entry("property query with IDs",
			"SELECT * FROM PROPERTY metadata WHERE ID = 'id1' OR ID = 'id2'",
			func(data map[string]any) bool {
				criteria, ok := data["criteria"].(map[string]any)
				return ok && criteria != nil
			}),
		Entry("query trace enabled",
			"SELECT * FROM STREAM sw WITH QUERY_TRACE",
			func(data map[string]any) bool {
				return data["trace"] == true
			}),
		Entry("stream query with projection tagFamilies format",
			"SELECT trace_id FROM STREAM sw",
			func(data map[string]any) bool {
				// Verify that projection is translated to tagFamilies format with snake_case
				projection, ok := data["projection"].(map[string]any)
				if !ok {
					return false
				}
				tagFamiliesRaw := projection["tagFamilies"]
				tagFamilies, ok := tagFamiliesRaw.([]interface{})
				if !ok || len(tagFamilies) == 0 {
					return false
				}
				tagFamily, ok := tagFamilies[0].(map[string]interface{})
				if !ok {
					return false
				}
				name, ok := tagFamily["name"].(string)
				if !ok || name != "searchable" {
					return false
				}
				tagsRaw := tagFamily["tags"]
				tags, ok := tagsRaw.([]interface{})
				if !ok || len(tags) == 0 {
					return false
				}
				tag, ok := tags[0].(string)
				return ok && tag == "trace_id"
			}),
	)
})

var _ = Describe("Complex Queries", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			DefaultGroup: "default",
			CurrentTime:  time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

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
		 FROM TRACE sw_trace
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
		 ORDER BY value DESC`,
	}

	for i, query := range complexQueries {
		It(fmt.Sprintf("parses and translates complex query %d", i+1), func() {
			// Parse the query
			parsed, errors := ParseQuery(query)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			// Translate to YAML
			translator := NewTranslator(context)
			yamlData, err := translator.TranslateToYAML(parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(yamlData).NotTo(BeEmpty())

			// Validate YAML structure by unmarshaling
			var result map[string]any
			err = yaml.Unmarshal(yamlData, &result)
			Expect(err).NotTo(HaveOccurred())
		})
	}
})

var _ = Describe("Error Handling", func() {
	invalidQueries := []string{
		"SELECT",                           // incomplete query
		"SELECT * FROM",                    // missing resource
		"SELECT * FROM INVALID sw",         // invalid resource type
		"SHOW TOP FROM MEASURE metrics",    // missing N
		"SELECT * WHERE service_id",        // incomplete condition
		"TIME > '2023-01-01'",              // missing SELECT
		"SELECT * FROM STREAM sw GROUP BY", // incomplete GROUP BY
	}

	for i, query := range invalidQueries {
		It(fmt.Sprintf("handles invalid query %d", i+1), func() {
			parsed, errors := ParseQuery(query)

			// Should have parsing errors
			Expect(errors).NotTo(BeEmpty(), "expected parsing errors for query: %s", query)

			// Parsed query should be nil for invalid queries
			Expect(parsed).To(BeNil(), "expected nil parsed query for invalid query: %s", query)
		})
	}
})

var _ = Describe("Time Format Parsing", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			CurrentTime: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

	DescribeTable("parses time formats correctly in queries",
		func(timeCondition string, validateFunc func(map[string]any) bool) {
			query := fmt.Sprintf("SELECT * FROM STREAM sw TIME %s", timeCondition)
			parsed, errors := ParseQuery(query)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			translator := NewTranslator(context)
			data, err := translator.TranslateToMap(parsed)
			Expect(err).NotTo(HaveOccurred())
			Expect(validateFunc(data)).To(BeTrue(), "time validation failed for data: %+v", data)
		},
		Entry("absolute time range",
			"BETWEEN '2023-01-01T10:00:00Z' AND '2023-01-01T11:00:00Z'",
			func(data map[string]any) bool {
				timeRange, ok := data["timeRange"].(map[string]any)
				return ok && timeRange["begin"] == "2023-01-01T10:00:00Z" &&
					timeRange["end"] == "2023-01-01T11:00:00Z"
			}),
		Entry("relative time condition",
			"> '-30m'",
			func(data map[string]any) bool {
				timeRange, ok := data["timeRange"].(map[string]any)
				return ok && timeRange["begin"] != nil && timeRange["end"] != nil
			}),
	)
})

var _ = Describe("Case Insensitivity", func() {
	// Test that keywords are case-insensitive
	queries := []string{
		"SELECT * FROM STREAM sw",
		"select * from stream sw",
		"Select * From Stream sw",
		"sElEcT * fRoM sTrEaM sw",
	}

	for i, query := range queries {
		It(fmt.Sprintf("parses case-insensitive query %d", i+1), func() {
			parsed, errors := ParseQuery(query)
			Expect(errors).To(BeEmpty(), "parsing errors for query '%s': %v", query, errors)
			Expect(parsed).NotTo(BeNil(), "failed to parse case-insensitive query: %s", query)
		})
	}
})

// Benchmark tests
func BenchmarkLexer(b *testing.B) {
	query := `SELECT trace_id, service_id, start_time
			  FROM STREAM sw IN (default, updated)
			  WHERE service_id = 'webapp' AND state = 1
			  ORDER BY start_time DESC
			  LIMIT 100`

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(query)
		_ = lexer.GetAllTokens()
	}
}

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

func BenchmarkTranslator(b *testing.B) {
	query := `SELECT trace_id, service_id, start_time
			  FROM STREAM sw IN (default, updated)
			  WHERE service_id = 'webapp' AND state = 1
			  ORDER BY start_time DESC
			  LIMIT 100`

	context := &QueryContext{
		CurrentTime: time.Now(),
	}

	// Parse once
	parsed, _ := ParseQuery(query)
	translator := NewTranslator(context)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = translator.TranslateToYAML(parsed)
	}
}

func BenchmarkEndToEnd(b *testing.B) {
	query := `SELECT trace_id, service_id, start_time
			  FROM STREAM sw IN (default, updated)
			  WHERE service_id = 'webapp' AND state = 1
			  ORDER BY start_time DESC
			  LIMIT 100`

	context := &QueryContext{
		CurrentTime: time.Now(),
	}

	for i := 0; i < b.N; i++ {
		_, _, _ = TranslateQuery(query, context)
	}
}

var _ = Describe("Criteria BinaryOp Tests", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			DefaultGroup:        "default",
			DefaultResourceName: "test_resource",
			DefaultResourceType: ResourceTypeStream,
			CurrentTime:         time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

	DescribeTable("translates all BinaryOp operators correctly",
		func(whereClause string, expectedOp string, validateValue func(map[string]any) bool) {
			query := fmt.Sprintf("SELECT * FROM STREAM sw WHERE %s", whereClause)
			parsed, errors := ParseQuery(query)
			Expect(errors).To(BeEmpty())
			Expect(parsed).NotTo(BeNil())

			translator := NewTranslator(context)
			data, err := translator.TranslateToMap(parsed)
			Expect(err).NotTo(HaveOccurred())

			// Extract condition from criteria
			criteria, ok := data["criteria"].(map[string]any)
			Expect(ok).To(BeTrue(), "criteria should be an object")
			Expect(criteria).NotTo(BeEmpty())

			condition, ok := criteria["condition"].(map[string]any)
			Expect(ok).To(BeTrue(), "should have condition field")

			Expect(condition["op"]).To(Equal(expectedOp))
			if validateValue != nil {
				Expect(validateValue(condition)).To(BeTrue(), "value validation failed for condition: %+v", condition)
			}
		},
		// BINARY_OP_EQ
		Entry("BINARY_OP_EQ with string",
			"service_id = 'mysql'",
			"BINARY_OP_EQ",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				str, ok := value["str"].(map[string]any)
				if !ok {
					return false
				}
				return str["value"] == "mysql" && condition["name"] == "service_id"
			}),
		Entry("BINARY_OP_EQ with integer",
			"status = 200",
			"BINARY_OP_EQ",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				intVal, ok := value["int"].(map[string]any)
				if !ok {
					return false
				}
				return intVal["value"] == "200" && condition["name"] == "status"
			}),
		// BINARY_OP_NE
		Entry("BINARY_OP_NE with string",
			"service_name != 'test-service'",
			"BINARY_OP_NE",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				str, ok := value["str"].(map[string]any)
				if !ok {
					return false
				}
				return str["value"] == "test-service" && condition["name"] == "service_name"
			}),
		// BINARY_OP_GT
		Entry("BINARY_OP_GT with integer",
			"latency > 1000",
			"BINARY_OP_GT",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				intVal, ok := value["int"].(map[string]any)
				if !ok {
					return false
				}
				return intVal["value"] == "1000" && condition["name"] == "latency"
			}),
		// BINARY_OP_LT
		Entry("BINARY_OP_LT with integer",
			"response_time < 500",
			"BINARY_OP_LT",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				intVal, ok := value["int"].(map[string]any)
				if !ok {
					return false
				}
				return intVal["value"] == "500" && condition["name"] == "response_time"
			}),
		// BINARY_OP_GE
		Entry("BINARY_OP_GE with integer",
			"status >= 10",
			"BINARY_OP_GE",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				intVal, ok := value["int"].(map[string]any)
				if !ok {
					return false
				}
				return intVal["value"] == "10" && condition["name"] == "status"
			}),
		// BINARY_OP_LE
		Entry("BINARY_OP_LE with integer",
			"duration <= 3000",
			"BINARY_OP_LE",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				intVal, ok := value["int"].(map[string]any)
				if !ok {
					return false
				}
				return intVal["value"] == "3000" && condition["name"] == "duration"
			}),
		// BINARY_OP_MATCH
		Entry("BINARY_OP_MATCH with string",
			"service_id MATCH 'mysql'",
			"BINARY_OP_MATCH",
			func(condition map[string]any) bool {
				value, ok := condition["value"].(map[string]any)
				if !ok {
					return false
				}
				str, ok := value["str"].(map[string]any)
				if !ok {
					return false
				}
				return str["value"] == "mysql" && condition["name"] == "service_id"
			}),
	)
})

var _ = Describe("Criteria LogicalExpression Tests", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			DefaultGroup:        "default",
			DefaultResourceName: "test_resource",
			DefaultResourceType: ResourceTypeStream,
			CurrentTime:         time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

	It("translates AND logical expression correctly", func() {
		query := "SELECT * FROM STREAM sw WHERE service_id = 'webapp' AND status = 200"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		// Extract criteria
		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		// Should have a logical expression
		le, ok := criteria["le"].(map[string]any)
		Expect(ok).To(BeTrue(), "should have logical expression")
		Expect(le["op"]).To(Equal("LOGICAL_OP_AND"))

		// Check left condition
		left, ok := le["left"].(map[string]any)
		Expect(ok).To(BeTrue())
		leftCond, ok := left["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(leftCond["name"]).To(Equal("service_id"))
		Expect(leftCond["op"]).To(Equal("BINARY_OP_EQ"))

		// Check right condition
		right, ok := le["right"].(map[string]any)
		Expect(ok).To(BeTrue())
		rightCond, ok := right["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(rightCond["name"]).To(Equal("status"))
		Expect(rightCond["op"]).To(Equal("BINARY_OP_EQ"))
	})

	It("translates multiple AND conditions correctly", func() {
		query := "SELECT * FROM STREAM sw WHERE service_id = 'webapp' AND status = 200 AND region = 'us-west'"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		// Extract criteria
		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		// Should have nested logical expressions
		le, ok := criteria["le"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(le["op"]).To(Equal("LOGICAL_OP_AND"))

		// The structure should be: ((service_id = 'webapp' AND status = 200) AND region = 'us-west')
		// Check that left is another logical expression
		left, ok := le["left"].(map[string]any)
		Expect(ok).To(BeTrue())
		innerLe, ok := left["le"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(innerLe["op"]).To(Equal("LOGICAL_OP_AND"))

		// Check the rightmost condition
		right, ok := le["right"].(map[string]any)
		Expect(ok).To(BeTrue())
		rightCond, ok := right["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(rightCond["name"]).To(Equal("region"))
		Expect(rightCond["op"]).To(Equal("BINARY_OP_EQ"))
	})

	It("translates complex nested criteria with different operators", func() {
		query := "SELECT * FROM STREAM sw WHERE service_id = 'webapp' AND latency > 1000 AND status != 500"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		// Verify structure exists
		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		// Verify it has nested logical expressions
		_, hasLe := criteria["le"]
		Expect(hasLe).To(BeTrue())
	})

	It("translates MATCH operator correctly", func() {
		query := "SELECT * FROM STREAM sw WHERE description MATCH 'error occurred'"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		condition, ok := criteria["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(condition["name"]).To(Equal("description"))
		Expect(condition["op"]).To(Equal("BINARY_OP_MATCH"))

		value, ok := condition["value"].(map[string]any)
		Expect(ok).To(BeTrue())
		str, ok := value["str"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(str["value"]).To(Equal("error occurred"))
	})

	It("translates mixed string and integer conditions", func() {
		query := "SELECT * FROM STREAM sw WHERE service_id = 'webapp' AND status >= 400 AND latency < 5000"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		yamlData, err := translator.TranslateToYAML(parsed)
		Expect(err).NotTo(HaveOccurred())
		Expect(yamlData).NotTo(BeEmpty())

		// Verify YAML structure by unmarshaling
		var result map[string]any
		err = yaml.Unmarshal(yamlData, &result)
		Expect(err).NotTo(HaveOccurred())
		Expect(result["criteria"]).NotTo(BeNil())
	})
})

var _ = Describe("Criteria Proto Format Verification", func() {
	var context *QueryContext

	BeforeEach(func() {
		context = &QueryContext{
			DefaultGroup:        "default",
			DefaultResourceName: "test_resource",
			DefaultResourceType: ResourceTypeStream,
			CurrentTime:         time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
	})

	It("generates criteria in correct proto format for MATCH operator", func() {
		query := "SELECT * FROM STREAM sw WHERE database_instance MATCH 'mysql'"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		yamlData, err := translator.TranslateToYAML(parsed)
		Expect(err).NotTo(HaveOccurred())

		// Print the YAML output to verify the format
		fmt.Printf("YAML Output for MATCH query:\n%s\n", string(yamlData))

		// Verify the structure matches the expected proto format
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		condition, ok := criteria["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(condition["name"]).To(Equal("database_instance"))
		Expect(condition["op"]).To(Equal("BINARY_OP_MATCH"))

		// Verify value structure
		value, ok := condition["value"].(map[string]any)
		Expect(ok).To(BeTrue())
		str, ok := value["str"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(str["value"]).To(Equal("mysql"))
	})

	It("generates criteria with LogicalExpression for AND conditions", func() {
		query := "SELECT * FROM STREAM sw WHERE service_id = 'webapp' AND status = 200"
		parsed, errors := ParseQuery(query)
		Expect(errors).To(BeEmpty())
		Expect(parsed).NotTo(BeNil())

		translator := NewTranslator(context)
		yamlData, err := translator.TranslateToYAML(parsed)
		Expect(err).NotTo(HaveOccurred())

		// Print the YAML output to verify the format
		fmt.Printf("YAML Output for AND conditions:\n%s\n", string(yamlData))

		// Verify the structure has proper LogicalExpression
		data, err := translator.TranslateToMap(parsed)
		Expect(err).NotTo(HaveOccurred())

		criteria, ok := data["criteria"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(criteria).NotTo(BeEmpty())

		// Should have a logical expression
		le, ok := criteria["le"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(le["op"]).To(Equal("LOGICAL_OP_AND"))

		// Verify left and right conditions
		left, ok := le["left"].(map[string]any)
		Expect(ok).To(BeTrue())
		leftCond, ok := left["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(leftCond["name"]).To(Equal("service_id"))
		Expect(leftCond["op"]).To(Equal("BINARY_OP_EQ"))

		right, ok := le["right"].(map[string]any)
		Expect(ok).To(BeTrue())
		rightCond, ok := right["condition"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(rightCond["name"]).To(Equal("status"))
		Expect(rightCond["op"]).To(Equal("BINARY_OP_EQ"))
	})
})
