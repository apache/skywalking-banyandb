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
	"math"
	"reflect"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func strParam(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func intParam(v int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
}

func strArrayParam(vs ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: vs}}}
}

func intArrayParam(vs ...int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: vs}}}
}

func nullParam() *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

func timestampParam(t time.Time) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(t)}}
}

func binaryParam(data []byte) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: data}}
}

var _ = Describe("BindParams", func() {
	parse := func(query string) *Grammar {
		grammar, err := ParseQuery(query)
		Expect(err).To(BeNil())
		return grammar
	}

	Describe("TIME clause binding", func() {
		It("binds a str parameter as a relative time", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("-30m")})).To(Succeed())
			Expect(grammar.Select.Time.Value.String).NotTo(BeNil())
			Expect(*grammar.Select.Time.Value.String).To(Equal("-30m"))
			Expect(grammar.Select.Time.Value.Param).To(BeFalse())
		})

		It("binds a timestamp parameter as an RFC3339 string", func() {
			ts := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{timestampParam(ts)})).To(Succeed())
			Expect(grammar.Select.Time.Value.String).NotTo(BeNil())
			parsed, err := time.Parse(time.RFC3339, *grammar.Select.Time.Value.String)
			Expect(err).To(BeNil())
			Expect(parsed.Equal(ts)).To(BeTrue())
		})

		It("binds both boundaries of TIME BETWEEN", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME BETWEEN ? AND ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("-1h"), strParam("now")})).To(Succeed())
			Expect(*grammar.Select.Time.Between.Begin.String).To(Equal("-1h"))
			Expect(*grammar.Select.Time.Between.End.String).To(Equal("now"))
		})

		It("rejects an array parameter in the TIME clause", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{strArrayParam("a", "b")})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("time clause"))
		})

		It("rejects an int parameter in the TIME clause", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{intParam(1720000000)})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only accepts str or timestamp"))
		})
	})

	Describe("WHERE clause binding", func() {
		It("binds a str parameter in a comparison", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("webapp")})).To(Succeed())
			value := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
			Expect(value.String).NotTo(BeNil())
			Expect(*value.String).To(Equal("webapp"))
			Expect(value.Param).To(BeFalse())
		})

		It("binds an int parameter in a comparison", func() {
			grammar := parse("SELECT * FROM MEASURE metrics IN default WHERE value > ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{intParam(100)})).To(Succeed())
			value := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
			Expect(value.Integer).NotTo(BeNil())
			Expect(*value.Integer).To(Equal(int64(100)))
		})

		It("binds a null parameter in a comparison", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{nullParam()})).To(Succeed())
			value := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
			Expect(value.Null).To(BeTrue())
		})

		It("binds a str parameter in MATCH", func() {
			grammar := parse("SELECT trace_id, message FROM STREAM logs IN group1 TIME > ? WHERE message MATCH(?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("-30m"), strParam("error")})).To(Succeed())
			match := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Match
			Expect(match.Values.Single).NotTo(BeNil())
			Expect(*match.Values.Single.String).To(Equal("error"))
		})

		It("expands a str_array parameter in MATCH into multiple values", func() {
			grammar := parse("SELECT * FROM STREAM logs IN group1 WHERE message MATCH(?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("error", "warn")})).To(Succeed())
			match := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Match
			Expect(match.Values.Single).To(BeNil())
			Expect(match.Values.Array).To(HaveLen(2))
			Expect(*match.Values.Array[0].String).To(Equal("error"))
			Expect(*match.Values.Array[1].String).To(Equal("warn"))
		})

		It("expands multiple array parameters and a scalar in one list in order", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN (?, ?, ?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("a", "b"), strParam("c"), strArrayParam("d", "e")})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.In.Values
			Expect(values).To(HaveLen(5))
			for idx, expected := range []string{"a", "b", "c", "d", "e"} {
				Expect(*values[idx].String).To(Equal(expected))
			}
		})

		It("expands an array parameter in IN alongside literal values", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN ('a', ?, 'b')")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("x", "y")})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.In.Values
			Expect(values).To(HaveLen(4))
			Expect(*values[0].String).To(Equal("a"))
			Expect(*values[1].String).To(Equal("x"))
			Expect(*values[2].String).To(Equal("y"))
			Expect(*values[3].String).To(Equal("b"))
		})

		It("expands an int_array parameter in HAVING", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE codes HAVING (?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{intArrayParam(200, 500)})).To(Succeed())
			having := grammar.Select.Where.Expr.Left.Left.Having
			Expect(having.Values.Array).To(HaveLen(2))
			Expect(*having.Values.Array[0].Integer).To(Equal(int64(200)))
			Expect(*having.Values.Array[1].Integer).To(Equal(int64(500)))
		})

		It("expands array parameters in multiple lists within one query", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN (?) AND codes HAVING (?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("a", "b"), intArrayParam(200, 500)})).To(Succeed())
			andExpr := grammar.Select.Where.Expr.Left
			inValues := andExpr.Left.In.Values
			Expect(inValues).To(HaveLen(2))
			Expect(*inValues[0].String).To(Equal("a"))
			Expect(*inValues[1].String).To(Equal("b"))
			havingValues := andExpr.Right[0].Right.Having.Values
			Expect(havingValues.Array).To(HaveLen(2))
			Expect(*havingValues.Array[0].Integer).To(Equal(int64(200)))
			Expect(*havingValues.Array[1].Integer).To(Equal(int64(500)))
		})

		It("expands an array parameter in MATCH while keeping the literal analyzer", func() {
			grammar := parse("SELECT * FROM STREAM logs IN group1 WHERE message MATCH(?, 'simple')")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("error", "warn")})).To(Succeed())
			match := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Match
			Expect(match.Values.Array).To(HaveLen(2))
			Expect(match.Analyzer).NotTo(BeNil())
			Expect(*match.Analyzer).To(Equal("simple"))
		})

		It("rejects an empty array parameter", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN (?)")
			err := BindParams(grammar, []*modelv1.TagValue{strArrayParam()})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must not be empty"))
		})

		It("rejects an array parameter in a scalar comparison", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{strArrayParam("a", "b")})
			Expect(err).To(HaveOccurred())
		})

		It("rejects a binary parameter anywhere", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{binaryParam([]byte{1, 2})})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("integer-only positions", func() {
		It("binds int parameters to LIMIT and OFFSET", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default LIMIT ? OFFSET ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{intParam(10), intParam(5)})).To(Succeed())
			Expect(grammar.Select.Limit.Value).To(Equal(10))
			Expect(grammar.Select.Limit.Param).To(BeFalse())
			Expect(grammar.Select.Offset.Value).To(Equal(5))
			Expect(grammar.Select.Offset.Param).To(BeFalse())
		})

		It("binds an int parameter to the SHOW TOP N count", func() {
			grammar := parse("SHOW TOP ? FROM MEASURE metrics IN default")
			Expect(BindParams(grammar, []*modelv1.TagValue{intParam(7)})).To(Succeed())
			Expect(grammar.TopN.N).To(Equal(7))
			Expect(grammar.TopN.NParam).To(BeFalse())
		})

		It("binds an int parameter to the SELECT TOP N projection count", func() {
			grammar := parse("SELECT TOP ? value FROM MEASURE metrics IN default")
			Expect(BindParams(grammar, []*modelv1.TagValue{intParam(3)})).To(Succeed())
			Expect(grammar.Select.Projection.TopN.N).To(Equal(3))
			Expect(grammar.Select.Projection.TopN.NParam).To(BeFalse())
		})

		It("rejects a str parameter in LIMIT", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default LIMIT ?")
			err := BindParams(grammar, []*modelv1.TagValue{strParam("10")})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only accepts int"))
		})

		It("rejects a negative int parameter in LIMIT", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default LIMIT ?")
			err := BindParams(grammar, []*modelv1.TagValue{intParam(-1)})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("accepts the uint32 maximum in LIMIT", func() {
			// LIMIT/OFFSET are uint32 on the wire, so their bound accepts up to
			// math.MaxUint32 — the same value the max-limit literal query uses.
			grammar := parse("SELECT * FROM STREAM sw IN default LIMIT ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{intParam(math.MaxUint32)})).To(Succeed())
			Expect(grammar.Select.Limit.Value).To(Equal(int(math.MaxUint32)))
		})

		It("rejects an int parameter beyond uint32 in OFFSET", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default OFFSET ?")
			err := BindParams(grammar, []*modelv1.TagValue{intParam(math.MaxUint32 + 1)})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})

		It("rejects an int parameter beyond int32 in the SHOW TOP count", func() {
			grammar := parse("SHOW TOP ? FROM MEASURE metrics IN default")
			err := BindParams(grammar, []*modelv1.TagValue{intParam(math.MaxInt32 + 1)})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("out of range"))
		})
	})

	Describe("re-binding semantics", func() {
		It("rejects binding the same grammar twice with the same parameters", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			params := []*modelv1.TagValue{strParam("svc")}
			Expect(BindParams(grammar, params)).To(Succeed())
			err := BindParams(grammar, params)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already bound"))
		})

		It("rejects a second bind even with no parameters", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("svc")})).To(Succeed())
			err := BindParams(grammar, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("already bound"))
			Expect(*grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value.String).To(Equal("svc"))
		})
	})

	Describe("ordering and counting", func() {
		It("binds parameters in order of appearance across clauses", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ? WHERE service_id = ? AND instance_id = ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("-15m"), strParam("svc"), strParam("inst")})).To(Succeed())
			Expect(*grammar.Select.Time.Value.String).To(Equal("-15m"))
			andExpr := grammar.Select.Where.Expr.Left
			Expect(*andExpr.Left.Binary.Tail.Compare.Value.String).To(Equal("svc"))
			Expect(*andExpr.Right[0].Right.Binary.Tail.Compare.Value.String).To(Equal("inst"))
		})

		It("binds parameters in a SHOW TOP statement", func() {
			grammar := parse("SHOW TOP 5 FROM MEASURE metrics IN default TIME > ? WHERE service = ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("-1h"), strParam("svc")})).To(Succeed())
			Expect(*grammar.TopN.Time.Value.String).To(Equal("-1h"))
			Expect(*grammar.TopN.Where.Expr.Left.Binary.Tail.Compare.Value.String).To(Equal("svc"))
		})

		It("rejects fewer parameters than placeholders", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ? WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{strParam("-30m")})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("2 placeholder(s) but 1 parameter(s)"))
		})

		It("rejects more parameters than placeholders", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default")
			err := BindParams(grammar, []*modelv1.TagValue{strParam("extra")})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("0 placeholder(s) but 1 parameter(s)"))
		})

		It("accepts a query without placeholders and no parameters", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = 'webapp'")
			Expect(BindParams(grammar, nil)).To(Succeed())
		})
	})

	Describe("binder coverage of grammar placeholder positions", func() {
		// Tripwire: binder.collect is the single registry of placeholder positions,
		// shared by BindParams and the Transform-entry guard. If a new @Param field
		// is added to the grammar without wiring it into the binder, the placeholder
		// would silently transform as a zero value. This test fails on any new
		// @Param field until the binder (and the test matrices) are updated.
		It("accounts for every @Param field in the grammar", func() {
			expected := map[string]bool{
				"GrammarValue.Param":           true,
				"GrammarTimeValue.Param":       true,
				"GrammarTopNStatement.NParam":  true,
				"GrammarTopNProjection.NParam": true,
				"GrammarLimitClause.Param":     true,
				"GrammarOffsetClause.Param":    true,
			}
			found := make(map[string]bool)
			visited := make(map[reflect.Type]bool)
			var walk func(t reflect.Type)
			walk = func(t reflect.Type) {
				for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
					t = t.Elem()
				}
				if t.Kind() != reflect.Struct || visited[t] {
					return
				}
				visited[t] = true
				for i := 0; i < t.NumField(); i++ {
					field := t.Field(i)
					if strings.Contains(field.Tag.Get("parser"), "@Param") {
						found[t.Name()+"."+field.Name] = true
					}
					walk(field.Type)
				}
			}
			walk(reflect.TypeOf(Grammar{}))
			Expect(found).To(Equal(expected),
				"grammar @Param positions changed: wire any new position into binder.collect, preparer.walkGrammar, the acceptance matrix, and this list")
		})
	})

	Describe("placeholder syntax restrictions", func() {
		// Placeholders are only legal in value positions. Every structural or
		// identifier position must stay a syntax error so a bound parameter can
		// never influence the query shape.
		forbidden := map[string]string{
			"resource name":       "SELECT * FROM STREAM ? IN default",
			"group name":          "SELECT * FROM STREAM sw IN ?",
			"stage name":          "SELECT * FROM STREAM sw IN default ON ? STAGES",
			"projection column":   "SELECT ? FROM STREAM sw IN default",
			"condition left side": "SELECT * FROM STREAM sw IN default WHERE ? = 'a'",
			"ORDER BY column":     "SELECT * FROM STREAM sw IN default ORDER BY ?",
			"GROUP BY column":     "SELECT * FROM MEASURE m IN default GROUP BY ?",
			"MATCH analyzer":      "SELECT * FROM STREAM sw IN default WHERE message MATCH('x', ?)",
			"doubled placeholder": "SELECT * FROM STREAM sw IN default WHERE service_id = ??",
		}
		for position, query := range forbidden {
			It("rejects a placeholder in "+position+" at parse time", func() {
				_, err := ParseQuery(query)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("syntax error"))
			})
		}
	})

	Describe("parameter type acceptance matrix across all placeholder paths", func() {
		timeAccepted := map[string]bool{"str": true, "timestamp": true}
		scalarAccepted := map[string]bool{"str": true, "int": true, "null": true}
		listAccepted := map[string]bool{"str": true, "int": true, "null": true, "str_array": true, "int_array": true}
		intAccepted := map[string]bool{"int": true}
		paths := []struct {
			accepted map[string]bool
			name     string
			query    string
		}{
			{timeAccepted, "TIME comparison value", "SELECT * FROM STREAM sw IN default TIME > ?"},
			{timeAccepted, "TIME BETWEEN boundary", "SELECT * FROM STREAM sw IN default TIME BETWEEN ? AND '2026-07-06T11:00:00Z'"},
			{intAccepted, "LIMIT count", "SELECT * FROM STREAM sw IN default LIMIT ?"},
			{intAccepted, "OFFSET count", "SELECT * FROM STREAM sw IN default OFFSET ?"},
			{intAccepted, "SHOW TOP N count", "SHOW TOP ? FROM MEASURE m IN default"},
			{intAccepted, "SELECT TOP N projection count", "SELECT TOP ? value FROM MEASURE m IN default"},
			{scalarAccepted, "scalar equality value", "SELECT * FROM STREAM sw IN default WHERE service_id = ?"},
			{scalarAccepted, "scalar ordered comparison value", "SELECT * FROM STREAM sw IN default WHERE duration > ?"},
			{listAccepted, "IN value list", "SELECT * FROM STREAM sw IN default WHERE service_id IN (?)"},
			{listAccepted, "MATCH value list", "SELECT * FROM STREAM sw IN default WHERE message MATCH(?)"},
			{listAccepted, "HAVING single value", "SELECT * FROM STREAM sw IN default WHERE tags HAVING ?"},
			{listAccepted, "HAVING value list", "SELECT * FROM STREAM sw IN default WHERE codes HAVING (?)"},
		}
		paramTypes := []struct {
			param *modelv1.TagValue
			name  string
		}{
			{strParam("value"), "str"},
			{intParam(42), "int"},
			{nullParam(), "null"},
			{strArrayParam("a", "b"), "str_array"},
			{intArrayParam(1, 2), "int_array"},
			{timestampParam(time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)), "timestamp"},
			{binaryParam([]byte{1, 2}), "binary"},
			{&modelv1.TagValue{}, "empty"},
			{nil, "nil"},
		}
		for _, path := range paths {
			for _, paramType := range paramTypes {
				expectAccept := path.accepted[paramType.name]
				verdict := "rejects"
				if expectAccept {
					verdict = "accepts"
				}
				It(verdict+" a "+paramType.name+" parameter in "+path.name, func() {
					grammar := parse(path.query)
					err := BindParams(grammar, []*modelv1.TagValue{paramType.param})
					if expectAccept {
						Expect(err).To(BeNil())
					} else {
						Expect(err).To(HaveOccurred())
					}
				})
			}
		}
	})

	Describe("single value and list form preservation", func() {
		It("keeps the single form when a one-element array binds a single MATCH value", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE message MATCH(?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("error")})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Match.Values
			Expect(values.Single).NotTo(BeNil())
			Expect(*values.Single.String).To(Equal("error"))
			Expect(values.Array).To(BeNil())
		})

		It("switches to the list form when a multi-element array binds a single HAVING value", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE tags HAVING ?")
			Expect(BindParams(grammar, []*modelv1.TagValue{strArrayParam("a", "b")})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.Having.Values
			Expect(values.Single).To(BeNil())
			Expect(values.Array).To(HaveLen(2))
		})

		It("keeps the list form when a one-element array binds a parenthesized HAVING list", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE codes HAVING (?)")
			Expect(BindParams(grammar, []*modelv1.TagValue{intArrayParam(200)})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.Having.Values
			Expect(values.Single).To(BeNil())
			Expect(values.Array).To(HaveLen(1))
			Expect(*values.Array[0].Integer).To(Equal(int64(200)))
		})

		It("keeps the list form when a scalar binds a parenthesized MATCH list", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE message MATCH((?))")
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam("error")})).To(Succeed())
			values := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Match.Values
			Expect(values.Single).To(BeNil())
			Expect(values.Array).To(HaveLen(1))
		})
	})

	Describe("parameter type rejections", func() {
		It("rejects a timestamp parameter in a scalar comparison", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{timestampParam(time.Now())})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("str, int, or null"))
		})

		It("rejects a timestamp parameter in a value list", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN (?)")
			err := BindParams(grammar, []*modelv1.TagValue{timestampParam(time.Now())})
			Expect(err).To(HaveOccurred())
		})

		It("rejects a null parameter in the TIME clause", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{nullParam()})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("time clause"))
		})

		It("rejects a binary parameter in the TIME clause", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{binaryParam([]byte{1})})
			Expect(err).To(HaveOccurred())
		})

		It("rejects a binary parameter in a value list", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id IN (?)")
			err := BindParams(grammar, []*modelv1.TagValue{binaryParam([]byte{1})})
			Expect(err).To(HaveOccurred())
		})

		It("rejects an int_array parameter in a scalar comparison", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE duration > ?")
			err := BindParams(grammar, []*modelv1.TagValue{intArrayParam(1, 2)})
			Expect(err).To(HaveOccurred())
		})

		It("rejects a nil inner timestamp in the TIME clause instead of decoding it as the epoch", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{{Value: &modelv1.TagValue_Timestamp{}}})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid timestamp"))
		})

		It("rejects an out-of-range timestamp in the TIME clause", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default TIME > ?")
			err := BindParams(grammar, []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Timestamp{Timestamp: &timestamppb.Timestamp{Seconds: math.MaxInt64}}},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid timestamp"))
		})

		It("rejects a parameter without a value", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{{}})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("has no value"))
		})

		It("rejects a nil parameter entry", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			err := BindParams(grammar, []*modelv1.TagValue{nil})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("has no value"))
		})

		It("reports the 1-based position of the failing parameter", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ? AND duration > ?")
			err := BindParams(grammar, []*modelv1.TagValue{strParam("ok"), strArrayParam("bad")})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("parameter #2"))
		})
	})

	Describe("injection safety", func() {
		It("keeps an injection payload as a pure string value", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE service_id = ?")
			payload := "x' OR '1'='1"
			Expect(BindParams(grammar, []*modelv1.TagValue{strParam(payload)})).To(Succeed())
			value := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
			Expect(*value.String).To(Equal(payload))
			// The bound value never becomes additional predicates
			Expect(grammar.Select.Where.Expr.Left.Right).To(BeEmpty())
			Expect(grammar.Select.Where.Expr.Right).To(BeEmpty())
		})

		It("does not treat a question mark inside a string literal as a placeholder", func() {
			grammar := parse("SELECT * FROM STREAM sw IN default WHERE message = 'why?'")
			Expect(BindParams(grammar, nil)).To(Succeed())
			value := grammar.Select.Where.Expr.Left.Left.Binary.Tail.Compare.Value
			Expect(*value.String).To(Equal("why?"))
		})
	})
})
