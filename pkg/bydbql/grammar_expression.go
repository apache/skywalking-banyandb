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

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/query/ql"
)

// GrammarExpression is a SELECT expression before schema and parameter resolution.
type GrammarExpression struct {
	Op         string
	Args       []*GrammarExpression
	Identifier *GrammarIdentifierPath
	TypeSpec   *string
	Literal    ql.ExpressionScalar
	Type       ql.ExpressionType
	Param      bool
	ParamIndex int
}

type expressionParser struct {
	tokens     []expressionToken
	pos, depth int
}

// parseExpressionProjection retains the existing grammar for every ordinary
// column and parses only the new SELECT expression boundary.
func parseExpressionProjection(query string) (*Grammar, error) {
	tokens, err := expressionTokens(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 || !strings.EqualFold(tokens[0].text, "SELECT") {
		return nil, fmt.Errorf("expression projection requires SELECT")
	}
	from := -1
	depth := 0
	for i := 1; i < len(tokens); i++ {
		switch tokens[i].text {
		case "(":
			depth++
		case ")":
			depth--
		}
		if depth == 0 && expressionFromToken(tokens, i) {
			from = i
			break
		}
	}
	if from < 0 {
		return nil, fmt.Errorf("missing FROM clause")
	}
	var segments [][]expressionToken
	begin := 1
	depth = 0
	for i := begin; i < from; i++ {
		switch tokens[i].text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				segments = append(segments, tokens[begin:i])
				begin = i + 1
			}
		}
	}
	segments = append(segments, tokens[begin:from])
	columns := make([]*GrammarColumn, 0, len(segments))
	placeholders := make([]string, 0, len(segments))
	for i, segment := range segments {
		if len(segment) == 0 {
			return nil, fmt.Errorf("empty SELECT expression")
		}
		text := query[segment[0].begin:segment[len(segment)-1].end]
		ordinary, ordinaryErr := particpleParser.ParseString("", "SELECT "+text+" FROM MEASURE m IN g")
		if ordinaryErr == nil && len(ordinary.Select.Projection.Columns) == 1 {
			column := ordinary.Select.Projection.Columns[0]
			column.SingleQuoted = segment[0].kind == "string"
			column.Pos.Offset = segment[0].begin
			columns = append(columns, column)
		} else {
			parser := expressionParser{tokens: segment}
			expr, parseErr := parser.parse(0)
			if parseErr != nil {
				return nil, parseErr
			}
			if parseErr = validateExpressionDepth(expr); parseErr != nil {
				return nil, parseErr
			}
			column := &GrammarColumn{Expression: expr}
			if parser.take("AS") {
				column.Alias, parseErr = parser.identifier()
				if parseErr != nil {
					return nil, parseErr
				}
			}
			if parser.pos != len(segment) {
				return nil, fmt.Errorf("unexpected expression token %q", segment[parser.pos].text)
			}
			columns = append(columns, column)
		}
		placeholders = append(placeholders, fmt.Sprintf("__ql_expression_%d", i))
	}
	rewritten := "SELECT " + strings.Join(placeholders, ", ") + " " + query[tokens[from].begin:]
	grammar, err := particpleParser.ParseString("", rewritten)
	if err != nil {
		return nil, err
	}
	grammar.Select.Projection.Columns = columns
	return grammar, nil
}

func (p *expressionParser) take(text string) bool {
	if p.pos < len(p.tokens) && strings.EqualFold(p.tokens[p.pos].text, text) {
		p.pos++
		return true
	}
	return false
}

func (p *expressionParser) need(text string) error {
	if p.take(text) {
		return nil
	}
	return fmt.Errorf("expected %s in SELECT expression", text)
}

func (p *expressionParser) parse(minPrecedence int) (*GrammarExpression, error) {
	p.depth++
	defer func() { p.depth-- }()
	if p.depth > maxParenDepth {
		return nil, fmt.Errorf("expression nesting exceeds maximum depth of %d", maxParenDepth)
	}
	left, err := p.prefix()
	if err != nil {
		return nil, err
	}
	for p.pos < len(p.tokens) {
		op := strings.ToUpper(p.tokens[p.pos].text)
		precedence := expressionPrecedence(op)
		if precedence < minPrecedence {
			break
		}
		if op == "NOT" && (p.pos+1 >= len(p.tokens) || !strings.EqualFold(p.tokens[p.pos+1].text, "IN")) {
			break
		}
		p.pos++
		switch op {
		case "::":
			left, err = p.castSuffix(left)
		case "IS":
			not := p.take("NOT")
			err = p.need("NULL")
			op = "is_null"
			if not {
				op = "is_not_null"
			}
			left = &GrammarExpression{Op: op, Args: []*GrammarExpression{left}}
		case "IN", "NOT":
			if op == "NOT" {
				err = p.need("IN")
			}
			if err == nil {
				left, err = p.in(left, op == "NOT")
			}
		default:
			var right *GrammarExpression
			right, err = p.parse(precedence + 1)
			if op == "<>" {
				op = "!="
			}
			left = &GrammarExpression{Op: strings.ToLower(op), Args: []*GrammarExpression{left, right}}
		}
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func expressionPrecedence(op string) int {
	switch op {
	case "OR":
		return 1
	case "AND":
		return 2
	case "=", "!=", "<>", "<", "<=", ">", ">=", "IS", "IN", "NOT":
		return 3
	case "+", "-":
		return 4
	case "*", "/", "%":
		return 5
	case "::":
		return 7
	default:
		return -1
	}
}

func (p *expressionParser) prefix() (*GrammarExpression, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("incomplete SELECT expression")
	}
	token := p.tokens[p.pos]
	if p.disambiguatedColumn() {
		identifier, err := p.identifier()
		return &GrammarExpression{Op: "column", Identifier: identifier}, err
	}
	if p.take("+") || p.take("-") || p.take("NOT") {
		op := strings.ToLower(token.text)
		if op == "-" && p.pos < len(p.tokens) && p.tokens[p.pos].text == "9223372036854775808" {
			p.pos++
			return &GrammarExpression{Op: "literal", Type: ql.ExpressionInt, Literal: ql.ExpressionScalar{Int64: math.MinInt64}}, nil
		}
		precedence := 6
		if op == "not" {
			precedence = 3
		}
		value, err := p.parse(precedence)
		if err != nil {
			return nil, err
		}
		if op == "+" {
			op = "positive"
		}
		if op == "-" {
			op = "negate"
		}
		return &GrammarExpression{Op: op, Args: []*GrammarExpression{value}}, nil
	}
	if p.take("(") {
		value, err := p.parse(0)
		if err == nil {
			err = p.need(")")
		}
		return value, err
	}
	if p.take("CAST") {
		return p.cast()
	}
	if p.take("CASE") {
		return p.caseExpr()
	}
	if p.take("COALESCE") {
		return p.coalesce()
	}
	p.pos++
	switch {
	case token.text == "?":
		return &GrammarExpression{Op: "literal", Param: true}, nil
	case strings.EqualFold(token.text, "NULL"):
		return &GrammarExpression{Op: "literal", Type: ql.ExpressionNull, Literal: ql.ExpressionScalar{Null: true}}, nil
	case strings.EqualFold(token.text, "TRUE"), strings.EqualFold(token.text, "FALSE"):
		return &GrammarExpression{Op: "literal", Type: ql.ExpressionBool, Literal: ql.ExpressionScalar{Bool: strings.EqualFold(token.text, "TRUE")}}, nil
	case token.kind == "number":
		return parseExpressionNumber(token.text)
	case token.kind == "string":
		value, err := expressionUnquote(token.text)
		if p.pos+1 < len(p.tokens) && p.tokens[p.pos].text == "::" &&
			(strings.EqualFold(p.tokens[p.pos+1].text, "TAG") || strings.EqualFold(p.tokens[p.pos+1].text, "FIELD")) {
			return &GrammarExpression{Op: "column", Identifier: &GrammarIdentifierPath{QuotedIdent: &value}}, err
		}
		return &GrammarExpression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: value}}, err
	case token.kind == "ident" || token.kind == "quoted":
		p.pos--
		identifier, err := p.identifier()
		return &GrammarExpression{Op: "column", Identifier: identifier}, err
	default:
		return nil, fmt.Errorf("unexpected SELECT expression token %q", token.text)
	}
}

func parseExpressionNumber(text string) (*GrammarExpression, error) {
	value := &GrammarExpression{Op: "literal", Type: ql.ExpressionInt}
	var err error
	if strings.ContainsAny(text, ".eE") {
		value.Type = ql.ExpressionFloat
		value.Literal.Float64, err = strconv.ParseFloat(text, 64)
	} else {
		value.Literal.Int64, err = strconv.ParseInt(text, 10, 64)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid numeric literal %q: %w", text, err)
	}
	return value, nil
}

func expressionUnquote(text string) (string, error) {
	// Match the existing grammar's backslash escapes for both quote styles.
	if text[0] == '\'' {
		text = "\"" + strings.ReplaceAll(strings.ReplaceAll(text[1:len(text)-1], "\\'", "'"), "\"", "\\\"") + "\""
	}
	value, err := strconv.Unquote(text)
	if err != nil {
		return "", fmt.Errorf("invalid quoted expression: %w", err)
	}
	return value, nil
}

func (p *expressionParser) identifier() (*GrammarIdentifierPath, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("expected identifier")
	}
	token := p.tokens[p.pos]
	p.pos++
	if token.kind == "string" || token.kind == "quoted" {
		value, err := expressionUnquote(token.text)
		return &GrammarIdentifierPath{QuotedIdent: &value}, err
	}
	if token.kind != "ident" {
		return nil, fmt.Errorf("expected identifier, got %q", token.text)
	}
	name := token.text
	path := &GrammarIdentifierPath{First: &GrammarIdentifierPart{Ident: &name}}
	for p.take(".") {
		if p.pos >= len(p.tokens) || p.tokens[p.pos].kind != "ident" {
			return nil, fmt.Errorf("expected identifier after dot")
		}
		part := p.tokens[p.pos].text
		p.pos++
		path.Rest = append(path.Rest, &GrammarIdentifierPart{Ident: &part})
	}
	return path, nil
}

func validateExpressionDepth(expression *GrammarExpression) error {
	type entry struct {
		expression *GrammarExpression
		depth      int
	}
	pending := []entry{{expression: expression, depth: 1}}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if item.depth > maxParenDepth {
			return fmt.Errorf("expression nesting exceeds maximum depth of %d", maxParenDepth)
		}
		for _, child := range item.expression.Args {
			pending = append(pending, entry{expression: child, depth: item.depth + 1})
		}
	}
	return nil
}

func markExpressionLiterals(grammar *Grammar, query string) {
	if grammar.Select == nil || grammar.Select.Projection == nil {
		return
	}
	from := grammar.Select.From
	if !strings.EqualFold(from.ResourceType, "MEASURE") && from.Shorthand == nil {
		return
	}
	for _, column := range grammar.Select.Projection.Columns {
		if column.Identifier == nil || column.TypeSpec != nil {
			continue
		}
		if column.Identifier.QuotedIdent != nil {
			if column.Pos.Offset < len(query) && query[column.Pos.Offset] == '\'' {
				column.SingleQuoted = true
			}
			continue
		}
		identifier := column.Identifier
		if identifier.First == nil || len(identifier.Rest) != 0 {
			continue
		}
		name := strings.ToUpper(identifier.First.Value())
		switch name {
		case "TRUE", "FALSE":
			column.Expression = &GrammarExpression{Op: "literal", Type: ql.ExpressionBool, Literal: ql.ExpressionScalar{Bool: name == "TRUE"}}
		case "NULL":
			column.Expression = &GrammarExpression{Op: "literal", Type: ql.ExpressionNull, Literal: ql.ExpressionScalar{Null: true}}
		}
	}
}

func expandExpressionLists(expression *GrammarExpression) {
	if expression == nil {
		return
	}
	for _, child := range expression.Args {
		expandExpressionLists(child)
	}
	if expression.Op != "in" && expression.Op != "not_in" {
		return
	}
	values := make([]*GrammarExpression, 0, len(expression.Args))
	for index, arg := range expression.Args {
		if index == 0 || arg.Op != "literal" {
			values = append(values, arg)
			continue
		}
		switch arg.Type {
		case ql.ExpressionIntArray:
			for _, value := range arg.Literal.Ints {
				values = append(values, &GrammarExpression{Op: "literal", Type: ql.ExpressionInt, Literal: ql.ExpressionScalar{Int64: value}})
			}
		case ql.ExpressionStringArray:
			for _, value := range arg.Literal.Strings {
				values = append(values, &GrammarExpression{Op: "literal", Type: ql.ExpressionString, Literal: ql.ExpressionScalar{String: value}})
			}
		default:
			values = append(values, arg)
		}
	}
	expression.Args = values
}

func expressionFromToken(tokens []expressionToken, index int) bool {
	if !strings.EqualFold(tokens[index].text, "FROM") || index > 0 && tokens[index-1].text == "." {
		return false
	}
	return index+1 == len(tokens) || tokens[index+1].text != "::" && tokens[index+1].text != "."
}

func (p *expressionParser) disambiguatedColumn() bool {
	if p.tokens[p.pos].kind != "ident" || p.pos+1 >= len(p.tokens) {
		return false
	}
	next := p.tokens[p.pos+1].text
	if next == "." {
		return true
	}
	return next == "::" && p.pos+2 < len(p.tokens) &&
		(strings.EqualFold(p.tokens[p.pos+2].text, "TAG") || strings.EqualFold(p.tokens[p.pos+2].text, "FIELD"))
}
