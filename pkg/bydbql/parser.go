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
	"strconv"
	"strings"
	"time"
)

// Parser represents a BydbQL parser
type Parser struct {
	lexer  *Lexer
	errors []string

	currentToken Token
	peekToken    Token
}

// NewParser creates a new parser instance
func NewParser(lexer *Lexer) *Parser {
	p := &Parser{
		lexer:  lexer,
		errors: []string{},
	}

	// Read two tokens, so currentToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

// nextToken advances to the next token
func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// addError adds a parsing error
func (p *Parser) addError(msg string, args ...interface{}) {
	p.errors = append(p.errors, fmt.Sprintf(msg, args...))
}

// Errors returns parsing errors
func (p *Parser) Errors() []string {
	return p.errors
}

// expectToken checks if the current token matches expected type and advances
func (p *Parser) expectToken(tokenType TokenType) bool {
	if p.currentTokenIs(tokenType) {
		p.nextToken()
		return true
	}

	p.addError("expected token %s, got %s at line %d, column %d",
		tokenType.String(), p.currentToken.Type.String(),
		p.currentToken.Line, p.currentToken.Column)
	return false
}

// currentTokenIs checks if current token matches the given type
func (p *Parser) currentTokenIs(tokenType TokenType) bool {
	return p.currentToken.Type == tokenType
}

// peekTokenIs checks if peek token matches the given type
func (p *Parser) peekTokenIs(tokenType TokenType) bool {
	return p.peekToken.Type == tokenType
}

// Parse parses the BydbQL query and returns a parsed query
func (p *Parser) Parse() *ParsedQuery {
	var stmt Statement

	switch p.currentToken.Type {
	case TokenSelect:
		stmt = p.parseSelectStatement()
	case TokenShow:
		stmt = p.parseTopNStatement()
	default:
		p.addError("unexpected token %s at line %d, column %d. Expected SELECT or SHOW",
			p.currentToken.Type.String(), p.currentToken.Line, p.currentToken.Column)
		return nil
	}

	if len(p.errors) > 0 {
		return nil
	}

	// Extract resource information from the statement
	query := &ParsedQuery{
		Statement: stmt,
		Context:   &QueryContext{CurrentTime: time.Now()},
	}

	switch s := stmt.(type) {
	case *SelectStatement:
		if s.From != nil {
			query.ResourceType = s.From.ResourceType
			query.ResourceName = s.From.ResourceName
			query.Groups = s.From.Groups
		}
	case *TopNStatement:
		if s.From != nil {
			query.ResourceType = s.From.ResourceType
			query.ResourceName = s.From.ResourceName
			query.Groups = s.From.Groups
		}
	}

	return query
}

// parseSelectStatement parses a SELECT statement
func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	if !p.expectToken(TokenSelect) {
		return nil
	}

	// Parse projection
	stmt.Projection = p.parseProjection()
	if stmt.Projection == nil {
		return nil
	}

	// Parse optional FROM clause
	if p.currentTokenIs(TokenFrom) {
		stmt.From = p.parseFromClause()
	}

	// Parse optional TIME clause
	if p.currentTokenIs(TokenTime) {
		stmt.Time = p.parseTimeCondition()
	}

	// Parse optional WHERE clause
	if p.currentTokenIs(TokenWhere) {
		stmt.Where = p.parseWhereClause()
	}

	// Parse optional GROUP BY clause
	if p.currentTokenIs(TokenGroupBy) {
		stmt.GroupBy = p.parseGroupByClause()
	}

	// Parse optional ORDER BY clause
	if p.currentTokenIs(TokenOrderBy) {
		stmt.OrderBy = p.parseOrderByClause()
	}

	// Parse optional LIMIT clause
	if p.currentTokenIs(TokenLimit) {
		p.nextToken()
		if p.currentTokenIs(TokenInteger) {
			if limit, err := strconv.Atoi(p.currentToken.Literal); err == nil {
				stmt.Limit = &limit
			} else {
				p.addError("invalid LIMIT value: %s", p.currentToken.Literal)
			}
			p.nextToken()
		} else {
			p.addError("expected integer after LIMIT")
		}
	}

	// Parse optional OFFSET clause
	if p.currentTokenIs(TokenOffset) {
		p.nextToken()
		if p.currentTokenIs(TokenInteger) {
			if offset, err := strconv.Atoi(p.currentToken.Literal); err == nil {
				stmt.Offset = &offset
			} else {
				p.addError("invalid OFFSET value: %s", p.currentToken.Literal)
			}
			p.nextToken()
		} else {
			p.addError("expected integer after OFFSET")
		}
	}

	// Parse optional WITH QUERY_TRACE clause
	if p.currentTokenIs(TokenWith) {
		p.nextToken()
		if p.currentTokenIs(TokenQueryTrace) {
			stmt.QueryTrace = true
			p.nextToken()
		} else {
			p.addError("expected QUERY_TRACE after WITH")
		}
	}

	return stmt
}

// parseTopNStatement parses a SHOW TOP N statement
func (p *Parser) parseTopNStatement() *TopNStatement {
	stmt := &TopNStatement{}

	if !p.expectToken(TokenShow) {
		return nil
	}

	if !p.expectToken(TokenTop) {
		return nil
	}

	// Parse N
	if p.currentTokenIs(TokenInteger) {
		if n, err := strconv.Atoi(p.currentToken.Literal); err == nil {
			stmt.TopN = n
		} else {
			p.addError("invalid TOP N value: %s", p.currentToken.Literal)
			return nil
		}
		p.nextToken()
	} else {
		p.addError("expected integer after TOP")
		return nil
	}

	// Parse FROM clause (mandatory for Top-N)
	if p.currentTokenIs(TokenFrom) {
		stmt.From = p.parseFromClause()
		if stmt.From == nil {
			return nil
		}
	} else {
		p.addError("FROM clause is mandatory for TOP N queries")
		return nil
	}

	// Parse optional TIME clause
	if p.currentTokenIs(TokenTime) {
		stmt.Time = p.parseTimeCondition()
	}

	// Parse optional WHERE clause
	if p.currentTokenIs(TokenWhere) {
		stmt.Where = p.parseWhereClause()
	}

	// Parse optional AGGREGATE BY clause
	if p.currentTokenIs(TokenAggregateBy) {
		p.nextToken()
		stmt.AggregateBy = p.parseAggregateFunction()
	}

	// Parse optional ORDER BY clause
	if p.currentTokenIs(TokenOrderBy) {
		stmt.OrderBy = p.parseOrderByClause()
	}

	// Parse optional WITH QUERY_TRACE clause
	if p.currentTokenIs(TokenWith) {
		p.nextToken()
		if p.currentTokenIs(TokenQueryTrace) {
			stmt.QueryTrace = true
			p.nextToken()
		} else {
			p.addError("expected QUERY_TRACE after WITH")
		}
	}

	return stmt
}

// parseProjection parses the SELECT projection
func (p *Parser) parseProjection() *Projection {
	projection := &Projection{}

	if p.currentTokenIs(TokenStar) {
		projection.All = true
		p.nextToken()
		return projection
	}

	// Check for empty projection SELECT ()
	if p.currentTokenIs(TokenLeftParen) {
		p.nextToken()
		if p.currentTokenIs(TokenRightParen) {
			projection.Empty = true
			p.nextToken()
			return projection
		} else {
			p.addError("expected ) after (")
			return nil
		}
	}

	// Check for TOP N projection
	if p.currentTokenIs(TokenTop) {
		p.nextToken()
		if p.currentTokenIs(TokenInteger) {
			if n, err := strconv.Atoi(p.currentToken.Literal); err == nil {
				projection.TopN = &TopNProjection{N: n}
				p.nextToken()

				// Parse the rest of the projection after TOP N
				if p.currentTokenIs(TokenStar) {
					projection.TopN.Projection = &Projection{All: true}
					p.nextToken()
				} else {
					// Parse column list
					projection.TopN.Projection = &Projection{}
					projection.TopN.Projection.Columns = p.parseColumnList()
				}
				return projection
			} else {
				p.addError("invalid TOP N value: %s", p.currentToken.Literal)
				return nil
			}
		} else {
			p.addError("expected integer after TOP")
			return nil
		}
	}

	// Parse column list
	projection.Columns = p.parseColumnList()
	if projection.Columns == nil {
		return nil
	}

	return projection
}

// parseColumnList parses a comma-separated list of columns
func (p *Parser) parseColumnList() []*Column {
	var columns []*Column

	for {
		col := p.parseColumn()
		if col == nil {
			return nil
		}
		columns = append(columns, col)

		if !p.currentTokenIs(TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return columns
}

// parseColumn parses a single column
func (p *Parser) parseColumn() *Column {
	col := &Column{}

	// Check for aggregate functions
	if p.isAggregateFunction(p.currentToken.Type) {
		col.Function = p.parseAggregateFunction()
		return col
	}

	// Parse column name
	if p.currentTokenIs(TokenIdentifier) {
		col.Name = p.currentToken.Literal
		p.nextToken()

		// Check for type disambiguator ::tag or ::field
		if p.currentTokenIs(TokenDoubleColon) {
			p.nextToken()
			if p.currentTokenIs(TokenIdentifier) {
				switch strings.ToLower(p.currentToken.Literal) {
				case "tag":
					col.Type = ColumnTypeTag
				case "field":
					col.Type = ColumnTypeField
				default:
					p.addError("invalid column type: %s. Expected 'tag' or 'field'", p.currentToken.Literal)
					return nil
				}
				p.nextToken()
			} else {
				p.addError("expected 'tag' or 'field' after ::")
				return nil
			}
		}
	} else {
		p.addError("expected column name")
		return nil
	}

	return col
}

// parseAggregateFunction parses an aggregate function
func (p *Parser) parseAggregateFunction() *AggregateFunction {
	fn := &AggregateFunction{}

	if p.isAggregateFunction(p.currentToken.Type) {
		fn.Function = strings.ToUpper(p.currentToken.Literal)
		p.nextToken()

		if !p.expectToken(TokenLeftParen) {
			return nil
		}

		if p.currentTokenIs(TokenIdentifier) {
			fn.Column = p.currentToken.Literal
			p.nextToken()
		} else {
			p.addError("expected column name in aggregate function")
			return nil
		}

		if !p.expectToken(TokenRightParen) {
			return nil
		}
	}

	return fn
}

// isAggregateFunction checks if token type is an aggregate function
func (p *Parser) isAggregateFunction(tokenType TokenType) bool {
	return tokenType == TokenSum || tokenType == TokenMean || tokenType == TokenAvg || 
		tokenType == TokenCount || tokenType == TokenMax || tokenType == TokenMin
}

// parseFromClause parses the FROM clause
func (p *Parser) parseFromClause() *FromClause {
	if !p.expectToken(TokenFrom) {
		return nil
	}

	from := &FromClause{}

	// Parse resource type
	switch p.currentToken.Type {
	case TokenStream:
		from.ResourceType = ResourceTypeStream
		p.nextToken()
	case TokenMeasure:
		from.ResourceType = ResourceTypeMeasure
		p.nextToken()
	case TokenTrace:
		from.ResourceType = ResourceTypeTrace
		p.nextToken()
	case TokenProperty:
		from.ResourceType = ResourceTypeProperty
		p.nextToken()
	case TokenIdentifier:
		// Only allow specific resource types, reject others
		switch strings.ToUpper(p.currentToken.Literal) {
		case "STREAM":
			from.ResourceType = ResourceTypeStream
		case "MEASURE":
			from.ResourceType = ResourceTypeMeasure
		case "TRACE":
			from.ResourceType = ResourceTypeTrace
		case "PROPERTY":
			from.ResourceType = ResourceTypeProperty
		default:
			p.addError("expected resource type (STREAM, MEASURE, TRACE, PROPERTY), got: %s", p.currentToken.Literal)
			return nil
		}
		p.nextToken()
	default:
		p.addError("expected resource type (STREAM, MEASURE, TRACE, PROPERTY) or resource name")
		return nil
	}

	// Parse resource name
	if p.currentTokenIs(TokenIdentifier) {
		from.ResourceName = p.currentToken.Literal
		p.nextToken()
	} else {
		p.addError("expected resource name")
		return nil
	}

	// Parse optional IN (groups) clause
	if p.currentTokenIs(TokenIn) {
		p.nextToken()
		if !p.expectToken(TokenLeftParen) {
			return nil
		}

		// Parse group list
		for {
			if p.currentTokenIs(TokenIdentifier) {
				from.Groups = append(from.Groups, p.currentToken.Literal)
				p.nextToken()
			} else {
				p.addError("expected group name")
				return nil
			}

			if !p.currentTokenIs(TokenComma) {
				break
			}
			p.nextToken() // consume comma
		}

		if !p.expectToken(TokenRightParen) {
			return nil
		}
	}

	return from
}

// parseTimeCondition parses TIME conditions
func (p *Parser) parseTimeCondition() *TimeCondition {
	if !p.expectToken(TokenTime) {
		return nil
	}

	condition := &TimeCondition{}

	// Parse operator
	switch p.currentToken.Type {
	case TokenEqual:
		condition.Operator = TimeOpEqual
		p.nextToken()
		condition.Timestamp = p.parseTimestamp()
	case TokenGreater:
		condition.Operator = TimeOpGreater
		p.nextToken()
		condition.Timestamp = p.parseTimestamp()
	case TokenLess:
		condition.Operator = TimeOpLess
		p.nextToken()
		condition.Timestamp = p.parseTimestamp()
	case TokenGreaterEqual:
		condition.Operator = TimeOpGreaterEqual
		p.nextToken()
		condition.Timestamp = p.parseTimestamp()
	case TokenLessEqual:
		condition.Operator = TimeOpLessEqual
		p.nextToken()
		condition.Timestamp = p.parseTimestamp()
	case TokenBetween:
		condition.Operator = TimeOpBetween
		p.nextToken()
		condition.Begin = p.parseTimestamp()
		if !p.expectToken(TokenAnd) {
			return nil
		}
		condition.End = p.parseTimestamp()
	default:
		p.addError("expected time operator (=, >, <, >=, <=, BETWEEN)")
		return nil
	}

	return condition
}

// parseTimestamp parses a timestamp (string or integer)
func (p *Parser) parseTimestamp() string {
	if p.currentTokenIs(TokenString) || p.currentTokenIs(TokenInteger) {
		timestamp := p.currentToken.Literal
		p.nextToken()
		return timestamp
	}

	p.addError("expected timestamp")
	return ""
}

// parseWhereClause parses WHERE clause
func (p *Parser) parseWhereClause() *WhereClause {
	if !p.expectToken(TokenWhere) {
		return nil
	}

	where := &WhereClause{}
	where.Conditions = p.parseConditions()

	return where
}

// parseConditions parses conditions with AND/OR logic
func (p *Parser) parseConditions() []*Condition {
	var conditions []*Condition

	condition := p.parseCondition()
	if condition == nil {
		return nil
	}
	conditions = append(conditions, condition)

	for p.currentTokenIs(TokenAnd) || p.currentTokenIs(TokenOr) {
		if p.currentTokenIs(TokenAnd) {
			condition.Logic = LogicAnd
		} else {
			condition.Logic = LogicOr
		}
		p.nextToken()

		condition = p.parseCondition()
		if condition == nil {
			return nil
		}
		conditions = append(conditions, condition)
	}

	return conditions
}

// parseCondition parses a single condition
func (p *Parser) parseCondition() *Condition {
	condition := &Condition{}

	// Parse left side (column name)
	if p.currentTokenIs(TokenIdentifier) {
		condition.Left = p.currentToken.Literal
		p.nextToken()
	} else {
		p.addError("expected column name in condition")
		return nil
	}

	// Parse operator
	switch p.currentToken.Type {
	case TokenEqual:
		condition.Operator = OpEqual
	case TokenNotEqual:
		condition.Operator = OpNotEqual
	case TokenGreater:
		condition.Operator = OpGreater
	case TokenLess:
		condition.Operator = OpLess
	case TokenGreaterEqual:
		condition.Operator = OpGreaterEqual
	case TokenLessEqual:
		condition.Operator = OpLessEqual
	case TokenIn:
		condition.Operator = OpIn
	case TokenNotIn:
		condition.Operator = OpNotIn
	case TokenHaving:
		condition.Operator = OpHaving
	case TokenNotHaving:
		condition.Operator = OpNotHaving
	case TokenMatch:
		condition.Operator = OpMatch
	default:
		p.addError("expected comparison operator")
		return nil
	}
	p.nextToken()

	// Parse right side
	if condition.Operator == OpIn || condition.Operator == OpNotIn {
		// Parse value list
		if !p.expectToken(TokenLeftParen) {
			return nil
		}

		for {
			value := p.parseValue()
			if value == nil {
				return nil
			}
			condition.Values = append(condition.Values, value)

			if !p.currentTokenIs(TokenComma) {
				break
			}
			p.nextToken() // consume comma
		}

		if !p.expectToken(TokenRightParen) {
			return nil
		}
	} else {
		// Parse single value
		condition.Right = p.parseValue()
		if condition.Right == nil {
			return nil
		}
	}

	return condition
}

// parseValue parses a value (string, integer, or null)
func (p *Parser) parseValue() *Value {
	value := &Value{}

	switch p.currentToken.Type {
	case TokenString:
		value.Type = ValueTypeString
		value.StringVal = p.currentToken.Literal
		p.nextToken()
	case TokenInteger:
		value.Type = ValueTypeInteger
		if i, err := strconv.ParseInt(p.currentToken.Literal, 10, 64); err == nil {
			value.Integer = i
		} else {
			p.addError("invalid integer: %s", p.currentToken.Literal)
			return nil
		}
		p.nextToken()
	case TokenNull:
		value.Type = ValueTypeNull
		value.IsNull = true
		p.nextToken()
	default:
		p.addError("expected value (string, integer, or NULL)")
		return nil
	}

	return value
}

// parseGroupByClause parses GROUP BY clause
func (p *Parser) parseGroupByClause() *GroupByClause {
	if !p.expectToken(TokenGroupBy) {
		return nil
	}

	groupBy := &GroupByClause{}

	// Parse column list
	for {
		if p.currentTokenIs(TokenIdentifier) {
			groupBy.Columns = append(groupBy.Columns, p.currentToken.Literal)
			p.nextToken()
		} else {
			p.addError("expected column name in GROUP BY")
			return nil
		}

		if !p.currentTokenIs(TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return groupBy
}

// parseOrderByClause parses ORDER BY clause
func (p *Parser) parseOrderByClause() *OrderByClause {
	if !p.expectToken(TokenOrderBy) {
		return nil
	}

	orderBy := &OrderByClause{}

	// Parse column name (or "value" for Top-N queries)
	if p.currentTokenIs(TokenIdentifier) {
		orderBy.Column = p.currentToken.Literal
		p.nextToken()
	} else if p.currentTokenIs(TokenValue) {
		orderBy.Column = "value"
		p.nextToken()
	} else {
		p.addError("expected column name in ORDER BY")
		return nil
	}

	// Parse optional ASC/DESC
	if p.currentTokenIs(TokenAsc) {
		orderBy.Desc = false
		p.nextToken()
	} else if p.currentTokenIs(TokenDesc) {
		orderBy.Desc = true
		p.nextToken()
	}

	return orderBy
}

// ParseQuery is a convenience function to parse a BydbQL query string
func ParseQuery(query string) (*ParsedQuery, []string) {
	lexer := NewLexer(query)
	parser := NewParser(lexer)
	parsed := parser.Parse()
	return parsed, parser.Errors()
}