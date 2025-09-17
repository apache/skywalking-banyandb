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
	"unicode"
	"unicode/utf8"
)

// TokenType represents the type of a token
type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenIllegal

	// Literals
	TokenIdentifier
	TokenString
	TokenInteger

	// Keywords
	TokenSelect
	TokenFrom
	TokenWhere
	TokenOrderBy
	TokenGroupBy
	TokenLimit
	TokenOffset
	TokenTime
	TokenBetween
	TokenAnd
	TokenOr
	TokenIn
	TokenNotIn
	TokenHaving
	TokenNotHaving
	TokenMatch
	TokenShow
	TokenTop
	TokenStream
	TokenMeasure
	TokenTrace
	TokenProperty
	TokenAsc
	TokenDesc
	TokenWith
	TokenQueryTrace
	TokenNull
	TokenAggregateBy

	// Aggregate functions
	TokenSum
	TokenMean
	TokenAvg
	TokenCount
	TokenMax
	TokenMin

	// Operators
	TokenEqual
	TokenNotEqual
	TokenGreater
	TokenLess
	TokenGreaterEqual
	TokenLessEqual

	// Punctuation
	TokenComma
	TokenSemicolon
	TokenLeftParen
	TokenRightParen
	TokenDot
	TokenDoubleColon

	// Special literals
	TokenStar
	TokenBy
	TokenValue
)

// Token represents a lexical token
type Token struct {
	Type     TokenType
	Literal  string
	Line     int
	Column   int
	Position int
}

func (t Token) String() string {
	return fmt.Sprintf("Token{Type: %s, Literal: %s, Line: %d, Column: %d}",
		t.Type.String(), t.Literal, t.Line, t.Column)
}

func (tt TokenType) String() string {
	switch tt {
	case TokenEOF:
		return "EOF"
	case TokenIllegal:
		return "ILLEGAL"
	case TokenIdentifier:
		return "IDENTIFIER"
	case TokenString:
		return "STRING"
	case TokenInteger:
		return "INTEGER"
	case TokenSelect:
		return "SELECT"
	case TokenFrom:
		return "FROM"
	case TokenWhere:
		return "WHERE"
	case TokenOrderBy:
		return "ORDER BY"
	case TokenGroupBy:
		return "GROUP BY"
	case TokenLimit:
		return "LIMIT"
	case TokenOffset:
		return "OFFSET"
	case TokenTime:
		return "TIME"
	case TokenBetween:
		return "BETWEEN"
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenIn:
		return "IN"
	case TokenNotIn:
		return "NOT IN"
	case TokenHaving:
		return "HAVING"
	case TokenNotHaving:
		return "NOT HAVING"
	case TokenMatch:
		return "MATCH"
	case TokenShow:
		return "SHOW"
	case TokenTop:
		return "TOP"
	case TokenStream:
		return "STREAM"
	case TokenMeasure:
		return "MEASURE"
	case TokenTrace:
		return "TRACE"
	case TokenProperty:
		return "PROPERTY"
	case TokenAsc:
		return "ASC"
	case TokenDesc:
		return "DESC"
	case TokenWith:
		return "WITH"
	case TokenQueryTrace:
		return "QUERY_TRACE"
	case TokenNull:
		return "NULL"
	case TokenAggregateBy:
		return "AGGREGATE BY"
	case TokenSum:
		return "SUM"
	case TokenMean:
		return "MEAN"
	case TokenAvg:
		return "AVG"
	case TokenCount:
		return "COUNT"
	case TokenMax:
		return "MAX"
	case TokenMin:
		return "MIN"
	case TokenEqual:
		return "="
	case TokenNotEqual:
		return "!="
	case TokenGreater:
		return ">"
	case TokenLess:
		return "<"
	case TokenGreaterEqual:
		return ">="
	case TokenLessEqual:
		return "<="
	case TokenComma:
		return ","
	case TokenSemicolon:
		return ";"
	case TokenLeftParen:
		return "("
	case TokenRightParen:
		return ")"
	case TokenDot:
		return "."
	case TokenDoubleColon:
		return "::"
	case TokenStar:
		return "*"
	case TokenBy:
		return "BY"
	case TokenValue:
		return "VALUE"
	default:
		return "UNKNOWN"
	}
}

// keywords maps keyword strings to their token types
var keywords = map[string]TokenType{
	"select":      TokenSelect,
	"from":        TokenFrom,
	"where":       TokenWhere,
	"order":       TokenOrderBy, // handled specially as "order by"
	"group":       TokenGroupBy, // handled specially as "group by"
	"limit":       TokenLimit,
	"offset":      TokenOffset,
	"time":        TokenTime,
	"between":     TokenBetween,
	"and":         TokenAnd,
	"or":          TokenOr,
	"in":          TokenIn,
	"not":         TokenNotIn, // handled specially
	"having":      TokenHaving,
	"match":       TokenMatch,
	"show":        TokenShow,
	"top":         TokenTop,
	"stream":      TokenStream,
	"measure":     TokenMeasure,
	"trace":       TokenTrace,
	"property":    TokenProperty,
	"asc":         TokenAsc,
	"desc":        TokenDesc,
	"with":        TokenWith,
	"query_trace": TokenQueryTrace,
	"null":        TokenNull,
	"aggregate":   TokenAggregateBy, // handled specially as "aggregate by"
	"sum":         TokenSum,
	"mean":        TokenMean,
	"avg":         TokenAvg,
	"count":       TokenCount,
	"max":         TokenMax,
	"min":         TokenMin,
	"by":          TokenBy,
	"value":       TokenValue,
}

// Lexer represents a lexical analyzer
type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
	line         int
	column       int
}

// NewLexer creates a new lexer instance
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++

	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

// peekChar returns the next character without advancing position
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readIdentifier reads an identifier or keyword
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '-' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a numeric literal
func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readString reads a string literal
func (l *Lexer) readString(delimiter byte) (string, error) {
	position := l.position + 1 // skip opening quote
	for {
		l.readChar()
		if l.ch == delimiter || l.ch == 0 {
			break
		}
		if l.ch == '\\' && l.peekChar() == delimiter {
			l.readChar() // skip escaped quote
		}
	}

	if l.ch == 0 {
		return "", fmt.Errorf("unterminated string literal at line %d, column %d", l.line, l.column)
	}

	str := l.input[position:l.position]
	l.readChar() // skip closing quote
	return str, nil
}

// lookupIdent checks if an identifier is a keyword
func (l *Lexer) lookupIdent(ident string) TokenType {
	lower := strings.ToLower(ident)
	if tok, ok := keywords[lower]; ok {
		// Handle compound keywords
		switch lower {
		case "order":
			if l.peekAhead("by") {
				l.consumeKeyword("by")
				return TokenOrderBy
			}
		case "group":
			if l.peekAhead("by") {
				l.consumeKeyword("by")
				return TokenGroupBy
			}
		case "aggregate":
			if l.peekAhead("by") {
				l.consumeKeyword("by")
				return TokenAggregateBy
			}
		case "not":
			if l.peekAhead("in") {
				l.consumeKeyword("in")
				return TokenNotIn
			} else if l.peekAhead("having") {
				l.consumeKeyword("having")
				return TokenNotHaving
			}
		}
		return tok
	}
	return TokenIdentifier
}

// peekAhead checks if the next identifier matches the expected keyword
func (l *Lexer) peekAhead(expected string) bool {
	savedPos := l.position
	savedReadPos := l.readPosition
	savedCh := l.ch
	savedLine := l.line
	savedColumn := l.column

	l.skipWhitespace()

	if !isLetter(l.ch) {
		// Restore lexer state
		l.position = savedPos
		l.readPosition = savedReadPos
		l.ch = savedCh
		l.line = savedLine
		l.column = savedColumn
		return false
	}

	ident := l.readIdentifier()
	match := strings.EqualFold(ident, expected)

	if !match {
		// Restore lexer state
		l.position = savedPos
		l.readPosition = savedReadPos
		l.ch = savedCh
		l.line = savedLine
		l.column = savedColumn
	} else {
		// When there's a match, we need to restore the lexer state
		// because consumeKeyword will consume the keyword again
		l.position = savedPos
		l.readPosition = savedReadPos
		l.ch = savedCh
		l.line = savedLine
		l.column = savedColumn
	}

	return match
}

// consumeKeyword consumes the expected keyword
func (l *Lexer) consumeKeyword(expected string) {
	l.skipWhitespace()
	l.readIdentifier() // consume the keyword
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	var tok Token
	tok.Line = l.line
	tok.Column = l.column
	tok.Position = l.position

	l.skipWhitespace()

	switch l.ch {
	case '=':
		tok.Type = TokenEqual
		tok.Literal = "="
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok.Type = TokenNotEqual
			tok.Literal = string(ch) + string(l.ch)
		} else {
			tok.Type = TokenIllegal
			tok.Literal = string(l.ch)
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok.Type = TokenGreaterEqual
			tok.Literal = string(ch) + string(l.ch)
		} else {
			tok.Type = TokenGreater
			tok.Literal = ">"
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok.Type = TokenLessEqual
			tok.Literal = string(ch) + string(l.ch)
		} else {
			tok.Type = TokenLess
			tok.Literal = "<"
		}
	case ',':
		tok.Type = TokenComma
		tok.Literal = ","
	case ';':
		tok.Type = TokenSemicolon
		tok.Literal = ";"
	case '(':
		tok.Type = TokenLeftParen
		tok.Literal = "("
	case ')':
		tok.Type = TokenRightParen
		tok.Literal = ")"
	case '.':
		tok.Type = TokenDot
		tok.Literal = "."
	case ':':
		if l.peekChar() == ':' {
			l.readChar()
			tok.Type = TokenDoubleColon
			tok.Literal = "::"
		} else {
			tok.Type = TokenIllegal
			tok.Literal = ":"
		}
	case '*':
		tok.Type = TokenStar
		tok.Literal = "*"
	case '\'', '"':
		str, err := l.readString(l.ch)
		if err != nil {
			tok.Type = TokenIllegal
			tok.Literal = err.Error()
		} else {
			tok.Type = TokenString
			tok.Literal = str
		}
	case 0:
		tok.Type = TokenEOF
		tok.Literal = ""
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = l.lookupIdent(tok.Literal)
			return tok // return early to avoid readChar()
		} else if isDigit(l.ch) {
			tok.Type = TokenInteger
			tok.Literal = l.readNumber()
			return tok // return early to avoid readChar()
		} else {
			tok.Type = TokenIllegal
			tok.Literal = string(l.ch)
		}
	}

	l.readChar()
	return tok
}

// GetAllTokens returns all tokens from the input
func (l *Lexer) GetAllTokens() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens
}

// isLetter checks if the character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if the character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// isAlphaNumeric checks if the character is alphanumeric
func isAlphaNumeric(ch byte) bool {
	return isLetter(ch) || isDigit(ch)
}

// ParseInteger parses a token literal as integer
func ParseInteger(literal string) (int64, error) {
	return strconv.ParseInt(literal, 10, 64)
}

// IsUnicodeIdentifier checks if a rune can be part of a Unicode identifier
func IsUnicodeIdentifier(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// IsUnicodeIdentifierStart checks if a rune can start a Unicode identifier
func IsUnicodeIdentifierStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

// ReadUnicodeIdentifier reads a Unicode identifier
func (l *Lexer) ReadUnicodeIdentifier() string {
	start := l.position
	for l.position < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.position:])
		if r == utf8.RuneError || !IsUnicodeIdentifier(r) {
			break
		}
		l.position += size
		l.readPosition = l.position + 1
		if l.position < len(l.input) {
			l.ch = l.input[l.position]
		} else {
			l.ch = 0
		}
	}
	return l.input[start:l.position]
}