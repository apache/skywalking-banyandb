// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	// MaxSQLPatternBytes bounds the UTF-8 bytes in one LIKE or ILIKE pattern.
	MaxSQLPatternBytes = 1024
	// MaxSQLPatterns bounds LIKE and ILIKE conjuncts in one native request.
	MaxSQLPatterns = 16
)

// WildcardSyntax selects the operators and newline semantics of a pattern.
type WildcardSyntax uint8

const (
	// WildcardSyntaxGlob recognizes '*' and '?'. Wildcards exclude newline,
	// matching the existing native wildcard contract.
	WildcardSyntaxGlob WildcardSyntax = iota
	// WildcardSyntaxSQL recognizes '%' and '_', including newline.
	WildcardSyntaxSQL
)

// WildcardPattern describes a whole-value pattern. Escape is empty to disable
// escaping, or one Unicode scalar. The zero syntax retains native glob semantics;
// SQL callers select WildcardSyntaxSQL and supply their SQL escape character.
type WildcardPattern struct {
	Pattern    string
	Escape     string
	IgnoreCase bool
	Syntax     WildcardSyntax
}

// WildcardLimits bounds work, expanded terms and cumulative allocation across
// one worker request. Zero fields select the bounded server defaults.
type WildcardLimits struct{ MaxWork, MaxTerms, MaxBytes uint64 }

// WildcardStats reports cumulative work, matching terms and reserved bytes.
type WildcardStats struct{ Work, Terms, Bytes uint64 }

// WildcardBudget is shared by all native scans in one query execution. Parsed
// programs are cached only with an explicit memory owner; mutable automaton
// states remain local to one native call.
type WildcardBudget struct {
	reservePrograms  func(int64) error
	releasePrograms  func(int64)
	programs         map[WildcardPattern]*wildcardProgram
	limits           WildcardLimits
	work             atomic.Uint64
	terms            atomic.Uint64
	bytes            atomic.Uint64
	mu               sync.Mutex
	reservedPrograms int64
	closed           bool
}

// NewWildcardBudget constructs a concurrency-safe, query-owned budget.
func NewWildcardBudget(limits WildcardLimits) *WildcardBudget {
	if limits.MaxWork == 0 {
		limits.MaxWork = 1_000_000
	}
	if limits.MaxTerms == 0 {
		limits.MaxTerms = 100_000
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = 16 << 20
	}
	return &WildcardBudget{limits: limits}
}

// NewWildcardBudgetWithMemory retains immutable programs against an execution
// memory owner. Both callbacks must be supplied to enable caching. Close must run
// after all native searches finish, including error and cancellation exits.
func NewWildcardBudgetWithMemory(limits WildcardLimits, reserve func(int64) error, release func(int64)) *WildcardBudget {
	budget := NewWildcardBudget(limits)
	if reserve != nil && release != nil {
		budget.reservePrograms, budget.releasePrograms = reserve, release
	}
	return budget
}

// Close drops cached programs before releasing their execution-owned charge.
// It is idempotent and must not overlap native searches using this budget.
func (b *WildcardBudget) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.programs = nil
	if b.releasePrograms != nil {
		b.releasePrograms(b.reservedPrograms)
	}
	b.reservedPrograms = 0
	return nil
}

// Stats returns counters, which never exceed their configured limits.
func (b *WildcardBudget) Stats() WildcardStats {
	return WildcardStats{Work: b.work.Load(), Terms: b.terms.Load(), Bytes: b.bytes.Load()}
}

func spendWildcardPattern(counter *atomic.Uint64, limit, amount uint64) error {
	for {
		current := counter.Load()
		if amount > limit-current {
			return ErrQueryLimit
		}
		if counter.CompareAndSwap(current, current+amount) {
			return nil
		}
	}
}

func (b *WildcardBudget) spendWork(ctx context.Context, amount uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return spendWildcardPattern(&b.work, b.limits.MaxWork, amount)
}

func (b *WildcardBudget) reserve(ctx context.Context, amount uint64) error {
	if err := spendWildcardPattern(&b.bytes, b.limits.MaxBytes, amount); err != nil {
		return err
	}
	return query.ReserveAllocation(ctx, amount)
}

// ValidateWildcardPattern checks syntax without compiling or opening storage.
func ValidateWildcardPattern(pattern WildcardPattern) error {
	if pattern.Syntax != WildcardSyntaxGlob && pattern.Syntax != WildcardSyntaxSQL {
		return fmt.Errorf("wildcard syntax %d: %w", pattern.Syntax, ErrInvalidQuery)
	}
	if pattern.Syntax == WildcardSyntaxSQL && len(pattern.Pattern) > MaxSQLPatternBytes {
		return fmt.Errorf("wildcard pattern exceeds %d bytes: %w", MaxSQLPatternBytes, ErrQueryLimit)
	}
	if len(pattern.Escape) > utf8.UTFMax || !utf8.ValidString(pattern.Pattern) || !utf8.ValidString(pattern.Escape) || utf8.RuneCountInString(pattern.Escape) > 1 {
		return fmt.Errorf("wildcard pattern and escape require valid UTF-8 and at most one escape scalar: %w", ErrInvalidQuery)
	}
	escape, _ := utf8.DecodeRuneInString(pattern.Escape)
	escaped := false
	for _, value := range pattern.Pattern {
		if escaped {
			escaped = false
		} else if pattern.Escape != "" && value == escape {
			escaped = true
		}
	}
	if escaped {
		return fmt.Errorf("wildcard pattern ends with escape: %w", ErrInvalidQuery)
	}
	return nil
}

const (
	wildcardLiteral uint8 = iota
	wildcardAny
	wildcardStar
	wildcardAutomatonBytes = 1 << 20
)

type wildcardToken struct {
	value rune
	kind  uint8
}
type wildcardProgram struct {
	tokens       []wildcardToken
	literal      []byte
	prefix       []byte
	ignoreCase   bool
	matchNewline bool
	exact        bool
}

func foldWildcardRune(value rune) rune {
	smallest := value
	for next := unicode.SimpleFold(value); next != value; next = unicode.SimpleFold(next) {
		if next < smallest {
			smallest = next
		}
	}
	return smallest
}

func (p WildcardPattern) tokenKind(value rune) uint8 {
	switch {
	case p.Syntax == WildcardSyntaxSQL && value == '%', p.Syntax == WildcardSyntaxGlob && value == '*':
		return wildcardStar
	case p.Syntax == WildcardSyntaxSQL && value == '_', p.Syntax == WildcardSyntaxGlob && value == '?':
		return wildcardAny
	default:
		return wildcardLiteral
	}
}

func (b *WildcardBudget) program(ctx context.Context, pattern WildcardPattern) (*wildcardProgram, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateWildcardPattern(pattern); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("wildcard pattern budget is closed: %w", ErrInvalidQuery)
	}
	programBytes := uint64(len(pattern.Pattern))*16 + 512
	if program := b.programs[pattern]; program != nil {
		return program, nil
	}
	if programBytes > wildcardAutomatonBytes {
		return nil, fmt.Errorf("wildcard program exceeds automaton allowance: %w", ErrQueryLimit)
	}
	if err := b.spendWork(ctx, uint64(len(pattern.Pattern))+1); err != nil {
		return nil, err
	}
	// Include program, maximum token and literal buffers, and a conservative
	// bounded map allowance before any of them are allocated.
	if err := spendWildcardPattern(&b.bytes, b.limits.MaxBytes, programBytes); err != nil {
		return nil, err
	}
	if b.reservePrograms != nil {
		if err := b.reservePrograms(int64(programBytes)); err != nil {
			return nil, err
		}
		b.reservedPrograms += int64(programBytes)
	} else if err := query.ReserveAllocation(ctx, programBytes); err != nil {
		return nil, err
	}
	program := &wildcardProgram{
		ignoreCase: pattern.IgnoreCase, matchNewline: pattern.Syntax == WildcardSyntaxSQL, exact: !pattern.IgnoreCase,
		tokens: make([]wildcardToken, 0, len(pattern.Pattern)), literal: make([]byte, 0, len(pattern.Pattern)),
	}
	escape, _ := utf8.DecodeRuneInString(pattern.Escape)
	escaped := false
	prefixEnd := -1
	for _, value := range pattern.Pattern {
		if !escaped && pattern.Escape != "" && value == escape {
			escaped = true
			continue
		}
		token := wildcardToken{value: value, kind: wildcardLiteral}
		if !escaped {
			token.kind = pattern.tokenKind(value)
		}
		escaped = false
		if token.kind != wildcardLiteral {
			if prefixEnd < 0 {
				prefixEnd = len(program.literal)
			}
			program.exact = false
		} else {
			program.literal = utf8.AppendRune(program.literal, value)
			if pattern.IgnoreCase {
				token.value = foldWildcardRune(value)
			}
		}
		if token.kind == wildcardStar && len(program.tokens) > 0 && program.tokens[len(program.tokens)-1].kind == wildcardStar {
			continue
		}
		program.tokens = append(program.tokens, token)
	}
	if !pattern.IgnoreCase && prefixEnd > 0 {
		program.prefix = program.literal[:prefixEnd]
	}
	if b.reservePrograms != nil {
		if b.programs == nil {
			b.programs = make(map[WildcardPattern]*wildcardProgram)
		}
		// A binding or legacy byte-to-string conversion may belong to a shorter
		// source scope. Cache keys must own their backing strings under the
		// retained program reservation, including any escape character.
		pattern.Pattern = strings.Clone(pattern.Pattern)
		pattern.Escape = strings.Clone(pattern.Escape)
		b.programs[pattern] = program
	}
	return program, nil
}
