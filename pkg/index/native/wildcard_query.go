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
	"math"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

type compiledTermPatterns []*wildcardAutomaton

func defaultWildcardBudget(requests []TermSetRequest) *WildcardBudget {
	for _, request := range requests {
		for _, pattern := range request.WildcardPatterns {
			if pattern.Syntax == WildcardSyntaxSQL {
				return NewWildcardBudget(WildcardLimits{})
			}
		}
	}
	// Existing native selectors, including the series universe, already supply
	// MaxTerms. Preserve that contract while bounding compilation and allocation.
	return NewWildcardBudget(WildcardLimits{MaxWork: math.MaxUint64, MaxTerms: math.MaxUint64})
}

func compileTermPatterns(ctx context.Context, request TermSetRequest, shared *WildcardBudget) (compiledTermPatterns, error) {
	count := len(request.Wildcard) + len(request.WildcardPatterns)
	if count == 0 {
		return nil, nil
	}
	var sqlCount int
	for _, pattern := range request.WildcardPatterns {
		if pattern.Syntax == WildcardSyntaxSQL {
			sqlCount++
		}
	}
	if sqlCount > MaxSQLPatterns {
		return nil, fmt.Errorf("SQL wildcard pattern count %d exceeds limit %d: %w", sqlCount, MaxSQLPatterns, ErrQueryLimit)
	}
	budget := request.PatternBudget
	if budget == nil {
		budget = shared
	}
	if budget == nil {
		budget = defaultWildcardBudget([]TermSetRequest{request})
	}
	if err := budget.reserve(ctx, uint64(count)*8); err != nil {
		return nil, err
	}
	result := make(compiledTermPatterns, 0, count)
	compile := func(pattern WildcardPattern) error {
		automaton, err := newWildcardAutomaton(ctx, budget, pattern)
		if err != nil {
			return err
		}
		result = append(result, automaton)
		return nil
	}
	for _, pattern := range request.Wildcard {
		// Converting caller-owned bytes to an immutable cache key allocates before
		// compilation. Charge that copy even when the program is already cached.
		if err := budget.reserve(ctx, wildcardAllocationSize(uint64(len(pattern)))); err != nil {
			return nil, err
		}
		if err := compile(WildcardPattern{Pattern: string(pattern)}); err != nil {
			return nil, err
		}
	}
	for _, pattern := range request.WildcardPatterns {
		if err := compile(pattern); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func wildcardCandidates(ctx context.Context, segment *memorySegment, request TermSetRequest,
	pattern *wildcardAutomaton, expanded *uint64,
) (*roaringpkg.Bitmap, error) {
	if err := pattern.budget.spendWork(ctx, 1); err != nil {
		return nil, err
	}
	if pattern.program.exact {
		bitmap, found, err := segment.handle.reader.TermPostingBitmapContext(ctx, request.Field, pattern.program.literal)
		if err != nil {
			return nil, err
		}
		if !found {
			return roaringpkg.New(), nil
		}
		if err = pattern.expand(expanded, request.MaxTerms); err != nil {
			return nil, err
		}
		return bitmap, nil
	}
	iterator, err := segment.handle.reader.NewDictionaryTermIterator(request.Field, pattern, pattern.program.prefix, successor(pattern.program.prefix))
	if iterator != nil {
		defer func() { _ = iterator.Close() }()
	}
	if pattern.err != nil {
		return nil, pattern.err
	}
	if err != nil {
		return nil, err
	}
	result := roaringpkg.New()
	for {
		term, present, nextErr := iterator.NextKey()
		if pattern.err != nil {
			return nil, pattern.err
		}
		if nextErr != nil {
			return nil, nextErr
		}
		if !present {
			return result, nil
		}
		if err = pattern.expand(expanded, request.MaxTerms); err != nil {
			return nil, err
		}
		posting, found, postingErr := segment.handle.reader.TermPostingBitmapContext(ctx, request.Field, term)
		if postingErr != nil {
			return nil, postingErr
		}
		if found {
			if err = reserveBitmapMerge(ctx, result, posting); err != nil {
				return nil, err
			}
			result.Or(posting)
			if exceedsCandidates(result, request.MaxCandidates) {
				return nil, fmt.Errorf("wildcard candidate count %d exceeds limit %d: %w", result.GetCardinality(), request.MaxCandidates, ErrQueryLimit)
			}
		}
	}
}

func (a *wildcardAutomaton) expand(expanded *uint64, localLimit uint64) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	if localLimit != 0 && *expanded >= localLimit {
		return fmt.Errorf("wildcard request expanded terms exceed limit %d: %w", localLimit, ErrQueryLimit)
	}
	if err := spendWildcardPattern(&a.budget.terms, a.budget.limits.MaxTerms, 1, "expanded terms"); err != nil {
		return err
	}
	*expanded++
	return nil
}
