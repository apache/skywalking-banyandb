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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	vellumregexp "github.com/blevesearch/vellum/regexp"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestNativeWildcardAllocationAndWorkBudgets(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, tc := range []struct {
			resource string
			limits   WildcardLimits
		}{
			{"work", WildcardLimits{MaxWork: 1}},
			{"expanded terms", WildcardLimits{MaxTerms: 1}},
			{"allocation bytes", WildcardLimits{MaxBytes: 1}},
		} {
			budget := NewWildcardBudget(tc.limits)
			_, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte("*")}, PatternBudget: budget, MaxTerms: math.MaxUint64})
			require.ErrorIs(t, err, ErrQueryLimit)
			require.ErrorContains(t, err, "wildcard "+tc.resource+" limit exceeded")
			require.ErrorContains(t, err, "limit 1")
			require.True(t, strings.HasSuffix(err.Error(), ": "+ErrQueryLimit.Error()), "remote receive mapping requires the canonical sentinel")
		}
		budget := NewWildcardBudget(WildcardLimits{MaxWork: 32})
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte("*never*")},
			PatternBudget: budget,
			MaxTerms:      math.MaxUint64,
		})
		require.ErrorIs(t, err, ErrQueryLimit)
		require.Zero(t, budget.Stats().Terms)
		allocationErr := errors.New("wildcard allocation rejected")
		ctx, scope := query.WithAllocationScope(context.Background(), func(int64) error { return allocationErr }, func(int64) {})
		_, err = view.MatchTermsSet(ctx, TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte("*never*")}, MaxTerms: math.MaxUint64})
		require.ErrorIs(t, err, allocationErr)
		require.NoError(t, scope.Close())
	})
}

func TestNativeWildcardCancellationInsideNoMatchWalk(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		body := func(t *testing.T, view *ReadView) {
			ctx := &wildcardCancelContext{Context: context.Background()}
			_, err := view.MatchTermsSet(ctx, TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte("*never*")}, MaxTerms: math.MaxUint64})
			require.ErrorIs(t, err, context.Canceled)
		}
		if persisted {
			withPersistedPatternTermView(t, body)
		} else {
			withPatternTermView(t, nil, body)
		}
	}
}

func TestNativeWildcardManyAndLongPatterns(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	long := strings.Repeat("x", 1200)
	documents := []Document{{Identifier: []byte("long"), Fields: []Field{{Name: "name", Value: []byte(long), Index: true}}}}
	patterns := [][]byte{[]byte(long)}
	for index := 0; index < 24; index++ {
		value := fmt.Sprintf("name-%d", index)
		documents = append(documents, Document{Identifier: []byte(value), Fields: []Field{{Name: "name", Value: []byte(value), Index: true}}})
		patterns = append(patterns, []byte(value))
	}
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Wildcard: patterns, MaxTerms: math.MaxUint64})
	require.NoError(t, err)
	require.Len(t, hits, len(documents))
	var owned int64
	budget := NewWildcardBudgetWithMemory(WildcardLimits{}, func(bytes int64) error {
		owned += bytes
		return nil
	}, func(bytes int64) { owned -= bytes })
	defer func() { require.NoError(t, budget.Close()) }()
	for range 2 {
		hits, err = view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: patterns, MaxTerms: math.MaxUint64, PatternBudget: budget,
		})
		require.NoError(t, err)
		require.Len(t, hits, len(documents))
		require.Len(t, budget.programs, len(patterns), "legacy caches are not constrained by the SQL conjunct limit")
	}
	require.Positive(t, owned)
	require.NoError(t, budget.Close())
	require.Zero(t, owned)
}

func TestNativeWildcardLegacyCompilerCompatibility(t *testing.T) {
	values := []string{"", "é", "💙", "\x00", "\r", "\n", "\u2028", "�", "a\nb", "a*b", `a\b`, `a\FOOb`, "a%b", "a_b", "a|b", "a+b", "a?b", string([]byte{0xff})}
	patterns := []string{"", "*", "?", "a?b", "a*b", "\n", "a\nb", `a\*b`, `a\?b`, "%", "_", "a|b", "a+b", "*é*", "💙", "\x00", "\r", "\u2028", "�"}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	documents := make([]Document, len(values))
	for index, value := range values {
		documents[index] = Document{Identifier: []byte(fmt.Sprint(index)), Fields: []Field{{Name: "name", Value: []byte(value), Index: true}}}
	}
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	// Preserve the old compiler only as an independent compatibility oracle.
	legacy := strings.NewReplacer(
		"+", `\+`, "(", `\(`, ")", `\)`, "^", `\^`, "$", `\$`, ".", `\.`, "{", `\{`, "}", `\}`,
		"[", `\[`, "]", `\]`, "|", `\|`, `\`, `\\`, "*", ".*", "?", ".",
	)
	for _, pattern := range patterns {
		t.Run(fmt.Sprintf("%q", pattern), func(t *testing.T) {
			automaton, compileErr := vellumregexp.New(legacy.Replace(pattern))
			require.NoError(t, compileErr)
			var expected []string
			for index, value := range values {
				state := automaton.Start()
				for _, input := range []byte(value) {
					state = automaton.Accept(state, input)
				}
				if automaton.IsMatch(state) {
					expected = append(expected, fmt.Sprint(index))
				}
			}
			hits, matchErr := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte(pattern)}, MaxTerms: math.MaxUint64})
			require.NoError(t, matchErr)
			require.ElementsMatch(t, expected, queryIDs(hits))
		})
	}
	_, err = view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Wildcard: [][]byte{{0xff}}, MaxTerms: math.MaxUint64})
	require.ErrorIs(t, err, ErrInvalidQuery)
}

func TestNativeWildcardExtendedGlobAndMixedBudget(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, pattern := range []WildcardPattern{
			{Pattern: "APPL*", IgnoreCase: true},
			{Pattern: "!appl*", Escape: "!"},
			{Pattern: "APPL%", Syntax: WildcardSyntaxSQL, IgnoreCase: true},
		} {
			hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", WildcardPatterns: []WildcardPattern{pattern}, MaxTerms: math.MaxUint64,
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(hits))
		}
		budget := NewWildcardBudget(WildcardLimits{MaxTerms: 3})
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: math.MaxUint64,
			WildcardPatterns: []WildcardPattern{{Pattern: "appl%", Syntax: WildcardSyntaxSQL}}, PatternBudget: budget,
		})
		require.ErrorIs(t, err, ErrQueryLimit)
		require.EqualValues(t, 3, budget.Stats().Terms)
	})
}

func TestNativeWildcardTypedGlobRequiresMaxTerms(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, budget := range []*WildcardBudget{nil, NewWildcardBudget(WildcardLimits{})} {
			_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", WildcardPatterns: []WildcardPattern{{Pattern: "appl*"}}, PatternBudget: budget,
			})
			require.ErrorIs(t, err, ErrQueryLimit, "typed glob has the same expansion-bound contract as legacy wildcard bytes")
			hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", WildcardPatterns: []WildcardPattern{{Pattern: "appl%", Syntax: WildcardSyntaxSQL}}, PatternBudget: budget,
			})
			require.NoError(t, err, "SQL patterns retain their execution-wide default budget")
			require.Len(t, hits, 2)
		}
	})
}

func TestNativeWildcardLongStateBitset(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		// More than 1,024 wildcard positions require more bits than the SQL
		// frontend's maximum-size program. Legacy patterns have no SQL byte cap.
		hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte(strings.Repeat("?", 1100))}, MaxTerms: math.MaxUint64,
		})
		require.NoError(t, err)
		require.Empty(t, hits)
	})
}

func TestNativeWildcardLongExactAutomatonCeiling(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, length := range []int{65500, 65536} {
			_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", Wildcard: [][]byte{[]byte(strings.Repeat("x", length))}, MaxTerms: math.MaxUint64,
			})
			require.ErrorIs(t, err, ErrQueryLimit, "exact lookup still reserves the complete automaton object")
			require.ErrorContains(t, err, "automaton")
			require.ErrorContains(t, err, "limit 1048576")
		}
	})
}

func TestNativeWildcardRequestLimitDiagnostics(t *testing.T) {
	patterns := make([]WildcardPattern, MaxSQLPatterns+1)
	for index := range patterns {
		patterns[index] = WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%"}
	}
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, tc := range []struct {
			message string
			request TermSetRequest
		}{
			{"SQL wildcard pattern count 17 exceeds limit 16", TermSetRequest{WildcardPatterns: patterns}},
			{"wildcard request expanded terms exceed limit 1", TermSetRequest{Wildcard: [][]byte{[]byte("*")}, MaxTerms: 1}},
			{"wildcard candidate count 2 exceeds limit 1", TermSetRequest{Wildcard: [][]byte{[]byte("*")}, MaxTerms: math.MaxUint64, MaxCandidates: 1}},
		} {
			t.Run(tc.message, func(t *testing.T) {
				tc.request.Field = "name"
				_, err := view.MatchTermsSet(context.Background(), tc.request)
				require.ErrorIs(t, err, ErrQueryLimit)
				require.ErrorContains(t, err, tc.message)
				require.True(t, strings.HasSuffix(err.Error(), ": "+ErrQueryLimit.Error()))
			})
		}
	})
}
