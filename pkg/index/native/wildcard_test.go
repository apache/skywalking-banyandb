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
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestNativeWildcardPatternSemantics(t *testing.T) {
	values := []string{"", "Claude-Sonnet", "claude-sonnet", "a_b", "a-b", "a中b", "a\nb", "20%", "a*b", `a\b`, "K", "k", "ß", "ss", string([]byte{'a', 0xff})}
	cases := []struct {
		pattern WildcardPattern
		want    []string
	}{
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "", Escape: `\`}, []string{""}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%", Escape: `\`}, values[:len(values)-1]},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%clau%", IgnoreCase: true}, []string{"Claude-Sonnet", "claude-sonnet"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%clau%"}, []string{"claude-sonnet"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%ude-Son%"}, []string{"Claude-Sonnet"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "Claude"}, nil},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "a_b"}, []string{"a_b", "a-b", "a中b", "a\nb", "a*b", `a\b`}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "a!_b", Escape: "!"}, []string{"a_b"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "20!%", Escape: "!"}, []string{"20%"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "a*b"}, []string{"a*b"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: `a\b`}, []string{`a\b`}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "k", IgnoreCase: true}, []string{"K", "k"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "ss", IgnoreCase: true}, []string{"ss"}},
		{WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "%%%a%%_%%b%%%"}, []string{"a_b", "a-b", "a中b", "a\nb", "a*b", `a\b`}},
	}
	for _, lifecycle := range []string{"admitted", "merged", "reopened"} {
		t.Run(lifecycle, func(t *testing.T) {
			path := t.TempDir()
			owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
			require.NoError(t, err)
			docs := make([]Document, 0, len(values))
			for i, value := range values {
				docs = append(docs, Document{Identifier: []byte(fmt.Sprint(i)), Fields: []Field{{Name: "name", Value: []byte(value), Index: true, Store: true}}})
			}
			require.NoError(t, owner.Batch(context.Background(), Batch{Documents: docs}))
			if lifecycle == "merged" {
				require.NoError(t, owner.forceMergeAll(context.Background()))
			}
			if lifecycle == "reopened" {
				require.NoError(t, owner.Close())
				owner, err = NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
				require.NoError(t, err)
			}
			defer func() { require.NoError(t, owner.Close()) }()
			view, acquireErr := owner.Acquire(context.Background())
			require.NoError(t, acquireErr)
			defer func() { require.NoError(t, view.Close()) }()
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%q/%t", tc.pattern.Pattern, tc.pattern.IgnoreCase), func(t *testing.T) {
					hits, matchErr := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", WildcardPatterns: []WildcardPattern{tc.pattern}})
					require.NoError(t, matchErr)
					var got []string
					for _, hit := range hits {
						projection, projectErr := view.ProjectHit(context.Background(), hit, "name")
						require.NoError(t, projectErr)
						got = append(got, string(projection.Fields["name"][0]))
					}
					require.ElementsMatch(t, tc.want, got)
				})
			}
		})
	}
}

func TestNativeWildcardPatternValidation(t *testing.T) {
	for _, pattern := range []WildcardPattern{
		{Syntax: WildcardSyntaxSQL, Pattern: string([]byte{0xff})},
		{Syntax: WildcardSyntaxSQL, Pattern: "a!", Escape: "!"},
		{Syntax: WildcardSyntaxSQL, Escape: "ab"},
		{Syntax: WildcardSyntaxSQL, Escape: string([]byte{0xff})},
	} {
		require.ErrorIs(t, ValidateWildcardPattern(pattern), ErrInvalidQuery)
	}
	require.NoError(t, ValidateWildcardPattern(WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: strings.Repeat("x", MaxSQLPatternBytes)}))
	err := ValidateWildcardPattern(WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: strings.Repeat("x", MaxSQLPatternBytes+1)})
	require.ErrorIs(t, err, ErrQueryLimit)
	require.ErrorContains(t, err, "SQL wildcard pattern length 1025 exceeds 1024 bytes")
	require.NoError(t, ValidateWildcardPattern(WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: "a☃_b", Escape: "☃"}))
}

func TestNativeWildcardBudgetsAndCleanup(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		for _, limits := range []WildcardLimits{{MaxWork: 1}, {MaxTerms: 1}, {MaxBytes: 1}} {
			budget := NewWildcardBudget(limits)
			_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field:            "name",
				WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%"}},
				PatternBudget:    budget,
			})
			require.ErrorIs(t, err, ErrQueryLimit)
		}
		budget := NewWildcardBudget(WildcardLimits{MaxWork: 32})
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field:            "name",
			WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%never%"}},
			PatternBudget:    budget,
		})
		require.ErrorIs(t, err, ErrQueryLimit)
		require.Zero(t, budget.Stats().Terms)

		allocationErr := errors.New("test allocation allowance")
		var reserved int64
		ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error {
			if reserved+bytes > 3000 {
				return allocationErr
			}
			reserved += bytes
			return nil
		}, func(bytes int64) { reserved -= bytes })
		_, err = view.MatchTermsSet(ctx, TermSetRequest{Field: "name", WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%no-match%"}}})
		require.ErrorIs(t, err, allocationErr)
		require.NoError(t, scope.Close())
		require.Zero(t, reserved)
	})
}

func TestNativeWildcardPatternIntersectionAndPaging(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	for batch := 0; batch < 2; batch++ {
		var documents []Document
		for index := 0; index < 3; index++ {
			name := fmt.Sprintf("name-%d", batch*3+index)
			documents = append(documents, Document{Identifier: []byte(name), Fields: []Field{{Name: "name", Value: []byte(name), Index: true, Sort: true}}})
		}
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	selection := TermSetRequest{
		Field:            "name",
		WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "name-%"}},
		PatternBudget:    NewWildcardBudget(WildcardLimits{MaxTerms: 5}),
	}
	_, err = view.MatchTermsSet(context.Background(), selection)
	require.ErrorIs(t, err, ErrQueryLimit)
	require.EqualValues(t, 5, selection.PatternBudget.Stats().Terms)
	selection.PatternBudget = NewWildcardBudget(WildcardLimits{})
	cursor, err := view.NewSortCursor(context.Background(), SortCursorRequest{Selection: selection, SortField: "name", PageSize: 2})
	require.NoError(t, err)
	defer func() { require.NoError(t, cursor.Close()) }()
	for page := 0; page < 3; page++ {
		hits, pageErr := cursor.NextPage(context.Background())
		require.NoError(t, pageErr)
		require.Equal(t, []string{fmt.Sprintf("name-%d", page*2), fmt.Sprintf("name-%d", page*2+1)}, sortedIDs(hits))
	}
	hits, err := view.MatchAllTermSets(context.Background(), []TermSetRequest{
		{Field: "name", WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "NAME-%", IgnoreCase: true}}},
		{Field: "name", WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%2"}}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"name-2"}, queryIDs(hits))
	filtered, err := view.FilterTermsSet(context.Background(), hits, TermSetRequest{
		Field:            "name",
		WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%3"}},
	})
	require.NoError(t, err)
	require.Empty(t, filtered)
}

type wildcardCancelContext struct {
	context.Context
	checks int
}

func (c *wildcardCancelContext) Err() error {
	c.checks++
	if c.checks > 20 {
		return context.Canceled
	}
	return nil
}

func TestNativeWildcardPatternCancellationInsideNoMatchWalk(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		body := func(t *testing.T, view *ReadView) {
			ctx := &wildcardCancelContext{Context: context.Background()}
			_, err := view.MatchTermsSet(ctx, TermSetRequest{Field: "name", WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%never%"}}})
			require.ErrorIs(t, err, context.Canceled)
		}
		if persisted {
			withPersistedPatternTermView(t, body)
		} else {
			withPatternTermView(t, nil, body)
		}
	}
}

func wildcardOracle(pattern WildcardPattern, value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	source, expression := []rune(value), []rune(pattern.Pattern)
	escape, _ := utf8.DecodeRuneInString(pattern.Escape)
	type pair struct{ pattern, source int }
	known := make(map[pair]bool)
	results := make(map[pair]bool)
	var match func(int, int) bool
	match = func(p, s int) bool {
		key := pair{p, s}
		if known[key] {
			return results[key]
		}
		known[key] = true
		result := false
		switch {
		case p == len(expression):
			result = s == len(source)
		case pattern.Escape != "" && expression[p] == escape:
			if s < len(source) && p+1 < len(expression) {
				equal := expression[p+1] == source[s]
				if pattern.IgnoreCase {
					equal = strings.EqualFold(string(expression[p+1]), string(source[s]))
				}
				result = equal && match(p+2, s+1)
			}
		case expression[p] == '%':
			result = match(p+1, s) || (s < len(source) && match(p, s+1))
		case expression[p] == '_':
			result = s < len(source) && match(p+1, s+1)
		default:
			if s < len(source) {
				equal := expression[p] == source[s]
				if pattern.IgnoreCase {
					equal = strings.EqualFold(string(expression[p]), string(source[s]))
				}
				result = equal && match(p+1, s+1)
			}
		}
		results[key] = result
		return result
	}
	return match(0, 0)
}

func TestNativeWildcardPatternIndependentOracle(t *testing.T) {
	random := rand.New(rand.NewPCG(71, 13)) //nolint:gosec // Reproducible neutral test corpus, not security material.
	alphabet := []rune("aA_-%!中\nßKk")
	generate := func() string {
		length := random.IntN(8)
		var value strings.Builder
		for range length {
			value.WriteRune(alphabet[random.IntN(len(alphabet))])
		}
		return value.String()
	}
	values := make([]string, 80)
	documents := make([]Document, len(values))
	for i := range values {
		values[i] = generate()
		documents[i] = Document{Identifier: []byte(fmt.Sprint(i)), Fields: []Field{{Name: "name", Value: []byte(values[i]), Index: true}}}
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: documents}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for i := 0; i < 200; i++ {
		pattern := WildcardPattern{Syntax: WildcardSyntaxSQL, Pattern: generate(), IgnoreCase: i%2 == 0}
		if i%3 == 0 {
			pattern.Escape = "!"
		}
		if ValidateWildcardPattern(pattern) != nil {
			pattern.Pattern += "!"
		}
		var want []string
		for index, value := range values {
			if wildcardOracle(pattern, value) {
				want = append(want, fmt.Sprint(index))
			}
		}
		hits, matchErr := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", WildcardPatterns: []WildcardPattern{pattern}})
		require.NoError(t, matchErr, "%+v", pattern)
		require.ElementsMatch(t, want, queryIDs(hits), "%+v", pattern)
	}
}

func TestNativeWildcardPatternSharedBudgetConcurrent(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		budget := NewWildcardBudgetWithMemory(WildcardLimits{}, func(int64) error { return nil }, func(int64) {})
		defer func() { require.NoError(t, budget.Close()) }()
		failures := make(chan error, 8)
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				for range 5 {
					hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
						Field:            "name",
						WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "appl%"}},
						PatternBudget:    budget,
					})
					if err != nil {
						failures <- err
						return
					}
					if len(hits) != 2 {
						failures <- fmt.Errorf("got %d hits", len(hits))
						return
					}
				}
			})
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		require.EqualValues(t, 80, budget.Stats().Terms)
	})
}

func TestNativeWildcardPatternCachedProgramRetainsExecutionOwner(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		var ownerLive int64
		budget := NewWildcardBudgetWithMemory(WildcardLimits{}, func(bytes int64) error { ownerLive += bytes; return nil }, func(bytes int64) { ownerLive -= bytes })
		defer func() { require.NoError(t, budget.Close()) }()
		var previousProgramBytes int64
		for source := 0; source < 2; source++ {
			var sourceLive int64
			ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error { sourceLive += bytes; return nil }, func(bytes int64) { sourceLive -= bytes })
			hits, err := view.MatchTermsSet(ctx, TermSetRequest{
				Field:            "name",
				WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "appl%"}},
				PatternBudget:    budget,
			})
			require.NoError(t, err)
			require.Len(t, hits, 2)
			require.Positive(t, ownerLive)
			if source == 0 {
				previousProgramBytes = ownerLive
			} else {
				require.Equal(t, previousProgramBytes, ownerLive)
			}
			require.NoError(t, scope.Close())
			require.Zero(t, sourceLive)
			require.Equal(t, previousProgramBytes, ownerLive, "program remains charged after source closes")
		}
		require.NoError(t, budget.Close())
		require.Zero(t, ownerLive)
		require.Empty(t, budget.programs)
	})
}

func TestNativeWildcardPatternUnownedBudgetDoesNotRetainPrograms(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		budget := NewWildcardBudget(WildcardLimits{})
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field:            "name",
			WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "appl%"}},
			PatternBudget:    budget,
		})
		require.NoError(t, err)
		require.Empty(t, budget.programs)
	})
}

func TestNativeWildcardPatternAutomatonCeiling(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	value := strings.Repeat("a", 800)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("long-name"), Fields: []Field{{Name: "name", Value: []byte(value), Index: true}}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	budget := NewWildcardBudget(WildcardLimits{MaxWork: 2_000_000})
	var live int64
	ctx, scope := query.WithAllocationScope(context.Background(), func(bytes int64) error { live += bytes; return nil }, func(bytes int64) { live -= bytes })
	_, err = view.MatchTermsSet(ctx, TermSetRequest{
		Field:            "name",
		WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: value, IgnoreCase: true}},
		PatternBudget:    budget,
	})
	require.ErrorIs(t, err, ErrQueryLimit)
	require.ErrorContains(t, err, "wildcard automaton byte limit exceeded")
	require.Less(t, budget.Stats().Bytes, uint64(2<<20))
	require.Less(t, budget.Stats().Work, uint64(2_000_000))
	require.NoError(t, scope.Close())
	require.Zero(t, live)
}

func TestNativeWildcardPatternOwnedCacheCleanupOnFailure(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		var live int64
		budget := NewWildcardBudgetWithMemory(WildcardLimits{MaxWork: 20}, func(bytes int64) error { live += bytes; return nil }, func(bytes int64) { live -= bytes })
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field:            "name",
			WildcardPatterns: []WildcardPattern{{Syntax: WildcardSyntaxSQL, Pattern: "%never%"}},
			PatternBudget:    budget,
		})
		require.ErrorIs(t, err, ErrQueryLimit)
		require.Positive(t, live)
		require.NoError(t, budget.Close())
		require.Zero(t, live)
		require.Empty(t, budget.programs)
	})
}
