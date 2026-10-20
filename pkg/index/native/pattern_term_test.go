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

package native

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// patternTermDocuments returns the fixed document set every pattern-term test
// matches against: a run of "appl*" names, a disjoint "banana" name, and two
// names carrying regexp metacharacters that must be matched literally.
func patternTermDocuments() []Document {
	name := func(value string) Field { return Field{Name: "name", Value: []byte(value), Index: true, Store: true} }
	return []Document{
		{Identifier: []byte("d-apple"), Fields: []Field{name("apple")}},
		{Identifier: []byte("d-application"), Fields: []Field{name("application")}},
		{Identifier: []byte("d-banana"), Fields: []Field{name("banana")}},
		{Identifier: []byte("d-pipe"), Fields: []Field{name("a|b")}},
		{Identifier: []byte("d-backslash"), Fields: []Field{name(`a\b`)}},
	}
}

// withPatternTermView builds a fresh owner over patternTermDocuments, applies
// arrange (nil, forceMergeAll, or a persist-and-reopen helper), and runs body
// with a pinned view. The owner and view are closed before returning.
func withPatternTermView(t *testing.T, arrange func(t *testing.T, owner *Owner), body func(t *testing.T, view *ReadView)) {
	t.Helper()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: patternTermDocuments()}))
	if arrange != nil {
		arrange(t, owner)
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	body(t, view)
}

func mergedArrange(t *testing.T, owner *Owner) {
	t.Helper()
	require.NoError(t, owner.forceMergeAll(context.Background()))
}

// withPersistedPatternTermView persists patternTermDocuments to disk and
// reopens a brand new owner over the same directory, so queries run against
// file-backed segments rather than the in-memory ones Batch just admitted.
func withPersistedPatternTermView(t *testing.T, body func(t *testing.T, view *ReadView)) {
	t.Helper()
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: patternTermDocuments()}))
	require.NoError(t, owner.Close())
	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	body(t, view)
}

func TestNativePrefixTermMatchesSharedByteRange(t *testing.T) {
	for _, tc := range []struct {
		arrange func(t *testing.T, owner *Owner)
		name    string
	}{
		{name: "memory"},
		{name: "merged", arrange: mergedArrange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPatternTermView(t, tc.arrange, func(t *testing.T, view *ReadView) {
				hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
					Field: "name", Prefix: [][]byte{[]byte("appl")}, MaxTerms: 2,
				})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(hits))
			})
		})
	}
	t.Run("persisted", func(t *testing.T) {
		withPersistedPatternTermView(t, func(t *testing.T, view *ReadView) {
			hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", Prefix: [][]byte{[]byte("appl")}, MaxTerms: 2,
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(hits))
		})
	})
}

func TestNativePrefixTermEmptyPrefixMatchesEverything(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Prefix: [][]byte{{}}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Len(t, hits, 5)
	})
}

func TestNativeWildcardTermMatchesStarAndQuestionMark(t *testing.T) {
	for _, tc := range []struct {
		arrange func(t *testing.T, owner *Owner)
		name    string
	}{
		{name: "memory"},
		{name: "merged", arrange: mergedArrange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPatternTermView(t, tc.arrange, func(t *testing.T, view *ReadView) {
				star, err := view.MatchTermsSet(context.Background(), TermSetRequest{
					Field: "name", Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: 5,
				})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(star))

				question, err := view.MatchTermsSet(context.Background(), TermSetRequest{
					Field: "name", Wildcard: [][]byte{[]byte("a???e")}, MaxTerms: 5,
				})
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"d-apple"}, queryIDs(question))
			})
		})
	}
	t.Run("persisted", func(t *testing.T) {
		withPersistedPatternTermView(t, func(t *testing.T, view *ReadView) {
			star, err := view.MatchTermsSet(context.Background(), TermSetRequest{
				Field: "name", Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: 5,
			})
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(star))
		})
	})
}

func TestNativeWildcardTermEscapesRegexpMetacharacters(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		pipe, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte("a|b")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Equal(t, []string{"d-pipe"}, queryIDs(pipe))

		backslash, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte(`a\b`)}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Equal(t, []string{"d-backslash"}, queryIDs(backslash))

		// "a.b" is not one of the stored names (literal '.' must not become a
		// wildcard "any character" match against "a|b" or "a\b").
		dot, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte("a.b")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Empty(t, dot)
	})
}

func TestNativePatternTermORedWithExactTerms(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Terms: [][]byte{[]byte("banana")}, Prefix: [][]byte{[]byte("appl")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"d-apple", "d-application", "d-banana"}, queryIDs(hits))
	})
}

func TestNativePatternTermAllModeIntersectsOperands(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		// "appl*" matches {apple, application}; the exact term "apple" is one
		// of those, so the All-mode intersection keeps only it.
		hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Mode: MatchAllTerms, Terms: [][]byte{[]byte("apple")},
			Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Equal(t, []string{"d-apple"}, queryIDs(hits))

		none, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Mode: MatchAllTerms, Terms: [][]byte{[]byte("banana")},
			Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.Empty(t, none)
	})
}

func TestNativePatternTermFilterTermsSet(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		universe, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "_id", Terms: [][]byte{[]byte("d-apple"), []byte("d-application"), []byte("d-banana")},
		})
		require.NoError(t, err)
		require.Len(t, universe, 3)

		filtered, err := view.FilterTermsSet(context.Background(), universe, TermSetRequest{
			Field: "name", Prefix: [][]byte{[]byte("appl")}, MaxTerms: 5,
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(filtered))
	})
}

func TestNativePatternTermRequiresMaxTerms(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Prefix: [][]byte{[]byte("appl")}})
		require.ErrorIs(t, err, ErrQueryLimit)

		_, err = view.MatchTermsSet(context.Background(), TermSetRequest{Field: "name", Wildcard: [][]byte{[]byte("appl*")}})
		require.ErrorIs(t, err, ErrQueryLimit)

		_, err = view.FilterTermsSet(context.Background(), nil, TermSetRequest{Field: "name", Prefix: [][]byte{[]byte("appl")}})
		require.ErrorIs(t, err, ErrQueryLimit)

		_, err = view.MatchAllTermSets(context.Background(), []TermSetRequest{
			{Field: "name", Prefix: [][]byte{[]byte("appl")}},
		})
		require.ErrorIs(t, err, ErrQueryLimit)
	})
}

func TestNativePatternTermMaxTermsOverflow(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Prefix: [][]byte{[]byte("appl")}, MaxTerms: 1,
		})
		require.ErrorIs(t, err, ErrQueryLimit)

		_, err = view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{[]byte("appl*")}, MaxTerms: 1,
		})
		require.ErrorIs(t, err, ErrQueryLimit)

		// The budget is shared across every Prefix/Wildcard entry of the request.
		_, err = view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Prefix: [][]byte{[]byte("appl")}, Wildcard: [][]byte{[]byte("banana")}, MaxTerms: 2,
		})
		require.ErrorIs(t, err, ErrQueryLimit)
	})
}

func TestNativePatternTermMatchAllTermSetsCombinesRequests(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		hits, err := view.MatchAllTermSets(context.Background(), []TermSetRequest{
			{Field: "_id", Terms: [][]byte{[]byte("d-apple"), []byte("d-application"), []byte("d-banana")}},
			{Field: "name", Prefix: [][]byte{[]byte("appl")}, MaxTerms: 5},
		})
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"d-apple", "d-application"}, queryIDs(hits))
	})
}

func TestNativeWildcardTermRejectsInvalidPattern(t *testing.T) {
	withPatternTermView(t, nil, func(t *testing.T, view *ReadView) {
		// Every regexp metacharacter the replacer recognizes is escaped before
		// compilation, so the only reachable compile failure is a pattern byte
		// sequence that is not valid UTF-8.
		_, err := view.MatchTermsSet(context.Background(), TermSetRequest{
			Field: "name", Wildcard: [][]byte{{0xFF, 0xFE}}, MaxTerms: 5,
		})
		require.Error(t, err)
	})
}

func TestSuccessorByteRange(t *testing.T) {
	require.Equal(t, []byte("apm"), successor([]byte("apl")))
	require.Nil(t, successor([]byte{0xFF, 0xFF}))
	require.Equal(t, []byte{0x01}, successor([]byte{0x00, 0xFF}))
	require.Nil(t, successor(nil))
}
