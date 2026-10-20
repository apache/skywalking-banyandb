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

func TestOwnerIdentifierDocValuesReadableThroughSortAPI(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, IdentifierDocValues: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Fields: []Field{statusField("a")}},
		{Identifier: []byte("d1"), Fields: []Field{statusField("b")}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("a"), []byte("b")},
	})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	for _, hit := range hits {
		value, missing, err := view.ProjectSortValue(context.Background(), hit, "_id")
		require.NoError(t, err)
		require.False(t, missing, "\"_id\" doc values must be present when IdentifierDocValues is set")
		require.Equal(t, hit.Identifier, value)
	}
}

func TestOwnerWithoutIdentifierDocValuesHasNoIDDocValues(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Fields: []Field{statusField("a")}},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "status", Terms: [][]byte{[]byte("a")}})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	_, missing, err := view.ProjectSortValue(context.Background(), hits[0], "_id")
	require.NoError(t, err)
	require.True(t, missing)
}

func TestOwnerIdentifierDocValuesSurviveMerge(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, IdentifierDocValues: true, CompactionThreshold: -1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Fields: []Field{statusField("a")}},
	}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d1"), Fields: []Field{statusField("b")}},
	}}))
	require.NoError(t, owner.forceMergeAll(context.Background()))

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{
		Field: "status", Terms: [][]byte{[]byte("a"), []byte("b")},
	})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	for _, hit := range hits {
		value, missing, err := view.ProjectSortValue(context.Background(), hit, "_id")
		require.NoError(t, err)
		require.False(t, missing, "merge must splice \"_id\" doc values forward like any other field")
		require.Equal(t, hit.Identifier, value)
	}
}

func TestOwnerIdentifierDocValuesSurvivePersistAndReopen(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, IdentifierDocValues: true})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("d0"), Fields: []Field{statusField("a")}},
	}}))
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "status", Terms: [][]byte{[]byte("a")}})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	value, missing, err := view.ProjectSortValue(context.Background(), hits[0], "_id")
	require.NoError(t, err)
	require.False(t, missing)
	require.Equal(t, []byte("d0"), value)
}
