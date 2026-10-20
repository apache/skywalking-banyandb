// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package native

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func TestNativeSeriesIteratorExactCorpusAndDeletion(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("cpu/a")},
		{Identifier: []byte("cpu/b")},
		{Identifier: []byte("db/a")},
		{Identifier: []byte("db/b"), Deleted: true},
		{Identifier: []byte("z/a")},
	}}))
	generation, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, generation.Close()) }()
	iterator, err := generation.NewSeriesIterator(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()
	var got [][]byte
	for {
		term, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		if term == nil {
			break
		}
		got = append(got, term)
	}
	require.Equal(t, [][]byte{[]byte("cpu/a"), []byte("cpu/b"), []byte("db/a"), []byte("db/b"), []byte("z/a")}, got)
}

func TestNativeSeriesIteratorCancellation(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{{Identifier: []byte("a")}, {Identifier: []byte("b")}}}))
	generation, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, generation.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	iterator, err := generation.NewSeriesIterator(ctx)
	require.NoError(t, err)
	term, err := iterator.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("a"), term)
	cancel()
	_, err = iterator.Next()
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, iterator.Close())
	require.NoError(t, iterator.Close())
}

func TestNativeSeriesIteratorIndependentCursors(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, nativeice.Encode(path, nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("a")}, {Identifier: []byte("b")}, {Identifier: []byte("c")},
	}}))
	generation, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, generation.Close()) }()
	first, err := generation.NewSeriesIterator(context.Background())
	require.NoError(t, err)
	second, err := generation.NewSeriesIterator(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()); require.NoError(t, second.Close()) }()
	value, err := first.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("a"), value)
	value, err = second.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("a"), value)
	value, err = first.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("b"), value)
	value, err = second.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("b"), value)
}
