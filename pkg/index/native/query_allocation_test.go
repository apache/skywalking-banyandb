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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestSelectedHitReservationRejectsBeforeStoredRead(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("selected"),
		Fields:     []Field{{Name: "status", Value: []byte("match"), Index: true, Store: true}},
	}}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	segment := view.root.segments[0].(*memorySegment)
	before := segment.handle.reader.VisitDocumentCalls()
	failure := errors.New("selected allocation denied")
	ctx, scope := query.WithAllocationScope(context.Background(), func(int64) error { return failure }, func(int64) {})
	defer func() { require.NoError(t, scope.Close()) }()
	_, err = view.MatchTermsSet(ctx, TermSetRequest{Field: "status", Terms: [][]byte{[]byte("match")}})
	require.ErrorIs(t, err, failure)
	require.Equal(t, before, segment.handle.reader.VisitDocumentCalls(), "candidate array guard must precede stored decoding")
	hits, err := view.MatchTermsSet(context.Background(), TermSetRequest{Field: "status", Terms: [][]byte{[]byte("match")}})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	before = segment.handle.reader.VisitDocumentCalls()
	_, err = view.ProjectHit(ctx, hits[0], "status")
	require.ErrorIs(t, err, failure)
	require.Equal(t, before, segment.handle.reader.VisitDocumentCalls(), "projection guard must precede stored decoding")
}
