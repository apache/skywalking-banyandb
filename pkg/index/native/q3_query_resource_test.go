// Licensed to Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeQ3QueryResourceBoundaries(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("one"), Fields: []Field{{Name: "presence", Terms: []Term{{Value: []byte("a")}}, Index: true}}},
		{Identifier: []byte("two"), Fields: []Field{{Name: "presence", Terms: []Term{{Value: []byte("b")}}, Index: true}}},
		{Identifier: []byte("three"), Fields: []Field{{Name: "presence", Terms: []Term{{Value: []byte("c")}}, Index: true}}},
	}}))

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	canceling := newCancelAfterChecks(4)
	_, err = view.MatchField(canceling, FieldRequest{Field: "presence", MaxTerms: 8})
	require.ErrorIs(t, err, context.Canceled)
	rangeCanceling := newCancelAfterChecks(4)
	_, err = view.MatchRange(rangeCanceling, RangeRequest{Field: "presence", MaxTerms: 8})
	require.ErrorIs(t, err, context.Canceled)

	present, err := view.MatchField(context.Background(), FieldRequest{Field: "presence", MaxTerms: 8})
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two", "three"}, queryIDs(present))

	_, err = view.MatchField(context.Background(), FieldRequest{Field: "presence", MaxTerms: 1})
	require.ErrorIs(t, err, ErrQueryLimit)
	_, err = view.MatchRange(context.Background(), RangeRequest{Field: "presence", MaxTerms: 8, MaxCandidates: 1})
	require.ErrorIs(t, err, ErrQueryLimit)

	ranged, err := view.MatchRange(context.Background(), RangeRequest{Field: "presence", MaxTerms: 8, MaxCandidates: 8})
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two", "three"}, queryIDs(ranged))
	require.NoError(t, view.Close())
	require.NoError(t, view.Close())
	_, err = view.MatchField(context.Background(), FieldRequest{Field: "presence", MaxTerms: 8})
	require.ErrorIs(t, err, ErrViewClosed)

	fresh, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	require.NoError(t, fresh.Close())
}

//nolint:govet // the test context groups embedded context and cancellation state.
type cancelAfterChecks struct {
	context.Context
	checks int
	limit  int
	cancel context.CancelFunc
	once   sync.Once
}

func newCancelAfterChecks(limit int) *cancelAfterChecks {
	ctx, cancel := context.WithCancel(context.Background())
	return &cancelAfterChecks{Context: ctx, limit: limit, cancel: cancel}
}

func (c *cancelAfterChecks) Err() error {
	c.checks++
	if c.checks > c.limit {
		c.once.Do(c.cancel)
	}
	return c.Context.Err()
}

var _ context.Context = (*cancelAfterChecks)(nil)
