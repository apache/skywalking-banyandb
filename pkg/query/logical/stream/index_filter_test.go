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

package stream

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	postingroaring "github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func TestMerge_DummyPostingList_NotMutated(t *testing.T) {
	t.Run("orNode merge does not mutate DummyPostingList when empty is first operand", func(t *testing.T) {
		b := postingroaring.NewPostingList()
		b.Insert(100)

		on := newOr(2)
		res, err := on.merge(postingroaring.DummyPostingList, b)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Len())
		assert.True(t, res.Contains(100))

		// Regression assertion: DummyPostingList must stay unmutated and empty.
		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})

	t.Run("andNode merge does not mutate DummyPostingList when empty is first operand", func(t *testing.T) {
		b := postingroaring.NewPostingList()
		b.Insert(100)

		an := newAnd(2)
		res, err := an.merge(postingroaring.DummyPostingList, b)
		require.NoError(t, err)
		assert.True(t, res.IsEmpty())

		// Regression assertion: DummyPostingList must stay unmutated and empty.
		assert.True(t, postingroaring.DummyPostingList.IsEmpty())
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len())
	})
}

func TestMerge_DummyPostingList_Concurrent(t *testing.T) {
	const concurrency = 50
	var ready sync.WaitGroup
	var done sync.WaitGroup
	start := make(chan struct{})

	ready.Add(concurrency)
	done.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id uint64) {
			defer done.Done()

			b := postingroaring.NewPostingList()
			b.Insert(id)

			// Signal ready, then wait for all workers to reach the barrier
			ready.Done()
			<-start

			on := newOr(2)
			res, err := on.merge(postingroaring.DummyPostingList, b)
			assert.NoError(t, err)
			assert.True(t, res.Contains(id))

			an := newAnd(2)
			andRes, err := an.merge(postingroaring.DummyPostingList, b)
			assert.NoError(t, err)
			assert.True(t, andRes.IsEmpty())
		}(uint64(i + 1))
	}

	// Wait until all 50 workers are spawned and standing at the gate
	ready.Wait()
	// Release all 50 goroutines simultaneously
	close(start)
	done.Wait()

	// Regression assertion: shared singleton was never written to across 50 concurrent queries
	assert.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must remain empty")
	assert.Equal(t, 0, postingroaring.DummyPostingList.Len(), "DummyPostingList was mutated by concurrent queries")
}

func TestNot_DummyPostingList_NotMutated(t *testing.T) {
	t.Run("not.Execute does not mutate DummyPostingList when inner filter matches nothing", func(t *testing.T) {
		allDocs := postingroaring.NewPostingList()
		allDocs.Insert(100)
		allDocs.Insert(200)

		require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must start empty")

		res, err := execNotFilter(allDocs, postingroaring.DummyPostingList)
		require.NoError(t, err)
		assert.Equal(t, 2, res.Len())
		assert.True(t, res.Contains(100))
		assert.True(t, res.Contains(200))

		// Regression assertion: DummyPostingList must remain intact and empty.
		assert.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must remain empty")
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len(), "DummyPostingList was mutated by not.Execute")
	})

	t.Run("not.Execute does not mutate DummyPostingList when scope scan matches nothing", func(t *testing.T) {
		innerDocs := postingroaring.NewPostingList()
		innerDocs.Insert(100)

		require.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must start empty")

		res, err := execNotFilter(postingroaring.DummyPostingList, innerDocs)
		require.NoError(t, err)
		assert.True(t, res.IsEmpty())

		// Regression assertion: Difference against empty universe must not mutate singleton.
		assert.True(t, postingroaring.DummyPostingList.IsEmpty(), "DummyPostingList must remain empty")
		assert.Equal(t, 0, postingroaring.DummyPostingList.Len(), "DummyPostingList was mutated by not.Execute")
	})
}

func execNotFilter(allDocs, innerDocs posting.List) (posting.List, error) {
	ms := &mockSearcher{list: allDocs}
	getSearcher := func(databasev1.IndexRule_Type) (index.Searcher, error) {
		return ms, nil
	}
	n := newNot(&databasev1.IndexRule{}, &mockFilter{list: innerDocs})
	res, _, err := n.Execute(getSearcher, 1, nil)
	return res, err
}

type mockSearcher struct {
	index.Searcher
	list posting.List
	ts   posting.List
}

func (m *mockSearcher) MatchField(index.FieldKey) (posting.List, posting.List, error) {
	return m.list, m.ts, nil
}

type mockFilter struct {
	list posting.List
	ts   posting.List
}

func (m *mockFilter) Execute(index.GetSearcher, common.SeriesID, *index.RangeOpts) (posting.List, posting.List, error) {
	return m.list, m.ts, nil
}

func (m *mockFilter) String() string { return "mockFilter" }

func (m *mockFilter) ShouldSkip(index.FilterOp) (bool, error) { return false, nil }
