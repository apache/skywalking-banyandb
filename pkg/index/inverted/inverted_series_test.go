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

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestStore_Search(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	tester.NoError(err)
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()

	// Setup some data
	setupData(tester, s)

	// Test cases
	tests := []struct {
		term [][]byte
		want []index.Series
	}{
		{
			term: [][]byte{[]byte("test1")},
			want: []index.Series{
				{
					ID:           common.SeriesID(1),
					EntityValues: []byte("test1"),
				},
			},
		},
		{
			term: [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			want: []index.Series{
				{
					ID:           common.SeriesID(1),
					EntityValues: []byte("test1"),
				},
				{
					ID:           common.SeriesID(2),
					EntityValues: []byte("test2"),
				},
				{
					ID:           common.SeriesID(3),
					EntityValues: []byte("test3"),
				},
			},
		},
		{
			term: [][]byte{[]byte("foo")},
			want: emptySeries,
		},
	}

	for _, tt := range tests {
		var matchers []index.SeriesMatcher
		var name string
		for _, term := range tt.term {
			matchers = append(matchers, index.SeriesMatcher{
				Type:  index.SeriesMatcherTypeExact,
				Match: term,
			})
			name += string(term) + "-"
		}
		t.Run(name, func(_ *testing.T) {
			got, err := s.Search(context.Background(), matchers)
			tester.NoError(err)
			tester.Equal(tt.want, got)
		})
	}
}

func TestStore_SearchWildcard(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	tester.NoError(err)
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()

	// Setup some data
	setupData(tester, s)

	// Test cases
	tests := []struct {
		wildcard []byte
		want     []index.Series
	}{
		{
			wildcard: []byte("test*"),
			want: []index.Series{
				{
					ID:           common.SeriesID(1),
					EntityValues: []byte("test1"),
				},
				{
					ID:           common.SeriesID(2),
					EntityValues: []byte("test2"),
				},
				{
					ID:           common.SeriesID(3),
					EntityValues: []byte("test3"),
				},
			},
		},
		{
			wildcard: []byte("*2"),
			want: []index.Series{
				{
					ID:           common.SeriesID(2),
					EntityValues: []byte("test2"),
				},
			},
		},
		{
			wildcard: []byte("t*st1"),
			want: []index.Series{
				{
					ID:           common.SeriesID(1),
					EntityValues: []byte("test1"),
				},
			},
		},
		{
			wildcard: []byte("foo*"),
			want:     []index.Series{},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.wildcard), func(_ *testing.T) {
			got, err := s.Search(context.Background(), []index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypeWildcard,
					Match: tt.wildcard,
				},
			})
			tester.NoError(err)
			tester.ElementsMatch(tt.want, got)
		})
	}
}

func TestStore_SearchPrefix(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	tester.NoError(err)
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()

	// Setup some data
	setupData(tester, s)

	// Test cases
	tests := []struct {
		prefix []byte
		want   []index.Series
	}{
		{
			prefix: []byte("test"),
			want: []index.Series{
				{
					ID:           common.SeriesID(1),
					EntityValues: []byte("test1"),
				},
				{
					ID:           common.SeriesID(2),
					EntityValues: []byte("test2"),
				},
				{
					ID:           common.SeriesID(3),
					EntityValues: []byte("test3"),
				},
			},
		},
		{
			prefix: []byte("foo"),
			want:   []index.Series{},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.prefix), func(_ *testing.T) {
			got, err := s.Search(context.Background(), []index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypePrefix,
					Match: tt.prefix,
				},
			})
			tester.NoError(err)
			tester.ElementsMatch(tt.want, got)
		})
	}
}

func setupData(tester *assert.Assertions, s *Store) {
	series1 := index.Document{
		DocID:        1,
		EntityValues: []byte("test1"),
	}

	series2 := index.Document{
		DocID:        2,
		EntityValues: []byte("test2"),
	}

	series3 := index.Document{
		DocID:        3,
		EntityValues: []byte("test3"),
	}
	tester.NoError(s.Batch(index.Batch{
		Documents: []index.Document{series1, series2, series3, series3},
	}))
}
