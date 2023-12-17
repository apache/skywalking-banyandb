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
		term []byte
		want common.SeriesID
	}{
		{
			term: []byte("test1"),
			want: common.SeriesID(1),
		},
		{
			term: []byte("foo"),
			want: common.SeriesID(0),
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.term), func(t *testing.T) {
			got, err := s.Search(tt.term)
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
		t.Run(string(tt.wildcard), func(t *testing.T) {
			got, err := s.SearchWildcard(tt.wildcard)
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
		t.Run(string(tt.prefix), func(t *testing.T) {
			got, err := s.SearchPrefix(tt.prefix)
			tester.NoError(err)
			tester.ElementsMatch(tt.want, got)
		})
	}
}

func setupData(tester *assert.Assertions, s index.SeriesStore) {
	series1 := index.Series{
		ID:           common.SeriesID(1),
		EntityValues: []byte("test1"),
	}
	tester.NoError(s.Create(series1))

	series2 := index.Series{
		ID:           common.SeriesID(2),
		EntityValues: []byte("test2"),
	}
	tester.NoError(s.Create(series2))

	series3 := index.Series{
		ID:           common.SeriesID(3),
		EntityValues: []byte("test3"),
	}
	tester.NoError(s.Create(series3))
	tester.NoError(s.Create(series3)) // duplicated data
	s.(*store).flush()
}
