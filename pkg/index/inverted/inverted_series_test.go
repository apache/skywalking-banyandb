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
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	fieldKeyDuration = index.FieldKey{
		IndexRuleID: indexRuleID,
	}
	fieldKeyServiceName = index.FieldKey{
		IndexRuleID: 6,
	}
)

func TestStore_Search(t *testing.T) {
	tester := require.New(t)
	path, fn := setUp(tester)
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
		term       [][]byte
		want       []index.SeriesDocument
		projection []index.FieldKey
	}{
		{
			term: [][]byte{[]byte("test1")},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			projection: []index.FieldKey{fieldKeyDuration, fieldKeyServiceName},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    nil,
						fieldKeyServiceName.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(100)),
						fieldKeyServiceName.Marshal(): []byte("svc2"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(3),
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(500)),
						fieldKeyServiceName.Marshal(): nil,
					},
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			projection: []index.FieldKey{fieldKeyDuration},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): convert.Int64ToBytes(int64(100)),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(3),
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): convert.Int64ToBytes(int64(500)),
					},
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			projection: []index.FieldKey{fieldKeyServiceName},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): []byte("svc2"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(3),
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): nil,
					},
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
		t.Run(name, func(t *testing.T) {
			query, err := s.BuildQuery(matchers, nil)
			require.NotEmpty(t, query.String())
			require.NoError(t, err)
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStore_SearchWildcard(t *testing.T) {
	tester := require.New(t)
	path, fn := setUp(tester)
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
		wildcard   []byte
		projection []index.FieldKey
		want       []index.SeriesDocument
	}{
		{
			wildcard: []byte("test*"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(3),
						EntityValues: []byte("test3"),
					},
				},
			},
		},
		{
			wildcard: []byte("*2"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
				},
			},
		},
		{
			wildcard: []byte("t*st1"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
				},
			},
		},
		{
			wildcard: []byte("foo*"),
			want:     []index.SeriesDocument{},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.wildcard), func(t *testing.T) {
			query, err := s.BuildQuery([]index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypeWildcard,
					Match: tt.wildcard,
				},
			}, nil)
			require.NoError(t, err)
			require.NotEmpty(t, query.String())
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestStore_SearchPrefix(t *testing.T) {
	tester := require.New(t)
	path, fn := setUp(tester)
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
		prefix     []byte
		projection []index.FieldKey
		want       []index.SeriesDocument
	}{
		{
			prefix: []byte("test"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						ID:           common.SeriesID(1),
						EntityValues: []byte("test1"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(2),
						EntityValues: []byte("test2"),
					},
				},
				{
					Key: index.Series{
						ID:           common.SeriesID(3),
						EntityValues: []byte("test3"),
					},
				},
			},
		},
		{
			prefix: []byte("foo"),
			want:   []index.SeriesDocument{},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.prefix), func(t *testing.T) {
			query, err := s.BuildQuery([]index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypePrefix,
					Match: tt.prefix,
				},
			}, nil)
			require.NoError(t, err)
			require.NotEmpty(t, query.String())
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func setupData(tester *require.Assertions, s index.SeriesStore) {
	series1 := index.Document{
		DocID:        1,
		EntityValues: []byte("test1"),
	}

	series2 := index.Document{
		DocID:        2,
		EntityValues: []byte("test2"),
		Fields: []index.Field{
			{
				Key:   fieldKeyDuration,
				Term:  convert.Int64ToBytes(int64(100)),
				Store: true,
			},
			{
				Key:   fieldKeyServiceName,
				Term:  []byte("svc2"),
				Store: true,
			},
		},
	}

	series3 := index.Document{
		DocID:        3,
		EntityValues: []byte("test3"),
		Fields: []index.Field{
			{
				Key:   fieldKeyDuration,
				Term:  convert.Int64ToBytes(int64(500)),
				Store: true,
			},
		},
	}
	tester.NoError(s.Batch(index.Batch{
		Documents: []index.Document{series1, series2, series3, series3},
	}))
}
