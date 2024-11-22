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
	"maps"
	"sort"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/numeric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	fieldKeyDuration = index.FieldKey{
		IndexRuleID: indexRuleID,
	}
	fieldKeyServiceName = index.FieldKey{
		IndexRuleID: 6,
	}
	fieldKeyStartTime = index.FieldKey{
		IndexRuleID: 21,
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
	insertData(tester, s)

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
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(100)),
						fieldKeyServiceName.Marshal(): []byte("svc2"),
					},
					Timestamp: int64(101),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    nil,
						fieldKeyServiceName.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(500)),
						fieldKeyServiceName.Marshal(): nil,
					},
					Timestamp: int64(1001),
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			projection: []index.FieldKey{fieldKeyDuration},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): convert.Int64ToBytes(int64(100)),
					},
					Timestamp: int64(101),
				},

				{
					Key: index.Series{
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal(): convert.Int64ToBytes(int64(500)),
					},
					Timestamp: int64(1001),
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2"), []byte("test3"), []byte("foo")},
			projection: []index.FieldKey{fieldKeyServiceName},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): []byte("svc2"),
					},
					Timestamp: int64(101),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test1"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): nil,
					},
				},
				{
					Key: index.Series{
						EntityValues: []byte("test3"),
					},
					Fields: map[string][]byte{
						fieldKeyServiceName.Marshal(): nil,
					},
					Timestamp: int64(1001),
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
			query, err := s.BuildQuery(matchers, nil, nil)
			require.NotEmpty(t, query.String())
			require.NoError(t, err)
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			sort.Slice(tt.want, func(i, j int) bool {
				return string(tt.want[i].Key.EntityValues) < string(tt.want[j].Key.EntityValues)
			})
			sort.Slice(got, func(i, j int) bool {
				return string(got[i].Key.EntityValues) < string(got[j].Key.EntityValues)
			})
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
	insertData(tester, s)

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
						EntityValues: []byte("test1"),
					},
				},
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Timestamp: int64(101),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test3"),
					},
					Timestamp: int64(1001),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test4"),
					},
					Timestamp: int64(2001),
				},
			},
		},
		{
			wildcard: []byte("*2"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Timestamp: int64(101),
				},
			},
		},
		{
			wildcard: []byte("t*st1"),
			want: []index.SeriesDocument{
				{
					Key: index.Series{
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
			}, nil, nil)
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
	insertData(tester, s)

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
						EntityValues: []byte("test1"),
					},
				},
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Timestamp: int64(101),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test3"),
					},
					Timestamp: int64(1001),
				},
				{
					Key: index.Series{
						EntityValues: []byte("test4"),
					},
					Timestamp: int64(2001),
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
			}, nil, nil)
			require.NoError(t, err)
			require.NotEmpty(t, query.String())
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestStore_SearchWithSecondaryQuery(t *testing.T) {
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
	insertData(tester, s)

	// Define the secondary query
	secondaryQuery := &queryNode{
		query: bluge.NewTermQuery("svc2").SetField(fieldKeyServiceName.Marshal()),
		node:  newTermNode("svc2", nil),
	}

	// Test cases
	tests := []struct {
		term       [][]byte
		want       []index.SeriesDocument
		projection []index.FieldKey
	}{
		{
			term:       [][]byte{[]byte("test2")},
			projection: []index.FieldKey{fieldKeyServiceName, fieldKeyDuration, {TagName: "short_name"}},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(100)),
						fieldKeyServiceName.Marshal(): []byte("svc2"),
						"short_name":                  []byte("t2"),
					},
					Timestamp: int64(101),
				},
			},
		},
		{
			term:       [][]byte{[]byte("test3")},
			projection: []index.FieldKey{fieldKeyServiceName, fieldKeyDuration},
			want:       []index.SeriesDocument{},
		},
		{
			term:       [][]byte{[]byte("test1")},
			projection: []index.FieldKey{fieldKeyServiceName, fieldKeyDuration},
			want:       []index.SeriesDocument{},
		},
		{
			term:       [][]byte{[]byte("test2"), []byte("test3")},
			projection: []index.FieldKey{fieldKeyServiceName, fieldKeyDuration, {TagName: "short_name"}},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(100)),
						fieldKeyServiceName.Marshal(): []byte("svc2"),
						"short_name":                  []byte("t2"),
					},
					Timestamp: int64(101),
				},
			},
		},
		{
			term:       [][]byte{[]byte("test1"), []byte("test2")},
			projection: []index.FieldKey{fieldKeyServiceName, fieldKeyDuration},
			want: []index.SeriesDocument{
				{
					Key: index.Series{
						EntityValues: []byte("test2"),
					},
					Fields: map[string][]byte{
						fieldKeyDuration.Marshal():    convert.Int64ToBytes(int64(100)),
						fieldKeyServiceName.Marshal(): []byte("svc2"),
					},
					Timestamp: int64(101),
				},
			},
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
			query, err := s.BuildQuery(matchers, secondaryQuery, nil)
			require.NotEmpty(t, query.String())
			require.NoError(t, err)
			got, err := s.Search(context.Background(), tt.projection, query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStore_SeriesSort(t *testing.T) {
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
	updateData(tester, s)

	// Define the order by field
	orderBy := &index.OrderBy{
		Index: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{
				Id: fieldKeyStartTime.IndexRuleID,
			},
		},
		Sort: modelv1.Sort_SORT_ASC,
		Type: index.OrderByTypeIndex,
	}

	// Test cases
	tests := []struct {
		name      string
		orderBy   *index.OrderBy
		timeRange *timestamp.TimeRange
		want      []index.DocumentResult
		fieldKeys []index.FieldKey
	}{
		{
			name:      "Sort by start_time ascending",
			orderBy:   orderBy,
			fieldKeys: []index.FieldKey{fieldKeyStartTime},
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(100)),
					},
					SortedValue: convert.Int64ToBytes(int64(100)),
				},
				{
					EntityValues: []byte("test3"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(1000)),
					},
					SortedValue: convert.Int64ToBytes(int64(1000)),
				},
				{
					EntityValues: []byte("test4"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(2000)),
					},
					SortedValue: convert.Int64ToBytes(int64(2000)),
				},
				{
					EntityValues: []byte("test1"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): nil,
					},
					SortedValue: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
			},
		},
		{
			name: "Sort by start_time descending",
			orderBy: &index.OrderBy{
				Index: &databasev1.IndexRule{
					Metadata: &commonv1.Metadata{
						Id: fieldKeyStartTime.IndexRuleID,
					},
				},
				Sort: modelv1.Sort_SORT_DESC,
				Type: index.OrderByTypeIndex,
			},
			fieldKeys: []index.FieldKey{fieldKeyStartTime},
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test4"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(2000)),
					},
					SortedValue: convert.Int64ToBytes(int64(2000)),
				},
				{
					EntityValues: []byte("test3"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(1000)),
					},
					SortedValue: convert.Int64ToBytes(int64(1000)),
				},
				{
					EntityValues: []byte("test2"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(100)),
					},
					SortedValue: convert.Int64ToBytes(int64(100)),
				},
				{
					EntityValues: []byte("test1"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): nil,
					},
					SortedValue: []byte{0x00},
				},
			},
		},
		{
			name:      "Sort by start_time ascending with time range 100 to 1000",
			orderBy:   orderBy,
			fieldKeys: []index.FieldKey{fieldKeyStartTime},
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 100), time.Unix(0, 1000))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(100)),
					},
					SortedValue: convert.Int64ToBytes(int64(100)),
				},
				{
					EntityValues: []byte("test3"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(1000)),
					},
					SortedValue: convert.Int64ToBytes(int64(1000)),
				},
			},
		},
		{
			name:      "Sort by start_time ascending with time range 0 to 2000",
			orderBy:   orderBy,
			fieldKeys: []index.FieldKey{fieldKeyStartTime},
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 0), time.Unix(0, 2000))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(100)),
					},
					SortedValue: convert.Int64ToBytes(int64(100)),
				},
				{
					EntityValues: []byte("test3"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(1000)),
					},
					SortedValue: convert.Int64ToBytes(int64(1000)),
				},
				{
					EntityValues: []byte("test4"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(2000)),
					},
					SortedValue: convert.Int64ToBytes(int64(2000)),
				},
			},
		},
		{
			name:      "Sort by start_time ascending with time range 500 to 1500",
			orderBy:   orderBy,
			fieldKeys: []index.FieldKey{fieldKeyStartTime},
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 500), time.Unix(0, 1500))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test3"),
					Values: map[string][]byte{
						fieldKeyStartTime.Marshal(): convert.Int64ToBytes(int64(1000)),
					},
					SortedValue: convert.Int64ToBytes(int64(1000)),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var secondaryQuery index.Query
			if tt.timeRange != nil {
				secondaryQuery = &queryNode{
					query: bluge.NewTermRangeInclusiveQuery(
						string(convert.Int64ToBytes(tt.timeRange.Start.Local().UnixNano())),
						string(convert.Int64ToBytes(tt.timeRange.End.Local().UnixNano())),
						tt.timeRange.IncludeStart,
						tt.timeRange.IncludeEnd,
					).SetField(fieldKeyStartTime.Marshal()),
				}
			}
			query, err := s.BuildQuery([]index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypePrefix,
					Match: []byte("test"),
				},
			}, secondaryQuery, nil)
			require.NoError(t, err)
			iter, err := s.SeriesSort(context.Background(), query, tt.orderBy, 10, tt.fieldKeys)
			require.NoError(t, err)
			defer iter.Close()

			var got []index.DocumentResult
			for iter.Next() {
				var g index.DocumentResult
				val := iter.Val()
				g.DocID = val.DocID
				g.EntityValues = val.EntityValues
				g.Values = maps.Clone(val.Values)
				g.SortedValue = val.SortedValue
				got = append(got, g)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStore_TimestampSort(t *testing.T) {
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
	updateData(tester, s)

	// Define the order by field
	orderBy := &index.OrderBy{
		Type: index.OrderByTypeTime,
		Sort: modelv1.Sort_SORT_ASC,
	}

	// Test cases
	tests := []struct {
		name      string
		orderBy   *index.OrderBy
		timeRange *timestamp.TimeRange
		want      []index.DocumentResult
		fieldKeys []index.FieldKey
	}{
		{
			name:    "Sort by timestamp ascending",
			orderBy: orderBy,
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Timestamp:    int64(101),
					SortedValue:  numeric.MustNewPrefixCodedInt64(101, 0),
				},
				{
					EntityValues: []byte("test3"),
					Timestamp:    int64(1001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(1001, 0),
				},
				{
					EntityValues: []byte("test4"),
					Timestamp:    int64(2001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(2001, 0),
				},
				{
					EntityValues: []byte("test1"),
					Timestamp:    0,
					SortedValue:  []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
			},
		},
		{
			name: "Sort by timestamp descending",
			orderBy: &index.OrderBy{
				Type: index.OrderByTypeTime,
				Sort: modelv1.Sort_SORT_DESC,
			},
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test4"),
					Timestamp:    int64(2001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(2001, 0),
				},
				{
					EntityValues: []byte("test3"),
					Timestamp:    int64(1001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(1001, 0),
				},
				{
					EntityValues: []byte("test2"),
					Timestamp:    int64(101),
					SortedValue:  numeric.MustNewPrefixCodedInt64(101, 0),
				},
				{
					EntityValues: []byte("test1"),
					Timestamp:    0,
					SortedValue:  []byte{0x00},
				},
			},
		},
		{
			name:    "Sort by timestamp ascending with time range 100 to 1000",
			orderBy: orderBy,
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 100), time.Unix(0, 1000))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Timestamp:    int64(101),
					SortedValue:  numeric.MustNewPrefixCodedInt64(101, 0),
				},
			},
		},
		{
			name:    "Sort by timestamp ascending with time range 0 to 2000",
			orderBy: orderBy,
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 0), time.Unix(0, 2000))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test2"),
					Timestamp:    int64(101),
					SortedValue:  numeric.MustNewPrefixCodedInt64(101, 0),
				},
				{
					EntityValues: []byte("test3"),
					Timestamp:    int64(1001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(1001, 0),
				},
			},
		},
		{
			name:    "Sort by timestamp ascending with time range 500 to 1500",
			orderBy: orderBy,
			timeRange: func() *timestamp.TimeRange {
				tr := timestamp.NewInclusiveTimeRange(time.Unix(0, 500), time.Unix(0, 1500))
				return &tr
			}(),
			want: []index.DocumentResult{
				{
					EntityValues: []byte("test3"),
					Timestamp:    int64(1001),
					SortedValue:  numeric.MustNewPrefixCodedInt64(1001, 0),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := s.BuildQuery([]index.SeriesMatcher{
				{
					Type:  index.SeriesMatcherTypePrefix,
					Match: []byte("test"),
				},
			}, nil, tt.timeRange)
			require.NoError(t, err)
			iter, err := s.SeriesSort(context.Background(), query, tt.orderBy, 10, tt.fieldKeys)
			require.NoError(t, err)
			defer iter.Close()

			var got []index.DocumentResult
			for iter.Next() {
				var g index.DocumentResult
				val := iter.Val()
				g.DocID = val.DocID
				g.EntityValues = val.EntityValues
				if len(val.Values) > 0 {
					g.Values = maps.Clone(val.Values)
				}
				g.SortedValue = val.SortedValue
				g.Timestamp = val.Timestamp
				got = append(got, g)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func insertData(tester *require.Assertions, s index.SeriesStore) {
	b1, b2 := generateDocs()
	tester.NoError(s.InsertSeriesBatch(b1))
	tester.NoError(s.InsertSeriesBatch(b2))
}

func generateDocs() (index.Batch, index.Batch) {
	series1 := index.Document{
		EntityValues: []byte("test1"),
	}

	series2 := index.Document{
		EntityValues: []byte("test2"),
		Fields: []index.Field{
			{
				Key:   fieldKeyDuration,
				Term:  convert.Int64ToBytes(int64(100)),
				Store: true,
				Index: true,
			},
			{
				Key:   fieldKeyServiceName,
				Term:  []byte("svc2"),
				Store: true,
				Index: true,
			},
			{
				Key:   fieldKeyStartTime,
				Term:  convert.Int64ToBytes(int64(100)),
				Store: true,
				Index: true,
			},
			{
				Key: index.FieldKey{
					TagName: "short_name",
				},
				Term:  []byte("t2"),
				Store: true,
				Index: false,
			},
		},
		Timestamp: int64(101),
	}

	series3 := index.Document{
		EntityValues: []byte("test3"),
		Fields: []index.Field{
			{
				Key:   fieldKeyDuration,
				Term:  convert.Int64ToBytes(int64(500)),
				Store: true,
				Index: true,
			},
			{
				Key:   fieldKeyStartTime,
				Term:  convert.Int64ToBytes(int64(1000)),
				Store: true,
				Index: true,
			},
			{
				Key: index.FieldKey{
					TagName: "short_name",
				},
				Term:  []byte("t3"),
				Store: true,
				Index: false,
			},
		},
		Timestamp: int64(1001),
	}
	series4 := index.Document{
		EntityValues: []byte("test4"),
		Fields: []index.Field{
			{
				Key:   fieldKeyDuration,
				Term:  convert.Int64ToBytes(int64(500)),
				Store: true,
				Index: true,
			},
			{
				Key:   fieldKeyStartTime,
				Term:  convert.Int64ToBytes(int64(2000)),
				Store: true,
				Index: true,
			},
		},
		Timestamp: int64(2001),
	}
	return index.Batch{
			Documents: []index.Document{series1, series2, series4, series3},
		}, index.Batch{
			Documents: []index.Document{series3},
		}
}

func updateData(tester *require.Assertions, s index.SeriesStore) {
	b1, b2 := generateDocs()
	tester.NoError(s.UpdateSeriesBatch(b1))
	tester.NoError(s.UpdateSeriesBatch(b2))
}
