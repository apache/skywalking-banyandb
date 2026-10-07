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
	"strings"
	"testing"

	"github.com/blugelabs/bluge/numeric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/index/testcases"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestStore_Match(t *testing.T) {
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
	serviceName := index.FieldKey{
		// http_method
		IndexRuleID: 6,
		SeriesID:    common.SeriesID(11),
		Analyzer:    index.AnalyzerURL,
	}
	setup(tester, s, serviceName)

	tests := []struct {
		want     posting.List
		matches  []string
		operator modelv1.Condition_MatchOption_Operator
		wantErr  bool
	}{
		{
			matches: []string{"root"},
			want:    roaring.NewPostingListWithInitialData(2),
		},
		{
			matches: []string{"product"},
			want:    roaring.NewPostingListWithInitialData(1, 2),
		},
		{
			matches: []string{"order"},
			want:    roaring.NewPostingListWithInitialData(1, 3),
		},
		{
			matches: []string{"/root/product"},
			want:    roaring.NewPostingListWithInitialData(1, 2),
		},
		{
			matches:  []string{"/root/product"},
			operator: modelv1.Condition_MatchOption_OPERATOR_AND,
			want:     roaring.NewPostingListWithInitialData(2),
		},
		{
			matches: []string{"/product/order"},
			want:    roaring.NewPostingListWithInitialData(1, 2, 3),
		},
		{
			matches:  []string{"/product/order"},
			operator: modelv1.Condition_MatchOption_OPERATOR_AND,
			want:     roaring.NewPostingListWithInitialData(1),
		},
		{
			matches: []string{"GET"},
			want:    roaring.NewPostingListWithInitialData(1, 2),
		},
		{
			matches: []string{"GET::/root"},
			want:    roaring.NewPostingListWithInitialData(1, 2),
		},
		{
			matches: []string{"org"},
			want:    roaring.NewPostingListWithInitialData(3),
		},
		{
			matches: []string{"org.apache"},
			want:    roaring.NewPostingListWithInitialData(3),
		},
		{
			matches: []string{"org.apache....OrderService"},
			want:    roaring.NewPostingListWithInitialData(3),
		},
		{
			matches: []string{"OrderService.order"},
			want:    roaring.NewPostingListWithInitialData(1, 3),
		},
		{
			matches: []string{"root", "product"},
			want:    roaring.NewPostingListWithInitialData(2),
		},
		{
			matches: []string{"OrderService", "order"},
			want:    roaring.NewPostingListWithInitialData(3),
		},
		{
			matches: []string{"test"},
			want:    roaring.NewPostingListWithInitialData(),
		},
		{
			matches: []string{"v1"},
			want:    roaring.NewPostingListWithInitialData(4),
		},
		{
			matches: []string{"v2"},
			want:    roaring.NewPostingListWithInitialData(5),
		},
	}
	for _, tt := range tests {
		name := strings.Join(tt.matches, "-")
		t.Run(name, func(t *testing.T) {
			tester := assert.New(t)
			list, _, err := s.Match(serviceName, tt.matches, &modelv1.Condition_MatchOption{
				Operator: tt.operator,
			})
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err, name)
			tester.NotNil(list, name)
			tester.Equal(tt.want, list, name)
		})
	}
}

func TestStore_SeriesMatch(t *testing.T) {
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
	serviceName := index.FieldKey{
		// http_method
		IndexRuleID: 6,
		Analyzer:    index.AnalyzerURL,
	}
	setupSeries(tester, s, serviceName)

	tests := []struct {
		want     posting.List
		matches  []string
		operator modelv1.Condition_MatchOption_Operator
		wantErr  bool
	}{
		{
			matches: []string{"test"},
			want:    roaring.NewPostingListWithInitialData(1, 2, 3),
		},
		{
			matches: []string{"a"},
			want:    roaring.NewPostingListWithInitialData(1),
		},
		{
			matches: []string{"root"},
			want:    roaring.DummyPostingList,
		},
	}
	for _, tt := range tests {
		name := strings.Join(tt.matches, " and ")
		t.Run(name, func(t *testing.T) {
			list, _, err := s.Match(serviceName, tt.matches, &modelv1.Condition_MatchOption{
				Operator: tt.operator,
			})
			if tt.wantErr {
				tester.Error(err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, list)
			assert.Equal(t, tt.want, list)
		})
	}
}

func setup(tester *require.Assertions, s index.Store, serviceName index.FieldKey) {
	var batch index.Batch
	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "GET::/product/order"),
			},
			DocID: 1,
		},
		index.Document{
			Fields: []index.Field{
				index.NewBytesField(serviceName, []byte("GET::/root/product")),
			},
			DocID: 2,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "org.apache.skywalking.examples.OrderService.order"),
			},
			DocID: 3,
		},
		index.Document{
			Fields: []index.Field{
				index.NewBytesField(serviceName, []byte("/svc1/v1/user")),
			},
			DocID: 4,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "/svc1/v2/user"),
			},
			DocID: 5,
		},
	)
	tester.NoError(s.Batch(batch))
}

func setupSeries(tester *assert.Assertions, s index.Store, serviceName index.FieldKey) {
	var batch index.Batch
	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "test.a"),
			},
			DocID: 1,
		},
		index.Document{
			Fields: []index.Field{
				index.NewBytesField(serviceName, []byte("test.b")),
			},
			DocID: 2,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "test.c"),
			},
			DocID: 3,
		},
	)
	tester.NoError(s.Batch(batch))
}

func TestStore_MatchTerm(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()
	tester.NoError(err)
	testcases.SetUp(tester, s)
	testcases.RunServiceName(t, s)
}

func TestStore_Iterator(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()
	tester.NoError(err)
	data := testcases.SetUpDuration(tester, s)
	testcases.RunDuration(t, data, s)
}

func TestStore_NumericMatch(t *testing.T) {
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
	var batch index.Batch
	serviceName := index.FieldKey{
		IndexRuleID: 6,
	}
	durationName := index.FieldKey{
		IndexRuleID: 7,
	}
	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "svc1"),
				index.NewIntField(durationName, 50),
			},
			DocID: 1,
		},
		index.Document{
			Fields: []index.Field{
				index.NewBytesField(serviceName, []byte("svc2")),
				index.NewIntField(durationName, 200),
			},
			DocID: 2,
		},
		index.Document{
			Fields: []index.Field{
				index.NewIntField(durationName, 500),
			},
			DocID: 3,
		},
	)
	tester.NoError(s.Batch(batch))
	l, _, err := s.MatchTerms(index.NewIntField(durationName, 50))
	tester.NoError(err)
	tester.NotNil(l)
	tester.True(roaring.NewPostingListWithInitialData(1).Equal(l))
}

func setUp(t *require.Assertions) (tempDir string, deferFunc func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	tempDir, deferFunc = test.Space(t)
	return tempDir, deferFunc
}

func TestStore_TakeFileSnapshot(t *testing.T) {
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

	var batch index.Batch
	sampleKey := index.FieldKey{
		IndexRuleID: 10,
		SeriesID:    common.SeriesID(99),
	}
	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{
				index.NewStringField(sampleKey, "snapshot-test"),
			},
			DocID: 1,
		},
	)
	tester.NoError(s.Batch(batch))

	// Take snapshot
	snapshotDir, cleanFn := test.Space(require.New(t))
	defer cleanFn()

	err = s.TakeFileSnapshot(snapshotDir)
	tester.NoError(err)

	fileSystem := fs.NewLocalFileSystem()
	entries := fileSystem.ReadDir(snapshotDir)
	tester.True(len(entries) > 0, "Expected snapshot to produce files")
}

func TestStore_TimeRangeFiltering(t *testing.T) {
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

	var batch index.Batch
	serviceName := index.FieldKey{
		IndexRuleID: 6,
		SeriesID:    common.SeriesID(11),
	}

	ts1 := int64(1609459200000000000) // 2021-01-01
	ts2 := int64(1609545600000000000) // 2021-01-02
	ts3 := int64(1609632000000000000) // 2021-01-03
	ts4 := int64(1609718400000000000) // 2021-01-04

	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "svc1"),
			},
			DocID:     1,
			Timestamp: ts1,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "svc2"),
			},
			DocID:     2,
			Timestamp: ts2,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "svc3"),
			},
			DocID:     3,
			Timestamp: ts3,
		},
		index.Document{
			Fields: []index.Field{
				index.NewStringField(serviceName, "svc4"),
			},
			DocID:     4,
			Timestamp: ts4,
		},
	)
	tester.NoError(s.Batch(batch))

	// Define a time range for querying (2021-01-02 to 2021-01-03)
	timeRange := &index.RangeOpts{
		Lower:         &index.FloatTermValue{Value: numeric.Int64ToFloat64(ts2)},
		Upper:         &index.FloatTermValue{Value: numeric.Int64ToFloat64(ts3)},
		IncludesLower: true,
		IncludesUpper: true,
	}

	t.Run("Iterator with time range", func(_ *testing.T) {
		fieldKey := serviceName
		fieldKey.TimeRange = timeRange

		iter, err := s.Iterator(context.Background(), fieldKey, index.RangeOpts{}, modelv1.Sort_SORT_ASC, 100)
		tester.NoError(err)
		defer iter.Close()

		docIDs := make(map[uint64]bool)
		timestamps := make(map[int64]bool)
		for iter.Next() {
			doc := iter.Val()
			docIDs[doc.DocID] = true
			timestamps[doc.Timestamp] = true
		}
		tester.Equal(2, len(docIDs), "Expected exactly 2 documents")
		tester.True(docIDs[2], "Expected DocID 2 to be in results")
		tester.True(docIDs[3], "Expected DocID 3 to be in results")

		tester.Equal(2, len(timestamps), "Expected exactly 2 timestamps")
		tester.True(timestamps[ts2], "Expected timestamp for 2021-01-02 to be in results")
		tester.True(timestamps[ts3], "Expected timestamp for 2021-01-03 to be in results")
	})

	t.Run("MatchField with time range", func(_ *testing.T) {
		fieldKey := serviceName
		fieldKey.TimeRange = timeRange

		list, timestamps, err := s.MatchField(fieldKey)
		tester.NoError(err)

		expected := roaring.NewPostingListWithInitialData(2, 3)
		tester.Equal(expected, list)

		expectedTimestamps := roaring.NewPostingListWithInitialData(uint64(ts2), uint64(ts3))
		tester.Equal(expectedTimestamps, timestamps, "Timestamps should match documents within the time range")
	})

	t.Run("MatchTerms with time range", func(_ *testing.T) {
		fieldKey := serviceName
		fieldKey.TimeRange = timeRange

		field := index.NewStringField(fieldKey, "svc2")
		list, timestamps, err := s.MatchTerms(field)
		tester.NoError(err)

		expected := roaring.NewPostingListWithInitialData(2)
		tester.Equal(expected, list)

		expectedTimestamps := roaring.NewPostingListWithInitialData(uint64(ts2))
		tester.Equal(expectedTimestamps, timestamps, "Timestamps should match the document with DocID 2")

		field = index.NewStringField(fieldKey, "svc1")
		list, timestamps, err = s.MatchTerms(field)
		tester.NoError(err)

		tester.True(list.IsEmpty())
		tester.True(timestamps.IsEmpty(), "Timestamps should be empty for empty result set")
	})

	t.Run("Match with time range", func(_ *testing.T) {
		fieldKey := serviceName
		fieldKey.TimeRange = timeRange
		fieldKey.Analyzer = index.AnalyzerKeyword

		matches := []string{"svc"}
		list, timestamps, err := s.Match(fieldKey, matches, nil)
		tester.NoError(err)

		tester.True(list.IsEmpty())
		tester.True(timestamps.IsEmpty(), "Timestamps should be empty for empty result set")

		matches = []string{"svc3"}
		list, timestamps, err = s.Match(fieldKey, matches, nil)
		tester.NoError(err)
		expected := roaring.NewPostingListWithInitialData(3)
		tester.Equal(expected, list)

		expectedTimestamps := roaring.NewPostingListWithInitialData(uint64(ts3))
		tester.Equal(expectedTimestamps, timestamps, "Timestamps should match the document with DocID 3")

		matches = []string{"svc4"}
		list, timestamps, err = s.Match(fieldKey, matches, nil)
		tester.NoError(err)
		tester.True(list.IsEmpty())
		tester.True(timestamps.IsEmpty(), "Timestamps should be empty for empty result set")
	})
}
