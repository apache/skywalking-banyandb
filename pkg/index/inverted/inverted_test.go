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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
			list, err := s.Match(serviceName, tt.matches, &modelv1.Condition_MatchOption{
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
			list, err := s.Match(serviceName, tt.matches, &modelv1.Condition_MatchOption{
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
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("GET::/product/order"),
			}},
			DocID: 1,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("GET::/root/product"),
			}},
			DocID: 2,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("org.apache.skywalking.examples.OrderService.order"),
			}},
			DocID: 3,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("/svc1/v1/user"),
			}},
			DocID: 4,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("/svc1/v2/user"),
			}},
			DocID: 5,
		},
	)
	tester.NoError(s.Batch(batch))
}

func setupSeries(tester *assert.Assertions, s index.Store, serviceName index.FieldKey) {
	var batch index.Batch
	batch.Documents = append(batch.Documents,
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("test.a"),
			}},
			DocID: 1,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("test.b"),
			}},
			DocID: 2,
		},
		index.Document{
			Fields: []index.Field{{
				Key:  serviceName,
				Term: []byte("test.c"),
			}},
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

func setUp(t *require.Assertions) (tempDir string, deferFunc func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	tempDir, deferFunc = test.Space(t)
	return tempDir, deferFunc
}
