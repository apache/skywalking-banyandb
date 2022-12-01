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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/index/testcases"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

var (
	serviceName = index.FieldKey{
		// http_method
		IndexRuleID: 6,
		SeriesID:    common.SeriesID(0),
		Analyzer:    databasev1.IndexRule_ANALYZER_SIMPLE,
	}
	serviceName1 = index.FieldKey{
		// http_method
		IndexRuleID: 6,
		SeriesID:    common.SeriesID(1),
		Analyzer:    databasev1.IndexRule_ANALYZER_SIMPLE,
	}
)

func TestStore_Match(t *testing.T) {
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
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName,
		Term: []byte("GET::/product/order"),
	}}, common.ItemID(1)))
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName,
		Term: []byte("GET::/root/product"),
	}}, common.ItemID(2)))
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName,
		Term: []byte("org.apache.skywalking.examples.OrderService.order"),
	}}, common.ItemID(3)))
	s.(*store).flush()
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName1,
		Term: []byte("test.1"),
	}}, common.ItemID(1)))
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName1,
		Term: []byte("test.2"),
	}}, common.ItemID(2)))
	tester.NoError(s.Write([]index.Field{{
		Key:  serviceName1,
		Term: []byte("test.3"),
	}}, common.ItemID(3)))
	s.(*store).flush()

	tests := []struct {
		want    posting.List
		matches []string
		wantErr bool
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
			matches: []string{"/product/order"},
			want:    roaring.NewPostingListWithInitialData(1, 2, 3),
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
	}
	for _, tt := range tests {
		name := strings.Join(tt.matches, " and ")
		t.Run(name, func(t *testing.T) {
			list, err := s.Match(serviceName, tt.matches)
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
	s.(*store).flush()
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
	s.(*store).flush()
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
