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

	"github.com/stretchr/testify/require"

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
	fieldKeyStartTime = index.FieldKey{
		IndexRuleID: 21,
	}
)

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
			field(fieldKeyDuration, convert.Int64ToBytes(100), true),
			field(fieldKeyServiceName, []byte("svc2"), true),
			field(fieldKeyStartTime, convert.Int64ToBytes(100), true),
			field(index.FieldKey{TagName: "short_name"}, []byte("t2"), false),
		},
		Timestamp: int64(101),
	}

	series3 := index.Document{
		EntityValues: []byte("test3"),
		Fields: []index.Field{
			field(fieldKeyDuration, convert.Int64ToBytes(500), true),
			field(fieldKeyStartTime, convert.Int64ToBytes(1000), true),
			field(index.FieldKey{TagName: "short_name"}, []byte("t3"), false),
		},
		Timestamp: int64(1001),
	}
	series4 := index.Document{
		EntityValues: []byte("test4"),
		Fields: []index.Field{
			field(fieldKeyDuration, convert.Int64ToBytes(500), true),
			field(fieldKeyStartTime, convert.Int64ToBytes(2000), true),
		},
		Timestamp: int64(2001),
	}
	return index.Batch{
			Documents: []index.Document{series1, series2, series4, series3},
		}, index.Batch{
			Documents: []index.Document{series3},
		}
}

func field(key index.FieldKey, value []byte, indexed bool) index.Field {
	f := index.NewBytesField(key, value)
	f.Index = indexed
	f.Store = true
	return f
}

func TestStore_StoredFields(t *testing.T) {
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
	insertData(tester, s)
	ctx := context.TODO()

	// No projection: every stored field of test2 is returned, internal
	// bookkeeping fields (_id, _timestamp, ...) excluded.
	all, err := s.StoredFields(ctx, []byte("test2"))
	tester.NoError(err)
	tester.Equal([][]byte{convert.Int64ToBytes(100)}, all[fieldKeyDuration.Marshal()])
	tester.Equal([][]byte{[]byte("svc2")}, all[fieldKeyServiceName.Marshal()])
	tester.Equal([][]byte{convert.Int64ToBytes(100)}, all[fieldKeyStartTime.Marshal()])
	tester.NotContains(all, docIDField)
	tester.NotContains(all, timestampField)

	// Projection: only the requested field is returned.
	only, err := s.StoredFields(ctx, []byte("test2"), fieldKeyDuration)
	tester.NoError(err)
	tester.Len(only, 1)
	tester.Equal([][]byte{convert.Int64ToBytes(100)}, only[fieldKeyDuration.Marshal()])

	// A two-field projection returns exactly those two.
	two, err := s.StoredFields(ctx, []byte("test2"), fieldKeyDuration, fieldKeyServiceName)
	tester.NoError(err)
	tester.Len(two, 2)
	tester.Contains(two, fieldKeyDuration.Marshal())
	tester.Contains(two, fieldKeyServiceName.Marshal())

	// A missing document yields nil.
	none, err := s.StoredFields(ctx, []byte("does-not-exist"))
	tester.NoError(err)
	tester.Nil(none)

	// An array indexed tag stores several values under one field name; they all
	// come back (the result is [][]byte per field, not a single value).
	arrKey := index.FieldKey{IndexRuleID: 4099}
	tester.NoError(s.InsertSeriesBatch(index.Batch{Documents: []index.Document{{
		EntityValues: []byte("arr1"),
		Fields: []index.Field{
			field(arrKey, []byte("a"), true),
			field(arrKey, []byte("b"), true),
			field(fieldKeyServiceName, []byte("svcArr"), true),
		},
	}}}))
	arr, err := s.StoredFields(ctx, []byte("arr1"))
	tester.NoError(err)
	tester.ElementsMatch([][]byte{[]byte("a"), []byte("b")}, arr[arrKey.Marshal()])
	tester.Equal([][]byte{[]byte("svcArr")}, arr[fieldKeyServiceName.Marshal()])

	// Internal bookkeeping fields are excluded even when explicitly projected.
	internalProj, err := s.StoredFields(ctx, []byte("test2"), index.FieldKey{TagName: docIDField}, fieldKeyDuration)
	tester.NoError(err)
	tester.NotContains(internalProj, docIDField)
	tester.Equal([][]byte{convert.Int64ToBytes(100)}, internalProj[fieldKeyDuration.Marshal()])
}
