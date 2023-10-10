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

package kv

import (
	"reflect"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/y"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var arenaSize = int64(4 << 20)

func TestTimeRangeIterator(t *testing.T) {
	// Create a new skiplist and insert some data
	sl := skl.NewSkiplist(arenaSize)
	sl.Put(y.KeyWithTs([]byte("key"), 1), y.ValueStruct{Value: []byte("value1"), Meta: 0})
	sl.Put(y.KeyWithTs([]byte("key"), 2), y.ValueStruct{Value: []byte("value2"), Meta: 0})
	sl.Put(y.KeyWithTs([]byte("key"), 3), y.ValueStruct{Value: []byte("value3"), Meta: 0})
	sl.Put(y.KeyWithTs([]byte("key"), 4), y.ValueStruct{Value: []byte("value4"), Meta: 0})

	// Create a new time range iterator for the skiplist
	iter := &timeRangeIterator{
		timeRange: timestamp.NewInclusiveTimeRange(
			timestamp.DefaultTimeRange.Begin.AsTime(),
			timestamp.DefaultTimeRange.End.AsTime()),
		UniIterator: sl.NewUniIterator(false),
	}

	// Test Next() and Value() methods
	var values []string
	for iter.Rewind(); iter.Valid(); iter.Next() {
		values = append(values, string(iter.Value().Value))
	}
	expectedValues := []string{"value4", "value3", "value2", "value1"}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("unexpected values: %v, expected: %v", values, expectedValues)
	}

	// Test Next() method with time range filtering
	iter = &timeRangeIterator{
		timeRange:   timestamp.NewSectionTimeRange(time.Unix(0, 2), time.Unix(0, 3)),
		UniIterator: sl.NewUniIterator(false),
	}
	values = nil
	for iter.Rewind(); iter.Valid(); iter.Next() {
		values = append(values, string(iter.Value().Value))
	}
	expectedValues = []string{"value2"}
	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("unexpected values: %v, expected: %v", values, expectedValues)
	}
}
