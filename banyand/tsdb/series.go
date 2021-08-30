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

package tsdb

import (
	"time"

	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
)

type Iterator interface {
	Next() bool
	Val() Item
	Close() error
}

type Item interface {
	Val(family string) []byte
	SortingVal() []byte
}

type ConditionValue struct {
	Values [][]byte
	Op     modelv2.PairQuery_BinaryOp
}

type Condition map[string][]ConditionValue

type ItemID struct {
}

type TimeRange struct {
	Start    time.Time
	Duration time.Duration
}

type Series interface {
	Span(timeRange TimeRange) (SeriesSpan, error)
	Get(id ItemID) (Item, error)
}

type SeriesSpan interface {
	WriterBuilder() WriterBuilder
	Iterator() Iterator
	SeekerBuilder() SeekerBuilder
}

type WriterBuilder interface {
	Family(name string) WriterBuilder
	Time(ts time.Time) WriterBuilder
	Val(val []byte) WriterBuilder
	OrderBy(order modelv2.QueryOrder_Sort) WriterBuilder
	Build() Writer
}

type Writer interface {
	Write() ItemID
}

type SeekerBuilder interface {
	Filter(condition Condition) SeekerBuilder
	OrderByIndex(name string, order modelv2.QueryOrder_Sort) SeekerBuilder
	OrderByTime(order modelv2.QueryOrder_Sort) SeekerBuilder
	Build() Seeker
}

type Seeker interface {
	Seek() Iterator
}
