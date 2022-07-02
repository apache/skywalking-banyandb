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

package api

import (
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type Outlet[O any] interface {
	Output() iter.Iterator[O]
}

type Operator[I, O any] interface {
	Transform(iter.Iterator[I]) iter.Iterator[O]
}

type Source interface {
	Shards(shardingKeys tsdb.Entity) iter.Iterator[tsdb.Series]
	TimeRange() timestamp.TimeRange
	Metadata() *commonv1.Metadata
}

type Sink[T any] interface {
	Drain(iter.Iterator[T]) error
	Val() []T
}
