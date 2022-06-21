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

package executor

import (
	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

type ExecutionContext interface {
	Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	Shard(id common.ShardID) (tsdb.Shard, error)
	ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
}

type StreamExecutionContext interface {
	ExecutionContext
	ParseElementID(item tsdb.Item) (string, error)
}

type StreamExecutable interface {
	Execute(StreamExecutionContext) ([]*streamv1.Element, error)
}

type MeasureExecutionContext interface {
	ExecutionContext
	ParseField(name string, item tsdb.Item) (*measurev1.DataPoint_Field, error)
}

type MIterator interface {
	Next() bool

	Current() []*measurev1.DataPoint

	Close() error
}

var EmptyMIterator = emptyMIterator{}

type emptyMIterator struct{}

func (ei emptyMIterator) Next() bool {
	return false
}

func (ei emptyMIterator) Current() []*measurev1.DataPoint {
	return nil
}

func (ei emptyMIterator) Close() error {
	return nil
}

type MeasureExecutable interface {
	Execute(MeasureExecutionContext) (MIterator, error)
}
