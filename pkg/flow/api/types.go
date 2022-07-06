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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Flow interface {
	Filter(f interface{}) Flow
	Map(f interface{}) Flow
	Window(WindowAssigner) WindowedFlow
	Offset(int) Flow
	Limit(int) Flow
	To(interface{}) Flow
	OpenAsync() <-chan error
	OpenSync() error
}

type WindowedFlow interface {
	TopN(topNum int, opts ...any) Flow
	Aggregate(aggrFunc AggregateFunction) Flow
}

type Window interface {
	MaxTimestamp() int64
}

type WindowAssigner interface {
	AssignWindows(int64) ([]Window, error)
}

//go:generate mockgen -destination=./aggregation_func_mock.go -package=api github.com/apache/skywalking-banyandb/pkg/flow/api AggregateFunction
type AggregateFunction interface {
	Add([]interface{})
	GetResult() interface{}
}

type StreamRecord struct {
	ts           int64
	hasTimestamp bool
	data         interface{}
}

func NewStreamRecord(data interface{}, ts int64) StreamRecord {
	return StreamRecord{
		data:         data,
		ts:           ts,
		hasTimestamp: true,
	}
}

func NewStreamRecordWithTimestampPb(data interface{}, timestamp *timestamppb.Timestamp) StreamRecord {
	return StreamRecord{
		data:         data,
		ts:           timestamp.GetSeconds()*1000 + int64(timestamp.GetNanos())/1000_000,
		hasTimestamp: true,
	}
}

func NewStreamRecordWithoutTS(data interface{}) StreamRecord {
	return StreamRecord{
		data:         data,
		ts:           0,
		hasTimestamp: false,
	}
}

func (sr StreamRecord) WithNewData(data interface{}) StreamRecord {
	return StreamRecord{
		ts:           sr.ts,
		hasTimestamp: sr.hasTimestamp,
		data:         data,
	}
}

func (sr StreamRecord) TimestampMillis() int64 {
	return sr.ts
}

func (sr StreamRecord) Data() interface{} {
	return sr.data
}
