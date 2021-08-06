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

package data

import (
	"github.com/apache/skywalking-banyandb/api/common"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var TraceKindVersion = common.KindVersion{Version: "v1", Kind: "data-trace"}

var WriteEventKindVersion = common.KindVersion{
Version: "v1",
Kind:    "trace-write",
}
var TopicWriteEvent = bus.UniTopic(WriteEventKindVersion.String())

type Trace struct {
	common.KindVersion
	Entities []Entity
}

type Entity struct {
	*v1.Entity
}

type EntityValue struct {
	*v1.EntityValue
}
type TraceWriteDate struct {
	ShardID      uint
	SeriesID     uint64
	WriteRequest *v1.WriteRequest
}
type Write struct {
	common.KindVersion
	Payload *TraceWriteDate
}
func NewTrace() *Trace {
	return &Trace{KindVersion: TraceKindVersion}
}
