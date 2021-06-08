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

package physical

import (
	"errors"
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
)

type DataType uint8

const (
	ChunkID DataType = iota
	Trace
	Unknown
)

type Data interface {
	DataType() DataType
	Unwrap() interface{}
}

var _ Data = (DataGroup)(nil)

// DataGroup is a group of Data with the same DataType
type DataGroup []Data

func (dg DataGroup) Unwrap() interface{} {
	return dg
}

func (dg DataGroup) Append(data Data) (DataGroup, error) {
	if data == nil {
		return dg, errors.New("fail to append nil data")
	}
	if dg.dataTypeOfLastData() != Unknown && dg.dataTypeOfLastData() != data.DataType() {
		return dg, fmt.Errorf("fail to append data due to different data type, expect %v, actual %v", dg.dataTypeOfLastData(), data.DataType())
	}
	return append(dg, data), nil
}

func (dg DataGroup) dataTypeOfLastData() DataType {
	if dg == nil || len(dg) == 0 {
		return Unknown
	}
	d := dg[len(dg)-1]
	if d == nil {
		return Unknown
	}
	return d.DataType()
}

func (dg DataGroup) DataType() DataType {
	return dg[len(dg)-1].DataType()
}

var _ Data = (*chunkIDs)(nil)

type chunkIDs struct {
	ids []common.ChunkID
}

func (c *chunkIDs) Unwrap() interface{} {
	return c.ids
}

func (c *chunkIDs) DataType() DataType {
	return ChunkID
}

func NewChunkIDs(ids ...common.ChunkID) Data {
	return &chunkIDs{
		ids: ids,
	}
}

var _ Data = (*traces)(nil)

type traces struct {
	traces []*data.Trace
}

func (t *traces) Unwrap() interface{} {
	return t.traces
}

func (t *traces) DataType() DataType {
	return Trace
}

func NewTraceData(input ...*data.Trace) Data {
	return &traces{
		traces: input,
	}
}
