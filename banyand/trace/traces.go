// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"bytes"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type tagValue struct {
	tag       string
	value     []byte
	valueArr  [][]byte
	valueType pbv1.ValueType
}

func (t *tagValue) reset() {
	t.tag = ""
	t.value = nil
	t.valueArr = nil
}

func (t *tagValue) size() int {
	s := len(t.tag)
	if t.value != nil {
		s += len(t.value)
	}
	if t.valueArr != nil {
		for i := range t.valueArr {
			s += len(t.valueArr[i])
		}
	}
	return s
}

func (t *tagValue) marshal() []byte {
	if t.valueArr != nil {
		var dst []byte
		for i := range t.valueArr {
			if t.valueType == pbv1.ValueTypeInt64Arr {
				dst = append(dst, t.valueArr[i]...)
				continue
			}
			dst = marshalVarArray(dst, t.valueArr[i])
		}
		return dst
	}
	return t.value
}

func generateTagValue() *tagValue {
	v := tagValuePool.Get()
	if v == nil {
		return &tagValue{}
	}
	return v
}

func releaseTagValue(v *tagValue) {
	v.reset()
	tagValuePool.Put(v)
}

var tagValuePool = pool.Register[*tagValue]("trace-tagValue")

const (
	entityDelimiter = '|'
	escape          = '\\'
)

func marshalVarArray(dest, src []byte) []byte {
	if bytes.IndexByte(src, entityDelimiter) < 0 && bytes.IndexByte(src, escape) < 0 {
		dest = append(dest, src...)
		dest = append(dest, entityDelimiter)
		return dest
	}
	for _, b := range src {
		if b == entityDelimiter || b == escape {
			dest = append(dest, escape)
		}
		dest = append(dest, b)
	}
	dest = append(dest, entityDelimiter)
	return dest
}

func unmarshalVarArray(dest, src []byte) ([]byte, []byte, error) {
	if len(src) == 0 {
		return nil, nil, errors.New("empty entity value")
	}
	if src[0] == entityDelimiter {
		return dest, src[1:], nil
	}
	for len(src) > 0 {
		switch {
		case src[0] == escape:
			if len(src) < 2 {
				return nil, nil, errors.New("invalid escape character")
			}
			src = src[1:]
			dest = append(dest, src[0])
		case src[0] == entityDelimiter:
			return dest, src[1:], nil
		default:
			dest = append(dest, src[0])
		}
		src = src[1:]
	}
	return nil, nil, errors.New("invalid variable array")
}

type tagValues struct {
	tag    string
	values []*tagValue
}

func (t *tagValues) reset() {
	t.tag = ""
	for i := range t.values {
		releaseTagValue(t.values[i])
	}
	t.values = t.values[:0]
}

type traces struct {
	traceIDs   []string
	timestamps []int64
	tags       [][]*tagValue
	spans      [][]byte
}

func (t *traces) reset() {
	t.traceIDs = t.traceIDs[:0]
	t.timestamps = t.timestamps[:0]
	for i := range t.tags {
		for j := range t.tags[i] {
			releaseTagValue(t.tags[i][j])
		}
	}
	t.tags = t.tags[:0]
	t.spans = t.spans[:0]
}

func (t *traces) Len() int {
	return len(t.traceIDs)
}

func (t *traces) Less(i, j int) bool {
	return t.traceIDs[i] < t.traceIDs[j]
}

func (t *traces) Swap(i, j int) {
	t.traceIDs[i], t.traceIDs[j] = t.traceIDs[j], t.traceIDs[i]
	t.timestamps[i], t.timestamps[j] = t.timestamps[j], t.timestamps[i]
	t.tags[i], t.tags[j] = t.tags[j], t.tags[i]
	t.spans[i], t.spans[j] = t.spans[j], t.spans[i]
}

func generateTraces() *traces {
	v := tracesPool.Get()
	if v == nil {
		return &traces{}
	}
	return v
}

func releaseTraces(t *traces) {
	t.reset()
	tracesPool.Put(t)
}

var tracesPool = pool.Register[*traces]("trace-traces")

type tracesInTable struct {
	segment   storage.Segment[*tsTable, option]
	tsTable   *tsTable
	traces    *traces
	timeRange timestamp.TimeRange
	shardID   common.ShardID
}

type tracesInGroup struct {
	tsdb     storage.TSDB[*tsTable, option]
	tables   []*tracesInTable
	segments []storage.Segment[*tsTable, option]
	latestTS int64
}

type tracesInQueue struct {
	name   string
	queue  *wqueue.Queue[*tsTable, option]
	tables []*tracesInTable
}
