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

package stream

import (
	"bytes"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type tagValue struct {
	tag       string
	value     []byte
	valueArr  [][]byte
	valueType pbv1.ValueType
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

type elements struct {
	seriesIDs   []common.SeriesID
	timestamps  []int64
	elementIDs  []uint64
	tagFamilies [][]tagValues
}

func (e *elements) Len() int {
	return len(e.seriesIDs)
}

func (e *elements) Less(i, j int) bool {
	if e.seriesIDs[i] != e.seriesIDs[j] {
		return e.seriesIDs[i] < e.seriesIDs[j]
	}
	return e.timestamps[i] < e.timestamps[j]
}

func (e *elements) Swap(i, j int) {
	e.seriesIDs[i], e.seriesIDs[j] = e.seriesIDs[j], e.seriesIDs[i]
	e.timestamps[i], e.timestamps[j] = e.timestamps[j], e.timestamps[i]
	e.elementIDs[i], e.elementIDs[j] = e.elementIDs[j], e.elementIDs[i]
	e.tagFamilies[i], e.tagFamilies[j] = e.tagFamilies[j], e.tagFamilies[i]
}

type elementsInTable struct {
	timeRange timestamp.TimeRange
	tsTable   storage.TSTableWrapper[*tsTable]

	elements elements
	docs     index.Documents
}

type elementsInGroup struct {
	tsdb     storage.TSDB[*tsTable, option]
	docs     index.Documents
	tables   []*elementsInTable
	latestTS int64
}
