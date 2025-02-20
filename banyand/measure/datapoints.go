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

package measure

import (
	"bytes"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type nameValue struct {
	name      string
	value     []byte
	valueArr  [][]byte
	valueType pbv1.ValueType
}

func (n *nameValue) reset() {
	n.name = ""
	n.value = nil
	n.valueArr = nil
}

func generateNameValue() *nameValue {
	v := nameValuePool.Get()
	if v == nil {
		return &nameValue{}
	}
	return v
}

func releaseNameValue(v *nameValue) {
	v.reset()
	nameValuePool.Put(v)
}

var nameValuePool = pool.Register[*nameValue]("measure-nameValue")

func (n *nameValue) size() int {
	s := len(n.name)
	if n.value != nil {
		s += len(n.value)
	}
	if n.valueArr != nil {
		for i := range n.valueArr {
			s += len(n.valueArr[i])
		}
	}
	return s
}

func (n *nameValue) marshal() []byte {
	if n.valueArr != nil {
		var dst []byte
		for i := range n.valueArr {
			if n.valueType == pbv1.ValueTypeInt64Arr {
				dst = append(dst, n.valueArr[i]...)
				continue
			}
			dst = marshalVarArray(dst, n.valueArr[i])
		}
		return dst
	}
	return n.value
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

type nameValues struct {
	name   string
	values []*nameValue
}

func (n *nameValues) reset() {
	n.name = ""
	for i := range n.values {
		releaseNameValue(n.values[i])
	}
	n.values = n.values[:0]
}

type dataPoints struct {
	seriesIDs   []common.SeriesID
	timestamps  []int64
	versions    []int64
	tagFamilies [][]nameValues
	fields      []nameValues
}

func (d *dataPoints) reset() {
	d.seriesIDs = d.seriesIDs[:0]
	d.timestamps = d.timestamps[:0]
	d.versions = d.versions[:0]
	for i := range d.tagFamilies {
		for j := range d.tagFamilies[i] {
			d.tagFamilies[i][j].reset()
		}
	}
	d.tagFamilies = d.tagFamilies[:0]
	for i := range d.fields {
		d.fields[i].reset()
	}
	d.fields = d.fields[:0]
}

func (d *dataPoints) skip(i int) {
	if len(d.timestamps) <= i {
		return
	}
	d.seriesIDs = append(d.seriesIDs[:i], d.seriesIDs[i+1:]...)
	d.timestamps = append(d.timestamps[:i], d.timestamps[i+1:]...)
	d.versions = append(d.versions[:i], d.versions[i+1:]...)
	d.tagFamilies = append(d.tagFamilies[:i], d.tagFamilies[i+1:]...)
	if len(d.fields) > 0 {
		d.fields = append(d.fields[:i], d.fields[i+1:]...)
	}
}

func (d *dataPoints) Len() int {
	return len(d.seriesIDs)
}

func (d *dataPoints) Less(i, j int) bool {
	if d.seriesIDs[i] != d.seriesIDs[j] {
		return d.seriesIDs[i] < d.seriesIDs[j]
	}
	if d.timestamps[i] != d.timestamps[j] {
		return d.timestamps[i] < d.timestamps[j]
	}
	return d.versions[i] > d.versions[j]
}

func (d *dataPoints) Swap(i, j int) {
	d.seriesIDs[i], d.seriesIDs[j] = d.seriesIDs[j], d.seriesIDs[i]
	d.timestamps[i], d.timestamps[j] = d.timestamps[j], d.timestamps[i]
	d.versions[i], d.versions[j] = d.versions[j], d.versions[i]
	d.tagFamilies[i], d.tagFamilies[j] = d.tagFamilies[j], d.tagFamilies[i]
	d.fields[i], d.fields[j] = d.fields[j], d.fields[i]
}

func generateDataPoints() *dataPoints {
	v := dataPointsPool.Get()
	if v == nil {
		return &dataPoints{}
	}
	return v
}

func releaseDataPoints(v *dataPoints) {
	v.reset()
	dataPointsPool.Put(v)
}

var dataPointsPool = pool.Register[*dataPoints]("measure-dataPoints")

type dataPointsInTable struct {
	tsTable    *tsTable
	dataPoints *dataPoints
	timeRange  timestamp.TimeRange
}

type dataPointsInGroup struct {
	tsdb          storage.TSDB[*tsTable, option]
	metadataDocs  index.Documents
	indexModeDocs index.Documents
	tables        []*dataPointsInTable
	segments      []storage.Segment[*tsTable, option]
	latestTS      int64
}
