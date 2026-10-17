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

package querybench

import (
	"fmt"
	"hash/fnv"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const qlbT17Measure = "qlb_t17"

type qlbT17Spec struct {
	name           string
	workers        int
	full, composed bool
}

func qlbT17Specs() []qlbT17Spec {
	return []qlbT17Spec{
		{name: "fullsort_slice", workers: 1, full: true},
		{name: "topk", workers: 1},
		{name: "composed_workers_1", workers: 1, composed: true},
		{name: "composed_workers_8", workers: 8, composed: true},
		{name: "composed_workers_32", workers: 32, composed: true},
	}
}

func (s qlbT17Spec) calls(rows int) int {
	if s.composed {
		return 96
	}
	calls, _, _ := qlbT17Schedule(rows)
	return calls
}

// qlbT17Keys defines a reproducible nullable two-key fixture. The unique rid is
// the final key, making the expected page independent of storage iteration.
func qlbT17Keys(row int) (first, second int64, firstValid, secondValid bool) {
	value := uint64(row+1) * 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64((value ^ (value >> 31)) % 4096), int64(row % 7), row%19 != 0, row%23 != 0
}

type qlbT17Oracle struct {
	expected                                          *measurev1.QueryResponse
	spec                                              qlbT17Spec
	rows, binding, scanned, passing, groups, sortRows int
	hash                                              uint64
}

func newQLBT17Oracle(spec qlbT17Spec, rows, binding int, base time.Time) *qlbT17Oracle {
	oracle := &qlbT17Oracle{spec: spec, rows: rows, binding: binding, scanned: rows, sortRows: rows}
	if spec.composed {
		oracle.expected = oracle.composed(base)
	} else {
		oracle.expected = oracle.raw(base)
	}
	oracle.hash = qlbT17OrderedHash(oracle.expected)
	return oracle
}

func (o *qlbT17Oracle) raw(base time.Time) *measurev1.QueryResponse {
	ids := make([]int, o.rows)
	for row := range ids {
		ids[row] = row
	}
	// This comparator operates directly on independent fixture tuples, never on
	// production expressions, columns or comparator helpers.
	slices.SortFunc(ids, func(left, right int) int {
		a, b, av, bv := qlbT17Keys(left)
		c, d, cv, dv := qlbT17Keys(right)
		if av != cv {
			if av {
				return -1
			}
			return 1
		}
		if av && a != c {
			if a > c {
				return -1
			}
			return 1
		}
		if bv != dv {
			if !bv {
				return -1
			}
			return 1
		}
		if bv && b != d {
			if b < d {
				return -1
			}
			return 1
		}
		return left - right
	})
	expected := &measurev1.QueryResponse{}
	for _, row := range ids[min(5, len(ids)):min(15, len(ids))] {
		value := &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}}
		if row%5 != 0 {
			value = intFieldValue(int64(row%97 - 48))
		}
		expected.DataPoints = append(expected.DataPoints, &measurev1.DataPoint{
			Timestamp: timestamppb.New(base.Add(time.Duration(row) * time.Millisecond)),
			Fields:    []*measurev1.DataPoint_Field{{Name: "rid", Value: intFieldValue(int64(row))}, {Name: "value", Value: value}},
		})
	}
	return expected
}

func (o *qlbT17Oracle) composed(_ time.Time) *measurev1.QueryResponse {
	type group struct {
		host        string
		total, peak int64
		present     bool
	}
	groups := make([]group, 16)
	for host := range groups {
		groups[host].host = fmt.Sprintf("host_%02d", host)
	}
	first, threshold := 0, int64(1)
	if o.binding == 1 {
		threshold = 10
	}
	if o.binding == 2 {
		first = o.rows / 2
	}
	o.scanned, o.passing = o.rows-first, 0
	for row := first; row < o.rows; row++ {
		if row%5 == 0 || int64(row%97-48) <= threshold {
			continue
		}
		o.passing++
		g := &groups[row%16]
		value := int64(row%97 - 48)
		g.total += value
		if !g.present || value > g.peak {
			g.peak = value
		}
		g.present = true
	}
	surviving := make([]group, 0, 16)
	for _, g := range groups {
		if g.present {
			o.groups++
		}
		if g.present && g.total > 3 {
			surviving = append(surviving, g)
		}
	}
	o.sortRows = len(surviving)
	slices.SortFunc(surviving, func(left, right group) int {
		if left.total > right.total {
			return -1
		}
		if left.total < right.total {
			return 1
		}
		if left.host < right.host {
			return -1
		}
		if left.host > right.host {
			return 1
		}
		return 0
	})
	expected := &measurev1.QueryResponse{}
	if len(surviving) < 2 {
		return expected
	}
	selected := surviving[1]
	expected.DataPoints = []*measurev1.DataPoint{{
		TagFamilies: []*modelv1.TagFamily{{Name: benchTagFamily, Tags: []*modelv1.Tag{{Key: "node", Value: strTagValue(selected.host)}}}},
		Fields:      []*measurev1.DataPoint_Field{{Name: "total", Value: intFieldValue(selected.total)}, {Name: "peak", Value: intFieldValue(selected.peak)}},
	}}
	return expected
}

func (o *qlbT17Oracle) query(base time.Time) *bydbqlv1.QueryRequest {
	query := "SELECT rid::FIELD, v::FIELD AS value FROM " + benchGroupName + "." + qlbT17Measure +
		" WHERE time >= ? AND time < ? ORDER BY key0::FIELD + 1 DESC NULLS LAST, key1::FIELD ASC NULLS FIRST, rid::FIELD ASC LIMIT 10 OFFSET 5"
	first := 0
	if o.spec.composed {
		query = "SELECT host AS node, SUM(v)::FIELD AS total, MAX(v)::FIELD AS peak FROM " + benchGroupName + "." + qlbT17Measure +
			" WHERE time >= ? AND time < ? AND v::FIELD > CAST(? AS DOUBLE) GROUP BY host HAVING SUM(v)::FIELD > 3" +
			" ORDER BY total DESC NULLS LAST, node ASC LIMIT 1 OFFSET 1"
		if o.binding == 2 {
			first = o.rows / 2
		}
	}
	request := &bydbqlv1.QueryRequest{Query: query, Params: []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(first) * time.Millisecond))}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(o.rows) * time.Millisecond))}},
	}}
	if o.spec.composed {
		threshold := "1"
		if o.binding == 1 {
			threshold = "10"
		}
		request.Params = append(request.Params, strTagValue(threshold))
	}
	return request
}

func (o *qlbT17Oracle) check(response *measurev1.QueryResponse) error {
	if response == nil {
		return fmt.Errorf("T17 %s returned no measure result", o.spec.name)
	}
	normalized := proto.Clone(response).(*measurev1.QueryResponse)
	for _, point := range normalized.DataPoints {
		point.Sid, point.Version = 0, 0
	}
	if !proto.Equal(normalized, o.expected) {
		return fmt.Errorf("T17 %s binding %d differs from independent ordered response: got %s, want %s", o.spec.name, o.binding, normalized, o.expected)
	}
	return nil
}

func qlbT17OrderedHash(response *measurev1.QueryResponse) uint64 {
	// Marshal the complete repeated-field envelope, preserving row order and
	// lengths. Only storage-generated SID/version are nondeterministic here.
	normalized := proto.Clone(response).(*measurev1.QueryResponse)
	for _, point := range normalized.DataPoints {
		point.Sid, point.Version = 0, 0
	}
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if err != nil {
		panic(err)
	}
	hash := fnv.New64a()
	_, _ = hash.Write(body)
	return hash.Sum64()
}
