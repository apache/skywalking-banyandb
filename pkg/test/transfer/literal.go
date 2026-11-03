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

package transfer

import (
	"math"
	"strconv"
	"strings"

	"github.com/onsi/gomega"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// literalRow is one exported row picked by the value of a column, with cells it must hold
// spelled out as literals. Columns are named "family.tag" (stream, measure), "tag" (trace,
// property), "field:<name>" (measure fields) or by the fixed header ("_timestamp", "_span").
type literalRow struct {
	cells              map[string]string
	catalog, group     string
	name, key, keyCell string
}

// literalHeader resolves a column name of literalRow to its header in layout; fixed columns
// are their own header.
func literalHeader(layout *csvfmt.Layout, catalog, resource, column string) string {
	var idx int
	var ok bool
	switch {
	case strings.HasPrefix(column, "_"), catalog == "trace" && (column == "trace_id" || column == "span_id"):
		return column
	case strings.HasPrefix(column, "field:"):
		idx, ok = layout.FieldIndex(resource, strings.TrimPrefix(column, "field:"))
	case catalog == "stream" || catalog == "measure":
		family, tag, _ := strings.Cut(column, ".")
		idx, ok = layout.TagIndex(resource, family, tag)
	default:
		idx, ok = layout.TagIndex(resource, "", column)
	}
	gomega.ExpectWithOffset(3, ok).To(gomega.BeTrue(), "the %s layout has the column %s of %s", catalog, column, resource)
	return layout.Header()[idx]
}

// VerifyLiteralCells cross-checks a few critical cells of the suite data against literal
// strings written by hand from the documented cell encoding, independently of the csvfmt encoders that
// both the data node and Expected use: a shared encoder bug would otherwise pass the
// cell-by-cell comparison. The suites write no float field and no int array, so those two
// encoders are pinned to literals directly.
func (a *Artifact) VerifyLiteralCells(d SuiteData, layouts map[string]*csvfmt.Layout) {
	nanos := func(v int64) string { return strconv.FormatInt(v, 10) }
	rows := []literalRow{
		{
			catalog: "stream", group: StreamGroup, name: StreamName, key: "searchable.trace_id", keyCell: "5",
			cells: map[string]string{
				// base64 of "abc123!?$*&()'-=@~", standard alphabet with padding.
				"data.data_binary":            "YWJjMTIzIT8kKiYoKSctPUB+",
				"searchable.extended_tags":    `["a","b","c"]`,
				"searchable.non_indexed_tags": `["a","b","c"]`,
				"searchable.duration":         "300",
				"searchable.mq.queue":         `\N`,
				"_timestamp":                  nanos(d.StreamBase.Add(4 * d.Interval).UnixNano()),
			},
		},
		{
			catalog: "measure", group: MeasureGroup, name: MeasureName, key: "default.entity_id", keyCell: "entity_6",
			cells: map[string]string{
				"default.id":  "svc3",
				"field:total": "300",
				"field:value": "6",
				"_timestamp":  nanos(d.MeasureBase.UnixNano()),
			},
		},
		{
			catalog: "trace", group: TraceGroup, name: TraceName, key: "span_id", keyCell: "span_001_1",
			cells: map[string]string{
				"trace_id": "trace_001",
				// base64 of "trace_001_span_1".
				"_span":     "dHJhY2VfMDAxX3NwYW5fMQ==",
				"timestamp": nanos(d.TraceBase.Add(d.Interval).UnixNano()),
			},
		},
	}
	for _, lr := range rows {
		layout := layouts[lr.catalog+"/"+lr.group]
		gomega.ExpectWithOffset(1, layout).NotTo(gomega.BeNil(), "layout of %s/%s", lr.catalog, lr.group)
		keyHeader := literalHeader(layout, lr.catalog, lr.name, lr.key)
		var found int
		for node, nodeRows := range a.RowsOf(lr.catalog, lr.group, lr.name) {
			for _, r := range nodeRows {
				if r[keyHeader] != lr.keyCell {
					continue
				}
				found++
				for column, want := range lr.cells {
					gomega.ExpectWithOffset(1, r).To(gomega.HaveKeyWithValue(literalHeader(layout, lr.catalog, lr.name, column), want),
						"%s/%s/%s row %s=%s on %s: literal cell %s", lr.catalog, lr.group, lr.name, lr.key, lr.keyCell, node, column)
				}
			}
		}
		gomega.ExpectWithOffset(1, found).To(gomega.BeNumerically(">", 0), "%s/%s/%s has the row %s=%s", lr.catalog, lr.group, lr.name, lr.key, lr.keyCell)
	}
	expectLiteralEncodings()
}

// expectLiteralEncodings pins the encoders of the value kinds the suites do not write.
func expectLiteralEncodings() {
	for _, c := range []struct {
		want string
		v    float64
	}{
		{"0.1", 0.1},
		{"-2.5", -2.5},
		{"1000000000000000000000", 1e21},
		{"100", 100},
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	} {
		gomega.ExpectWithOffset(2, csvfmt.EncodeFieldValue(&modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: c.v}}})).
			To(gomega.Equal(c.want), "float field %v", c.v)
	}
	intArray := func(v ...int64) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: v}}}
	}
	gomega.ExpectWithOffset(2, csvfmt.EncodeTagValue(intArray(1, -2, 300))).To(gomega.Equal("1;-2;300"))
	gomega.ExpectWithOffset(2, csvfmt.EncodeTagValue(intArray())).To(gomega.Equal(""))
	strArray := &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a\"b", "<c>"}}}}
	gomega.ExpectWithOffset(2, csvfmt.EncodeTagValue(strArray)).To(gomega.Equal(`["a\"b","<c>"]`))
	binary := &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{0xff, 0x00, 0x01}}}
	gomega.ExpectWithOffset(2, csvfmt.EncodeTagValue(binary)).To(gomega.Equal("/wAB"))
	gomega.ExpectWithOffset(2, csvfmt.EncodeTagValue(&modelv1.TagValue{Value: &modelv1.TagValue_Null{}})).To(gomega.Equal(`\N`))
}
