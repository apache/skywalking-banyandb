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

// Package topncheck asserts the Top-N results of a CSV export. It is kept apart from
// package transfer because decoding Top-N values needs banyand/measure, which the liaison
// tests that use package transfer cannot import without a cycle.
package topncheck

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/onsi/gomega"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// Row is one _top_n_result row of an artifact, decoded the way a query reads it.
type Row struct {
	// Tags are the row's Top-N tag cells by tag name (measure.TopNTagNames).
	Tags map[string]string
	// Node is the node directory the row was exported from.
	Node      string
	Timestamp string
	// Decoded renders the decoded result: its name, tags, values and entities.
	Decoded string
	// Items is the number of values of the result.
	Items int
}

// Key identifies the data point of the row: its Top-N tags and timestamp.
func (r Row) Key() string {
	key := make([]string, 0, len(measure.TopNTagNames)+1)
	for _, tag := range measure.TopNTagNames {
		key = append(key, r.Tags[tag])
	}
	return strings.Join(append(key, r.Timestamp), " ")
}

// DecodeRows decodes with g every _top_n_result row of measure group of a, across nodes.
func DecodeRows(g gomega.Gomega, a *transfer.Artifact, group string) []Row {
	key := "measure/" + group
	l := transfer.Layouts(a.Dir)[key]
	g.Expect(l).NotTo(gomega.BeNil(), "the artifact has the layout of %s", key)
	headers := map[string]string{}
	for _, tag := range measure.TopNTagNames {
		idx, ok := l.TagIndex(csvfmt.TopNResultMeasure, measure.TopNTagFamily, tag)
		g.Expect(ok).To(gomega.BeTrue(), "the layout of %s has %s.%s", key, measure.TopNTagFamily, tag)
		headers[tag] = l.Columns[idx].Header
	}
	fieldIdx, ok := l.FieldIndex(csvfmt.TopNResultMeasure, measure.TopNFieldName)
	g.Expect(ok).To(gomega.BeTrue(), "the layout of %s has the Top-N result field", key)
	field := l.Columns[fieldIdx].Header
	var out []Row
	for node, nodeRows := range a.RowsOf("measure", group, csvfmt.TopNResultMeasure) {
		for _, r := range nodeRows {
			desc := fmt.Sprintf("%s row %v on %s", csvfmt.TopNResultMeasure, r, node)
			row := Row{Tags: map[string]string{}, Node: node, Timestamp: r["_timestamp"]}
			for tag, h := range headers {
				row.Tags[tag] = r[h]
			}
			params, err := measure.ParseTopNParameters(row.Tags["parameters"])
			g.Expect(err).NotTo(gomega.HaveOccurred(), desc)
			raw, err := base64.StdEncoding.DecodeString(r[field])
			g.Expect(err).NotTo(gomega.HaveOccurred(), desc)
			row.Decoded, row.Items = decode(g, raw, params.FieldType, desc)
			out = append(out, row)
		}
	}
	return out
}

// ExpectTopNExported asserts that every exported measure group with a Top-N aggregation
// exports the rows of its _top_n_result measure without a warning, each one a Top-N result
// of one of the group's aggregations whose engine-encoded field decodes. It returns the
// number of such rows across every node.
func ExpectTopNExported(a *transfer.Artifact) int {
	gomega.ExpectWithOffset(1, a.Manifest.Warnings).NotTo(gomega.ContainElement(gomega.ContainSubstring(csvfmt.TopNResultMeasure)))
	aggs := map[string][]string{}
	for _, agg := range transfer.Schemas(a.Dir, "topNAggregation", func() *databasev1.TopNAggregation { return &databasev1.TopNAggregation{} }) {
		aggs[agg.GetMetadata().GetGroup()] = append(aggs[agg.GetMetadata().GetGroup()], agg.GetMetadata().GetName())
	}
	var rows, groups int
	for _, key := range a.ExportedGroups() {
		group, isMeasure := strings.CutPrefix(key, "measure/")
		if !isMeasure || len(aggs[group]) == 0 {
			continue
		}
		groups++
		for _, r := range DecodeRows(gomega.Default, a, group) {
			rows++
			desc := fmt.Sprintf("%s row %v on %s", csvfmt.TopNResultMeasure, r.Tags, r.Node)
			gomega.ExpectWithOffset(1, aggs[group]).To(gomega.ContainElement(r.Tags["name"]), desc)
			gomega.ExpectWithOffset(1, r.Tags["direction"]).To(gomega.BeElementOf(
				csvfmt.EncodeInt(int64(modelv1.Sort_SORT_ASC)), csvfmt.EncodeInt(int64(modelv1.Sort_SORT_DESC))), desc)
			gomega.ExpectWithOffset(1, r.Items).To(gomega.BeNumerically(">", 0), desc)
		}
	}
	gomega.ExpectWithOffset(1, groups).To(gomega.BeNumerically(">", 0), "an exported measure group has a Top-N aggregation")
	return rows
}

// decode decodes an engine-encoded Top-N result, renders it and returns its number of values.
func decode(g gomega.Gomega, raw []byte, fieldType databasev1.FieldType, desc string) (string, int) {
	decoder := measure.GenerateTopNValuesDecoder()
	defer measure.ReleaseTopNValuesDecoder(decoder)
	if fieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
		v := &measure.TopNValue[float64]{}
		g.Expect(v.Unmarshal(raw, decoder)).To(gomega.Succeed(), desc)
		name, tags, values, entities := v.Values()
		return fmt.Sprintf("%s %v %v %s", name, tags, values, renderEntities(entities)), len(values)
	}
	v := &measure.TopNValue[int64]{}
	g.Expect(v.Unmarshal(raw, decoder)).To(gomega.Succeed(), desc)
	name, tags, values, entities := v.Values()
	return fmt.Sprintf("%s %v %v %s", name, tags, values, renderEntities(entities)), len(values)
}

func renderEntities(entities [][]*modelv1.TagValue) string {
	var sb strings.Builder
	for _, entity := range entities {
		sb.WriteString("[")
		for _, tv := range entity {
			sb.WriteString(tv.String())
			sb.WriteString(";")
		}
		sb.WriteString("]")
	}
	return sb.String()
}
