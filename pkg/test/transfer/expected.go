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
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// streamBinaryTag is the data_binary value test/cases/stream/data writes into every element.
const streamBinaryTag = "YWJjMTIzIT8kKiYoKSctPUB+"

// Expected is the set of rows one resource must export, keyed by an identity column.
type Expected struct {
	// Rows maps the key to the expected row, keyed by header.
	Rows map[string]map[string]string
	// Key is the header whose cell identifies a row.
	Key string
	// Ignore lists headers whose cells are not predictable and are not compared.
	Ignore []string
}

// Match asserts that got holds exactly the expected rows, cell by cell.
func (e Expected) Match(got []map[string]string, desc string) {
	byKey := map[string]map[string]string{}
	for _, r := range got {
		k := r[e.Key]
		_, dup := byKey[k]
		gomega.ExpectWithOffset(1, dup).To(gomega.BeFalse(), "%s: row %s=%s exported twice", desc, e.Key, k)
		byKey[k] = r
	}
	keys := func(m map[string]map[string]string) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	gomega.ExpectWithOffset(1, keys(byKey)).To(gomega.ConsistOf(keys(e.Rows)), "%s: exported row keys", desc)
	for k, want := range e.Rows {
		row := byKey[k]
		gomega.ExpectWithOffset(1, row).To(gomega.HaveLen(len(want)), "%s: row %s has the layout's columns", desc, k)
		for h, cell := range want {
			if slices.Contains(e.Ignore, h) {
				continue
			}
			gomega.ExpectWithOffset(1, row).To(gomega.HaveKeyWithValue(h, cell), "%s: row %s column %s", desc, k, h)
		}
	}
}

// repoRoot finds the repository root (the directory holding go.mod) above the working directory.
func repoRoot() string {
	dir, err := os.Getwd()
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		gomega.ExpectWithOffset(2, parent).NotTo(gomega.Equal(dir), "no go.mod above the working directory")
		dir = parent
	}
}

// readTestData reads test/cases/<catalog>/data/testdata/<file>.
func readTestData(catalog, file string) []json.RawMessage {
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "test", "cases", catalog, "data", "testdata", file))
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	var out []json.RawMessage
	gomega.ExpectWithOffset(2, json.Unmarshal(raw, &out)).To(gomega.Succeed())
	return out
}

func rowMap(layout *csvfmt.Layout, cells []string) map[string]string {
	out := make(map[string]string, len(cells))
	for i, h := range layout.Header() {
		out[h] = cells[i]
	}
	return out
}

func setTag(layout *csvfmt.Layout, cells []string, resource, family, tag string, v *modelv1.TagValue) {
	idx, ok := layout.TagIndex(resource, family, tag)
	gomega.ExpectWithOffset(2, ok).To(gomega.BeTrue(), "layout has %s %s.%s", resource, family, tag)
	cells[idx] = csvfmt.EncodeTagValue(v)
}

// ExpectedStream is what test/cases/stream/data.WriteToGroup(conn, name, group, name, base, interval)
// wrote, laid out with layout; rows are keyed by _element_id.
func ExpectedStream(layout *csvfmt.Layout, stream *databasev1.Stream, base time.Time, interval time.Duration) Expected {
	name, group := stream.GetMetadata().GetName(), stream.GetMetadata().GetGroup()
	bb, err := base64.StdEncoding.DecodeString(streamBinaryTag)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	e := Expected{Key: "_element_id", Rows: map[string]map[string]string{}}
	for i, raw := range readTestData("stream", name+".json") {
		fam := &modelv1.TagFamilyForWrite{}
		gomega.ExpectWithOffset(1, protojson.Unmarshal(raw, fam)).To(gomega.Succeed())
		families := []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_BinaryData{BinaryData: bb}}}}, fam}
		cells := layout.NewRow()
		cells[0] = name
		id := csvfmt.EncodeElementID(convert.HashStr(group + "|" + name + "|" + strconv.Itoa(i)))
		cells[1] = id
		cells[2] = csvfmt.EncodeTimestamp(base.Add(interval * time.Duration(i)).UnixNano())
		for fi, f := range families {
			spec := stream.GetTagFamilies()[fi]
			for ti, v := range f.GetTags() {
				setTag(layout, cells, name, spec.GetName(), spec.GetTags()[ti].GetName(), v)
			}
		}
		e.Rows[id] = rowMap(layout, cells)
	}
	return e
}

// ExpectedMeasure is what test/cases/measure/data.Write(conn, name, group, file, base, interval)
// wrote, laid out with layout; rows are keyed by keyTag ("family.tag"). _version is not compared.
func ExpectedMeasure(layout *csvfmt.Layout, measure *databasev1.Measure, file, keyTag string, base time.Time, interval time.Duration) Expected {
	name := measure.GetMetadata().GetName()
	e := Expected{Key: keyTag, Rows: map[string]map[string]string{}, Ignore: []string{"_version"}}
	points := readTestData("measure", file)
	for i, raw := range points {
		dp := &measurev1.DataPointValue{}
		gomega.ExpectWithOffset(1, protojson.Unmarshal(raw, dp)).To(gomega.Succeed())
		cells := layout.NewRow()
		cells[0] = name
		cells[1] = csvfmt.EncodeTimestamp(base.Add(-time.Duration(len(points)-i-1) * interval).UnixNano())
		for fi, f := range dp.GetTagFamilies() {
			spec := measure.GetTagFamilies()[fi]
			for ti, v := range f.GetTags() {
				setTag(layout, cells, name, spec.GetName(), spec.GetTags()[ti].GetName(), v)
			}
		}
		for fi, v := range dp.GetFields() {
			idx, ok := layout.FieldIndex(name, measure.GetFields()[fi].GetName())
			gomega.ExpectWithOffset(1, ok).To(gomega.BeTrue())
			cells[idx] = csvfmt.EncodeFieldValue(v)
		}
		row := rowMap(layout, cells)
		e.Rows[row[keyTag]] = row
	}
	return e
}

// ExpectedTrace is what test/cases/trace/data.WriteToGroup(conn, name, group, name, base, interval)
// wrote, laid out with layout; rows are keyed by _span.
func ExpectedTrace(layout *csvfmt.Layout, trace *databasev1.Trace, base time.Time, interval time.Duration) Expected {
	name := trace.GetMetadata().GetName()
	e := Expected{Key: "_span", Rows: map[string]map[string]string{}}
	for i, raw := range readTestData("trace", name+".json") {
		var tmpl struct {
			Span string            `json:"span"`
			Tags []json.RawMessage `json:"tags"`
		}
		gomega.ExpectWithOffset(1, json.Unmarshal(raw, &tmpl)).To(gomega.Succeed())
		values := make([]*modelv1.TagValue, 0, len(tmpl.Tags)+1)
		for _, t := range tmpl.Tags {
			v := &modelv1.TagValue{}
			gomega.ExpectWithOffset(1, protojson.Unmarshal(t, v)).To(gomega.Succeed())
			values = append(values, v)
		}
		// Versions start at 1 and set the timestamp tag the writer appends.
		ts := base.Add(interval * time.Duration(i+1))
		values = append(values, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: ts.UnixNano()}}})
		cells := layout.NewRow()
		cells[0] = name
		cells[3] = csvfmt.EncodeBinary([]byte(tmpl.Span))
		for ti, v := range values {
			tagName := trace.GetTags()[ti].GetName()
			switch tagName {
			case trace.GetTraceIdTagName():
				cells[1] = csvfmt.EncodeTagValue(v)
			case trace.GetSpanIdTagName():
				cells[2] = csvfmt.EncodeTagValue(v)
			default:
				setTag(layout, cells, name, "", tagName, v)
			}
		}
		row := rowMap(layout, cells)
		e.Rows[row["_span"]] = row
	}
	return e
}

// ExpectedProperty is the property test/cases/property/data.WriteToGroup applied from file,
// laid out with layout; rows are keyed by _id. The revisions and _updated_at are not compared
// and _delete_time is expected to be 0.
func ExpectedProperty(layout *csvfmt.Layout, file string) Expected {
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "test", "cases", "property", "data", "testdata", file))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	req := &propertyv1.ApplyRequest{}
	gomega.ExpectWithOffset(1, protojson.Unmarshal(raw, req)).To(gomega.Succeed())
	e := Expected{Key: "_id", Rows: map[string]map[string]string{}, Ignore: []string{"_mod_revision", "_create_revision", "_updated_at"}}
	e.Add(layout, req.GetProperty(), 0)
	return e
}

// Add expects one more property row with the given delete time.
func (e Expected) Add(layout *csvfmt.Layout, p *propertyv1.Property, deleteTime int64) {
	name := p.GetMetadata().GetName()
	cells := layout.NewRow()
	cells[0] = name
	cells[1] = csvfmt.EncodeString(p.GetId())
	cells[5] = csvfmt.EncodeTimestamp(deleteTime)
	for _, t := range p.GetTags() {
		setTag(layout, cells, name, "", t.GetKey(), t.GetValue())
	}
	e.Rows[p.GetId()] = rowMap(layout, cells)
}

// IsNull reports whether a cell is the CSV null marker.
func IsNull(cell string) bool { return cell == csvfmt.NullCell }
