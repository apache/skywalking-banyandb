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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

func loadSchema[T proto.Message](t *testing.T, msg T, rel string) T {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	if err = protojson.Unmarshal(raw, msg); err != nil {
		t.Fatal(err)
	}
	return msg
}

// suiteArtifact lays the rows Expected computes for the suite data out as one file per group.
func suiteArtifact(t *testing.T, d SuiteData) (*Artifact, map[string]*csvfmt.Layout) {
	t.Helper()
	stream := loadSchema(t, &databasev1.Stream{}, "pkg/test/stream/testdata/streams/sw.json")
	measure := loadSchema(t, &databasev1.Measure{}, "pkg/test/measure/testdata/measures/service_cpm_minute.json")
	trace := loadSchema(t, &databasev1.Trace{}, "pkg/test/trace/testdata/traces/sw.json")
	sl, err := csvfmt.StreamLayout([]*databasev1.Stream{stream})
	if err != nil {
		t.Fatal(err)
	}
	ml, err := csvfmt.MeasureLayout([]*databasev1.Measure{measure})
	if err != nil {
		t.Fatal(err)
	}
	tl, err := csvfmt.TraceLayout([]*databasev1.Trace{trace})
	if err != nil {
		t.Fatal(err)
	}
	a := &Artifact{}
	add := func(catalog, group string, l *csvfmt.Layout, e Expected) {
		f := &CSVFile{NodeDir: "node-a", Catalog: catalog, Group: group, Header: l.Header()}
		for _, row := range e.Rows {
			cells := make([]string, len(f.Header))
			for i, h := range f.Header {
				cells[i] = row[h]
			}
			f.Rows = append(f.Rows, cells)
		}
		a.Files = append(a.Files, f)
	}
	add("stream", StreamGroup, sl, ExpectedStream(sl, stream, d.StreamBase, d.Interval))
	add("measure", MeasureGroup, ml, ExpectedMeasure(ml, measure, MeasureFile, "_timestamp", d.MeasureBase, d.Interval))
	add("trace", TraceGroup, tl, ExpectedTrace(tl, trace, d.TraceBase, d.Interval))
	return a, map[string]*csvfmt.Layout{"stream/" + StreamGroup: sl, "measure/" + MeasureGroup: ml, "trace/" + TraceGroup: tl}
}

func TestVerifyLiteralCells(t *testing.T) {
	gomega.RegisterTestingT(t)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	d := SuiteData{StreamBase: base, MeasureBase: base.Add(time.Hour), TraceBase: base.Add(2 * time.Hour), Interval: time.Minute}
	a, layouts := suiteArtifact(t, d)
	if failures := gomega.InterceptGomegaFailures(func() { a.VerifyLiteralCells(d, layouts) }); len(failures) != 0 {
		t.Fatalf("the suite rows must hold the literal cells: %v", failures)
	}

	// A wrong cell is caught even though the rows were built by the same encoders.
	header := a.Files[0].Header
	for _, row := range a.Files[0].Rows {
		for i, h := range header {
			if h == "data.data_binary" {
				row[i] = "YWJjMTIzIT8kKiYoKSctPUB-"
			}
		}
	}
	if failures := gomega.InterceptGomegaFailures(func() { a.VerifyLiteralCells(d, layouts) }); len(failures) == 0 {
		t.Fatal("a changed binary cell must fail the literal check")
	}
}
