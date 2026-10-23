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

package measure_test

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	measuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

//go:embed ql/*
var qlCases embed.FS

// RegisterQLTable registers QL-only RPC verification against the shared fixture F.
// Optional observers verify node-local fixture contents before liaison queries run.
func RegisterQLTable(description string, observeNodes ...func(*measurev1.QueryRequest, *measurev1.QueryResponse)) bool {
	return g.Describe(description, g.Ordered, g.Label("measure-ql", "QLB-T01"), func() {
		var base time.Time
		render := func(name, host string) string { return renderQLFixture(base, name, host) }
		verify := func(query, expected string) *measurev1.QueryResponse {
			var result *measurev1.QueryResponse
			gm.Eventually(func(inner gm.Gomega) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var err error
				result, err = measuredata.VerifyQL(ctx, SharedContext.Connection, query, []byte(expected), false)
				inner.Expect(err).NotTo(gm.HaveOccurred())
			}, flags.EventuallyTimeout).Should(gm.Succeed())
			return result
		}
		g.BeforeAll(func() {
			// BaseTime is minute aligned by both vectorized suites and is within retention.
			base = SharedContext.BaseTime.Add(-time.Minute).Truncate(2 * time.Second)
			// Write an obsolete version first: all six-row expectations also gate dedup.
			measuredata.Write(SharedContext.Connection, "qlb_fixture", "replicated_group", "qlb_fixture_old.json", base, time.Second)
			measuredata.Write(SharedContext.Connection, "qlb_fixture", "replicated_group", "qlb_fixture.json", base.Add(5*time.Second), time.Second)
			// Distributed suites observe each node directly before accepting liaison results.
			request := &measurev1.QueryRequest{}
			helpers.UnmarshalYAML([]byte(`name: qlb_fixture
groups: [replicated_group]
tagProjection:
   tagFamilies:
   - name: default
     tags: [rid, host, zone]
fieldProjection:
   names: [v, f]
`), request)
			request.TimeRange = helpers.TimeRange(helpers.Args{Offset: -time.Minute - time.Second, Duration: 7 * time.Second}, SharedContext)
			want := &measurev1.QueryResponse{}
			helpers.UnmarshalYAML([]byte(render("six-rows.yaml", "")), want)
			for _, observe := range observeNodes {
				observe(request, want)
			}
		})
		g.It("legacy-select-golden", func() {
			verify(render("legacy-select.ql", ""), render("six-rows.yaml", ""))
		})
		g.It("legacy-parameter-rebind", func() {
			verify(render("legacy-parameter.ql", "a"), render("host-a.yaml", ""))
			verify(render("legacy-parameter.ql", "b"), render("host-b.yaml", ""))
		})
		g.It("invalid-ql-status", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			gm.Expect(measuredata.VerifyQLError(ctx, SharedContext.Connection, render("invalid-ql.ql", ""), codes.InvalidArgument)).To(gm.Succeed())
		})
		g.It("native-equivalence", func() {
			got := verify(render("legacy-select.ql", ""), render("six-rows.yaml", ""))
			request := &measurev1.QueryRequest{}
			helpers.UnmarshalYAML([]byte(`name: qlb_fixture
groups: [replicated_group]
tagProjection:
   tagFamilies:
   - name: default
     tags: [rid, host, zone]
fieldProjection:
   names: [v, f]
`), request)
			request.TimeRange = helpers.TimeRange(helpers.Args{Offset: -time.Minute - time.Second, Duration: 7 * time.Second}, SharedContext)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			native, err := measurev1.NewMeasureServiceClient(SharedContext.Connection).Query(ctx, request)
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(measuredata.CompareQLResult(got, native, false)).To(gm.Succeed())
		})
	})
}

func renderQLFixture(base time.Time, name, host string) string {
	content, err := qlCases.ReadFile("ql/" + name)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	pairs := []string{
		"{{start}}", base.Add(-time.Second).Format(time.RFC3339Nano),
		"{{end}}", base.Add(6 * time.Second).Format(time.RFC3339Nano), "{{host}}", host,
	}
	for i := range 6 {
		pairs = append(pairs, fmt.Sprintf("{{t%d}}", i), base.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano))
	}
	return strings.NewReplacer(pairs...).Replace(string(content))
}
