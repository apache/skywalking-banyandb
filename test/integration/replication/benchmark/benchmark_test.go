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

package benchmark

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Replication Benchmark", func() {
	It("runs replication benchmarks across RFs", func() {
		cfg := LoadConfig()
		report := BenchReport{GeneratedAt: time.Now(), Config: cfg}

		for _, rf := range []int{1, 2, 3} {
			By(fmt.Sprintf("Running benchmark with RF=%d", rf))
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
			result, err := runBenchmarkRF(ctx, repoRoot, cfg, rf)
			cancel()
			Expect(err).NotTo(HaveOccurred())
			report.Results = append(report.Results, result)
			AddReportEntry(fmt.Sprintf("RF=%d benchmark summary", rf), formatRFResultSummary(result))
			By(fmt.Sprintf("RF=%d write throughput %.2f points/s, read p95 %.2f ms", rf, result.Write.ThroughputPps, result.Read.P95Ms))
		}

		path, err := writeReport(report, cfg.ReportDir)
		Expect(err).NotTo(HaveOccurred())
		By("Report written to " + path)
	})
})
