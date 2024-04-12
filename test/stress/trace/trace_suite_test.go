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

package trace_test

import (
	"flag"
	"net/http"
	"path"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/query"
)

func TestIntegrationLoad(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stress Trace Suite", Label("slow"))
}

var _ = Describe("Query", func() {
	const timeout = 30 * time.Minute
	var (
		fs       *flag.FlagSet
		basePath string
	)

	BeforeEach(func() {
		// Check if the URL is reachable
		resp, err := http.Get("http://localhost:12800/graphql")
		if err != nil || resp.StatusCode != 200 {
			// If the request fails or the status code is not 200, skip the test
			Skip("http://localhost:12800/graphql is not reachable")
		}
		fs = flag.NewFlagSet("", flag.PanicOnError)
		fs.String("base-url", "http://localhost:12800/graphql", "")
		fs.String("service-id", "c2VydmljZV8x.1", "")
		_, basePath, _, _ = runtime.Caller(0)
		basePath = path.Dir(basePath)
	})

	It("TraceListOrderByDuration", func() {
		query.TraceListOrderByDuration(basePath, timeout, fs)
	})

	It("TraceListOrderByTime", func() {
		query.TraceListOrderByTime(basePath, timeout, fs)
	})

	It("TraceByID", func() {
		query.TraceByID(basePath, timeout, fs)
	})
})
