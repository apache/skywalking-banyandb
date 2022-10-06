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

package integration_other_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = Describe("Query service_cpm_minute", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpclib.ClientConn

	BeforeEach(func() {
		var addr string
		addr, deferFn = setup.SetUp()
		var err error
		conn, err = grpclib.Dial(
			addr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		baseTime = timestamp.NowMilli()
		interval = 500 * time.Millisecond
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
	})
	It("queries service_cpm_minute by id after updating", func() {
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data1.json", baseTime, interval)
		Eventually(func(g Gomega) {
			casesMeasureData.VerifyFn(g, helpers.SharedContext{
				Connection: conn,
				BaseTime:   baseTime,
			}, helpers.Args{Input: "all", Want: "update", Duration: 1 * time.Hour})
		})
	})
})
