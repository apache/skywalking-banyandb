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

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = g.Describe("Query service_cpm_minute", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpc.ClientConn

	g.BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.SetUp()
		gm.Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(gm.Succeed())
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		baseTime = timestamp.NowMilli()
		interval = 500 * time.Millisecond
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)
	})
	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
	})
	g.It("queries service_cpm_minute by id after updating", func() {
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data1.json", baseTime, interval)
		gm.Eventually(func(innerGm gm.Gomega) {
			casesMeasureData.VerifyFn(innerGm, helpers.SharedContext{
				Connection: conn,
				BaseTime:   baseTime,
			}, helpers.Args{Input: "all", Want: "update", Duration: 1 * time.Hour})
		}, flags.EventuallyTimeout)
	})
})
