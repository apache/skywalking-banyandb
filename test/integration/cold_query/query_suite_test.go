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

package integration_cold_query_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casesStream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casesStreamData "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casesTopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

func TestIntegrationColdQuery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Query Cold Data Suite", Label("integration"))
}

var (
	connection *grpc.ClientConn
	now        time.Time
	deferFunc  func()
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	var addr string
	addr, _, deferFunc = setup.Common()
	Eventually(
		helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
		flags.EventuallyTimeout).Should(Succeed())
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute)).Add(-time.Hour * 24)
	interval := 500 * time.Millisecond
	casesStreamData.Write(conn, "data.json", now, interval)
	interval = time.Minute
	casesMeasureData.Write(conn, "service_traffic", "sw_metric", "service_traffic_data.json", now, interval)
	casesMeasureData.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	Expect(conn.Close()).To(Succeed())
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesStream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casesMeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casesTopn.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {
	deferFunc()
})
