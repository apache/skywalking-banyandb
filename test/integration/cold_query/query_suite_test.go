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

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casesStream "github.com/apache/skywalking-banyandb/test/cases/stream"
)

func TestIntegrationColdQuery(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Integration Query Cold Data Suite")
}

var (
	connection *grpclib.ClientConn
	now        time.Time
	deferFunc  func()
)

var _ = g.SynchronizedBeforeSuite(func() []byte {
	gm.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	})).To(gm.Succeed())
	var addr string
	addr, deferFunc = setup.SetUp()
	conn, err := grpclib.Dial(
		addr,
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	now = timestamp.NowMilli().Add(-time.Hour * 24)
	interval := 500 * time.Millisecond
	casesStream.Write(conn, "data.json", now, interval)
	casesMeasureData.Write(conn, "service_traffic", "sw_metric", "service_traffic_data.json", now, interval)
	casesMeasureData.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	gm.Expect(conn.Close()).To(gm.Succeed())
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpclib.Dial(
		string(address),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithBlock(),
	)
	casesStream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casesMeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	gm.Expect(err).NotTo(gm.HaveOccurred())
})

var _ = g.SynchronizedAfterSuite(func() {
	if connection != nil {
		gm.Expect(connection.Close()).To(gm.Succeed())
	}
}, func() {
	deferFunc()
})
