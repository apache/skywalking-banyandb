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

package integration_query_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	cases_measure "github.com/apache/skywalking-banyandb/test/cases/measure"
	cases_measure_data "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	cases_stream "github.com/apache/skywalking-banyandb/test/cases/stream"
	cases_stream_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	cases_topn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

func TestIntegrationQuery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Query Suite")
}

var (
	connection *grpclib.ClientConn
	now        time.Time
	deferFunc  func()
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	})).To(Succeed())
	var addr string
	addr, _, deferFunc = setup.SetUp()
	conn, err := grpclib.Dial(
		addr,
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	Expect(err).NotTo(HaveOccurred())
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	interval := 500 * time.Millisecond
	// stream
	cases_stream_data.Write(conn, "data.json", now, interval)
	// measure
	interval = time.Minute
	cases_measure_data.Write(conn, "service_traffic", "sw_metric", "service_traffic_data.json", now, interval)
	cases_measure_data.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	Expect(conn.Close()).To(Succeed())
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpclib.Dial(
		string(address),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithBlock(),
	)
	cases_stream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	cases_measure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	cases_topn.SharedContext = helpers.SharedContext{
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
