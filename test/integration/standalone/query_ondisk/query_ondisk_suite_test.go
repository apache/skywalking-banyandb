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

package integration_query_ondisk_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestIntegrationQueryOnDisk(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Query OnDisk Suite", Label(integration_standalone.Labels...))
}

var (
	connection *grpc.ClientConn
	now        time.Time
	deferFunc  func()
	goods      []gleak.Goroutine
)

var _ = SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	path, diskCleanupFn, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	Expect(err).NotTo(HaveOccurred())
	addr, _, closeFunc := setup.ClosableStandalone(path, ports)
	ns := timestamp.NowMilli().UnixNano()
	now = time.Unix(0, ns-ns%int64(time.Minute))
	test_cases.Initialize(addr, now)
	closeFunc()
	time.Sleep(time.Second)
	addr, _, closeFunc = setup.EmptyClosableStandalone(path, ports)
	deferFunc = func() {
		closeFunc()
		diskCleanupFn()
	}
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesstream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	casestopn.SharedContext = helpers.SharedContext{
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
	Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
