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

// Package query provides shared test setup for query integration tests.
package query

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesproperty "github.com/apache/skywalking-banyandb/test/cases/property"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

// SetupResult contains all info returned by SetupFunc.
type SetupResult struct {
	Now      time.Time
	StopFunc func()
	Addr     string
}

// SetupFunc is provided by sub-packages to start the environment.
var SetupFunc func() SetupResult

var (
	result     SetupResult
	connection *grpc.ClientConn
	goods      []gleak.Goroutine
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	pool.EnableStackTracking(true)
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	result = SetupFunc()
	return []byte(result.Addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesstream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.Now,
	}
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.Now,
	}
	casestopn.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.Now,
	}
	casestrace.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.Now,
	}
	casesproperty.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.Now,
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Integration Query Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if result.StopFunc != nil {
			result.StopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
