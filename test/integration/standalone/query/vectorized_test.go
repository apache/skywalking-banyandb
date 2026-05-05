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

package query_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
)

// Vectorized parity gate (G4 §"Integration Test Plan").
//
// Boots a *separate* standalone with --measure-vectorized-enabled=true and
// replays the same Measure / TopN test entries the row-path integration suite
// already covers in suite_test.go. Each case asserts the row-path's expected
// output, so every greenness here is a parity check: vectorized produced the
// row-path's reference InternalDataPoints. The cluster is fresh and isolated
// so neither side observes the other's state.
//
// This block runs after the on-disk-data Describe in round2.go, which closes
// the original cluster. The integration suite remains a release-candidate
// gate; the unit-level differential tests in pkg/query/vectorized/measure
// gate every PR.
var _ = ginkgo.Describe("vectorized parity", ginkgo.Ordered, func() {
	var (
		vectorizedConn *grpc.ClientConn
		stopFn         func()
	)
	ginkgo.BeforeAll(func() {
		path, diskCleanupFn, pathErr := test.NewSpace()
		gomega.Expect(pathErr).NotTo(gomega.HaveOccurred())
		ports, portsErr := test.AllocateFreePorts(5)
		gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		addr, _, closeFn := setup.ClosableStandalone(config, path, ports,
			"--measure-vectorized-enabled=true",
		)
		stopFn = func() {
			closeFn()
			diskCleanupFn()
			tmpDirCleanup()
		}
		var connErr error
		vectorizedConn, connErr = grpchelper.Conn(addr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		test_cases.Initialize(addr, now)
		sharedCtx := helpers.SharedContext{
			Connection: vectorizedConn,
			BaseTime:   now,
		}
		casesmeasure.SharedContext = sharedCtx
		casestopn.SharedContext = sharedCtx
	})
	ginkgo.AfterAll(func() {
		if vectorizedConn != nil {
			gomega.Expect(vectorizedConn.Close()).To(gomega.Succeed())
		}
		if stopFn != nil {
			stopFn()
		}
	})

	casesmeasure.RegisterTable("Vectorized: scanning measures")
	casestopn.RegisterTable("Vectorized: TopN")
})
