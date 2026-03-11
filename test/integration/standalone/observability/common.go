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

// Package observability provides shared setup and helpers for standalone observability integration tests.
package observability

import (
	"fmt"
	"net"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

type sharedEnv struct {
	closeFn  func()
	grpcAddr string
	httpAddr string
}

var (
	env           sharedEnv
	conn          *grpc.ClientConn
	goods         []gleak.Goroutine
	sharedContext helpers.SharedContext
)

// Setup initializes the shared standalone BanyanDB instance with native observability enabled.
func Setup() {
	gm.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gm.Succeed())

	// tmpDir is only used for discovery file; the actual data root will be created
	// by setup.EmptyStandalone and printed separately.
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gm.Expect(tmpErr).NotTo(gm.HaveOccurred())

	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)

	// Use EmptyStandalone so we can get the real data root path used by the server.
	grpcAddr, httpAddr, closeFn := setup.EmptyStandalone(config,
		"--observability-modes=native",
		"--observability-listener-addr=:2121",
		"--observability-metrics-interval=2s",
		"--observability-native-flush-interval=2s",
		"--measure-flush-timeout=100ms",
	)

	// The measure root path is embedded in the standalone server's root path.
	// For EmptyStandalone, setup.StandaloneWithSchemaLoaders internally calls test.NewSpace()
	// and passes that path as --measure-root-path. We reuse the discovery tmpDir just to
	// construct the discovery file, but the actual data lives under that internal root.
	env = sharedEnv{
		grpcAddr: grpcAddr,
		httpAddr: httpAddr,
		closeFn: func() {
			closeFn()
			tmpDirCleanup()
		},
	}

	waitForHTTPReady(httpAddr)

	var err error
	conn, err = grpchelper.Conn(env.grpcAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gm.Expect(err).NotTo(gm.HaveOccurred())

	sharedContext = helpers.SharedContext{
		Connection: conn,
	}

	goods = gleak.Goroutines()

	g.DeferCleanup(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		env.closeFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
}

func waitForHTTPReady(httpAddr string) {
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return
	}
	if host == "" {
		host = "localhost"
	}
	addr := fmt.Sprintf("%s:%s", host, port)
	client := &net.Dialer{Timeout: 2 * time.Second}
	gm.Eventually(func() error {
		conn, dialErr := client.Dial("tcp", addr)
		if dialErr != nil {
			return dialErr
		}
		_ = conn.Close()
		return nil
	}, flags.EventuallyTimeout).Should(gm.Succeed())
}
