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

package nativelog_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/nativelog"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestNativeLogStandalone(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Suite", Label(integration_standalone.Labels...))
}

// console collects what the nodes print. The cases run servers in this
// process, so the console of a node is the console of the test binary.
var console = &safeBuffer{}

var restoreConsole func()

var _ = BeforeSuite(func() {
	restoreConsole = logger.UseConsoleTarget(console)
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())

	// The recovery case stops a node and starts the next one on its data, so
	// that pair keeps one directory and one set of ports.
	recoveryDir, recoveryPorts := newSpace(), mustPorts(6)
	cases.SharedContext = cases.Context{
		Start: func(extra ...string) (cases.Node, func()) {
			return start(newSpace(), mustPorts(6), append(append([]string{}, cases.NativeFlags...), extra...)...)
		},
		StartDisabled: func(extra ...string) (cases.Node, func()) {
			return start(newSpace(), mustPorts(6), append(append([]string{}, cases.DisabledFlags...), extra...)...)
		},
		Restart: func(extra ...string) (cases.Node, func()) {
			return start(recoveryDir, recoveryPorts, append(append([]string{}, cases.NativeFlags...), extra...)...)
		},
		Console: console.String,
	}
})

var _ = AfterSuite(func() {
	if restoreConsole != nil {
		restoreConsole()
	}
})

// start runs one standalone server in this process. ports holds the five the
// launcher needs and a sixth for the metrics listener, which the cases read.
func start(dataDir string, ports []int, flags ...string) (cases.Node, func()) {
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[5])
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	addr, _, closeFn := setup.ClosableStandalone(config, dataDir, ports[:5],
		append([]string{"--observability-listener-addr=" + metricsAddr}, flags...)...)
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	node := cases.Node{
		Conn: conn,
		// The launcher sets --node-host=127.0.0.1, and a node's id is its host
		// and gRPC port.
		NodeID:     fmt.Sprintf("127.0.0.1:%d", ports[0]),
		NodeType:   "standalone",
		MetricsURL: "http://" + metricsAddr + "/metrics",
	}
	return node, func() {
		_ = conn.Close()
		closeFn()
	}
}

// safeBuffer is written by the server's logging goroutines and read by a case.
type safeBuffer struct {
	buf bytes.Buffer
	mux sync.Mutex
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.buf.String()
}

func mustPorts(n int) []int {
	ports, err := test.AllocateFreePorts(n)
	Expect(err).NotTo(HaveOccurred())
	return ports
}

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
