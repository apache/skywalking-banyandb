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
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestNativeLogDistributed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Cluster Suite", Label(integration_distributed.Labels...))
}

// console collects what the nodes print. The nodes run in this process, so
// their console is the console of the test binary.
var console = &safeBuffer{}

var (
	restoreConsole func()
	stops          []func()
)

var _ = BeforeSuite(func() {
	restoreConsole = logger.UseConsoleTarget(console)
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())

	// One data node produces the log events, and the liaison answers the
	// queries. A second data node would take the native sink over: the sink is
	// process-global, so its identity would be stamped on the first node's
	// lines too. Attribution across nodes therefore lives in the e2e case
	// test/e2e-v2/cases/nativelog.
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	dataAddr, _, _, stopData := setup.DataNodeWithAddrAndDir(config, cases.NativeFlags...)
	stops = append(stops, stopData)
	// The liaison carries the same flags: without them it would turn native
	// logging off for every node in this process.
	liaisonAddr, stopLiaison := setup.LiaisonNode(config, cases.NativeFlags...)
	stops = append(stops, stopLiaison)

	conn, err := grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	stops = append(stops, func() { _ = conn.Close() })

	node := cases.Node{
		Conn: conn,
		// The launcher sets --node-host=127.0.0.1, and a node's id is its host
		// and gRPC port.
		NodeID:   "127.0.0.1:" + port(dataAddr),
		NodeType: "data",
	}
	// The suite owns the node, so a case starts nothing and stops nothing.
	shared := func(_ ...string) (cases.Node, func()) { return node, func() {} }
	cases.SharedContext = cases.Context{
		Distributed:   true,
		Start:         shared,
		StartDisabled: shared,
		Restart:       shared,
		Console:       console.String,
	}
	cases.AwaitQueryable(conn)
})

var _ = AfterSuite(func() {
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
	if restoreConsole != nil {
		restoreConsole()
	}
})

// port returns the port of a host:port address.
func port(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[i+1:]
		}
	}
	return addr
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

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
