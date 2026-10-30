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

package nativelogliaison_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/nativelog"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestNativeLogLiaison(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Liaison Suite", Label(integration_distributed.Labels...))
}

// fastSchemaSync shortens schema convergence, as in the data node suite: the
// liaison creates the log group after the data nodes have started.
const fastSchemaSync = "--schema-property-client-sync-interval=1s"

var (
	console *test.CapturedStderr
	stops   []func()
	conn    *grpc.ClientConn
	// liaisonID is the identity the liaison stamps on its events.
	liaisonID string
)

var _ = BeforeSuite(func() {
	var err error
	console, err = test.CaptureStderr()
	Expect(err).NotTo(HaveOccurred())
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())

	// A plain data node, then the liaison with native logging on. The native
	// sink is process-global, so the liaison is the only producer and has to
	// be the last role to configure the logger; a liaison also needs a data
	// node's schema server to start.
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	_, _, _, stopData := setup.DataNodeWithAddrAndDir(config, fastSchemaSync)
	stops = append(stops, stopData)
	liaisonAddr, stopLiaison := setup.LiaisonNode(config, append(append([]string{}, cases.NativeFlags...), fastSchemaSync)...)
	stops = append(stops, stopLiaison)

	conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	stops = append(stops, func() { _ = conn.Close() })

	// A liaison's id is its host and internal port, which the launcher does
	// not return. The liaison lists itself in its own tier-1 route table.
	Eventually(func(g Gomega) {
		state, stateErr := databasev1.NewClusterStateServiceClient(conn).
			GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		g.Expect(stateErr).NotTo(HaveOccurred())
		liaisons := state.GetRouteTables()["tire1"].GetRegistered()
		g.Expect(liaisons).To(HaveLen(1))
		liaisonID = liaisons[0].GetMetadata().GetName()
	}, flags.EventuallyTimeout, time.Second).Should(Succeed())

	node := cases.Node{Conn: conn, NodeID: liaisonID, NodeType: "liaison"}
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
	if console != nil {
		console.Restore()
	}
})

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
