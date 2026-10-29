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

package nativelogfailover_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestNativeLogFailover(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Liaison Failover Suite", Label(integration_distributed.Labels...))
}

// fastSchemaSync shortens schema convergence, as in the data node suite: the
// liaison creates the log group after the data nodes have started.
const fastSchemaSync = "--schema-property-client-sync-interval=1s"

// nativeFlags turns native logging on at info, as the shared cases do. They
// are not imported here: importing them would run them in this suite.
var nativeFlags = []string{"--logging-native-enabled", "--logging-native-level=info", "--logging-level=error"}

var (
	console *test.CapturedStderr
	stops   []func()
	conn    *grpc.ClientConn
	// liaisonID is the identity the liaison stamps on its events.
	liaisonID string
	// dataNodes maps each data node's id to the function that stops it.
	dataNodes = map[string]func(){}
)

var _ = BeforeSuite(func() {
	var err error
	console, err = test.CaptureStderr()
	Expect(err).NotTo(HaveOccurred())
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())

	// Two plain data nodes, then the liaison with native logging on. The
	// native sink is process-global, so the liaison is the only producer and
	// has to be the last role to configure the logger; a liaison also needs a
	// data node's schema server to start. Two data nodes give the liaison's
	// logs a node to move to when the one that owns shard 0 stops.
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	for range 2 {
		addr, _, _, stop := setup.DataNodeWithAddrAndDir(config, fastSchemaSync)
		stops = append(stops, stop)
		// The launcher sets --node-host=127.0.0.1, and a node's id is its host
		// and gRPC port.
		dataNodes["127.0.0.1:"+port(addr)] = stop
	}
	liaisonAddr, stopLiaison := setup.LiaisonNode(config, append(append([]string{}, nativeFlags...), fastSchemaSync)...)
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
})

var _ = AfterSuite(func() {
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
	if console != nil {
		console.Restore()
	}
})

var _ = Describe("The liaison route", func() {
	It("TC8 stores a liaison's events on the owner of shard 0, and on the next data node once that one stops", func() {
		before := mark()
		var owner string
		Eventually(func(g Gomega) {
			modules, perNode := liaisonRows(g)
			g.Expect(modules).To(HaveKey(before), "the liaison's marker is not stored")
			owner = only(g, perNode)
		}, flags.EventuallyTimeout, time.Second).Should(Succeed())

		// A node that stops cleanly closes its connections, so the client
		// evicts it at once and the next attempt locates the other node. A
		// node that stops answering without closing them is evicted by the
		// health check instead; the retries that bridge that window are
		// covered by the queue package's tests.
		dataNodes[owner]()
		delete(dataNodes, owner)
		after := mark()
		Eventually(func(g Gomega) {
			modules, perNode := liaisonRows(g)
			g.Expect(modules).To(HaveKey(after), "the marker logged after %s stopped is not stored", owner)
			g.Expect(perNode).NotTo(HaveKey(owner))
			g.Expect(only(g, perNode)).NotTo(Equal(owner))
		}, flags.EventuallyTimeout, time.Second).Should(Succeed())
	})
})

// mark logs a line with a module of its own through the process logger. The
// liaison's sink is the process's native sink, so the liaison stores it.
func mark() string {
	module := fmt.Sprintf("NL-MARK-%d", time.Now().UnixNano())
	logger.GetLogger(module).Info().Msg("marker")
	return module
}

// only returns the one data node that returned rows, and fails when there is
// not exactly one.
func only(g Gomega, perNode map[string]int) string {
	var nodes []string
	for node, n := range perNode {
		if n > 0 {
			nodes = append(nodes, node)
		}
	}
	g.Expect(nodes).To(HaveLen(1), "the liaison's rows are on %v", perNode)
	return nodes[0]
}

// liaisonRows reads the liaison's rows through a traced query and returns the
// modules they carry and the rows each data node returned. The liaison asks
// every data node, and the trace has one span per node, data-<node id>, with
// the rows that node returned. The group has no replicas, so a row a node
// returned is a row that node stores.
func liaisonRows(g Gomega) (map[string]bool, map[string]int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().Truncate(time.Millisecond)
	resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups:    []string{logger.NativeLogGroup},
		Name:      "log",
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Hour)), End: timestamppb.New(now.Add(time.Hour))},
		Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  "node_id",
			Op:    modelv1.Condition_BINARY_OP_EQ,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: liaisonID}}},
		}}},
		Limit: 900,
		Trace: true,
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "searchable", Tags: []string{"module"}},
		}},
	})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(resp.GetElements())).To(BeNumerically("<", 900), "the query was cut at its limit")
	modules := map[string]bool{}
	for _, e := range resp.GetElements() {
		modules[e.GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue()] = true
	}
	perNode := map[string]int{}
	var walk func(spans []*commonv1.Span)
	walk = func(spans []*commonv1.Span) {
		for _, s := range spans {
			if node, ok := strings.CutPrefix(s.GetMessage(), "data-"); ok {
				for _, t := range s.GetTags() {
					if t.GetKey() == "resp_count" {
						n, convErr := strconv.Atoi(t.GetValue())
						g.Expect(convErr).NotTo(HaveOccurred())
						perNode[node] += n
					}
				}
			}
			walk(s.GetChildren())
		}
	}
	walk(resp.GetTrace().GetSpans())
	return modules, perNode
}

// port returns the port of a host:port address.
func port(addr string) string {
	return addr[strings.LastIndex(addr, ":")+1:]
}

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
