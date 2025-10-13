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

package handoff_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	testtrace "github.com/apache/skywalking-banyandb/pkg/test/trace"
)

const (
	nodeHost = "127.0.0.1"
	grpcHost = "127.0.0.1"
)

type dataNodeHandle struct {
	dataDir    string
	grpcPort   int
	gossipPort int
	addr       string
	closeFn    func()
}

func newDataNodeHandle(dataDir string, grpcPort, gossipPort int) *dataNodeHandle {
	return &dataNodeHandle{
		dataDir:    dataDir,
		grpcPort:   grpcPort,
		gossipPort: gossipPort,
		addr:       fmt.Sprintf("%s:%d", nodeHost, grpcPort),
	}
}

func (h *dataNodeHandle) start(etcdEndpoint string) {
	Expect(h.closeFn).To(BeNil())
	args := []string{
		"data",
		"--grpc-host=" + grpcHost,
		fmt.Sprintf("--grpc-port=%d", h.grpcPort),
		fmt.Sprintf("--property-repair-gossip-grpc-port=%d", h.gossipPort),
		"--stream-root-path=" + h.dataDir,
		"--measure-root-path=" + h.dataDir,
		"--property-root-path=" + h.dataDir,
		"--trace-root-path=" + h.dataDir,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--node-labels", "type=handoff",
		"--logging-modules", "trace,sidx",
		"--logging-levels", "debug,debug",
		"--measure-flush-timeout=0s",
		"--stream-flush-timeout=0s",
		"--trace-flush-timeout=0s",
	}
	h.closeFn = setup.CMD(args...)

	Eventually(helpers.HealthCheck(fmt.Sprintf("%s:%d", grpcHost, h.grpcPort), 10*time.Second, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials())), flags.EventuallyTimeout).Should(Succeed())

	Eventually(func() (map[string]struct{}, error) {
		m, err := helpers.ListKeys(etcdEndpoint, h.etcdKey())
		return keysToSet(m), err
	}, flags.EventuallyTimeout).Should(HaveLen(1))
}

func (h *dataNodeHandle) stop(etcdEndpoint string) {
	if h.closeFn != nil {
		h.closeFn()
		h.closeFn = nil
	}
	Eventually(func() (map[string]struct{}, error) {
		m, err := helpers.ListKeys(etcdEndpoint, h.etcdKey())
		return keysToSet(m), err
	}, flags.EventuallyTimeout).Should(HaveLen(0))
}

func (h *dataNodeHandle) etcdKey() string {
	return fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, h.grpcPort)
}

type liaisonHandle struct {
	rootPath   string
	grpcPort   int
	httpPort   int
	serverPort int
	addr       string
	httpAddr   string
	closeFn    func()
}

func newLiaisonHandle(rootPath string, grpcPort, httpPort, serverPort int) *liaisonHandle {
	return &liaisonHandle{
		rootPath:   rootPath,
		grpcPort:   grpcPort,
		httpPort:   httpPort,
		serverPort: serverPort,
		addr:       fmt.Sprintf("%s:%d", grpcHost, grpcPort),
		httpAddr:   fmt.Sprintf("%s:%d", grpcHost, httpPort),
	}
}

func (l *liaisonHandle) start(etcdEndpoint string, dataNodes []string) {
	Expect(l.closeFn).To(BeNil())
	joined := strings.Join(dataNodes, ",")
	Expect(os.Setenv("BYDB_DATA_NODE_LIST", joined)).To(Succeed())
	Expect(os.Setenv("BYDB_DATA_NODE_SELECTOR", "type=handoff")).To(Succeed())
	args := []string{
		"liaison",
		"--grpc-host=" + grpcHost,
		fmt.Sprintf("--grpc-port=%d", l.grpcPort),
		"--http-host=" + grpcHost,
		fmt.Sprintf("--http-port=%d", l.httpPort),
		"--liaison-server-grpc-host=" + grpcHost,
		fmt.Sprintf("--liaison-server-grpc-port=%d", l.serverPort),
		"--http-grpc-addr=" + l.addr,
		"--etcd-endpoints", etcdEndpoint,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--stream-root-path=" + l.rootPath,
		"--measure-root-path=" + l.rootPath,
		"--trace-root-path=" + l.rootPath,
		"--stream-flush-timeout=500ms",
		"--measure-flush-timeout=500ms",
		"--trace-flush-timeout=500ms",
		"--stream-sync-interval=1s",
		"--measure-sync-interval=1s",
		"--trace-sync-interval=1s",
		"--handoff-max-size-percent=100",
		"--logging-modules", "trace,sidx",
		"--logging-levels", "debug,debug",
	}

	l.closeFn = setup.CMD(args...)

	Eventually(helpers.HTTPHealthCheck(l.httpAddr, ""), flags.EventuallyTimeout).Should(Succeed())
	Eventually(func() (map[string]struct{}, error) {
		m, err := helpers.ListKeys(etcdEndpoint, l.etcdKey())
		return keysToSet(m), err
	}, flags.EventuallyTimeout).Should(HaveLen(1))
}

func (l *liaisonHandle) stop(etcdEndpoint string) {
	if l.closeFn != nil {
		l.closeFn()
		l.closeFn = nil
	}
	_ = os.Unsetenv("BYDB_DATA_NODE_LIST")
	_ = os.Unsetenv("BYDB_DATA_NODE_SELECTOR")
	Eventually(func() (map[string]struct{}, error) {
		m, err := helpers.ListKeys(etcdEndpoint, l.etcdKey())
		return keysToSet(m), err
	}, flags.EventuallyTimeout).Should(HaveLen(0))
}

func (l *liaisonHandle) etcdKey() string {
	return fmt.Sprintf("/%s/nodes/%s:%d", metadata.DefaultNamespace, nodeHost, l.serverPort)
}

type suiteInfo struct {
	LiaisonAddr  string   `json:"liaison_addr"`
	EtcdEndpoint string   `json:"etcd_endpoint"`
	HandoffRoot  string   `json:"handoff_root"`
	DataNodes    []string `json:"data_nodes"`
}

func keysToSet[K comparable, V any](m map[K]V) map[K]struct{} {
	if m == nil {
		return nil
	}
	out := make(map[K]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

var (
	connection   *grpc.ClientConn
	goods        []gleak.Goroutine
	cleanupFunc  func()
	handoffRoot  string
	etcdEndpoint string

	dnHandles [2]*dataNodeHandle
	liaison   *liaisonHandle

	etcdServer   embeddedetcd.Server
	etcdSpaceDef func()
	liaisonDef   func()
	dataDefs     []func()
)

func TestHandoff(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Trace Handoff Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())
	goods = gleak.Goroutines()

	etcdPorts, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())

	var etcdDir string
	etcdDir, etcdSpaceDef, err = test.NewSpace()
	Expect(err).NotTo(HaveOccurred())

	clientEP := fmt.Sprintf("http://%s:%d", nodeHost, etcdPorts[0])
	peerEP := fmt.Sprintf("http://%s:%d", nodeHost, etcdPorts[1])

	etcdServer, err = embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{clientEP}, []string{peerEP}),
		embeddedetcd.RootDir(etcdDir),
		embeddedetcd.AutoCompactionMode("periodic"),
		embeddedetcd.AutoCompactionRetention("1h"),
		embeddedetcd.QuotaBackendBytes(2*1024*1024*1024),
	)
	Expect(err).NotTo(HaveOccurred())
	<-etcdServer.ReadyNotify()
	etcdEndpoint = clientEP

	registry, err := schema.NewEtcdSchemaRegistry(
		schema.Namespace(metadata.DefaultNamespace),
		schema.ConfigureServerEndpoints([]string{clientEP}),
	)
	Expect(err).NotTo(HaveOccurred())
	defer registry.Close()
	Expect(testtrace.PreloadSchema(context.Background(), registry)).To(Succeed())

	dataDefs = make([]func(), 0, 2)
	for i := range dnHandles {
		dataDir, def, errDir := test.NewSpace()
		Expect(errDir).NotTo(HaveOccurred())
		dataDefs = append(dataDefs, def)
		ports, errPorts := test.AllocateFreePorts(2)
		Expect(errPorts).NotTo(HaveOccurred())
		dnHandles[i] = newDataNodeHandle(dataDir, ports[0], ports[1])
		dnHandles[i].start(etcdEndpoint)
	}

	liaisonPath, def, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	liaisonDef = def
	liaisonPorts, err := test.AllocateFreePorts(3)
	Expect(err).NotTo(HaveOccurred())
	liaison = newLiaisonHandle(liaisonPath, liaisonPorts[0], liaisonPorts[1], liaisonPorts[2])
	nodeAddrs := []string{dnHandles[0].addr, dnHandles[1].addr}
	liaison.start(etcdEndpoint, nodeAddrs)

	handoffRoot = filepath.Join(liaisonPath, "trace", "data", "handoff", "nodes")

	cleanupFunc = func() {
		if liaison != nil {
			liaison.stop(etcdEndpoint)
		}
		for i := range dnHandles {
			if dnHandles[i] != nil {
				dnHandles[i].stop(etcdEndpoint)
			}
		}
		if etcdServer != nil {
			_ = etcdServer.Close()
		}
		if liaisonDef != nil {
			liaisonDef()
		}
		for _, def := range dataDefs {
			if def != nil {
				def()
			}
		}
		if etcdSpaceDef != nil {
			etcdSpaceDef()
		}
	}

	info := suiteInfo{
		LiaisonAddr:  liaison.addr,
		EtcdEndpoint: etcdEndpoint,
		HandoffRoot:  handoffRoot,
		DataNodes:    nodeAddrs,
	}
	payload, err := json.Marshal(info)
	Expect(err).NotTo(HaveOccurred())
	return payload
}, func(data []byte) {
	var info suiteInfo
	Expect(json.Unmarshal(data, &info)).To(Succeed())
	etcdEndpoint = info.EtcdEndpoint
	handoffRoot = info.HandoffRoot
	if liaison == nil {
		liaison = &liaisonHandle{addr: info.LiaisonAddr}
	} else {
		liaison.addr = info.LiaisonAddr
	}

	var err error
	connection, err = grpchelper.Conn(info.LiaisonAddr, 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Trace Handoff Suite", func(report Report) {
	if report.SuiteSucceeded {
		if cleanupFunc != nil {
			cleanupFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	}
})

var _ = Describe("trace handoff", func() {
	It("replays data to recovered nodes and empties the queue", func() {
		Expect(connection).NotTo(BeNil())

		By("ensuring the handoff queue starts empty")
		Expect(countPendingParts(dnHandles[0].addr)).To(Equal(0))

		traceID := fmt.Sprintf("handoff-trace-%d", time.Now().UnixNano())

		By("stopping the first data node")
		dnHandles[0].stop(etcdEndpoint)

		By("waiting for the liaison to observe the offline node")
		time.Sleep(7 * time.Second)

		By("writing a trace while the node is offline")
		writeTime := writeTrace(connection, traceID)

		By("waiting for pending parts to appear in the queue")
		Eventually(func() int {
			return countPendingParts(dnHandles[0].addr)
		}, flags.EventuallyTimeout).Should(BeNumerically(">", 0))

		By("restarting the first node")
		dnHandles[0].start(etcdEndpoint)

		By("waiting for the queue to drain")
		Eventually(func() int {
			return countPendingParts(dnHandles[0].addr)
		}, flags.EventuallyTimeout).Should(Equal(0))

		By("verifying the replayed trace can be queried")
		Eventually(func() error {
			return queryTrace(connection, traceID, writeTime)
		}, flags.EventuallyTimeout).Should(Succeed())

		By("stopping the second node to ensure the trace resides on the recovered node")
		dnHandles[1].stop(etcdEndpoint)
		defer dnHandles[1].start(etcdEndpoint)

		Eventually(func() error {
			return queryTrace(connection, traceID, writeTime)
		}, flags.EventuallyTimeout).Should(Succeed())
	})
})

func writeTrace(conn *grpc.ClientConn, traceID string) time.Time {
	client := tracev1.NewTraceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.Write(ctx)
	Expect(err).NotTo(HaveOccurred())

	baseTime := time.Now().UTC().Truncate(time.Millisecond)

	tags := []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: traceID}}},
		{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 1}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "handoff_service"}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "handoff_instance"}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "/handoff"}}},
		{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 321}}},
		{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(baseTime)}},
	}

	err = stream.Send(&tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: "sw", Group: "test-trace-group"},
		Tags:     tags,
		Span:     []byte("span-" + traceID),
		Version:  1,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(stream.CloseSend()).To(Succeed())

	Eventually(func() error {
		_, err = stream.Recv()
		return err
	}, flags.EventuallyTimeout).Should(Equal(io.EOF))

	return baseTime
}

func queryTrace(conn *grpc.ClientConn, traceID string, ts time.Time) error {
	client := tracev1.NewTraceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &tracev1.QueryRequest{
		Groups: []string{"test-trace-group"},
		Name:   "sw",
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(ts.Add(-5 * time.Minute)),
			End:   timestamppb.New(ts.Add(5 * time.Minute)),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: "trace_id",
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{Value: traceID},
					}},
				},
			},
		},
		TagProjection: []string{"trace_id"},
	}

	resp, err := client.Query(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.GetTraces()) == 0 {
		return fmt.Errorf("trace %s not found", traceID)
	}
	return nil
}

func countPendingParts(nodeAddr string) int {
	sanitized := sanitizeNodeAddr(nodeAddr)
	nodeDir := filepath.Join(handoffRoot, sanitized)

	entries, err := os.ReadDir(nodeDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		Fail(fmt.Sprintf("failed to read handoff directory %s: %v", nodeDir, err))
	}

	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			count++
		}
	}
	return count
}

func sanitizeNodeAddr(addr string) string {
	replacer := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return replacer.Replace(addr)
}
