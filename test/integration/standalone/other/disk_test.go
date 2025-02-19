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

package integration_other_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = g.Describe("Disk", func() {
	var goods []gleak.Goroutine
	g.BeforeEach(func() {
		goods = gleak.Goroutines()
	})

	g.AfterEach(func() {
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gm.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	})
	g.It(" is a standalone server, blocking writing, with disk full", func() {
		addr, _, _, _, deferFn := setup.Standalone(
			"--measure-max-disk-usage-percent",
			"0",
		)
		defer deferFn()
		wc, connDeferFn := writeData(addr)
		defer connDeferFn()
		for {
			resp, err := wc.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			gm.Expect(err).ShouldNot(gm.HaveOccurred())
			gm.Expect(resp.GetStatus()).To(gm.Equal(modelv1.Status_name[int32(modelv1.Status_STATUS_DISK_FULL)]))
		}
	})
	g.It(" is a cluster, blocking writing on node 0, with disk full", func() {
		g.By("Starting etcd server")
		ports, err := test.AllocateFreePorts(2)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		dir, spaceDef, err := test.NewSpace()
		gm.Expect(err).NotTo(gm.HaveOccurred())
		ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
		server, err := embeddedetcd.NewServer(
			embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
			embeddedetcd.RootDir(dir))
		gm.Expect(err).ShouldNot(gm.HaveOccurred())
		<-server.ReadyNotify()
		g.By("Loading schema")
		schemaRegistry, err := schema.NewEtcdSchemaRegistry(
			schema.Namespace(metadata.DefaultNamespace),
			schema.ConfigureServerEndpoints([]string{ep}),
		)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer schemaRegistry.Close()
		ctx := context.Background()
		test_measure.PreloadSchema(ctx, schemaRegistry)
		g.By("Starting data node 0")
		closeDataNode0 := setup.DataNode(ep,
			"--measure-max-disk-usage-percent",
			"0")
		g.By("Starting data node 1")
		closeDataNode1 := setup.DataNode(ep)
		g.By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(ep)
		defer func() {
			closerLiaisonNode()
			closeDataNode0()
			closeDataNode1()
			_ = server.Close()
			<-server.StopNotify()
			spaceDef()
		}()
		wc, deferFn := writeData(liaisonAddr)
		defer deferFn()
		errNum := 0
		successNum := 0
		for {
			resp, err := wc.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			gm.Expect(err).ShouldNot(gm.HaveOccurred())
			if resp.Status == modelv1.Status_name[int32(modelv1.Status_STATUS_SUCCEED)] {
				successNum++
			} else {
				errNum++
				gm.Expect(resp.GetStatus()).To(gm.Equal(modelv1.Status_name[int32(modelv1.Status_STATUS_DISK_FULL)]))
			}
		}
		gm.Expect(errNum).To(gm.BeNumerically(">", 0))
		gm.Expect(successNum).To(gm.BeNumerically(">", 0))
	})
})

func writeData(addr string) (measurev1.MeasureService_WriteClient, func()) {
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gm.Expect(err).NotTo(gm.HaveOccurred())
	ns := timestamp.NowMilli().UnixNano()
	baseTime := time.Unix(0, ns-ns%int64(time.Minute))
	interval := 500 * time.Millisecond
	wc := casesMeasureData.WriteOnly(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)
	gm.Expect(wc.CloseSend()).To(gm.Succeed())
	return wc, func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
	}
}
