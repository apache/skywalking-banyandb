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

package other

import (
	"context"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/fileformat"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/host"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// A standalone process registers no route table, so GetClusterState comes back
// empty and a client has to read the node's capabilities off GetCurrentNode
// instead. That only works if the single process reports both roles along with
// its file format version and time zone.
var _ = g.Describe("Current node", func() {
	var conn *grpc.ClientConn
	var deferFn func()

	g.BeforeEach(func() {
		var addr string
		addr, _, deferFn = setup.Standalone(NewTestConfig())
		var err error
		conn, err = grpchelper.Conn(addr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
	})

	g.It("reports the file format version and the time zone", func() {
		resp, err := databasev1.NewNodeQueryServiceClient(conn).
			GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		node := resp.GetNode()
		gm.Expect(node.GetRoles()).To(gm.ContainElements(databasev1.Role_ROLE_LIAISON, databasev1.Role_ROLE_DATA))
		gm.Expect(node.GetVersion()).NotTo(gm.BeNil())
		gm.Expect(node.GetVersion().GetFileFormatVersion()).To(gm.Equal(fileformat.CurrentVersion))
		gm.Expect(node.GetVersion().GetCompatibleFileFormatVersion()).To(gm.ContainElement(fileformat.CurrentVersion))
		gm.Expect(node.GetVersion().GetApiVersion()).To(gm.Equal(apiversion.Version))
		gm.Expect(node.GetTzName()).NotTo(gm.BeEmpty())
		gm.Expect(node.GetTzName()).To(gm.Equal(host.TimeZoneName()))
	})

	g.It("returns an empty route table so the client falls back to the current node", func() {
		resp, err := databasev1.NewClusterStateServiceClient(conn).
			GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.GetRouteTables()).To(gm.BeEmpty())
	})
})
