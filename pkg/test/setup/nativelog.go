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

package setup

import (
	"context"
	"time"

	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// FastSchemaSync shortens schema convergence. A native log producer creates
// the log group after the other nodes have started, and a node that does not
// know a group fails the whole distributed query rather than returning what
// the others hold (apache/skywalking#14104). With the default 30s interval the
// full reconcile that repairs it is 150s away.
const FastSchemaSync = "--schema-property-client-sync-interval=1s"

// NativeLogFlags turns native logging on with the thresholds the native log
// cases assume: the console at error and native storage at info, so an info
// line is stored and not printed.
var NativeLogFlags = []string{"--logging-native-enabled", "--logging-native-level=info", "--logging-level=error"}

// LiaisonID returns the id of the only liaison of the cluster that conn
// reaches, which is the identity the liaison stamps on its native log events.
// The id is the liaison's host and internal port, which the launcher does not
// return; the liaison lists itself in its own tier-1 route table.
func LiaisonID(conn *grpclib.ClientConn) string {
	var id string
	gomega.Eventually(func(g gomega.Gomega) {
		state, err := databasev1.NewClusterStateServiceClient(conn).
			GetClusterState(context.Background(), &databasev1.GetClusterStateRequest{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		liaisons := state.GetRouteTables()["tire1"].GetRegistered()
		g.Expect(liaisons).To(gomega.HaveLen(1))
		id = liaisons[0].GetMetadata().GetName()
	}, testflags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
	return id
}
