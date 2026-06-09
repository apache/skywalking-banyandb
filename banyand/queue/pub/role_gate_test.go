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

package pub

import (
	"testing"

	"github.com/onsi/gomega"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// TestRoleLessNodeRejectedByGate verifies the publisher's role gate drops a node
// whose roles do not intersect allowedRoles - before any dial. This is the exact
// failure mode the lifecycle native-metrics registration must avoid: it MUST
// register the co-located data node with ROLE_DATA, otherwise OnAddOrUpdate
// silently returns and the node never becomes active. The positive (ROLE_DATA ->
// active) path is covered by the register/unregister specs in client_test.go.
func TestRoleLessNodeRejectedByGate(t *testing.T) {
	g := gomega.NewWithT(t)
	p := newPub() // allowedRoles defaults to ROLE_DATA
	defer p.GracefulStop()

	node := getDataNode("lifecycle-local", "127.0.0.1:0")
	node.Spec.(*databasev1.Node).Roles = nil
	p.OnAddOrUpdate(node)

	g.Expect(p.connMgr.ActiveCount()).To(gomega.Equal(0), "role-less node must not become active")
	g.Expect(p.connMgr.EvictableCount()).To(gomega.Equal(0), "role-less node must not even be dialed")
}
