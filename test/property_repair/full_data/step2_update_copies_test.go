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

package full_data

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
	propertyrepair "github.com/apache/skywalking-banyandb/test/property_repair"
)

func TestPropertyRepairPropagation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Property Repair Propagation Test Suite", Label("integration", "slow"))
}

var _ = Describe("Property Repair Propagation Test", Label("update_copies"), func() {
	var conn *grpc.ClientConn
	var groupClient databasev1.GroupRegistryServiceClient

	BeforeEach(func() {
		var err error
		conn, err = grpchelper.Conn(propertyrepair.LiaisonAddr, 30*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())

		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
	})

	AfterEach(func() {
		if conn != nil {
			_ = conn.Close()
		}
	})

	It("Update group to 2 copies", func() {
		ctx := context.Background()

		// Update group replicas to 2
		propertyrepair.UpdateGroupReplicas(ctx, groupClient, 2)
	})
})
