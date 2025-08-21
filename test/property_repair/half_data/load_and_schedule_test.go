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

package half_data

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	propertyrepair "github.com/apache/skywalking-banyandb/test/property_repair"
)

func TestHalfDataStep1(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Half Data Step1: Initial Load Test Suite", Label("integration", "slow"))
}

var _ = Describe("Initial Load with 2 Copies", func() {
	var conn *grpc.ClientConn
	var groupClient databasev1.GroupRegistryServiceClient
	var propertyClient databasev1.PropertyRegistryServiceClient
	var propertyServiceClient propertyv1.PropertyServiceClient

	BeforeEach(func() {
		var err error

		conn, err = grpchelper.Conn(propertyrepair.LiaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())

		groupClient = databasev1.NewGroupRegistryServiceClient(conn)
		propertyClient = databasev1.NewPropertyRegistryServiceClient(conn)
		propertyServiceClient = propertyv1.NewPropertyServiceClient(conn)
	})

	AfterEach(func() {
		if conn != nil {
			conn.Close()
		}
	})

	It("should create group with 2 copies, write 5k properties, then reduce copies to 1", func() {
		ctx := context.Background()

		propertyrepair.CreateGroup(ctx, groupClient, 2)
		propertyrepair.CreatePropertySchema(ctx, propertyClient)
		propertyrepair.WriteProperties(ctx, propertyServiceClient, 0, 50000)

		time.Sleep(time.Second * 10)
		fmt.Println("ready to update the copies to 1")
		propertyrepair.UpdateGroupReplicas(ctx, groupClient, 1)

		time.Sleep(time.Second * 10)
		fmt.Println("write other 5k properties")
		propertyrepair.WriteProperties(ctx, propertyServiceClient, 50000, 100000)

		time.Sleep(time.Second * 10)
		fmt.Println("ready to update the copies to 2")
		propertyrepair.UpdateGroupReplicas(ctx, groupClient, 2)
	})
})
