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

// Package query provides shared test setup for query integration tests.
package query

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesproperty "github.com/apache/skywalking-banyandb/test/cases/property"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

var round2Conn *grpc.ClientConn

var _ = ginkgo.Describe("on-disk data", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		gomega.Expect(result.restart).NotTo(gomega.BeNil())
		newAddr, _ := result.restart()
		var connErr error
		round2Conn, connErr = grpchelper.Conn(newAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(connErr).NotTo(gomega.HaveOccurred())
		sharedCtx := helpers.SharedContext{
			Connection: round2Conn,
			BaseTime:   result.now,
		}
		casesstream.SharedContext = sharedCtx
		casesmeasure.SharedContext = sharedCtx
		casestopn.SharedContext = sharedCtx
		casestrace.SharedContext = sharedCtx
		casesproperty.SharedContext = sharedCtx
	})

	casesstream.RegisterTable("Scanning Streams")
	casesmeasure.RegisterTable("Scanning Measures")
	casestopn.RegisterTable("TopN Tests")
	casestrace.RegisterTable("Scanning Traces")
	casesproperty.RegisterTable("Scanning Properties")
})
