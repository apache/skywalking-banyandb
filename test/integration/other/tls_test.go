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
	"path/filepath"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = Describe("Query service_cpm_minute", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpclib.ClientConn

	BeforeEach(func() {
		_, currentFile, _, _ := runtime.Caller(0)
		basePath := filepath.Dir(currentFile)
		certFile := filepath.Join(basePath, "testdata/server_cert.pem")
		keyFile := filepath.Join(basePath, "testdata/server_key.pem")
		var addr string
		addr, deferFn = setup.SetUp("--tls=true", "--cert-file="+certFile, "--key-file="+keyFile)
		var err error
		creds, err := credentials.NewClientTLSFromFile(certFile, "localhost")
		Expect(err).NotTo(HaveOccurred())
		conn, err = grpclib.Dial(addr, grpclib.WithTransportCredentials(creds))
		Expect(err).NotTo(HaveOccurred())
		baseTime = timestamp.NowMilli()
		interval = 500 * time.Millisecond
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)
	})
	AfterEach(func() {
		Expect(conn.Close()).To(Succeed())
		deferFn()
	})
	It("queries a tls server", func() {
		Eventually(func(g Gomega) {
			casesMeasureData.VerifyFn(g, helpers.SharedContext{
				Connection: conn,
				BaseTime:   baseTime,
			}, helpers.Args{Input: "all", Duration: 1 * time.Hour})
		})
	})
})
