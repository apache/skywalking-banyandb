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
	"os"
	"path/filepath"
	"runtime"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/tls"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = g.Describe("Query service_cpm_minute", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpclib.ClientConn
	var goods []gleak.Goroutine
	var addr string
	var certFile, keyFile string

	g.BeforeEach(func() {
		_, currentFile, _, _ := runtime.Caller(0)
		basePath := filepath.Dir(currentFile)
		certFile = filepath.Join(basePath, "testdata/server_cert.pem")
		keyFile = filepath.Join(basePath, "testdata/server_key.pem")
		addr, _, deferFn = setup.StandaloneWithTLS(certFile, keyFile)
		var err error
		creds, err := credentials.NewClientTLSFromFile(certFile, "localhost")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		conn, err = grpchelper.Conn(addr, 10*time.Second, grpclib.WithTransportCredentials(creds))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		baseTime = time.Unix(0, ns-ns%int64(time.Minute))
		interval = 500 * time.Millisecond
		casesMeasureData.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, interval)
		goods = gleak.Goroutines()
	})
	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
	g.It("queries a tls server", func() {
		gm.Eventually(func(innerGm gm.Gomega) {
			casesMeasureData.VerifyFn(innerGm, helpers.SharedContext{
				Connection: conn,
				BaseTime:   baseTime,
			}, helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute})
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	})

	g.It("queries an updated TLS server", func() {
		// Create a temporary directory for certificate files
		tempDir, err := os.MkdirTemp("", "tls-test-*")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer os.RemoveAll(tempDir)

		// Copy the original certificate and key to the temporary directory
		tempCertFile := filepath.Join(tempDir, "cert.pem")
		tempKeyFile := filepath.Join(tempDir, "key.pem")

		// Read original certificate and key
		originalCert, err := os.ReadFile(certFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		originalKey, err := os.ReadFile(keyFile)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Write to temporary location
		err = os.WriteFile(tempCertFile, originalCert, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = os.WriteFile(tempKeyFile, originalKey, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Start a new server using the temporary certificate files
		tempAddr, _, tempDeferFn := setup.StandaloneWithTLS(tempCertFile, tempKeyFile)
		defer tempDeferFn()

		// Create initial connection with the original certificate
		creds, err := credentials.NewClientTLSFromFile(tempCertFile, "localhost")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		tempConn, err := grpchelper.Conn(tempAddr, 10*time.Second, grpclib.WithTransportCredentials(creds))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer tempConn.Close()

		// Populate test data and verify with original certificate connection first
		ns := timestamp.NowMilli().UnixNano()
		testBaseTime := time.Unix(0, ns-ns%int64(time.Minute))
		casesMeasureData.Write(tempConn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", testBaseTime, interval)

		// Verify using the initial connection before updating certificates
		gm.Eventually(func(innerGm gm.Gomega) {
			casesMeasureData.VerifyFn(innerGm, helpers.SharedContext{
				Connection: tempConn,
				BaseTime:   testBaseTime,
			}, helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute})
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		// Generate a new certificate with a different CommonName
		certPEM, keyPEM, err := tls.GenerateSelfSignedCert("updated-localhost", []string{"localhost"})
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Update the certificate files in the temporary location
		err = os.WriteFile(tempCertFile, certPEM, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = os.WriteFile(tempKeyFile, keyPEM, 0o600)
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Wait for the server to reload the certificates
		time.Sleep(1 * time.Second)

		// Create a new connection with the updated certificates
		newCreds, err := credentials.NewClientTLSFromFile(tempCertFile, "updated-localhost")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		newConn, err := grpchelper.Conn(tempAddr, 10*time.Second, grpclib.WithTransportCredentials(newCreds))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		defer newConn.Close()

		// Verify using the connection with new certificates
		gm.Eventually(func(innerGm gm.Gomega) {
			casesMeasureData.VerifyFn(innerGm, helpers.SharedContext{
				Connection: newConn,
				BaseTime:   testBaseTime,
			}, helpers.Args{Input: "all", Duration: 25 * time.Minute, Offset: -20 * time.Minute})
		}, flags.EventuallyTimeout).Should(gm.Succeed())
	})
})
