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

package cmd_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = Describe("health check after launching banyandb server", func() {
	var deferFunc func()
	var grpcAddr string
	var rootCmd *cobra.Command
	BeforeEach(func() {
		grpcAddr, _, deferFunc = setup.StandaloneWithTLS("../../../test/integration/standalone/other/testdata/server_cert.pem",
			"../../../test/integration/standalone/other/testdata/server_key.pem")
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("health", func() {
		rootCmd.SetArgs([]string{"health", "--grpc-addr", grpcAddr, "--grpc-cert", "../../../test/integration/standalone/other/testdata/server_cert.pem"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("connected"))
	})

	It("health", func() {
		rootCmd.SetArgs([]string{"health", "--grpc-addr", grpcAddr})
		err := rootCmd.Execute()
		Expect(err).To(HaveOccurred())
	})

	AfterEach(func() {
		deferFunc()
	})
})

var _ = Describe("health check without launching banyandb server", func() {
	var rootCmd *cobra.Command
	BeforeEach(func() {
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("health", func() {
		rootCmd.SetArgs([]string{"health"})
		err := rootCmd.Execute()
		Expect(err).To(HaveOccurred())
	})
})
