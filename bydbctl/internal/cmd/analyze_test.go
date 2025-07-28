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
	"path"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases_measure_data "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

var _ = Describe("Measure Data Query", func() {
	var addr, grpcAddr, directory string
	var serverDeferFunc func()
	var rootCmd *cobra.Command
	var now time.Time
	BeforeEach(func() {
		var err error
		now, err = time.ParseInLocation("2006-01-02T15:04:05", "2021-09-01T23:30:00", time.Local)
		Expect(err).NotTo(HaveOccurred())
		directory, _, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		var ports []int
		ports, err = test.AllocateFreePorts(4)
		Expect(err).NotTo(HaveOccurred())
		grpcAddr, addr, serverDeferFunc = setup.ClosableStandalone(directory, ports)
		addr = httpSchema + addr
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
	})

	It("analyzes all series", func() {
		conn, err := grpclib.NewClient(
			grpcAddr,
			grpclib.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())
		cases_measure_data.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, time.Minute)
		serverDeferFunc()

		rootCmd.SetArgs([]string{"analyze", "series", path.Join(directory, "measure/sw_metric/seg-20210901/sidx")})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		GinkgoWriter.Println(out)
		Expect(out).To(ContainSubstring("total"))
	})

	AfterEach(func() {
	})
})
