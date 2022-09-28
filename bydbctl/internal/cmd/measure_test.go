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
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/cmd"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"github.com/zenizh/go-capturer"

	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

var _ = Describe("Measure", func() {
	var path string
	var gracefulStop, deferFunc func()
	var listenClientURL, listenPeerURL string
	var rootCmd *cobra.Command
	BeforeEach(func() {
		var err error
		path, deferFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		listenClientURL, listenPeerURL, err = test.NewEtcdListenUrls()
		Expect(err).NotTo(HaveOccurred())
		flags := []string{
			"--stream-root-path=" + path, "--measure-root-path=" + path, "--metadata-root-path=" + path,
			"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
		}
		gracefulStop = setup(false, flags)
		Eventually(helpers.HTTPHealthCheck("localhost:17913"), 10*time.Second).Should(Succeed())
		time.Sleep(1 * time.Second)
		// extracting the operation of creating measure schema
		rootCmd = &cobra.Command{Use: "root"}
		cmd.RootCmdFlags(rootCmd)
		rootCmd.SetArgs([]string{"group", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: group1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("group group1 is created"))
		rootCmd.SetArgs([]string{"measure", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1`))
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name1 is created"))
	})

	It("get measure schema", func() {
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.MeasureRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure.Metadata.Group).To(Equal("group1"))
		Expect(resp.Measure.Metadata.Name).To(Equal("name1"))
	})

	It("update measure schema", func() {
		rootCmd.SetArgs([]string{"measure", "update", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name1
  group: group1
entity:
  tagNames: ["tag1"]`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name1 is updated"))
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.MeasureRegistryServiceGetResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure.Metadata.Group).To(Equal("group1"))
		Expect(resp.Measure.Metadata.Name).To(Equal("name1"))
		Expect(resp.Measure.Entity.TagNames[0]).To(Equal("tag1"))
	})

	It("delete measure schema", func() {
		// delete
		rootCmd.SetArgs([]string{"measure", "delete", "-g", "group1", "-n", "name1"})
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name1 is deleted"))
		// get again
		rootCmd.SetArgs([]string{"measure", "get", "-g", "group1", "-n", "name1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("resource not found"))
	})

	It("list measure schema", func() {
		// create another measure schema for list operation
		rootCmd.SetArgs([]string{"measure", "create", "-f", "-"})
		rootCmd.SetIn(strings.NewReader(`
metadata:
  name: name2
  group: group1`))
		out := capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		Expect(out).To(ContainSubstring("measure group1.name2 is created"))
		// list
		rootCmd.SetArgs([]string{"measure", "list", "-g", "group1"})
		out = capturer.CaptureStdout(func() {
			err := rootCmd.Execute()
			Expect(err).NotTo(HaveOccurred())
		})
		resp := new(database_v1.MeasureRegistryServiceListResponse)
		helpers.UnmarshalYAML([]byte(out), resp)
		Expect(resp.Measure).To(HaveLen(2))
	})

	It("query measure data", func() {
		// todo
	})

	AfterEach(func() {
		gracefulStop()
		deferFunc()
	})
})
