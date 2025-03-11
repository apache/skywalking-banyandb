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

// Package lifecycle_test is the test cases for the lifecycle package.
package lifecycle_test

import (
	"os"
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/backup/lifecycle"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// SharedContext is the shared context for the snapshot test cases.
var SharedContext helpers.LifecycleSharedContext

var _ = ginkgo.Describe("Lifecycle", func() {
	ginkgo.It("should migrate data correctly", func() {
		dir, err := os.MkdirTemp("", "lifecycle-restore-dest")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(dir)
		pf := filepath.Join(dir, "progress.json")
		lifecycleCmd := lifecycle.NewCommand()
		lifecycleCmd.SetArgs([]string{
			"--grpc-addr", SharedContext.DataAddr,
			"--stream-root-path", SharedContext.SrcDir,
			"--measure-root-path", SharedContext.SrcDir,
			"--etcd-endpoints", SharedContext.EtcdAddr,
			"--progress-file", pf,
		})
		err = lifecycleCmd.Execute()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
})
