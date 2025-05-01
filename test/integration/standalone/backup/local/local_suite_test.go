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

package local_test

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote/local"
	"github.com/apache/skywalking-banyandb/test/integration/standalone/backup"
)

func TestBackup(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Backup Suite", ginkgo.Label(backup.GetTestLabels()...))
}

var testVars *backup.CommonTestVars

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	var err error
	var addr string
	testVars, addr, err = backup.InitStandaloneEnv()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	fs, err := local.NewFS(testVars.DestDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testVars.FS = fs

	return []byte(addr)
}, func(address []byte) {
	addr := string(address)
	err := backup.SetupConnection(testVars, addr, "file"+"://"+testVars.DestDir, nil)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if testVars.Connection != nil {
		gomega.Expect(testVars.Connection.Close()).To(gomega.Succeed())
	}
}, func() {
	backup.TeardownSuite(testVars)
})
