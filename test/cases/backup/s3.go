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

package backup_test

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/aws"
	"github.com/apache/skywalking-banyandb/test/integration/dockertesthelper"
)

var _ = ginkgo.Describe("s3FS Integration", func() {
	var (
		fs       remote.FS
		ctx      context.Context
		bucket   = dockertesthelper.BucketName
		prefix   = "test-prefix"
		testKey  = "hello.txt"
		testData = "Hello, World!"
	)

	ginkgo.BeforeEach(func() {
		ctx = context.Background()

		fsConfig := &remote.FsConfig{
			S3ConfigFilePath:     dockertesthelper.ConfigPath,
			S3CredentialFilePath: dockertesthelper.CredentialsPath,
		}

		var err error
		fs, err = aws.NewFS(path.Join(bucket, prefix), fsConfig)
		fmt.Println(path.Join(bucket, prefix))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("should upload, download, list, and delete object successfully", func() {
		ginkgo.By("Uploading object to S3")
		err := fs.Upload(ctx, testKey, strings.NewReader(testData))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Downloading object and verifying content")
		reader, err := fs.Download(ctx, testKey)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer reader.Close()
		body, err := io.ReadAll(reader)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(string(body)).To(gomega.Equal(testData))

		ginkgo.By("Listing objects")
		files, err := fs.List(ctx, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(files).To(gomega.ContainElement(testKey))

		ginkgo.By("Deleting object")
		err = fs.Delete(ctx, testKey)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Ensuring object is deleted")
		files, err = fs.List(ctx, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(files).NotTo(gomega.ContainElement(testKey))
	})

	ginkgo.It("should return an error when no bucket name is provided", func() {
		// Pass an empty path which means no bucket name
		fs, err := aws.NewFS("", &remote.FsConfig{})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("bucket"))
		gomega.Expect(fs).To(gomega.BeNil())
	})
})
