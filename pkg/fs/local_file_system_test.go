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

// Package fs (file system) is an independent component to operate file and directory.
package fs

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Loacl File System", func() {
	const (
		data          string = "BanyanDB"
		dirName       string = "dir"
		fileName      string = "dir/file"
		flushFileName string = "dir/flushFile"
	)

	var fs FileSystem

	ginkgo.Context("Local File", func() {
		ginkgo.BeforeEach(func() {
			fs = NewLocalFileSystem()
		})

		ginkgo.AfterEach(func() {
			err := os.RemoveAll(dirName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("File Operation", func() {
			var err error
			var size int
			var buffer []byte
			_, err = fs.CreateDirectory(dirName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// test flush write
			size, err = fs.FlushWriteFile([]byte(data), flushFileName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())
			err = os.Remove(flushFileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// test create file
			file, err := fs.CreateFile(fileName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(fileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// test write and read
			size, err = file.AppendWriteFile([]byte(data))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())

			buffer = make([]byte, len(data))
			size, err = file.ReadFile(0, buffer)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())
			gomega.Expect(bytes.Equal(buffer, []byte(data))).To(gomega.BeTrue())

			// test writev and readv
			var iov [][]byte
			for i := 0; i < 2; i++ {
				iov = append(iov, []byte(data))
			}
			size, err = file.AppendWritevFile(&iov)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == 2*len(data)).To(gomega.BeTrue())

			var riov [][]byte
			for i := 0; i < 3; i++ {
				riov = append(iov, make([]byte, len(data)))
			}
			size, err = file.ReadvFile(0, &riov)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == 3*len(data)).To(gomega.BeTrue())
			for _, buffer := range riov {
				gomega.Expect(bytes.Equal(buffer, []byte(data))).To(gomega.BeTrue())
			}

			// test stream read
			iter, err := file.StreamReadFile(buffer)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			for {
				size, err = iter.Next()
				if err == nil {
					gomega.Expect(size == len(data)).To(gomega.BeTrue())
				} else {
					gomega.Expect(errors.Is(err, io.EOF)).To(gomega.BeTrue())
					break
				}
			}

			// test get size
			n, err := file.GetFileSize()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(n == 3*int64(len(data))).To(gomega.BeTrue())

			// test close
			err = file.CloseFile()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			buffer = make([]byte, len(data))
			_, err = file.ReadFile(0, buffer)
			gomega.Expect(err).To(gomega.HaveOccurred())

			// test delete
			err = file.DeleteFile()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(fileName)
			gomega.Expect(err).To(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("Directory", func() {
		ginkgo.BeforeEach(func() {
			fs = NewLocalFileSystem()
		})

		ginkgo.AfterEach(func() {
			err := os.RemoveAll(dirName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("Directory Operation", func() {
			var err error
			// test open directory
			dir, err := fs.CreateDirectory(dirName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(dirName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// test read directory
			_, err = fs.CreateFile(fileName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			entries, err := dir.ReadDirectory()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			for _, entry := range entries {
				gomega.Expect(entry.Name() == "file").To(gomega.BeTrue())
			}

			// test close
			err = dir.CloseDirectory()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// test delete
			err = dir.DeleteDirectory()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(dirName)
			gomega.Expect(err).To(gomega.HaveOccurred())
		})
	})
})
