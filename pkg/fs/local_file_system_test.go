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
	"path/filepath"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Local File System", func() {
	const (
		data          string = "BanyanDB"
		testData      string = "Hello BanyanDB World"
		dirName       string = "tmpDir"
		fileName      string = "tmpDir/temFile"
		flushFileName string = "tmpDir/tempFlushFile"
	)

	var (
		fs   FileSystem
		file File
	)

	ginkgo.Context("Local File", func() {
		ginkgo.BeforeEach(func() {
			fs = NewLocalFileSystem()
			err := os.MkdirAll(dirName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			file, err = fs.CreateFile(fileName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(fileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			err := os.RemoveAll(dirName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("Flush File Operation", func() {
			size, err := fs.Write([]byte(data), flushFileName, 0o777)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())
			err = os.Remove(flushFileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("Create File Test", func() {
			_, err := os.Stat(fileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("Write And Read File Test", func() {
			size, err := file.Write([]byte(data))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())

			buffer := make([]byte, len(data))
			size, err = file.Read(0, buffer)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())
			gomega.Expect(bytes.Equal(buffer, []byte(data))).To(gomega.BeTrue())
		})

		ginkgo.It("Writev And Readv File Test", func() {
			var iov [][]byte
			for i := 0; i < 2; i++ {
				iov = append(iov, []byte(data))
			}
			size, err := file.Writev(&iov)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == 2*len(data)).To(gomega.BeTrue())

			var riov [][]byte
			for i := 0; i < 2; i++ {
				riov = append(riov, make([]byte, len(data)))
			}
			size, err = file.Readv(0, &riov)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == 2*len(data)).To(gomega.BeTrue())
			for _, buffer := range riov {
				gomega.Expect(bytes.Equal(buffer, []byte(data))).To(gomega.BeTrue())
			}
		})

		ginkgo.It("Stream Read Test", func() {
			size, err := file.Write([]byte(data))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())

			buffer := make([]byte, len(data))
			iter := file.SequentialRead()
			defer iter.Close()
			for {
				size, err := iter.Read(buffer)
				if err == nil {
					gomega.Expect(size == len(data)).To(gomega.BeTrue())
				} else {
					gomega.Expect(errors.Is(err, io.EOF)).To(gomega.BeTrue())
					break
				}
			}
		})

		ginkgo.It("SeqReader Multiple Complete Reads", func() {
			// Write test data
			testStr := testData
			size, err := file.Write([]byte(testStr))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(testStr)).To(gomega.BeTrue())

			// First SeqReader - read entire file
			reader1 := file.SequentialRead()
			buffer1 := make([]byte, len(testStr))
			readSize1, err := reader1.Read(buffer1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize1).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer1)).To(gomega.Equal(testStr))
			err = reader1.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Second SeqReader - should read entire file again (fixed behavior)
			reader2 := file.SequentialRead()
			buffer2 := make([]byte, len(testStr))
			readSize2, err := reader2.Read(buffer2)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize2).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer2)).To(gomega.Equal(testStr))
			err = reader2.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Third SeqReader - should read entire file again (fixed behavior)
			reader3 := file.SequentialRead()
			buffer3 := make([]byte, len(testStr))
			readSize3, err := reader3.Read(buffer3)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize3).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer3)).To(gomega.Equal(testStr))
			err = reader3.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("SeqReader Partial Reads", func() {
			// Write test data
			testStr := testData
			size, err := file.Write([]byte(testStr))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(testStr)).To(gomega.BeTrue())

			// First SeqReader - read first part
			reader1 := file.SequentialRead()
			buffer1 := make([]byte, 5) // Read only first 5 bytes
			readSize1, err := reader1.Read(buffer1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize1).To(gomega.Equal(5))
			gomega.Expect(string(buffer1)).To(gomega.Equal("Hello"))
			err = reader1.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Second SeqReader - should read from beginning again (fixed behavior)
			reader2 := file.SequentialRead()
			buffer2 := make([]byte, 5) // Read only first 5 bytes
			readSize2, err := reader2.Read(buffer2)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize2).To(gomega.Equal(5))
			gomega.Expect(string(buffer2)).To(gomega.Equal("Hello"))
			err = reader2.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Third SeqReader - should read from beginning again (fixed behavior)
			reader3 := file.SequentialRead()
			buffer3 := make([]byte, 5) // Read only first 5 bytes
			readSize3, err := reader3.Read(buffer3)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize3).To(gomega.Equal(5))
			gomega.Expect(string(buffer3)).To(gomega.Equal("Hello"))
			err = reader3.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("SeqReader Mixed Read Patterns", func() {
			// Write test data
			testStr := testData
			size, err := file.Write([]byte(testStr))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(testStr)).To(gomega.BeTrue())

			// First SeqReader - read entire file
			reader1 := file.SequentialRead()
			buffer1 := make([]byte, len(testStr))
			readSize1, err := reader1.Read(buffer1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize1).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer1)).To(gomega.Equal(testStr))
			err = reader1.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Second SeqReader - read only part
			reader2 := file.SequentialRead()
			buffer2 := make([]byte, 5) // Read only first 5 bytes
			readSize2, err := reader2.Read(buffer2)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize2).To(gomega.Equal(5))
			gomega.Expect(string(buffer2)).To(gomega.Equal("Hello"))
			err = reader2.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Third SeqReader - read entire file again
			reader3 := file.SequentialRead()
			buffer3 := make([]byte, len(testStr))
			readSize3, err := reader3.Read(buffer3)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize3).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer3)).To(gomega.Equal(testStr))
			err = reader3.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("SeqReader File Position Reset", func() {
			// Write test data
			testStr := testData
			size, err := file.Write([]byte(testStr))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(testStr)).To(gomega.BeTrue())

			// Cast to LocalFile to access the underlying file
			localFile, ok := file.(*LocalFile)
			gomega.Expect(ok).To(gomega.BeTrue())

			// Reset file position to beginning for testing
			_, err = localFile.file.Seek(0, 0) // Seek to beginning
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// First SeqReader - read only part of the data and close
			reader1 := file.SequentialRead()
			buffer1 := make([]byte, 5) // Read only first 5 bytes
			readSize1, err := reader1.Read(buffer1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(readSize1).To(gomega.Equal(5))
			gomega.Expect(string(buffer1)).To(gomega.Equal("Hello"))
			err = reader1.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Second SeqReader - should now read from beginning (fixed behavior)
			reader2 := file.SequentialRead()

			// Check file position after creating second reader
			posAfterSecond, err := localFile.file.Seek(0, 1) // Get current position
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			// Fixed: position should be 0 (reset to beginning)
			gomega.Expect(posAfterSecond).To(gomega.Equal(int64(0)))

			buffer2 := make([]byte, len(testStr))
			readSize2, err := reader2.Read(buffer2)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Should read the full data from beginning (fixed behavior)
			gomega.Expect(readSize2).To(gomega.Equal(len(testStr)))
			gomega.Expect(string(buffer2)).To(gomega.Equal(testStr))

			err = reader2.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("Size Test", func() {
			size, err := file.Write([]byte(data))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(size == len(data)).To(gomega.BeTrue())
			n, err := file.Size()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(n == int64(len(data))).To(gomega.BeTrue())
		})

		ginkgo.It("Close Test", func() {
			err := file.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			buffer := make([]byte, len(data))
			_, err = file.Read(0, buffer)
			gomega.Expect(err).To(gomega.HaveOccurred())
		})

		ginkgo.It("Delete Test", func() {
			err := fs.DeleteFile(fileName)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			_, err = os.Stat(fileName)
			gomega.Expect(err).To(gomega.HaveOccurred())
		})
	})

	ginkgo.Context("Hard Link Operations", func() {
		const (
			srcDir  = "test_src"
			destDir = "test_dest"
		)

		var fs FileSystem

		ginkgo.BeforeEach(func() {
			fs = NewLocalFileSystem()
			// Create source directory structure
			gomega.Expect(os.MkdirAll(srcDir, 0o755)).To(gomega.Succeed())
			gomega.Expect(os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("data1"), 0o600)).To(gomega.Succeed())
			gomega.Expect(os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)).To(gomega.Succeed())
			gomega.Expect(os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("data2"), 0o600)).To(gomega.Succeed())
		})

		ginkgo.AfterEach(func() {
			gomega.Expect(os.RemoveAll(srcDir)).To(gomega.Succeed())
			gomega.Expect(os.RemoveAll(destDir)).To(gomega.Succeed())
		})

		ginkgo.It("should create hard links for files passing the filter", func() {
			// Filter only "file1.txt"
			filter := func(path string) bool {
				return filepath.Base(path) == "file1.txt"
			}

			err := fs.CreateHardLink(srcDir, destDir, filter)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			srcFile := filepath.Join(srcDir, "file1.txt")
			destFile := filepath.Join(destDir, "file1.txt")
			gomega.Expect(destFile).To(gomega.BeAnExistingFile())

			gomega.Expect(CompareINode(srcFile, destFile)).To(gomega.Succeed())

			_, err = os.Stat(filepath.Join(destDir, "subdir", "file2.txt"))
			gomega.Expect(os.IsNotExist(err)).To(gomega.BeTrue())
		})

		ginkgo.It("should return error if source path does not exist", func() {
			err := fs.CreateHardLink("non_existent_src", destDir, nil)
			gomega.Expect(err).To(gomega.HaveOccurred())
			var errFS *FileSystemError
			gomega.Expect(errors.As(err, &errFS)).To(gomega.BeTrue())
			gomega.Expect(errFS.Code).To(gomega.Equal(IsNotExistError))
		})

		ginkgo.It("should return error if destination file already exists", func() {
			// Pre-create destination file
			gomega.Expect(os.MkdirAll(destDir, 0o755)).To(gomega.Succeed())
			gomega.Expect(os.WriteFile(filepath.Join(destDir, "file1.txt"), []byte("existing"), 0o600)).To(gomega.Succeed())

			err := fs.CreateHardLink(srcDir, destDir, nil) // No filter
			var errFS *FileSystemError
			gomega.Expect(errors.As(err, &errFS)).To(gomega.BeTrue())
			gomega.Expect(errFS.Code).To(gomega.Equal(isExistError))
			gomega.Expect(errFS.Code).To(gomega.Equal(isExistError))
		})
	})

	ginkgo.Context("Limit2IOSize", func() {
		const multi = 10 * 1024 * 1024

		ginkgo.It("protector memory limit to io size test", func() {
			gomega.Expect(limit2IOSize(100 * multi)).To(gomega.Equal(100 * 1024))
			gomega.Expect(limit2IOSize(0 * multi)).To(gomega.Equal(4 * 1024))
			gomega.Expect(limit2IOSize(4 * multi)).To(gomega.Equal(4 * 1024))
			gomega.Expect(limit2IOSize(300 * multi)).To(gomega.Equal(256 * 1024))
			gomega.Expect(limit2IOSize(256 * multi)).To(gomega.Equal(256 * 1024))
		})
	})
})
