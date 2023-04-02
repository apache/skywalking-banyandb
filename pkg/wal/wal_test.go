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

// Package version can be used to implement embedding versioning details from
// git branches and tags into the binary importing this package.
package wal_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/wal"
)

var _ = ginkgo.Describe("WAL", func() {
	var (
		log     wal.WAL
		options *wal.Options
		goods   []gleak.Goroutine
	)
	ginkgo.BeforeEach(func() {
		options = &wal.Options{
			Compression: true,
			FileSize:    67108864, // 20MB
			BufferSize:  16,       // 16B
		}
		goods = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("Write and Read", func() {
		ginkgo.BeforeEach(func() {
			var err error
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			path, err := filepath.Abs("test")
			os.RemoveAll(path)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log.Close()
		})

		ginkgo.It("should write and read data correctly", func() {
			var err error
			var wg sync.WaitGroup
			wg.Add(100)
			for i := 0; i < 100; i++ {
				go func(idx int) {
					defer wg.Done()
					id := int64(idx)
					bytesBuffer := bytes.NewBuffer([]byte{})
					binary.Write(bytesBuffer, binary.LittleEndian, &id)
					seriesID := bytesBuffer.Bytes()
					timestamp := time.Now()
					value := []byte(fmt.Sprintf("value-%d", idx))
					err = log.Write(seriesID, timestamp, value)
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}(i)
			}
			wg.Wait()

			err = log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			segments, err := log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			for _, segment := range segments {
				entries := segment.GetLogEntries()
				for index, entity := range entries {
					seriesID := entity.GetSeriesID()
					gomega.Expect(seriesID == common.SeriesID(index)).To(gomega.BeTrue())
					value := entity.GetBinary()
					gomega.Expect(bytes.Equal(value, []byte(fmt.Sprintf("value-%d", index)))).To(gomega.BeTrue())
				}
			}
		})
	})

	ginkgo.Context("Rotate", func() {
		ginkgo.BeforeEach(func() {
			var err error
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			path, err := filepath.Abs("test")
			os.RemoveAll(path)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log.Close()
		})

		ginkgo.It("should rotate correctly", func() {
			var err error
			var wg sync.WaitGroup
			wg.Add(100)
			for i := 0; i < 100; i++ {
				go func(idx int) {
					defer wg.Done()
					id := int64(idx)
					bytesBuffer := bytes.NewBuffer([]byte{})
					binary.Write(bytesBuffer, binary.LittleEndian, &id)
					seriesID := bytesBuffer.Bytes()
					timestamp := time.Now()
					value := []byte(fmt.Sprintf("value-%d", idx))
					err = log.Write(seriesID, timestamp, value)
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
				}(i)
			}
			wg.Wait()

			segment, err := log.Rotate()
			id := segment.GetSegmentID()
			log.Close()
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			segment, err = log.Read(id)
			entries := segment.GetLogEntries()
			gomega.Expect(len(entries) == 0).To(gomega.BeTrue())
		})
	})

	ginkgo.Context("Delete", func() {
		ginkgo.BeforeEach(func() {
			var err error
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			path, err := filepath.Abs("test")
			os.RemoveAll(path)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log.Close()
		})

		ginkgo.It("should delete correctly", func() {
			var err error

			segments, err := log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(len(segments) == 1).To(gomega.BeTrue())

			segment, err := log.Rotate()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			err = log.Delete(segment.GetSegmentID())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			log.Close()
			log, err = wal.New("test", options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			segments, err = log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(len(segments) == 1).To(gomega.BeTrue())
		})
	})
})
