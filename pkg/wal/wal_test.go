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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/wal"
)

var _ = ginkgo.Describe("WAL", func() {
	var (
		path    string
		log     wal.WAL
		options *wal.Options
		goods   []gleak.Goroutine
	)
	ginkgo.BeforeEach(func() {
		options = &wal.Options{
			BufferSize:          1024, // 1KB
			BufferBatchInterval: 1 * time.Second,
		}
		goods = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		err := os.RemoveAll(path)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.Context("Write and Read", func() {
		ginkgo.BeforeEach(func() {
			var err error
			path, err = filepath.Abs("test1")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			options = &wal.Options{
				BufferSize:          1024, // 1KB
				BufferBatchInterval: 1 * time.Second,
			}
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			err := log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("should write and read data correctly", func() {
			seriesIDCount := 100
			seriesIDElementCount := 20
			writeLogCount := seriesIDCount * seriesIDElementCount

			var wg sync.WaitGroup
			wg.Add(writeLogCount)
			baseTime := time.Now()
			for i := 0; i < seriesIDCount; i++ {
				seriesID := []byte(fmt.Sprintf("series-%d", i))
				go func() {
					for j := 0; j < seriesIDElementCount; j++ {
						timestamp := time.UnixMilli(baseTime.UnixMilli() + int64(j))
						value := []byte(fmt.Sprintf("value-%d", j))
						callback := func(seriesID []byte, t time.Time, bytes []byte, err error) {
							gomega.Expect(err).ToNot(gomega.HaveOccurred())

							wg.Done()
						}
						log.Write(seriesID, timestamp, value, callback)
					}
				}()
			}
			wg.Wait()

			err := log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			segments, err := log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			readLogCount := 0

			for _, segment := range segments {
				entries := segment.GetLogEntries()
				for _, entity := range entries {
					seriesID := entity.GetSeriesID()
					// Check seriesID
					gomega.Expect(seriesID != nil).To(gomega.BeTrue())

					timestamps := entity.GetTimestamps()
					values := entity.GetValues()
					var timestamp time.Time
					element := values.Front()
					for i := 0; i < len(timestamps); i++ {
						timestamp = timestamps[i]

						// Check timestamp
						gomega.Expect(timestamp.UnixMilli() >= baseTime.UnixMilli()).To(gomega.BeTrue())
						gomega.Expect(timestamp.UnixMilli() <= baseTime.UnixMilli()+int64(seriesIDElementCount)).To(gomega.BeTrue())

						// Check binary
						elementSequence := timestamp.UnixMilli() - baseTime.UnixMilli()
						value := element.Value.([]byte)
						gomega.Expect(bytes.Equal([]byte(fmt.Sprintf("value-%d", elementSequence)), value)).To(gomega.BeTrue())

						readLogCount++
						element = element.Next()
					}
				}
			}

			// Check write/read log count
			gomega.Expect(writeLogCount == readLogCount).To(gomega.BeTrue())
		})
	})

	ginkgo.Context("Rotate", func() {
		ginkgo.BeforeEach(func() {
			var err error
			path, err = filepath.Abs("test2")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			options = &wal.Options{
				BufferSize: 1,
			}
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			err := log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("should rotate correctly", func() {
			var wg sync.WaitGroup
			writeLogCount := 3

			wg.Add(writeLogCount)
			expectSegments := make(map[wal.SegmentID][]byte)
			for i := 0; i < writeLogCount; i++ {
				seriesID := []byte(fmt.Sprintf("series-%d", i))
				timestamp := time.Now()
				value := []byte(fmt.Sprintf("value-%d", i))
				callback := func(seriesID []byte, t time.Time, bytes []byte, err error) {
					gomega.Expect(err).ToNot(gomega.HaveOccurred())

					// Rotate
					segment, err := log.Rotate()
					gomega.Expect(err).ToNot(gomega.HaveOccurred())
					expectSegments[segment.GetSegmentID()] = seriesID

					wg.Done()
				}
				log.Write(seriesID, timestamp, value, callback)
			}
			wg.Wait()

			err := log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Check segment files
			gomega.Expect(len(expectSegments) == writeLogCount).To(gomega.BeTrue())
			for segmentID, seriesID := range expectSegments {
				segment, err := log.Read(segmentID)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				entries := segment.GetLogEntries()
				gomega.Expect(len(entries) == 1).To(gomega.BeTrue())
				gomega.Expect(bytes.Equal(entries[0].GetSeriesID(), seriesID)).To(gomega.BeTrue())
			}
		})
	})

	ginkgo.Context("Delete", func() {
		ginkgo.BeforeEach(func() {
			var err error
			path, err = filepath.Abs("test3")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			err := log.Close()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})

		ginkgo.It("should delete correctly", func() {
			var err error

			segments, err := log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(len(segments) == 1).To(gomega.BeTrue())

			segment, err := log.Rotate()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			segments, err = log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(len(segments) == 2).To(gomega.BeTrue())

			err = log.Delete(segment.GetSegmentID())
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			log.Close()
			log, err = wal.New(path, options)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			segments, err = log.ReadAllSegments()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			// Check segment files
			gomega.Expect(len(segments) == 1).To(gomega.BeTrue())
		})
	})
})
