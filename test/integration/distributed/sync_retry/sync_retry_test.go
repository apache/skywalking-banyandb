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

package integration_sync_retry_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	g "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestracedata "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

const (
	DefaultMaxRetries  = 3
	FailedPartsDirName = "failed-parts"
)

var _ = g.Describe("Chunked sync failure handling", g.Ordered, func() {
	g.BeforeEach(func() {
		Expect(clearFailedPartsDirs()).To(Succeed())
	})

	g.AfterEach(func() {
		queue.ClearChunkedSyncFailureInjector()
	})

	g.It("retries stream parts and records failed segments", func() {
		conn := dialLiaison()
		defer conn.Close()

		// With 2 data nodes and 3 max retries, we need 2 * (1 initial + 3 retries) = 8 failures
		// to ensure parts permanently fail
		injector, cleanup := withChunkedSyncFailureInjector(map[string]int{
			data.TopicStreamPartSync.String(): 2 * (DefaultMaxRetries + 1),
		})
		defer cleanup()

		baseTime := time.Now().Add(-5 * time.Minute).Truncate(time.Millisecond)
		casesstreamdata.Write(conn, "sw", baseTime, 500*time.Millisecond)

		g.By("waiting for injected stream sync failures")
		Eventually(func() int {
			return injector.attemptsFor(data.TopicStreamPartSync.String())
		}, flags.EventuallyTimeout, 500*time.Millisecond).Should(BeNumerically(">=", 2*(DefaultMaxRetries+1)))

		g.By("waiting for stream failed-parts directory to be populated")
		Eventually(func() string {
			return locateFailedPartsDir("stream")
		}, flags.EventuallyTimeout, time.Second).ShouldNot(BeEmpty())
		dir := locateFailedPartsDir("stream")
		Expect(dir).NotTo(BeEmpty())
		g.By(fmt.Sprintf("stream failed parts recorded at %s", dir))
	})

	g.It("retries measure parts and records failed segments", func() {
		conn := dialLiaison()
		defer conn.Close()

		// With 2 data nodes, 2 shards (2 parts), and 3 max retries:
		// 2 parts * 2 nodes * (1 initial + 3 retries) = 16 failures needed
		injector, cleanup := withChunkedSyncFailureInjector(map[string]int{
			data.TopicMeasurePartSync.String(): 2 * 2 * (DefaultMaxRetries + 1),
		})
		defer cleanup()

		baseTime := time.Now().Add(-24 * time.Hour).Truncate(time.Millisecond)
		casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", baseTime, time.Minute)

		g.By("waiting for injected measure sync failures")
		Eventually(func() int {
			return injector.attemptsFor(data.TopicMeasurePartSync.String())
		}, flags.EventuallyTimeout, time.Second).Should(BeNumerically(">=", 2*2*(DefaultMaxRetries+1)))

		g.By("waiting for measure failed-parts directory to be populated")
		Eventually(func() string {
			return locateFailedPartsDir("measure")
		}, flags.EventuallyTimeout, 2*time.Second).ShouldNot(BeEmpty())
		dir := locateFailedPartsDir("measure")
		Expect(dir).NotTo(BeEmpty())
		g.By(fmt.Sprintf("measure failed parts recorded at %s", dir))
	})

	g.It("retries trace parts and records failed segments", func() {
		conn := dialLiaison()
		defer conn.Close()

		// With 2 data nodes and 3 max retries, we need 2 * (1 initial + 3 retries) = 8 failures
		// to ensure parts permanently fail
		injector, cleanup := withChunkedSyncFailureInjector(map[string]int{
			data.TopicTracePartSync.String(): 2 * (DefaultMaxRetries + 1),
		})
		defer cleanup()

		baseTime := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
		casestracedata.WriteToGroup(conn, "sw", "test-trace-group", "sw", baseTime, 500*time.Millisecond)

		g.By("waiting for injected trace sync failures")
		Eventually(func() int {
			return injector.attemptsFor(data.TopicTracePartSync.String())
		}, flags.EventuallyTimeout, time.Second).Should(BeNumerically(">=", 2*(DefaultMaxRetries+1)))

		g.By("waiting for trace failed-parts directory to be populated")
		Eventually(func() string {
			return locateFailedPartsDir("trace")
		}, flags.EventuallyTimeout, 2*time.Second).ShouldNot(BeEmpty())
		dir := locateFailedPartsDir("trace")
		Expect(dir).NotTo(BeEmpty())
		g.By(fmt.Sprintf("trace failed parts recorded at %s", dir))
	})
})

func dialLiaison() *grpc.ClientConn {
	var conn *grpc.ClientConn
	var err error
	// Retry connection with Eventually to handle liaison startup timing
	Eventually(func() error {
		conn, err = grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return err
	}, 30*time.Second, time.Second).Should(Succeed())
	return conn
}

func clearFailedPartsDirs() error {
	for _, root := range dataPaths {
		if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() && d.Name() == FailedPartsDirName {
				if removeErr := os.RemoveAll(path); removeErr != nil {
					return removeErr
				}
				return fs.SkipDir
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func locateFailedPartsDir(module string) string {
	errFound := errors.New("found")

	// Search both data node paths and any temp directories (for liaison)
	searchPaths := append([]string{}, dataPaths...)

	// Also search common temp directory patterns for liaison write queues
	tmpDirs, _ := filepath.Glob("/tmp/banyandb-test-*")
	searchPaths = append(searchPaths, tmpDirs...)
	tmpDirs2, _ := filepath.Glob(os.TempDir() + "/banyandb-test-*")
	searchPaths = append(searchPaths, tmpDirs2...)

	for _, root := range searchPaths {
		moduleRoot := filepath.Join(root, module)
		info, err := os.Stat(moduleRoot)
		if err != nil || !info.IsDir() {
			continue
		}
		var match string
		walkErr := filepath.WalkDir(moduleRoot, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if !d.IsDir() || d.Name() != FailedPartsDirName {
				return nil
			}
			entries, readErr := os.ReadDir(path)
			if readErr == nil && len(entries) > 0 {
				match = path
				return errFound
			}
			return fs.SkipDir
		})
		if walkErr != nil && !errors.Is(walkErr, errFound) {
			continue
		}
		if match != "" {
			return match
		}
	}
	return ""
}
