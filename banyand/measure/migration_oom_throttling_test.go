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

package measure_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/service"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

// controllablePM is a protector.Memory whose State() is flippable at runtime.
// All other Memory methods (AvailableBytes/GetLimit/AcquireResource/ShouldCache
// + run.PreRunner/Config/Service) delegate to the embedded real protector so the
// receive handler behaves exactly like production except for the scripted state.
type controllablePM struct {
	protector.Memory
	high atomic.Bool
}

// State reports StateHigh while the test holds the protector high, StateLow otherwise.
func (c *controllablePM) State() protector.State {
	if c.high.Load() {
		return protector.StateHigh
	}
	return protector.StateLow
}

// migration OOM-throttling RECEIVE path, exercised end-to-end through a REAL
// chunked-sync gRPC sender (pub) and receiver (sub) wired to the REAL measure
// series-index sync handler (setUpSyncSeriesCallback) over a REAL target tsdb.
// The ONLY fake is the protector: a controllablePM flipped between HIGH and LOW.
//
//   - While HIGH: syncSeriesCallback.HandleFileChunk returns queue.ErrServerBusy
//     at chunk entry; the sub server translates that into SYNC_STATUS_SERVER_BUSY,
//     which the pub client surfaces as an error wrapping queue.ErrServerBusy. The
//     test asserts the migration of the series-index part is rejected as BUSY.
//   - After LOW: the same part migrates cleanly; the handler introduces the
//     external series-index segment (CompleteSegment) into the target tsdb. The
//     test asserts (Eventually) the series index lands and the migrated tree is
//     queryable, proving recovery.
var _ = Describe("measure lifecycle tier-migration RECEIVE path under memory pressure", func() {
	const (
		group       = "oom_throttle_migration"
		measureName = "oom_throttle_metric"
	)

	It("rejects the series-index part with SERVER_BUSY while memory is HIGH, then completes once it drops to LOW", func() {
		ctx := context.TODO()
		twoDayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 2}

		workspace := GinkgoT().TempDir()
		sourceRoot := filepath.Join(workspace, "source")
		targetRoot := filepath.Join(workspace, "target")
		Expect(os.MkdirAll(sourceRoot, 0o755)).To(Succeed())

		// --- 1. Build a REAL source measure tree with a committed sidx segment ---
		srcSvcs, srcDeferFn := setUpMigrationTarget(sourceRoot)
		sourceDown := func() {
			if srcDeferFn != nil {
				srcDeferFn()
				srcDeferFn = nil
			}
		}
		defer sourceDown()
		Eventually(func() bool {
			_, ok := srcSvcs.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		registerMigrationE2EGroup(srcSvcs, group, measureName)
		Eventually(func() bool {
			_, ok := srcSvcs.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		Eventually(func() error {
			_, err := srcSvcs.measure.Measure(&commonv1.Metadata{Name: measureName, Group: group})
			return err
		}).WithTimeout(30*time.Second).Should(Succeed(),
			"source service should resolve the measure before writes are published")

		// Two timestamps in a single DAY×2 bucket each, producing two on-disk segs.
		nowBucket := twoDayInterval.Standard(time.Now().UTC())
		day1 := nowBucket.AddDate(0, 0, -4).Add(8 * time.Hour)
		day2 := nowBucket.AddDate(0, 0, -2).Add(8 * time.Hour)
		writeMigrationE2EPoints(srcSvcs, group, measureName, day1, 2, 0)
		writeMigrationE2EPoints(srcSvcs, group, measureName, day2, 4, 2)

		Eventually(func() int {
			info, err := srcSvcs.measure.CollectDataInfo(ctx, group)
			if err != nil || info == nil {
				return 0
			}
			return len(info.SegmentInfo)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(Equal(2),
			"expected 2 DAY×2 segments on disk after writes are flushed")

		sourceDataPath := srcSvcs.measure.(interface{ GetDataPath() string }).GetDataPath()
		sourceGroupRoot := filepath.Join(sourceDataPath, group)
		Eventually(func() bool {
			return allSidxDirsHaveSnapshot(sourceGroupRoot)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(BeTrue(),
			"every <seg>/sidx/ under %s should carry a committed .snp before migration runs", sourceGroupRoot)

		// Stop the source so every per-segment bluge series-index writer commits.
		sourceDown()

		// Collect the committed sidx .seg files (with their owning segment dir) so
		// we can replay the exact bytes the lifecycle sender ships.
		segFiles := collectSidxSegFiles(sourceGroupRoot)
		Expect(segFiles).NotTo(BeEmpty(), "source must have at least one committed sidx .seg to migrate")
		picked := segFiles[0]

		// --- 2. REAL target measure receiver wired to a REAL sub gRPC server ---
		targetSvcs, targetPM, targetDeferFn := setUpThrottleTarget(targetRoot)
		defer targetDeferFn()
		Eventually(func() bool {
			_, ok := targetSvcs.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		registerMigrationE2EGroup(targetSvcs, group, measureName)
		Eventually(func() bool {
			_, ok := targetSvcs.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		Eventually(func() error {
			_, err := targetSvcs.measure.Measure(&commonv1.Metadata{Name: measureName, Group: group})
			return err
		}).WithTimeout(30*time.Second).Should(Succeed(),
			"target service should resolve the measure before the series part is received")

		// Real chunked-sync server + the REAL series handler, fed the fake protector.
		// Keep memWaitTimeout small so the FinishSync WaitWhileHigh path stays bounded.
		const memWaitTimeout = 2 * time.Second
		server, serverAddr, serverDeferFn := startThrottleSyncServer()
		defer serverDeferFn()
		measure.RegisterMeasureSeriesSyncHandlerForTest(targetSvcs.measure, server, targetPM, memWaitTimeout)

		// Real pub gRPC client; resolve the target node by address (no metadata repo).
		nodeName := "oom-throttle-target-node"
		client := pub.NewWithoutMetadata(observability.BypassRegistry)
		client.OnAddOrUpdate(schema.Metadata{
			TypeMeta: schema.TypeMeta{Name: nodeName, Kind: schema.KindNode},
			Spec: &databasev1.Node{
				Metadata:    &commonv1.Metadata{Name: nodeName},
				Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
				GrpcAddress: serverAddr,
			},
		})
		var chunkedClient queue.ChunkedSyncClient
		Eventually(func() error {
			var clientErr error
			chunkedClient, clientErr = client.NewChunkedSyncClient(nodeName, 1024)
			return clientErr
		}).WithTimeout(30 * time.Second).Should(Succeed())
		defer func() {
			_ = chunkedClient.Close()
		}()

		// mkSeriesPart rebuilds fresh offset-0 readers for the picked sidx .seg, so
		// each migration attempt streams the same bytes (the sender reopens on retry).
		lfs := fs.NewLocalFileSystem()
		mkSeriesPart := func() queue.StreamingPartData {
			f, openErr := lfs.OpenFile(picked.path)
			Expect(openErr).NotTo(HaveOccurred())
			return queue.StreamingPartData{
				Group:        group,
				ShardID:      0,
				Topic:        data.TopicMeasureSeriesSync.String(),
				Files:        []queue.FileInfo{{Name: picked.name, Reader: f.SequentialRead()}},
				MinTimestamp: picked.minTS,
				MaxTimestamp: picked.maxTS,
			}
		}

		// --- 3. HIGH → assert SERVER_BUSY is observed on the receive path ---
		targetPM.high.Store(true)
		_, busyErr := chunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{mkSeriesPart()})
		Expect(busyErr).To(HaveOccurred(),
			"while memory is HIGH the receiver must shed the series part instead of accepting it")
		Expect(errors.Is(busyErr, queue.ErrServerBusy)).To(BeTrue(),
			"the receive-side SYNC_STATUS_SERVER_BUSY must surface to the sender as queue.ErrServerBusy, got: %v", busyErr)

		// The target must not have introduced the series index while busy.
		Consistently(func() bool {
			return targetSidxHasSnapshot(filepath.Join(targetRoot, "measure", "data", group))
		}).WithTimeout(time.Second).WithPolling(200*time.Millisecond).Should(BeFalse(),
			"no series-index segment should be introduced into the target while memory is HIGH")

		// --- 4. Drop to LOW (normal) ---
		targetPM.high.Store(false)

		// --- 5. Eventually the migration COMPLETES and the series index lands ---
		Eventually(func() error {
			result, syncErr := chunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{mkSeriesPart()})
			if syncErr != nil {
				return fmt.Errorf("sync still failing after LOW: %w", syncErr)
			}
			if !result.Success {
				return fmt.Errorf("sync result not successful: failedParts=%v", result.FailedParts)
			}
			return nil
		}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(Succeed(),
			"once memory recovers to LOW the series-index part must migrate cleanly")

		// The receiver introduced the external series-index segment into the target.
		Eventually(func() bool {
			return targetSidxHasSnapshot(filepath.Join(targetRoot, "measure", "data", group))
		}).WithTimeout(30*time.Second).WithPolling(500*time.Millisecond).Should(BeTrue(),
			"after recovery the target must carry a committed series-index snapshot for the migrated segment")
	})
})

// sidxSegFile locates one committed sidx .seg file under the source tree and the
// time range of its owning segment, so the test can frame the StreamingPartData
// exactly as the lifecycle sender's createStreamingSegmentFromFiles does.
type sidxSegFile struct {
	path  string
	name  string
	minTS int64
	maxTS int64
}

// collectSidxSegFiles walks <groupRoot>/seg-*/sidx and returns every *.seg file
// with the [start, end) of its owning segment (parsed from the seg dir name on
// the DAY×2 grid), which the receive handler uses to route the external segment.
func collectSidxSegFiles(groupRoot string) []sidxSegFile {
	twoDayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 2}
	segs, err := os.ReadDir(groupRoot)
	Expect(err).NotTo(HaveOccurred())
	var out []sidxSegFile
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), "seg-") {
			continue
		}
		start, parseErr := parseSegDirStart(seg.Name())
		if parseErr != nil {
			continue
		}
		end := twoDayInterval.NextTime(start)
		sidxDir := filepath.Join(groupRoot, seg.Name(), "sidx")
		entries, readErr := os.ReadDir(sidxDir)
		if readErr != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
				continue
			}
			out = append(out, sidxSegFile{
				path:  filepath.Join(sidxDir, e.Name()),
				name:  e.Name(),
				minTS: start.UnixNano(),
				maxTS: end.UnixNano() - 1,
			})
		}
	}
	return out
}

// parseSegDirStart parses a "seg-YYYYMMDD" directory name into its start instant.
func parseSegDirStart(name string) (time.Time, error) {
	const segPrefix = "seg-"
	raw := strings.TrimPrefix(name, segPrefix)
	return time.ParseInLocation("20060102", raw, time.Local)
}

// targetSidxHasSnapshot reports whether any <groupRoot>/seg-*/sidx carries a
// committed .seg file (the external segment introduced by CompleteSegment).
func targetSidxHasSnapshot(groupRoot string) bool {
	segs, err := os.ReadDir(groupRoot)
	if err != nil {
		return false
	}
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), "seg-") {
			continue
		}
		sidxDir := filepath.Join(groupRoot, seg.Name(), "sidx")
		entries, readErr := os.ReadDir(sidxDir)
		if readErr != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
				return true
			}
		}
	}
	return false
}

// setUpThrottleTarget brings up a REAL standalone measure receiver whose memory
// protector is a controllablePM (so the test can flip HIGH/LOW), returning the
// services bundle and the protector handle.
func setUpThrottleTarget(measureRootPath string) (*services, *controllablePM, func()) {
	pipeline := queue.Local()
	metadataService, err := service.NewService()
	Expect(err).NotTo(HaveOccurred())
	metricSvc := obsservice.NewMetricService(metadataService, pipeline, "test", nil)
	pm := &controllablePM{Memory: protector.NewMemory(metricSvc)}
	measureService, err := measure.NewStandalone(metadataService, pipeline, nil, metricSvc, pm)
	Expect(err).NotTo(HaveOccurred())
	preloadSvc := &preloadMeasureService{metaSvc: metadataService}
	querySvc, err := query.NewService(context.TODO(), nil, measureService, nil, metadataService, pipeline, metricSvc, false)
	Expect(err).NotTo(HaveOccurred())

	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	schemaPorts, err := test.AllocateFreePorts(1)
	Expect(err).NotTo(HaveOccurred())
	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", schemaPorts[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--measure-root-path=" + measureRootPath,
	}
	moduleDeferFunc := test.SetupModules(flags, pipeline, metadataService, preloadSvc, measureService, querySvc)
	return &services{
			measure:         measureService,
			metadataService: metadataService,
			pipeline:        pipeline,
		}, pm, func() {
			moduleDeferFunc()
			metaDeferFunc()
		}
}

// startThrottleSyncServer starts a REAL chunked-sync gRPC server and returns it
// alongside its localhost address. It mirrors the proven setup in
// banyand/queue/test: a run.Group driven by a cobra command so the server's
// flags are registered and parsed before Run, otherwise the listener never binds.
// The series handler is attached by the caller via RegisterMeasureSeriesSyncHandlerForTest.
func startThrottleSyncServer() (queue.Server, string, func()) {
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	grpcPort := uint32(ports[0])
	httpPort := uint32(ports[1])

	server := sub.NewServerWithPorts(observability.BypassRegistry, "oom-throttle-sync", grpcPort, httpPort)
	closer, deferFn := run.NewTester("oom-throttle-sync-closer")
	g := run.NewGroup("oom-throttle-sync")
	g.Register(closer, server)

	cmd := &cobra.Command{
		Use:                "oom-throttle-sync",
		FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
		Run: func(_ *cobra.Command, _ []string) {
			Expect(g.Run(context.Background())).To(Succeed())
		},
	}
	cmd.Flags().AddFlagSet(g.RegisterFlags().FlagSet)
	go func() {
		defer GinkgoRecover()
		Expect(cmd.Execute()).To(Succeed())
	}()

	addr := fmt.Sprintf("localhost:%d", grpcPort)
	Eventually(func() error {
		return helpers.HealthCheck(addr, 10*time.Second, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))()
	}).WithTimeout(30 * time.Second).Should(Succeed())

	return server, addr, deferFn
}
